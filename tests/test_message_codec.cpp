#define EMQ_TEST_MODULE "message_codec"
#include "test_framework.h"
#include "../src/core/message_codec.h"
#include <cstring>

using namespace embedmq;

TEST(encode_decode_basic) {
    QoSProfile qos;
    Payload payload("hello world");
    auto data = MessageCodec::encode(
        MessageType::PUBLISH, 0x1234, 0x5678,
        "sensor/temp", payload, qos, 42, 0, 0, 0);

    CHECK_GT(data.size(), HEADER_FIXED_SIZE);

    auto result = MessageCodec::decode(data.data(), data.size());
    CHECK_TRUE(result.valid);
    CHECK_EQ(result.header.magic,       EMBEDMQ_MAGIC);
    CHECK_EQ(result.header.version,     EMBEDMQ_VERSION);
    CHECK_EQ(result.header.msgType,     static_cast<uint8_t>(MessageType::PUBLISH));
    CHECK_EQ(result.header.sourceId,    0x1234u);
    CHECK_EQ(result.header.destId,      0x5678u);
    CHECK_EQ(result.header.sequenceId,  42u);
    CHECK_STR_EQ(result.topic, "sensor/temp");
    CHECK_EQ(result.payload.size(), payload.size());
    CHECK_EQ(std::memcmp(result.payload.data(), payload.data(), payload.size()), 0);
}

TEST(encode_decode_empty_payload) {
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::HEARTBEAT, 1, 0xFFFF,
        "", Payload{}, qos, 1);
    auto result = MessageCodec::decode(data.data(), data.size());
    CHECK_TRUE(result.valid);
    CHECK_EQ(static_cast<MessageType>(result.header.msgType), MessageType::HEARTBEAT);
    CHECK_TRUE(result.payload.empty());
}

TEST(decode_invalid_magic) {
    std::vector<uint8_t> bad(HEADER_FIXED_SIZE + 4, 0);
    auto result = MessageCodec::decode(bad.data(), bad.size());
    CHECK_FALSE(result.valid);
}

TEST(decode_too_short) {
    std::vector<uint8_t> tiny(10, 0);
    auto result = MessageCodec::decode(tiny.data(), tiny.size());
    CHECK_FALSE(result.valid);
}

TEST(encode_decode_qos_level) {
    QoSProfile qos;
    qos.level = QoSLevel::Reliable;
    auto data = MessageCodec::encode(
        MessageType::PUBLISH, 1, 2,
        "t", Payload("data"), qos, 99);
    auto result = MessageCodec::decode(data.data(), data.size());
    CHECK_TRUE(result.valid);
    CHECK_EQ(result.header.qosLevel, static_cast<uint8_t>(QoSLevel::Reliable));
}

TEST(encode_decode_flags) {
    QoSProfile qos;
    uint8_t flags = MsgFlags::RETAIN | MsgFlags::WILL;
    auto data = MessageCodec::encode(
        MessageType::PUBLISH, 1, 2, "t", Payload("x"), qos, 1, 0, flags);
    auto result = MessageCodec::decode(data.data(), data.size());
    CHECK_TRUE(result.valid);
    CHECK_EQ(result.header.flags, flags);
}

TEST(crc_integrity) {
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::PUBLISH, 1, 2, "topic", Payload("payload"), qos, 1);

    // 损坏一个字节
    data[HEADER_FIXED_SIZE + 2]++;

    auto result = MessageCodec::decode(data.data(), data.size());
    CHECK_FALSE(result.valid);
}

TEST(large_payload) {
    QoSProfile qos;
    std::vector<uint8_t> bigData(60000, 0xAB);
    Payload payload(bigData.data(), bigData.size());
    auto data = MessageCodec::encode(
        MessageType::PUBLISH, 1, 2, "big", payload, qos, 1);
    auto result = MessageCodec::decode(data.data(), data.size());
    CHECK_TRUE(result.valid);
    CHECK_EQ(result.payload.size(), bigData.size());
}

// ---- encodeInto / encodeHeaderInto：复用缓冲变体（嵌入式零分配热路径）----

// encodeInto 必须与 encode 产出逐字节一致（CRC 开/关、带/不带载荷）。
// 用控制类消息（HEARTBEAT 无 timestamp 字段）做严格逐字节对比——数据类消息
// （PUBLISH 等）每次编码会写入实时 timestamp，两次独立编码本就会不同。
TEST(encode_into_matches_encode) {
    QoSProfile qos;
    qos.level = QoSLevel::Reliable;
    Payload payload("sensor payload 1234567890");

    for (bool withCrc : { true, false }) {
        auto want = MessageCodec::encode(
            MessageType::HEARTBEAT, 0xAABB, 0xCCDD,
            "sensor/temp", payload, qos, 7, 11, 0x05, 0, withCrc);

        std::vector<uint8_t> got;
        bool ok = MessageCodec::encodeInto(got,
            MessageType::HEARTBEAT, 0xAABB, 0xCCDD,
            "sensor/temp", payload, qos, 7, 11, 0x05, 0, withCrc);
        CHECK_TRUE(ok);
        CHECK_EQ(got.size(), want.size());
        CHECK_EQ(std::memcmp(got.data(), want.data(), want.size()), 0);

        // 产物可正常解码
        auto r = MessageCodec::decode(got.data(), got.size());
        CHECK_TRUE(r.valid);
        CHECK_STR_EQ(r.topic, "sensor/temp");
    }
}

// encodeHeaderInto 必须与 encodeHeader 逐字节一致（同上，用无 timestamp 的控制消息）
TEST(encode_header_into_matches_encode_header) {
    QoSProfile qos;
    Payload payload("body");
    auto want = MessageCodec::encodeHeader(
        MessageType::HEARTBEAT, 1, 0xFFFF, "a/b/c", payload, qos, 3, 0, 0, 0, true);

    std::vector<uint8_t> got;
    bool ok = MessageCodec::encodeHeaderInto(got,
        MessageType::HEARTBEAT, 1, 0xFFFF, "a/b/c", payload, qos, 3, 0, 0, 0, true);
    CHECK_TRUE(ok);
    CHECK_EQ(got.size(), want.size());
    CHECK_EQ(std::memcmp(got.data(), want.data(), want.size()), 0);
}

// 数据类消息（PUBLISH 带 timestamp）：除 timestamp 字段外 Into 与非 Into 头部一致，
// 覆盖热路径实际使用的 PUBLISH 编码。
TEST(encode_header_into_publish_matches_except_ts) {
    QoSProfile qos;
    Payload payload("xyz");
    std::vector<uint8_t> got;
    bool ok = MessageCodec::encodeHeaderInto(got,
        MessageType::PUBLISH, 5, 0xFFFF, "p/q", payload, qos, 8, 0, 0, 0, true);
    CHECK_TRUE(ok);
    auto want = MessageCodec::encodeHeader(
        MessageType::PUBLISH, 5, 0xFFFF, "p/q", payload, qos, 8, 0, 0, 0, true);
    CHECK_EQ(got.size(), want.size());
    // 对比 timestamp 之前的基础头（含所有寻址/序号字段）
    CHECK_EQ(std::memcmp(got.data(), want.data(), HEADER_BASE_SIZE), 0);
}

// 复用同一缓冲连续编码不同消息：每次产物都正确，且容量被保留（不缩小）
TEST(encode_into_buffer_reuse) {
    QoSProfile qos;
    std::vector<uint8_t> buf;

    // 先编一条大消息撑大容量
    Payload big(std::string(4096, 'x'));
    MessageCodec::encodeInto(buf, MessageType::PUBLISH, 1, 2, "big", big, qos, 1);
    size_t capAfterBig = buf.capacity();
    CHECK_GE(capAfterBig, buf.size());

    // 复用同一缓冲编一条小消息：内容正确，容量不应缩小（无重新分配）
    Payload small("hi");
    bool ok = MessageCodec::encodeInto(buf, MessageType::PUBLISH, 9, 8, "t", small, qos, 2);
    CHECK_TRUE(ok);
    CHECK_EQ(buf.capacity(), capAfterBig);   // 复用既有容量，未再分配
    auto r = MessageCodec::decode(buf.data(), buf.size());
    CHECK_TRUE(r.valid);
    CHECK_EQ(r.header.sourceId, 9u);
    CHECK_STR_EQ(r.topic, "t");
    CHECK_EQ(r.payload.size(), small.size());
    CHECK_EQ(std::memcmp(r.payload.data(), small.data(), small.size()), 0);
}

// 字段超限时 encodeInto 返回 false 并清空缓冲
TEST(encode_into_rejects_oversized_topic) {
    QoSProfile qos;
    std::string hugeTopic(MAX_TOPIC_LEN + 1, 'a');
    std::vector<uint8_t> buf{ 1, 2, 3 };
    bool ok = MessageCodec::encodeInto(buf, MessageType::PUBLISH, 1, 2,
                                       hugeTopic, Payload{}, qos, 1);
    CHECK_FALSE(ok);
    CHECK_TRUE(buf.empty());
}
