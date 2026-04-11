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
