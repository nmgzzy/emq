#define EMQ_TEST_MODULE "refactor_v2"
#include "test_framework.h"
#include "embedmq/types.h"
#include "core/message_codec.h"
#include "core/qos_engine.h"

#include <cstring>
#include <string>
#include <vector>

using namespace embedmq;

// ===================== Payload 小缓冲优化（SBO） =====================

TEST(payload_inline_small_roundtrip) {
    const char* s = "embedmq"; // 7 字节 <= kInlineCap，内联存储
    Payload p(s, std::strlen(s));
    CHECK_EQ(p.size(), std::strlen(s));
    CHECK_FALSE(p.empty());
    CHECK_EQ(std::memcmp(p.data(), s, p.size()), 0);
    CHECK_STR_EQ(std::string(p.asText()), "embedmq");
    // 内联小载荷也能完整取回字节（toVector 对两种形态语义一致）
    auto v = p.toVector();
    CHECK_EQ(v.size(), std::strlen(s));
    CHECK_EQ(std::memcmp(v.data(), s, v.size()), 0);
}

TEST(payload_large_roundtrip) {
    std::vector<uint8_t> big(1000);
    for (size_t i = 0; i < big.size(); ++i) big[i] = static_cast<uint8_t>(i & 0xFF);
    Payload p(big.data(), big.size());
    CHECK_EQ(p.size(), big.size());
    CHECK_EQ(std::memcmp(p.data(), big.data(), big.size()), 0);
    // 大块形态：toVector 取回的副本与原始数据一致
    auto v = p.toVector();
    CHECK_EQ(v.size(), big.size());
    CHECK_EQ(std::memcmp(v.data(), big.data(), big.size()), 0);

    // 拷贝应保持数据一致（大块共享、小块复制，语义对外一致）
    Payload q = p;
    CHECK_EQ(q.size(), p.size());
    CHECK_EQ(std::memcmp(q.data(), p.data(), p.size()), 0);
}

TEST(payload_boundary_exact_inline_cap) {
    std::vector<uint8_t> v(Payload::kInlineCap, 0xAB); // 恰好内联上限
    Payload p(v.data(), v.size());
    CHECK_EQ(p.size(), Payload::kInlineCap);
    CHECK_EQ(p.toVector().size(), Payload::kInlineCap);  // 内联边界字节完整
    std::vector<uint8_t> v2(Payload::kInlineCap + 1, 0xCD);
    Payload p2(v2.data(), v2.size());
    CHECK_EQ(p2.toVector().size(), Payload::kInlineCap + 1); // 越界转大块仍完整
}

// ===================== 协议 v2：显式小端 / 紧凑头 / 可选 CRC =====================

// 头部字段以显式小端写出，跨架构一致
TEST(codec_v2_explicit_little_endian) {
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::PUBLISH, 0x1234, 0x5678, "", Payload("x"), qos, 7);
    CHECK_GE(data.size(), HEADER_BASE_SIZE);
    CHECK_EQ(data[0], 0xDC); CHECK_EQ(data[1], 0xEB); // magic LE 0xEBDC
    CHECK_EQ(data[2], EMBEDMQ_VERSION);
    CHECK_EQ(data[8], 0x34); CHECK_EQ(data[9], 0x12); // sourceId LE
    CHECK_EQ(data[10], 0x78); CHECK_EQ(data[11], 0x56); // destId LE
}

// 关闭 CRC：头更短，且损坏字节不再被校验拦截
TEST(codec_v2_crc_optional) {
    QoSProfile qos;
    auto withCrc = MessageCodec::encode(
        MessageType::PUBLISH, 1, 2, "t", Payload("hello"), qos, 1, 0, 0, 0, true);
    auto noCrc = MessageCodec::encode(
        MessageType::PUBLISH, 1, 2, "t", Payload("hello"), qos, 1, 0, 0, 0, false);
    CHECK_EQ(withCrc.size(), noCrc.size() + 4); // 少一个 4 字节 checksum

    auto r1 = MessageCodec::decode(noCrc.data(), noCrc.size());
    CHECK_TRUE(r1.valid);
    CHECK_STR_EQ(std::string(r1.payload.asText()), "hello");

    // 关闭 CRC 时，篡改 payload 仍解码成功（无完整性校验）
    noCrc.back() ^= 0xFF;
    auto r2 = MessageCodec::decode(noCrc.data(), noCrc.size());
    CHECK_TRUE(r2.valid);
}

// 紧凑头：控制包不带 timestamp，数据包带 timestamp
TEST(codec_v2_compact_header_sizes) {
    QoSProfile qos;
    auto ack = MessageCodec::encode(
        MessageType::ACK, 1, 0, "", Payload{}, qos, 9); // 控制包：无 TS，有 CRC
    CHECK_EQ(ack.size(), HEADER_BASE_SIZE + 4);

    auto ackNoCrc = MessageCodec::encode(
        MessageType::ACK, 1, 0, "", Payload{}, qos, 9, 0, 0, 0, false);
    CHECK_EQ(ackNoCrc.size(), HEADER_BASE_SIZE);

    auto pub = MessageCodec::encode(
        MessageType::PUBLISH, 1, 0, "", Payload{}, qos, 9); // 数据包：含 TS(8)+CRC(4)
    CHECK_EQ(pub.size(), HEADER_BASE_SIZE + 8 + 4);

    // 解码后字段一致
    auto r = MessageCodec::decode(pub.data(), pub.size());
    CHECK_TRUE(r.valid);
    CHECK_EQ(static_cast<MessageType>(r.header.msgType), MessageType::PUBLISH);
}

// ===================== QoS2：滑动去重窗口 + 握手状态机 =====================

TEST(qos2_dedup_sliding_window_bounded) {
    QoSEngine engine;
    CHECK_FALSE(engine.isDuplicate(7, 1));
    CHECK_TRUE(engine.isDuplicate(7, 1)); // 立即重复

    // 推进 highWater 远超窗口（WINDOW=1024）
    for (uint32_t i = 2; i <= 2000; ++i)
        CHECK_FALSE(engine.isDuplicate(7, i));

    // seq=1 已落在窗口下界之外，应被判为“旧/重复”
    CHECK_TRUE(engine.isDuplicate(7, 1));
    // 窗口内最近的仍能识别重复
    CHECK_TRUE(engine.isDuplicate(7, 2000));
    // 不同 source 独立
    CHECK_FALSE(engine.isDuplicate(8, 1));
}

TEST(qos2_handshake_state_transitions) {
    QoSEngine engine;
    QoSProfile qos = QoSProfile::exactlyOnce();
    qos.retryIntervalMs = 100000; // 不触发超时

    // 发送方 PUBLISH 阶段挂起
    engine.addPending(42, {0x01, 0x02}, qos, [](const std::vector<uint8_t>&) {});
    CHECK_EQ(engine.pendingCount(), 1u);

    // 收到 PUBREC：停止 PUBLISH 重传
    CHECK_TRUE(engine.onPubrec(42));
    CHECK_EQ(engine.pendingCount(), 0u);
    CHECK_FALSE(engine.onPubrec(42)); // 再次无挂起

    // PUBREL 阶段挂起，直到 PUBCOMP
    engine.addPendingRel(42, {0x03}, qos, [](const std::vector<uint8_t>&) {});
    CHECK_EQ(engine.pendingRelCount(), 1u);
    engine.onPubcomp(42);
    CHECK_EQ(engine.pendingRelCount(), 0u);
}
