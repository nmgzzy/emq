#define EMQ_TEST_MODULE "review_fixes"
#include "test_framework.h"
#include "embedmq/embedmq.h"
#include "core/message_codec.h"
#include "discovery/peer_registry.h"
#include "util/timer_wheel.h"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace embedmq;

// ===================== 编解码加固 =====================

// 超长 topic：encode 应拒绝（返回空），避免 static_cast 截断
TEST(encode_rejects_oversized_topic) {
    QoSProfile qos;
    std::string big(MAX_TOPIC_LEN + 1, 'x');
    auto data = MessageCodec::encode(
        MessageType::PUBLISH, 1, 2, big, Payload("p"), qos, 1);
    CHECK_TRUE(data.empty());
}

// decode：声明的 topicLen/payloadLen 超过实际缓冲应判为无效（防越界读）
// 使用协议 v2 紧凑头偏移：topicLen@12..13，payloadLen@22..25，hdrFlags@4。
TEST(decode_rejects_length_overflow) {
    std::vector<uint8_t> buf(HEADER_BASE_SIZE, 0);
    buf[0] = static_cast<uint8_t>(EMBEDMQ_MAGIC & 0xFF);
    buf[1] = static_cast<uint8_t>((EMBEDMQ_MAGIC >> 8) & 0xFF);
    buf[2] = EMBEDMQ_VERSION;
    buf[3] = static_cast<uint8_t>(MessageType::PUBLISH);
    buf[4] = 0; // hdrFlags：无 TS / 无 CRC，使基础头自洽
    // topicLen = 0xFFFF（偏移 12..13）
    buf[12] = 0xFF; buf[13] = 0xFF;
    // payloadLen = 0xFFFFFFFF（偏移 22..25）
    buf[22] = 0xFF; buf[23] = 0xFF; buf[24] = 0xFF; buf[25] = 0xFF;

    auto result = MessageCodec::decode(buf.data(), buf.size());
    CHECK_FALSE(result.valid);
}

// ===================== TimerWheel =====================

// 超过单层轮长度（512*10ms=5120ms）的定时器不应提前触发
TEST(timer_long_delay_no_early_fire) {
    util::TimerWheel tw;
    tw.start();

    std::atomic<int> shortFired{0};
    std::atomic<int> longFired{0};
    tw.addOnce(50, [&]() { shortFired++; });
    tw.addOnce(5200, [&]() { longFired++; }); // 远超一圈

    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    CHECK_EQ(shortFired.load(), 1); // 短定时器已触发
    CHECK_EQ(longFired.load(), 0);  // 长定时器不得提前触发（旧实现会在 ~80ms 误触发）

    tw.stop();
}

// 周期定时器可被取消，取消后不再触发
TEST(timer_periodic_cancel) {
    util::TimerWheel tw;
    tw.start();

    std::atomic<int> fired{0};
    auto id = tw.addPeriodic(20, [&]() { fired++; });

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    int before = fired.load();
    CHECK_GE(before, 2);

    tw.cancel(id);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    int after = fired.load();
    // 取消后最多再触发一次（取消与正在执行的 tick 存在竞态窗口）
    CHECK_LE(after - before, 1);

    tw.stop();
}

// 停止后不再触发回调
TEST(timer_stop_no_fire) {
    util::TimerWheel tw;
    tw.start();
    std::atomic<int> fired{0};
    tw.addPeriodic(20, [&]() { fired++; });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    tw.stop();
    int snapshot = fired.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    CHECK_EQ(fired.load(), snapshot);
}

// ===================== 对端更新同步 =====================

static PeerInfo makePeer(uint16_t id, std::vector<std::string> subs) {
    PeerInfo p;
    p.id = id;
    p.name = "node" + std::to_string(id);
    p.subscribedTopics = std::move(subs);
    return p;
}

// 已知对端订阅变化时应触发 onUpdated，而非静默忽略
TEST(peer_registry_update_fires_callback) {
    PeerRegistry reg;
    std::atomic<int> discovered{0};
    std::atomic<int> updated{0};
    std::vector<std::string> lastTopics;

    reg.setOnDiscovered([&](const PeerInfo&) { discovered++; });
    reg.setOnUpdated([&](const PeerInfo& p) {
        updated++;
        lastTopics = p.subscribedTopics;
    });

    reg.addOrUpdate(makePeer(1, {"a"}));
    CHECK_EQ(discovered.load(), 1);
    CHECK_EQ(updated.load(), 0);

    // 订阅变化 → 应触发 onUpdated
    reg.addOrUpdate(makePeer(1, {"a", "b"}));
    CHECK_EQ(discovered.load(), 1);
    CHECK_EQ(updated.load(), 1);
    CHECK_EQ(lastTopics.size(), 2u);

    // 无变化 → 不应再触发
    reg.addOrUpdate(makePeer(1, {"a", "b"}));
    CHECK_EQ(updated.load(), 1);
}

// ===================== Req/Rep 不挂起 =====================

// 无任何服务提供方时，请求应快速返回 nullopt 而非永久挂起
TEST(request_no_provider_returns_quickly) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);
    auto req = p->createRequester("no_such_service");

    auto t0 = std::chrono::steady_clock::now();
    auto r = req->request(Payload("x"), std::chrono::milliseconds(1000));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();

    CHECK_FALSE(r.has_value());
    CHECK_LT(elapsed, 500); // 立即结束，不等到客户端超时
    p->shutdown();
}
