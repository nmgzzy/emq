#define EMQ_TEST_MODULE "last_will"
#include "test_framework.h"
#include "embedmq/embedmq.h"
#include "core/message_bus.h"
#include "discovery/peer_registry.h"
#include <atomic>
#include <chrono>
#include <string>
#include <thread>

using namespace embedmq;

// 构造一个带遗嘱的对端信息
static PeerInfo makePeerWithWill(uint16_t id, const std::string& name,
                                  const std::string& willTopic,
                                  const std::string& willPayload,
                                  bool retain) {
    PeerInfo p;
    p.id          = id;
    p.name        = name;
    p.hasWill     = true;
    p.willTopic   = willTopic;
    p.willPayload = Payload(willPayload);
    p.willRetain  = retain;
    p.willQos     = 1;
    return p;
}

// 超时（异常掉线）应触发遗嘱回调
TEST(registry_timeout_triggers_will) {
    PeerRegistry reg;
    std::atomic<int> willCount{0};
    std::atomic<int> lostCount{0};
    std::string willTopic;

    reg.setOnWill([&](const PeerInfo& p) {
        willCount++;
        willTopic = p.willTopic;
    });
    reg.setOnLost([&](uint16_t, const std::string&) { lostCount++; });

    reg.addOrUpdate(makePeerWithWill(1, "nodeA", "status/nodeA", "offline", true));
    CHECK_EQ(reg.count(), 1u);

    // 等待超过超时阈值后检查（elapsed > timeoutMs 判定掉线）
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    auto dead = reg.checkTimeouts(1);
    CHECK_EQ(dead.size(), 1u);
    CHECK_EQ(willCount.load(), 1);
    CHECK_EQ(lostCount.load(), 1);
    CHECK_STR_EQ(willTopic, "status/nodeA");
    CHECK_EQ(reg.count(), 0u);
}

// 优雅退出（FAREWELL → remove(graceful)）不应触发遗嘱
TEST(registry_graceful_no_will) {
    PeerRegistry reg;
    std::atomic<int> willCount{0};
    std::atomic<int> lostCount{0};

    reg.setOnWill([&](const PeerInfo&) { willCount++; });
    reg.setOnLost([&](uint16_t, const std::string&) { lostCount++; });

    reg.addOrUpdate(makePeerWithWill(2, "nodeB", "status/nodeB", "offline", true));
    reg.remove(2); // 默认 triggerWill=false（优雅退出）

    CHECK_EQ(willCount.load(), 0);
    CHECK_EQ(lostCount.load(), 1);
    CHECK_EQ(reg.count(), 0u);
}

// 无遗嘱的对端超时不触发遗嘱回调，但仍触发 lost
TEST(registry_timeout_no_will_config) {
    PeerRegistry reg;
    std::atomic<int> willCount{0};
    std::atomic<int> lostCount{0};

    reg.setOnWill([&](const PeerInfo&) { willCount++; });
    reg.setOnLost([&](uint16_t, const std::string&) { lostCount++; });

    PeerInfo p;
    p.id   = 3;
    p.name = "nodeC";
    p.hasWill = false;
    reg.addOrUpdate(p);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    reg.checkTimeouts(1);
    CHECK_EQ(willCount.load(), 0);
    CHECK_EQ(lostCount.load(), 1);
}

// MessageBus::deliverWill 应将遗嘱投递给本地订阅者
TEST(deliver_will_routes_local) {
    MessageBus bus(99, nullptr);
    bus.start();

    std::atomic<int> count{0};
    std::string gotTopic, gotPayload;
    auto sub = bus.createSubscriber("status/nodeX",
        [&](const ReceivedMessage& msg) {
            count++;
            gotTopic   = msg.topic;
            gotPayload = std::string(msg.payload.asText());
        }, QoSProfile::bestEffort());

    bus.deliverWill("status/nodeX", Payload("offline"), false, 42);

    CHECK_EQ(count.load(), 1);
    CHECK_STR_EQ(gotTopic, "status/nodeX");
    CHECK_STR_EQ(gotPayload, "offline");

    bus.stop();
}

// 带 retain 的遗嘱：投递后新订阅者也能收到（保留消息）
TEST(deliver_will_retained) {
    MessageBus bus(100, nullptr);
    bus.start();

    bus.deliverWill("status/nodeY", Payload("offline"), /*retain=*/true, 43);

    QoSProfile qos = QoSProfile::bestEffort();
    qos.durability = DurabilityKind::TransientLocal;

    std::atomic<int> count{0};
    auto sub = bus.createSubscriber("status/nodeY",
        [&](const ReceivedMessage&) { count++; }, qos);

    CHECK_GE(count.load(), 1);
    bus.stop();
}
