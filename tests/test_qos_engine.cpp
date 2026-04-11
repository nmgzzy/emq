#define EMQ_TEST_MODULE "qos_engine"
#include "test_framework.h"
#include "../src/core/qos_engine.h"
#include <thread>
#include <chrono>

using namespace embedmq;

TEST(ack_removes_pending) {
    QoSEngine engine;
    QoSProfile qos;
    qos.level           = QoSLevel::Reliable;
    qos.maxRetries      = 3;
    qos.retryIntervalMs = 5000; // 不触发超时

    int retryCalled = 0;
    engine.addPending(1, {0x01}, qos, [&](const std::vector<uint8_t>&) {
        retryCalled++;
    });
    CHECK_EQ(engine.pendingCount(), 1u);

    engine.onAck(1);
    CHECK_EQ(engine.pendingCount(), 0u);
    CHECK_EQ(retryCalled, 0);
}

TEST(timeout_triggers_retry) {
    QoSEngine engine;
    QoSProfile qos;
    qos.level           = QoSLevel::Reliable;
    qos.maxRetries      = 2;
    qos.retryIntervalMs = 20; // 20ms

    int retryCalled = 0;
    engine.addPending(1, {0x01}, qos, [&](const std::vector<uint8_t>&) {
        retryCalled++;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    engine.processTimeouts();
    CHECK_GE(retryCalled, 1);
    CHECK_EQ(engine.pendingCount(), 1u); // 还在，还没超过 maxRetries
}

TEST(abandon_after_max_retries) {
    QoSEngine engine;
    QoSProfile qos;
    qos.level           = QoSLevel::Reliable;
    qos.maxRetries      = 1;
    qos.retryIntervalMs = 10;

    engine.addPending(1, {0x01}, qos, [](const std::vector<uint8_t>&) {});

    // 第一次重试
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    engine.processTimeouts();

    // 第二次触发 -> 超过 maxRetries，应被放弃
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto abandoned = engine.processTimeouts();
    CHECK_EQ(engine.pendingCount(), 0u);
    CHECK_EQ(abandoned.size(), 1u);
    CHECK_EQ(abandoned[0], 1u);
}

TEST(dedup_qos2) {
    QoSEngine engine;
    CHECK_FALSE(engine.isDuplicate(1, 100));
    CHECK_TRUE(engine.isDuplicate(1, 100));  // 重复
    CHECK_FALSE(engine.isDuplicate(1, 101)); // 不同 seqId
    CHECK_FALSE(engine.isDuplicate(2, 100)); // 不同 sourceId
}

TEST(multiple_pending) {
    QoSEngine engine;
    QoSProfile qos;
    qos.level           = QoSLevel::Reliable;
    qos.maxRetries      = 3;
    qos.retryIntervalMs = 5000;

    for (uint32_t i = 1; i <= 5; i++)
        engine.addPending(i, {}, qos, [](const std::vector<uint8_t>&) {});
    CHECK_EQ(engine.pendingCount(), 5u);

    engine.onAck(3);
    CHECK_EQ(engine.pendingCount(), 4u);
}
