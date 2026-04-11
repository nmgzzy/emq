#define EMQ_TEST_MODULE "pub_sub"
#include "test_framework.h"
#include "embedmq/embedmq.h"
#include <atomic>
#include <chrono>
#include <thread>

using namespace embedmq;

// 测试同进程内的 Pub/Sub（不经过网络）
TEST(local_pub_sub_basic) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp = false; // 仅测试本地，不启动网络
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);
    CHECK_TRUE(p->isRunning());

    std::atomic<int> received{0};
    std::string receivedTopic;
    std::string receivedPayload;

    auto sub = p->createSubscriber("test/topic",
        [&](const ReceivedMessage& msg) {
            received++;
            receivedTopic   = msg.topic;
            receivedPayload = std::string(msg.payload.asText());
        });

    auto pub = p->createPublisher("test/topic");
    pub->publish("hello embedmq");

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    CHECK_EQ(received.load(), 1);
    CHECK_STR_EQ(receivedTopic,   "test/topic");
    CHECK_STR_EQ(receivedPayload, "hello embedmq");

    p->shutdown();
}

TEST(local_pub_sub_multiple_messages) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);
    std::atomic<int> count{0};
    auto sub = p->createSubscriber("data/#",
        [&](const ReceivedMessage&) { count++; });

    auto pub1 = p->createPublisher("data/temp");
    auto pub2 = p->createPublisher("data/humid");
    auto pub3 = p->createPublisher("other/topic");

    pub1->publish("25.0");
    pub2->publish("60%");
    pub3->publish("ignored");

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    CHECK_EQ(count.load(), 2); // data/# 不匹配 other/topic
    p->shutdown();
}

TEST(retained_message) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);

    // 先发布保留消息
    QoSProfile retainQos = QoSProfile::bestEffort();
    retainQos.retain      = true;
    retainQos.durability  = DurabilityKind::TransientLocal;

    auto pub = p->createPublisher("status/node", retainQos);
    pub->publish("online");

    // 后来的订阅者也应该收到保留消息
    std::atomic<int> count{0};
    auto sub = p->createSubscriber("status/node",
        [&](const ReceivedMessage& msg) { count++; },
        retainQos);

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK_GE(count.load(), 1);
    p->shutdown();
}

TEST(subscriber_pause_resume) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);
    std::atomic<int> count{0};

    auto sub = p->createSubscriber("ctrl",
        [&](const ReceivedMessage&) { count++; });
    auto pub = p->createPublisher("ctrl");

    pub->publish("1");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK_EQ(count.load(), 1);

    sub->pause();
    pub->publish("2");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK_EQ(count.load(), 1); // paused，不增加

    sub->resume();
    pub->publish("3");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK_EQ(count.load(), 2);

    p->shutdown();
}

TEST(message_count) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);
    auto sub = p->createSubscriber("count", [](const ReceivedMessage&){});
    auto pub = p->createPublisher("count");

    for (int i = 0; i < 5; i++) pub->publish("msg");
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    CHECK_EQ(sub->messageCount(), 5u);
    p->shutdown();
}
