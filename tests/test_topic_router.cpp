#define EMQ_TEST_MODULE "topic_router"
#include "test_framework.h"
#include "../src/core/topic_router.h"
#include <atomic>

using namespace embedmq;

// ---- 精确匹配 ----
TEST(exact_match_basic) {
    TopicRouter router;
    std::atomic<int> count{0};

    router.addSubscription("sensor/temp", [&](const ReceivedMessage&) { count++; },
                            QoSProfile::bestEffort());

    ReceivedMessage msg;
    msg.topic = "sensor/temp";
    router.route("sensor/temp", msg);
    CHECK_EQ(count.load(), 1);
}

TEST(exact_match_no_cross) {
    TopicRouter router;
    std::atomic<int> count{0};
    router.addSubscription("sensor/temp", [&](const ReceivedMessage&) { count++; },
                            QoSProfile{});
    ReceivedMessage msg;
    router.route("sensor/humidity", msg);
    CHECK_EQ(count.load(), 0);
}

// ---- 通配符 * ----
TEST(wildcard_single_level) {
    TopicRouter router;
    std::atomic<int> count{0};
    router.addSubscription("sensor/*/room1", [&](const ReceivedMessage&) { count++; },
                            QoSProfile{});
    ReceivedMessage msg;
    router.route("sensor/temperature/room1", msg);
    CHECK_EQ(count.load(), 1);
    router.route("sensor/humidity/room1", msg);
    CHECK_EQ(count.load(), 2);
    router.route("sensor/temperature/room2", msg);
    CHECK_EQ(count.load(), 2); // 不匹配
}

// ---- 通配符 # ----
TEST(wildcard_multi_level) {
    TopicRouter router;
    std::atomic<int> count{0};
    router.addSubscription("sensor/#", [&](const ReceivedMessage&) { count++; },
                            QoSProfile{});
    ReceivedMessage msg;
    router.route("sensor/temp",             msg); // +1
    router.route("sensor/temp/room1",       msg); // +1
    router.route("sensor/temp/room1/detail",msg); // +1
    router.route("other/temp",              msg); // 不匹配
    CHECK_EQ(count.load(), 3);
}

TEST(wildcard_hash_matches_single) {
    TopicRouter router;
    std::atomic<int> count{0};
    router.addSubscription("a/#", [&](const ReceivedMessage&) { count++; },
                            QoSProfile{});
    ReceivedMessage msg;
    router.route("a/b", msg);
    CHECK_EQ(count.load(), 1);
}

// ---- 静态 matchWildcard ----
TEST(match_wildcard_static) {
    CHECK_TRUE(TopicRouter::matchWildcard("a/b/c", "a/b/c"));
    CHECK_FALSE(TopicRouter::matchWildcard("a/b/c", "a/b/d"));
    CHECK_TRUE(TopicRouter::matchWildcard("a/*/c", "a/b/c"));
    CHECK_FALSE(TopicRouter::matchWildcard("a/*/c", "a/b/d"));
    CHECK_TRUE(TopicRouter::matchWildcard("a/#",   "a/b/c/d"));
    CHECK_TRUE(TopicRouter::matchWildcard("#",     "anything"));
    CHECK_FALSE(TopicRouter::matchWildcard("a/b",  "a/b/c"));
    // 末段 '#' 也匹配“剩余零段”
    CHECK_TRUE(TopicRouter::matchWildcard("a/#",   "a"));
}

// '#' 仅在末段才作 match-all（MQTT 语义）；出现在中间段不得过度匹配
TEST(match_wildcard_hash_must_be_last) {
    CHECK_FALSE(TopicRouter::matchWildcard("a/#/b", "a/x/y/z"));
    CHECK_FALSE(TopicRouter::matchWildcard("a/#/b", "a/x/b"));
    CHECK_FALSE(TopicRouter::matchWildcard("#/b",   "a/b"));
    // 末段 '#' 仍正常工作
    CHECK_TRUE(TopicRouter::matchWildcard("a/b/#",  "a/b/c"));
    CHECK_TRUE(TopicRouter::matchWildcard("a/b/#",  "a/b"));
}

// ---- 取消订阅 ----
TEST(unsubscribe) {
    TopicRouter router;
    std::atomic<int> count{0};
    uint64_t id = router.addSubscription("foo",
                    [&](const ReceivedMessage&) { count++; }, QoSProfile{});
    ReceivedMessage msg;
    router.route("foo", msg);
    CHECK_EQ(count.load(), 1);
    router.removeSubscription(id);
    router.route("foo", msg);
    CHECK_EQ(count.load(), 1); // 不再触发
}

// ---- hasSubscribers ----
TEST(has_subscribers) {
    TopicRouter router;
    CHECK_FALSE(router.hasSubscribers("data/x"));
    router.addSubscription("data/x", [](const ReceivedMessage&){}, QoSProfile{});
    CHECK_TRUE(router.hasSubscribers("data/x"));
}

// ---- 多订阅者 ----
TEST(multiple_subscribers) {
    TopicRouter router;
    std::atomic<int> c1{0}, c2{0};
    router.addSubscription("t", [&](const ReceivedMessage&){ c1++; }, QoSProfile{});
    router.addSubscription("t", [&](const ReceivedMessage&){ c2++; }, QoSProfile{});
    ReceivedMessage msg;
    router.route("t", msg);
    CHECK_EQ(c1.load(), 1);
    CHECK_EQ(c2.load(), 1);
}
