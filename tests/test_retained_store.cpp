#define EMQ_TEST_MODULE "retained_store"
#include "test_framework.h"
#include "../src/core/retained_store.h"
#include <chrono>
#include <thread>

using namespace embedmq;

static ReceivedMessage makeMsg(const std::string& topic, const char* text) {
    ReceivedMessage m;
    m.topic   = topic;
    m.payload = Payload(std::string_view(text));
    return m;
}

// 默认行为：TTL=0 时保留消息永不过期，expire() 不丢弃任何条目。
TEST(retained_no_ttl_never_expires) {
    RetainedStore store;
    store.store("a/b", makeMsg("a/b", "hello"));
    CHECK_EQ(store.size(), 1u);

    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    CHECK_EQ(store.expire(), 0u);
    CHECK_EQ(store.size(), 1u);
    CHECK(store.get("a/b").has_value());
}

// 全局默认 TTL：超过生存期的条目被 expire() 丢弃。
TEST(retained_global_ttl_expires) {
    RetainedStore store;
    store.configure(/*defaultTtlMs=*/30, /*maxCount=*/0);
    store.store("sensor/temp", makeMsg("sensor/temp", "25C"));
    CHECK_EQ(store.size(), 1u);

    // 未到期：不丢弃
    CHECK_EQ(store.expire(), 0u);
    CHECK_EQ(store.size(), 1u);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    CHECK_EQ(store.expire(), 1u);
    CHECK_EQ(store.size(), 0u);
    CHECK(!store.get("sensor/temp").has_value());
}

// 单条 lifespanMs 覆盖全局默认 TTL。
TEST(retained_per_message_lifespan_overrides) {
    RetainedStore store;
    store.configure(/*defaultTtlMs=*/100000, /*maxCount=*/0); // 全局很长
    store.store("fast", makeMsg("fast", "x"), /*lifespanMs=*/30); // 覆盖为很短

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    CHECK_EQ(store.expire(), 1u);
    CHECK_EQ(store.size(), 0u);
}

// 覆盖写入会刷新存入时间，使 TTL 重新计时。
TEST(retained_store_refreshes_ttl) {
    RetainedStore store;
    store.configure(/*defaultTtlMs=*/50, /*maxCount=*/0);
    store.store("k", makeMsg("k", "v1"));

    std::this_thread::sleep_for(std::chrono::milliseconds(35));
    store.store("k", makeMsg("k", "v2")); // 刷新计时
    std::this_thread::sleep_for(std::chrono::milliseconds(35));

    // 距首次 70ms（已超 50），但距刷新仅 35ms：不应过期
    CHECK_EQ(store.expire(), 0u);
    CHECK_EQ(store.size(), 1u);
    CHECK_EQ(std::string(store.get("k")->payload.asText()), "v2");
}

// 条目数上限：超出时驱逐最早存入的条目。
TEST(retained_max_count_evicts_oldest) {
    RetainedStore store;
    store.configure(/*defaultTtlMs=*/0, /*maxCount=*/2);

    store.store("t1", makeMsg("t1", "1"));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    store.store("t2", makeMsg("t2", "2"));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    store.store("t3", makeMsg("t3", "3")); // 触发驱逐最早的 t1

    CHECK_EQ(store.size(), 2u);
    CHECK(!store.get("t1").has_value()); // 最早条目被逐出
    CHECK(store.get("t2").has_value());
    CHECK(store.get("t3").has_value());
}
