#define EMQ_TEST_MODULE "pal"
#include "test_framework.h"
#include "../src/platform/process.h"
#include "../src/util/ring_buffer.h"
#include "../src/util/crc32.h"
#include "../src/util/timer_wheel.h"
#include <chrono>
#include <thread>

using namespace embedmq;
using namespace embedmq::platform;
using namespace embedmq::util;

// ---- 进程工具 ----
TEST(get_process_id) {
    uint64_t pid = getProcessId();
    CHECK_GT(pid, 0u);
}

TEST(get_hostname) {
    std::string host = getHostName();
    CHECK_FALSE(host.empty());
}

TEST(get_temp_dir) {
    std::string dir = getEmbedMqTempDir();
    CHECK_FALSE(dir.empty());
}

// ---- CRC32 ----
TEST(crc32_known_value) {
    // CRC32("123456789") = 0xCBF43926
    const uint8_t data[] = "123456789";
    uint32_t crc = util::crc32(data, 9);
    CHECK_EQ(crc, 0xCBF43926u);
}

TEST(crc32_empty) {
    uint32_t crc = util::crc32(nullptr, 0);
    CHECK_EQ(crc, 0xFFFFFFFFu ^ 0xFFFFFFFFu); // ~0xFFFFFFFF = 0
}

TEST(crc32_consistency) {
    const uint8_t data[] = "EmbedMQ Test";
    uint32_t c1 = util::crc32(data, sizeof(data) - 1);
    uint32_t c2 = util::crc32(data, sizeof(data) - 1);
    CHECK_EQ(c1, c2);
}

// ---- SPSC Ring Buffer ----
TEST(ring_buffer_push_pop) {
    SPSCRingBuffer<int, 8> rb;
    CHECK_TRUE(rb.empty());

    CHECK_TRUE(rb.push(1));
    CHECK_TRUE(rb.push(2));
    CHECK_TRUE(rb.push(3));
    CHECK_FALSE(rb.empty());
    CHECK_EQ(rb.size(), 3u);

    auto v1 = rb.pop();
    CHECK_TRUE(v1.has_value());
    CHECK_EQ(*v1, 1);
    auto v2 = rb.pop();
    CHECK_EQ(*v2, 2);
    auto v3 = rb.pop();
    CHECK_EQ(*v3, 3);
    CHECK_TRUE(rb.empty());
}

TEST(ring_buffer_full) {
    SPSCRingBuffer<int, 4> rb;
    // 容量 4，最多存 3 个（因为满时 head+1==tail）
    CHECK_TRUE(rb.push(1));
    CHECK_TRUE(rb.push(2));
    CHECK_TRUE(rb.push(3));
    CHECK_FALSE(rb.push(4)); // 满
}

TEST(ring_buffer_empty_pop) {
    SPSCRingBuffer<int, 4> rb;
    auto v = rb.pop();
    CHECK_FALSE(v.has_value());
}

// ---- Timer Wheel ----
TEST(timer_wheel_once) {
    TimerWheel tw;
    tw.start();
    std::atomic<int> fired{0};
    tw.addOnce(50, [&]() { fired++; });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    tw.stop();
    CHECK_EQ(fired.load(), 1);
}

TEST(timer_wheel_periodic) {
    TimerWheel tw;
    tw.start();
    std::atomic<int> fired{0};
    tw.addPeriodic(30, [&]() { fired++; });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    tw.stop();
    CHECK_GE(fired.load(), 2); // 至少触发2次
}
