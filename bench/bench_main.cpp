// EmbedMQ 性能基准测试（Phase 3）
//
// 覆盖：内存池 vs malloc、MPSC/SPSC 无锁队列吞吐、CRC32 吞吐、
//       本地 Pub/Sub 吞吐与延迟。
//
// 运行：xmake run emq_bench

#include "embedmq/embedmq.h"
#include "util/memory_pool.h"
#include "util/mpsc_queue.h"
#include "util/ring_buffer.h"
#include "util/crc32.h"
#include "platform/process.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <thread>
#include <vector>

using namespace embedmq;
using Clock = std::chrono::steady_clock;

static double elapsedMs(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count() / 1e6;
}

static void printHeader(const char* title) {
    std::printf("\n===== %s =====\n", title);
}

static void benchMemoryPool() {
    printHeader("内存池 vs malloc/free");
    constexpr int N = 1'000'000;
    constexpr size_t BLK = 256;

    auto t0 = Clock::now();
    {
        std::vector<void*> ptrs(1024, nullptr);
        for (int i = 0; i < N; ++i) {
            size_t k = i & 1023;
            if (ptrs[k]) ::operator delete(ptrs[k]);
            ptrs[k] = ::operator new(BLK);
        }
        for (auto p : ptrs) if (p) ::operator delete(p);
    }
    auto t1 = Clock::now();
    {
        util::FixedBlockPool pool(BLK, 2048);
        std::vector<void*> ptrs(1024, nullptr);
        for (int i = 0; i < N; ++i) {
            size_t k = i & 1023;
            if (ptrs[k]) pool.deallocate(ptrs[k]);
            ptrs[k] = pool.allocate(BLK);
        }
        for (auto p : ptrs) if (p) pool.deallocate(p);
    }
    auto t2 = Clock::now();

    double mallocMs = elapsedMs(t0, t1);
    double poolMs   = elapsedMs(t1, t2);
    std::printf("  malloc/free : %8.2f ms  (%6.1f M ops/s)\n",
                mallocMs, N / mallocMs / 1000.0);
    std::printf("  FixedPool   : %8.2f ms  (%6.1f M ops/s)  加速 %.2fx\n",
                poolMs, N / poolMs / 1000.0, mallocMs / poolMs);
}

static void benchMpsc() {
    printHeader("无锁 MPSC 队列吞吐（4 生产者 → 1 消费者）");
    constexpr int PER = 500'000;
    constexpr int PRODUCERS = 4;
    util::MpscQueue<uint64_t> q;
    std::atomic<bool> start{false};

    auto t0 = Clock::now();
    std::vector<std::thread> prod;
    for (int p = 0; p < PRODUCERS; ++p) {
        prod.emplace_back([&]() {
            while (!start.load()) {}
            for (int i = 0; i < PER; ++i) q.push(static_cast<uint64_t>(i));
        });
    }
    std::atomic<long long> consumed{0};
    std::thread cons([&]() {
        long long c = 0;
        const long long target = static_cast<long long>(PER) * PRODUCERS;
        while (c < target) {
            if (q.pop().has_value()) ++c;
        }
        consumed.store(c);
    });

    start.store(true);
    for (auto& t : prod) t.join();
    cons.join();
    auto t1 = Clock::now();

    double ms = elapsedMs(t0, t1);
    long long total = consumed.load();
    std::printf("  搬运 %lld 条消息: %8.2f ms  (%6.1f M msg/s)\n",
                total, ms, total / ms / 1000.0);
}

static void benchSpsc() {
    printHeader("无锁 SPSC 环形缓冲吞吐");
    constexpr int N = 5'000'000;
    util::SPSCRingBuffer<uint64_t, 4096> ring;

    auto t0 = Clock::now();
    std::thread producer([&]() {
        for (int i = 0; i < N; ) {
            if (ring.push(static_cast<uint64_t>(i))) ++i;
        }
    });
    long long got = 0;
    while (got < N) {
        if (ring.pop().has_value()) ++got;
    }
    producer.join();
    auto t1 = Clock::now();

    double ms = elapsedMs(t0, t1);
    std::printf("  搬运 %d 条消息: %8.2f ms  (%6.1f M msg/s)\n",
                N, ms, N / ms / 1000.0);
}

static void benchCrc32() {
    printHeader("CRC32 吞吐");
    constexpr size_t SIZE = 64 * 1024;
    constexpr int    ITERS = 4'000;
    std::vector<uint8_t> data(SIZE);
    for (size_t i = 0; i < SIZE; ++i) data[i] = static_cast<uint8_t>(i);

    auto t0 = Clock::now();
    uint32_t acc = 0;
    for (int i = 0; i < ITERS; ++i) acc ^= util::crc32(data.data(), SIZE);
    auto t1 = Clock::now();

    double ms = elapsedMs(t0, t1);
    double mb = (static_cast<double>(SIZE) * ITERS) / (1024.0 * 1024.0);
    std::printf("  处理 %.0f MB: %8.2f ms  (%6.1f MB/s)  [acc=%u]\n",
                mb, ms, mb / ms * 1000.0, acc);
}

static void benchPubSub() {
    printHeader("本地 Pub/Sub 吞吐与延迟");
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.transport.enableShm       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);

    constexpr int N = 200'000;
    std::atomic<long long> received{0};
    auto sub = p->createSubscriber("bench/topic",
        [&](const ReceivedMessage&) { received.fetch_add(1, std::memory_order_relaxed); });
    auto pub = p->createPublisher("bench/topic");

    const char* msg = "embedmq-benchmark-payload-32bytes!";
    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) pub->publish(msg);
    auto t1 = Clock::now();

    double ms = elapsedMs(t0, t1);
    std::printf("  发布 %d 条 (本地直达路由): %8.2f ms  (%6.2f M msg/s)\n",
                N, ms, N / ms / 1000.0);
    std::printf("  平均单条发布延迟: %.3f us, 接收计数: %lld\n",
                ms * 1000.0 / N, received.load());
    p->shutdown();
}

int main() {
    std::printf("EmbedMQ 性能基准测试 (Phase 3)\n");
    std::printf("硬件并发: %u 核\n", platform::hardwareConcurrency());

    benchMemoryPool();
    benchMpsc();
    benchSpsc();
    benchCrc32();
    benchPubSub();

    std::printf("\n基准测试完成。\n");
    return 0;
}
