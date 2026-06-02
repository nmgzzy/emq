#define EMQ_TEST_MODULE "phase3"
#include "test_framework.h"

#include "util/memory_pool.h"
#include "util/mpsc_queue.h"
#include "platform/process.h"
#include "core/message_codec.h"

#ifdef EMBEDMQ_ENABLE_SHM
#include "transport/shm_transport.h"
#endif

#include <atomic>
#include <chrono>
#include <cstring>
#include <set>
#include <thread>
#include <vector>

using namespace embedmq;

// ===================== 内存池 =====================

TEST(mempool_alloc_dealloc) {
    util::FixedBlockPool pool(64, 8);
    void* a = pool.allocate();
    void* b = pool.allocate();
    CHECK_TRUE(a != nullptr);
    CHECK_TRUE(b != nullptr);
    CHECK_TRUE(a != b);
    CHECK_EQ(pool.inUse(), 2u);
    pool.deallocate(a);
    pool.deallocate(b);
    CHECK_EQ(pool.inUse(), 0u);
}

TEST(mempool_reuse_block) {
    util::FixedBlockPool pool(64, 4);
    void* a = pool.allocate();
    pool.deallocate(a);
    void* b = pool.allocate(); // 应复用刚归还的块
    CHECK_EQ(a, b);
    pool.deallocate(b);
}

TEST(mempool_grow) {
    util::FixedBlockPool pool(32, 2, /*allowGrow=*/true);
    std::vector<void*> ptrs;
    for (int i = 0; i < 10; ++i) {
        void* p = pool.allocate();
        CHECK_TRUE(p != nullptr);
        ptrs.push_back(p);
    }
    CHECK_GE(pool.capacity(), 10u);
    for (auto p : ptrs) pool.deallocate(p);
    CHECK_EQ(pool.inUse(), 0u);
}

TEST(mempool_no_grow_exhaust) {
    util::FixedBlockPool pool(32, 2, /*allowGrow=*/false);
    void* a = pool.allocate();
    void* b = pool.allocate();
    void* c = pool.allocate(); // 池已耗尽且不增长
    CHECK_TRUE(a != nullptr);
    CHECK_TRUE(b != nullptr);
    CHECK_TRUE(c == nullptr);
    pool.deallocate(a);
    pool.deallocate(b);
}

TEST(mempool_oversized_fallback) {
    util::FixedBlockPool pool(16, 4);
    void* big = pool.allocate(1024); // 超过块大小 → 回退堆分配
    CHECK_TRUE(big != nullptr);
    std::memset(big, 0xAB, 1024);    // 可安全写入 1024 字节
    pool.deallocate(big);
}

// ===================== MPSC 无锁队列 =====================

TEST(mpsc_basic_fifo) {
    util::MpscQueue<int> q;
    CHECK_TRUE(q.empty());
    q.push(1); q.push(2); q.push(3);
    CHECK_FALSE(q.empty());
    CHECK_EQ(q.pop().value(), 1);
    CHECK_EQ(q.pop().value(), 2);
    CHECK_EQ(q.pop().value(), 3);
    CHECK_FALSE(q.pop().has_value());
    CHECK_TRUE(q.empty());
}

TEST(mpsc_multi_producer) {
    util::MpscQueue<uint64_t> q;
    constexpr int PRODUCERS = 4;
    constexpr int PER       = 10000;
    std::atomic<bool> go{false};

    std::vector<std::thread> producers;
    for (int p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&, p]() {
            while (!go.load()) {}
            for (int i = 0; i < PER; ++i)
                q.push(static_cast<uint64_t>(p) * 1000000ull + i);
        });
    }

    go.store(true);
    long long count = 0;
    const long long target = static_cast<long long>(PRODUCERS) * PER;
    while (count < target) {
        if (q.pop().has_value()) ++count;
    }
    for (auto& t : producers) t.join();

    CHECK_EQ(count, target);
    CHECK_FALSE(q.pop().has_value());
}

// ===================== CPU 亲和性 =====================

TEST(affinity_current_thread) {
    // 绑定到 0 号核心；不支持的平台返回 false，均不应崩溃
    bool ok = platform::setCurrentThreadAffinity(0);
#if defined(EMQ_PLATFORM_LINUX) || defined(EMQ_PLATFORM_WINDOWS)
    CHECK_TRUE(ok);
#else
    (void)ok; // macOS 无硬亲和性 API
    CHECK_TRUE(true);
#endif
    CHECK_GE(platform::hardwareConcurrency(), 1u);
    // cpu < 0 视为不绑定
    CHECK_FALSE(platform::setCurrentThreadAffinity(-1));
}

// ===================== 共享内存传输 =====================

#ifdef EMBEDMQ_ENABLE_SHM
TEST(shm_transport_send_recv) {
    // 两个实例：receiver 拥有收件箱，sender 向其写入
    ShmTransport receiver;
    ShmTransport sender;

    std::string rxName = "emq_test_shm_rx_" +
        std::to_string(platform::getProcessId());
    std::string txName = "emq_test_shm_tx_" +
        std::to_string(platform::getProcessId());

    CHECK_TRUE(receiver.init("{\"shm_name\":\"" + rxName + "\"}"));
    CHECK_TRUE(sender.init("{\"shm_name\":\"" + txName + "\"}"));

    std::atomic<int> got{0};
    std::string payload;
    receiver.setRecvCallback([&](const Endpoint&, const uint8_t* d, size_t n) {
        got++;
        payload.assign(reinterpret_cast<const char*>(d), n);
    });

    Endpoint to;
    to.transportType = "shm";
    to.address       = rxName;

    const char* msg = "hello-shm";
    CHECK_TRUE(sender.send(to, reinterpret_cast<const uint8_t*>(msg),
                           std::strlen(msg)));

    // 等待轮询线程消费
    for (int i = 0; i < 200 && got.load() == 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    CHECK_EQ(got.load(), 1);
    CHECK_STR_EQ(payload, "hello-shm");

    sender.shutdown();
    receiver.shutdown();
}

TEST(shm_transport_many_messages) {
    ShmTransport receiver, sender;
    std::string rxName = "emq_test_shm_many_" +
        std::to_string(platform::getProcessId());
    CHECK_TRUE(receiver.init("{\"shm_name\":\"" + rxName + "\"}"));
    CHECK_TRUE(sender.init("{\"shm_name\":\"emq_test_shm_many_tx_" +
        std::to_string(platform::getProcessId()) + "\"}"));

    std::atomic<int> got{0};
    receiver.setRecvCallback([&](const Endpoint&, const uint8_t*, size_t) {
        got++;
    });

    Endpoint to; to.transportType = "shm"; to.address = rxName;
    constexpr int N = 500;
    int sent = 0;
    for (int i = 0; i < N; ++i) {
        uint32_t v = static_cast<uint32_t>(i);
        // 收件箱为有界环，写满时返回 false（背压）；重试直到成功
        int attempts = 0;
        while (!sender.send(to, reinterpret_cast<const uint8_t*>(&v), sizeof(v))) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            if (++attempts > 10000) break;
        }
        if (attempts <= 10000) ++sent;
    }

    for (int i = 0; i < 1000 && got.load() < sent; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    CHECK_EQ(sent, N);
    CHECK_EQ(got.load(), N);

    sender.shutdown();
    receiver.shutdown();
}
#endif // EMBEDMQ_ENABLE_SHM

// ===================== 零拷贝编码 =====================

TEST(zerocopy_encode_header_matches_decode) {
    QoSProfile qos = QoSProfile::bestEffort();
    std::string topic = "sensor/temp";
    Payload payload("25.6");

    // 连续编码（基准）
    auto contiguous = MessageCodec::encode(
        MessageType::PUBLISH, 7, 0xFFFF, topic, payload, qos, 123, 0, 0);

    // 零拷贝头编码 + 手动拼接 topic/payload，应得到等价字节流
    auto header = MessageCodec::encodeHeader(
        MessageType::PUBLISH, 7, 0xFFFF, topic, payload, qos, 123, 0, 0);

    std::vector<uint8_t> gathered = header;
    gathered.insert(gathered.end(), topic.begin(), topic.end());
    const uint8_t* pd = payload.data();
    gathered.insert(gathered.end(), pd, pd + payload.size());

    CHECK_EQ(gathered.size(), contiguous.size());

    // 解码 gather 版本应有效且字段一致
    auto res = MessageCodec::decode(gathered.data(), gathered.size());
    CHECK_TRUE(res.valid);
    CHECK_STR_EQ(res.topic, "sensor/temp");
    CHECK_STR_EQ(std::string(res.payload.asText()), "25.6");
    CHECK_EQ(res.header.sequenceId, 123u);
    CHECK_EQ(res.header.sourceId, 7u);
}
