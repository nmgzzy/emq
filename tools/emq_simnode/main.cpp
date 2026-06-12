/**
 * emq_simnode —— 真实多节点场景模拟（一个进程 = 一个网络节点）
 *
 * 与 emq_stress（进程内、关网络）不同，本工具走真实的 UDP + 多播发现：
 * 每个节点订阅一组共享主题，并以固定频率向随机主题发布小消息，从而与
 * 其它节点真实地互相收发。用于模拟"N 个节点、每节点每秒若干次随机收发"
 * 这类稳态业务负载，便于在外部用 /proc 采样统计 CPU / 内存占用。
 *
 * 用法:
 *   emq_simnode <name> [--topics N] [--duration S] [--rate Hz]
 *                       [--payload B] [--reliable]
 *
 * 选项:
 *   --topics   N   共享主题数（节点订阅全部，发布时随机选一个）默认 8
 *   --duration S   运行时长秒，默认 20
 *   --rate     Hz  每秒发布次数，默认 10
 *   --payload  B   每条载荷字节，默认 64
 *   --reliable     用 Reliable QoS（默认 BestEffort，贴近 10Hz 遥测场景）
 *
 * 结束时打印一行：NODE <name> sent=.. recv=.. peers=.. topics=.. rateHz=..
 */
#include "embedmq/embedmq.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#if !defined(_WIN32)
#  include <unistd.h>
#endif

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr,
            "用法: emq_simnode <name> [--topics N] [--duration S] "
            "[--rate Hz] [--payload B] [--reliable]\n");
        return 2;
    }
    std::string name     = argv[1];
    int         topics   = 8;
    int         duration = 20;
    int         rate     = 10;
    int         payload  = 64;
    bool        reliable = false;

    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&](int& dst) { if (i + 1 < argc) dst = std::atoi(argv[++i]); };
        if      (a == "--topics")   next(topics);
        else if (a == "--duration") next(duration);
        else if (a == "--rate")     next(rate);
        else if (a == "--payload")  next(payload);
        else if (a == "--reliable") reliable = true;
    }
    if (topics  < 1) topics  = 1;
    if (rate    < 1) rate    = 1;
    if (payload < 1) payload = 1;

    embedmq::ParticipantConfig cfg;
    cfg.nodeName = name;
    // 真实联网：UDP + 多播自发现（默认即开启，这里显式写明意图）
    cfg.transport.enableUdp       = true;
    cfg.discovery.enableMulticast = true;

    auto p = embedmq::Participant::create(cfg);
    if (!p) { std::fprintf(stderr, "participant create failed: %s\n", name.c_str()); return 1; }

    const auto qos = reliable ? embedmq::QoSProfile::reliable()
                              : embedmq::QoSProfile::bestEffort();

    std::atomic<uint64_t> recv{0};
    std::vector<std::unique_ptr<embedmq::Subscriber>> subs;
    std::vector<std::unique_ptr<embedmq::Publisher>>  pubs;
    subs.reserve(topics);
    pubs.reserve(topics);
    for (int t = 0; t < topics; ++t) {
        std::string topic = "sim/t/" + std::to_string(t);
        subs.push_back(p->createSubscriber(topic,
            [&recv](const embedmq::ReceivedMessage&) {
                recv.fetch_add(1, std::memory_order_relaxed);
            }, qos));
        pubs.push_back(p->createPublisher(topic, qos));
    }

    // 给发现一点时间，让对端的订阅路由先建立，避免前若干条发空
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 随机数：用 pid 播种，保证各节点序列不同（不依赖全局状态）
    std::mt19937 rng(static_cast<uint32_t>(::getpid()));
    std::uniform_int_distribution<int> pick(0, topics - 1);
    const std::string body(static_cast<size_t>(payload), 'x');

    const auto period = std::chrono::microseconds(1000000 / rate);
    const auto tEnd   = std::chrono::steady_clock::now() +
                        std::chrono::seconds(duration);
    uint64_t sent = 0;
    auto nextTick = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() < tEnd) {
        pubs[static_cast<size_t>(pick(rng))]->publish(
            embedmq::Payload(std::string_view(body)));
        ++sent;
        nextTick += period;
        std::this_thread::sleep_until(nextTick);
    }

    std::printf("NODE %s sent=%llu recv=%llu peers=%zu topics=%d rateHz=%d qos=%s\n",
                name.c_str(),
                static_cast<unsigned long long>(sent),
                static_cast<unsigned long long>(recv.load()),
                p->discoveredPeers().size(), topics, rate,
                reliable ? "reliable" : "besteffort");
    p->shutdown();
    return 0;
}
