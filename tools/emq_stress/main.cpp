/**
 * emq_stress —— EmbedMQ 压力测试 / 稳定性测试（Phase 5 测试补强）
 *
 * 目标：在高负载、高并发、长时间运行、频繁生命周期切换等场景下，验证
 *   - 正确性：本地投递为同步内联，计数应精确无丢失；
 *   - 线程安全：多生产者并发发布、并发订阅不崩溃、不死锁；
 *   - 稳定性：长时间 soak 与高频 churn 下进程存活、RSS 不持续膨胀；
 *   - 性能：报告吞吐(msg/s)与请求往返延迟。
 *
 * 所有场景默认走**进程内**通信（关闭 UDP/多播），使本地投递确定可计数，
 * 不依赖网络环境（多播在容器/WSL 等环境常不可用）。
 *
 * 用法:
 *   emq_stress <scenario> [options]
 *
 * 场景:
 *   throughput   单发布者→单订阅者，发 N 条，测吞吐并校验零丢失
 *   fanout       单发布者→M 订阅者，校验每个订阅者均收到 N 条
 *   concurrent   T 个生产者线程并发发布(持续 duration)，K 订阅者计数校验
 *   reqrep       1 个 echo 服务 + R 个并发请求者，校验全部应答并测延迟
 *   churn        高频 create/destroy 参与者+pub/sub，校验生命周期稳定
 *   soak         混合负载持续运行 duration，校验存活与计数单调增长
 *   all          依次跑各场景的"短"版本作为自测(带阈值，返回 0/非0)
 *
 * 选项:
 *   -d, --duration <sec>   持续型场景时长(默认 3s)
 *   -n, --messages <N>     消息条数(默认 200000)
 *   -t, --threads  <T>     生产者/请求者线程数(默认 4)
 *   -s, --subs     <M>     订阅者数(默认 8)
 *   -p, --payload  <bytes> 载荷字节数(默认 16)
 *   -h, --help
 */
#include "embedmq/embedmq.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint> // for fixed-width via <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#if defined(__linux__)
#  include <unistd.h>
#endif

using Clock = std::chrono::steady_clock;

namespace {

struct Options {
    int    durationSec = 3;
    long   messages    = 200000;
    int    threads     = 4;
    int    subs        = 8;
    int    payload     = 16;
};

double nowSec(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

std::unique_ptr<embedmq::Participant> makeLocal(const std::string& name) {
    embedmq::ParticipantConfig cfg;
    cfg.nodeName                  = name;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;
    cfg.discovery.enableLocalDiscovery = false;
    return embedmq::Participant::create(cfg);
}

std::string makePayload(int n) { return std::string(static_cast<size_t>(n), 'x'); }

// 读取当前进程 RSS(KB)，仅 Linux 有效，其他平台返回 0
long currentRssKb() {
#if defined(__linux__)
    long rss = 0;
    if (FILE* f = std::fopen("/proc/self/statm", "r")) {
        long pages = 0, resident = 0;
        if (std::fscanf(f, "%ld %ld", &pages, &resident) == 2)
            rss = resident * (sysconf(_SC_PAGESIZE) / 1024);
        std::fclose(f);
    }
    return rss;
#else
    return 0;
#endif
}

// 统一的结果记录
struct Result {
    std::string name;
    bool        passed{true};
    std::string detail;
};

void report(const Result& r) {
    std::printf("[%s] %-12s %s\n", r.passed ? "PASS" : "FAIL",
                r.name.c_str(), r.detail.c_str());
}

// ---------------- 场景实现 ----------------

Result scenarioThroughput(const Options& opt) {
    Result r; r.name = "throughput";
    auto p = makeLocal("stress_tp");
    if (!p) { r.passed = false; r.detail = "participant create failed"; return r; }

    std::atomic<uint64_t> got{0};
    auto sub = p->createSubscriber("bench/topic",
        [&](const embedmq::ReceivedMessage&) { got.fetch_add(1, std::memory_order_relaxed); });
    auto pub = p->createPublisher("bench/topic");

    const std::string payload = makePayload(opt.payload);
    auto t0 = Clock::now();
    for (long i = 0; i < opt.messages; ++i)
        pub->publish(std::string_view(payload));
    auto t1 = Clock::now();

    double sec  = nowSec(t0, t1);
    double rate = opt.messages / (sec > 0 ? sec : 1e-9);
    // 本地投递为同步内联：零丢失
    r.passed = (got.load() == static_cast<uint64_t>(opt.messages));
    char buf[256];
    std::snprintf(buf, sizeof(buf),
        "sent=%ld recv=%llu in %.3fs => %.2f Kmsg/s (payload=%dB)%s",
        opt.messages, static_cast<unsigned long long>(got.load()), sec,
        rate / 1000.0, opt.payload, r.passed ? "" : "  [LOSS DETECTED]");
    r.detail = buf;
    return r;
}

Result scenarioFanout(const Options& opt) {
    Result r; r.name = "fanout";
    auto p = makeLocal("stress_fo");
    if (!p) { r.passed = false; r.detail = "participant create failed"; return r; }

    const int M = std::max(1, opt.subs);
    std::vector<std::unique_ptr<embedmq::Subscriber>> subs;
    std::vector<std::atomic<uint64_t>> counts(M);
    for (int i = 0; i < M; ++i) counts[i].store(0);
    for (int i = 0; i < M; ++i) {
        subs.push_back(p->createSubscriber("fan/#",
            [&counts, i](const embedmq::ReceivedMessage&) {
                counts[i].fetch_add(1, std::memory_order_relaxed);
            }));
    }
    auto pub = p->createPublisher("fan/data");
    const std::string payload = makePayload(opt.payload);
    long n = std::min<long>(opt.messages, 100000); // 扇出场景适当降量

    auto t0 = Clock::now();
    for (long i = 0; i < n; ++i) pub->publish(std::string_view(payload));
    auto t1 = Clock::now();

    bool ok = true;
    for (int i = 0; i < M; ++i)
        if (counts[i].load() != static_cast<uint64_t>(n)) ok = false;
    r.passed = ok;
    double sec = nowSec(t0, t1);
    char buf[256];
    std::snprintf(buf, sizeof(buf),
        "%d subs x %ld msgs = %ld deliveries in %.3fs => %.2f Kdeliv/s%s",
        M, n, (long)M * n, sec, (M * n) / (sec > 0 ? sec : 1e-9) / 1000.0,
        ok ? "" : "  [COUNT MISMATCH]");
    r.detail = buf;
    return r;
}

Result scenarioConcurrent(const Options& opt) {
    Result r; r.name = "concurrent";
    auto p = makeLocal("stress_cc");
    if (!p) { r.passed = false; r.detail = "participant create failed"; return r; }

    const int K = std::max(1, opt.subs);
    const int T = std::max(1, opt.threads);
    std::vector<std::unique_ptr<embedmq::Subscriber>> subs;
    std::atomic<uint64_t> totalRecv{0};
    for (int i = 0; i < K; ++i) {
        subs.push_back(p->createSubscriber("cc/#",
            [&](const embedmq::ReceivedMessage&) {
                totalRecv.fetch_add(1, std::memory_order_relaxed);
            }));
    }

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> totalSent{0};
    const std::string payload = makePayload(opt.payload);

    auto t0 = Clock::now();
    std::vector<std::thread> producers;
    for (int t = 0; t < T; ++t) {
        producers.emplace_back([&, t]() {
            auto pub = p->createPublisher("cc/p" + std::to_string(t));
            uint64_t local = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                for (int b = 0; b < 256; ++b) pub->publish(std::string_view(payload));
                local += 256;
            }
            totalSent.fetch_add(local, std::memory_order_relaxed);
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(opt.durationSec));
    stop.store(true);
    for (auto& th : producers) th.join();
    auto t1 = Clock::now();

    // 同步内联投递：每条被 K 个订阅者各收一次
    uint64_t expect = totalSent.load() * static_cast<uint64_t>(K);
    r.passed = (totalRecv.load() == expect);
    double sec = nowSec(t0, t1);
    char buf[300];
    std::snprintf(buf, sizeof(buf),
        "%d producers x %d subs, sent=%llu recv=%llu (expect=%llu) in %.2fs => %.2f Kmsg/s%s",
        T, K, (unsigned long long)totalSent.load(),
        (unsigned long long)totalRecv.load(), (unsigned long long)expect, sec,
        totalSent.load() / (sec > 0 ? sec : 1e-9) / 1000.0,
        r.passed ? "" : "  [COUNT MISMATCH]");
    r.detail = buf;
    return r;
}

Result scenarioReqRep(const Options& opt) {
    Result r; r.name = "reqrep";
    auto p = makeLocal("stress_rr");
    if (!p) { r.passed = false; r.detail = "participant create failed"; return r; }

    std::atomic<uint64_t> served{0};
    auto rep = p->createReplier("echo",
        [&](const embedmq::ReceivedMessage& req) -> embedmq::Payload {
            served.fetch_add(1, std::memory_order_relaxed);
            return req.payload;
        }, embedmq::QoSProfile::reliable());

    const int R = std::max(1, opt.threads);
    const long perThread = std::max<long>(1, std::min<long>(opt.messages, 20000) / R);
    std::atomic<uint64_t> ok{0}, fail{0};
    std::atomic<long long> totalLatNs{0};
    std::atomic<long long> maxLatNs{0};
    const std::string payload = makePayload(opt.payload);

    auto t0 = Clock::now();
    std::vector<std::thread> reqs;
    for (int t = 0; t < R; ++t) {
        reqs.emplace_back([&]() {
            auto requester = p->createRequester("echo", embedmq::QoSProfile::reliable());
            for (long i = 0; i < perThread; ++i) {
                auto a = Clock::now();
                auto res = requester->request(embedmq::Payload(std::string_view(payload)),
                                              std::chrono::milliseconds(2000));
                auto b = Clock::now();
                if (res.has_value() && res->size() == payload.size()) {
                    ok.fetch_add(1, std::memory_order_relaxed);
                    long long ns = std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count();
                    totalLatNs.fetch_add(ns, std::memory_order_relaxed);
                    long long prev = maxLatNs.load();
                    while (ns > prev && !maxLatNs.compare_exchange_weak(prev, ns)) {}
                } else {
                    fail.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& th : reqs) th.join();
    auto t1 = Clock::now();

    long total = perThread * R;
    r.passed = (fail.load() == 0 && ok.load() == static_cast<uint64_t>(total));
    double sec = nowSec(t0, t1);
    double avgUs = ok.load() ? (totalLatNs.load() / 1000.0 / ok.load()) : 0.0;
    char buf[320];
    std::snprintf(buf, sizeof(buf),
        "%d threads x %ld req = %ld, ok=%llu fail=%llu served=%llu in %.2fs => "
        "%.1f Kreq/s, avg=%.1fus max=%.1fus%s",
        R, perThread, total, (unsigned long long)ok.load(),
        (unsigned long long)fail.load(), (unsigned long long)served.load(), sec,
        total / (sec > 0 ? sec : 1e-9) / 1000.0, avgUs, maxLatNs.load() / 1000.0,
        r.passed ? "" : "  [REQUESTS LOST]");
    r.detail = buf;
    return r;
}

Result scenarioChurn(const Options& opt) {
    Result r; r.name = "churn";
    long iters = std::max<long>(1, std::min<long>(opt.messages, 2000));
    long rssBefore = currentRssKb();
    std::atomic<uint64_t> delivered{0};
    const std::string payload = makePayload(opt.payload);

    auto t0 = Clock::now();
    bool ok = true;
    for (long i = 0; i < iters && ok; ++i) {
        auto p = makeLocal("churn_" + std::to_string(i));
        if (!p) { ok = false; break; }
        uint64_t got = 0;
        {
            auto sub = p->createSubscriber("c/#",
                [&](const embedmq::ReceivedMessage&) { ++got; delivered.fetch_add(1); });
            auto pub = p->createPublisher("c/x");
            for (int k = 0; k < 8; ++k) pub->publish(std::string_view(payload));
            if (got != 8) ok = false;  // 同步投递必然命中
        }
        // p 在此析构：触发 shutdown，验证不崩溃/不死锁
    }
    auto t1 = Clock::now();
    long rssAfter = currentRssKb();
    r.passed = ok;
    double sec = nowSec(t0, t1);
    char buf[300];
    std::snprintf(buf, sizeof(buf),
        "%ld create/destroy cycles in %.2fs (%.0f cyc/s), delivered=%llu, "
        "RSS %ldKB->%ldKB (Δ%+ldKB)%s",
        iters, sec, iters / (sec > 0 ? sec : 1e-9),
        (unsigned long long)delivered.load(), rssBefore, rssAfter,
        rssAfter - rssBefore, ok ? "" : "  [LIFECYCLE FAILURE]");
    r.detail = buf;
    return r;
}

Result scenarioSoak(const Options& opt) {
    Result r; r.name = "soak";
    auto p = makeLocal("stress_soak");
    if (!p) { r.passed = false; r.detail = "participant create failed"; return r; }

    std::atomic<uint64_t> recv{0};
    std::atomic<bool> stop{false};
    const std::string payload = makePayload(opt.payload);

    // 常驻订阅者
    auto stableSub = p->createSubscriber("soak/#",
        [&](const embedmq::ReceivedMessage&) { recv.fetch_add(1, std::memory_order_relaxed); });

    // 发布线程
    std::atomic<uint64_t> sent{0};
    std::thread pubThread([&]() {
        auto pub = p->createPublisher("soak/data");
        while (!stop.load(std::memory_order_relaxed)) {
            for (int b = 0; b < 128; ++b) pub->publish(std::string_view(payload));
            sent.fetch_add(128, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    });

    // churn 线程：不断增删临时订阅者，制造路由表抖动
    std::thread churnThread([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            auto s = p->createSubscriber("soak/#",
                [&](const embedmq::ReceivedMessage&) { recv.fetch_add(1, std::memory_order_relaxed); });
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            // s 析构 → 移除订阅
        }
    });

    long rssStart = currentRssKb();
    uint64_t mid = 0;
    auto t0 = Clock::now();
    // 运行中途采样一次，确保计数在“增长”
    std::this_thread::sleep_for(std::chrono::milliseconds(opt.durationSec * 1000 / 2));
    mid = recv.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(opt.durationSec * 1000 / 2));
    stop.store(true);
    pubThread.join();
    churnThread.join();
    auto t1 = Clock::now();
    long rssEnd = currentRssKb();

    bool grew    = recv.load() > mid && mid > 0;
    bool alive   = p->isRunning();
    r.passed = grew && alive;
    double sec = nowSec(t0, t1);
    char buf[300];
    std::snprintf(buf, sizeof(buf),
        "%.1fs mixed load: sent=%llu recv=%llu (mid=%llu), running=%d, "
        "RSS %ldKB->%ldKB (Δ%+ldKB)%s",
        sec, (unsigned long long)sent.load(), (unsigned long long)recv.load(),
        (unsigned long long)mid, alive ? 1 : 0, rssStart, rssEnd, rssEnd - rssStart,
        r.passed ? "" : "  [SOAK FAILURE]");
    r.detail = buf;
    return r;
}

void printHelp() {
    std::printf(
        "emq_stress %s —— EmbedMQ 压力/稳定性测试\n\n"
        "用法: emq_stress <scenario> [options]\n\n"
        "场景: throughput | fanout | concurrent | reqrep | churn | soak | all\n\n"
        "选项:\n"
        "  -d, --duration <sec>   持续型场景时长(默认 3)\n"
        "  -n, --messages <N>     消息条数(默认 200000)\n"
        "  -t, --threads  <T>     生产者/请求者线程数(默认 4)\n"
        "  -s, --subs     <M>     订阅者数(默认 8)\n"
        "  -p, --payload  <bytes> 载荷字节数(默认 16)\n"
        "  -h, --help\n",
        EMQ_VERSION_STRING);
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) { printHelp(); return 1; }
    std::string scenario = argv[1];
    if (scenario == "-h" || scenario == "--help") { printHelp(); return 0; }

    Options opt;
    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&](int& dst) { if (i + 1 < argc) dst = std::atoi(argv[++i]); };
        auto nextL = [&](long& dst) { if (i + 1 < argc) dst = std::atol(argv[++i]); };
        if (a == "-d" || a == "--duration") next(opt.durationSec);
        else if (a == "-n" || a == "--messages") nextL(opt.messages);
        else if (a == "-t" || a == "--threads")  next(opt.threads);
        else if (a == "-s" || a == "--subs")      next(opt.subs);
        else if (a == "-p" || a == "--payload")   next(opt.payload);
        else if (a == "-h" || a == "--help") { printHelp(); return 0; }
    }
    if (opt.durationSec < 1) opt.durationSec = 1;

    std::printf("=== emq_stress (%s) duration=%ds messages=%ld threads=%d subs=%d payload=%dB ===\n",
                EMQ_VERSION_STRING, opt.durationSec, opt.messages, opt.threads,
                opt.subs, opt.payload);

    std::vector<Result> results;
    auto runOne = [&](const std::string& s) -> Result {
        if (s == "throughput") return scenarioThroughput(opt);
        if (s == "fanout")     return scenarioFanout(opt);
        if (s == "concurrent") return scenarioConcurrent(opt);
        if (s == "reqrep")     return scenarioReqRep(opt);
        if (s == "churn")      return scenarioChurn(opt);
        if (s == "soak")       return scenarioSoak(opt);
        Result r; r.name = s; r.passed = false; r.detail = "unknown scenario"; return r;
    };

    if (scenario == "all") {
        // 自测：用较短参数跑全部场景
        Options shortOpt = opt;
        if (shortOpt.messages > 200000) shortOpt.messages = 200000;
        Options saved = opt;
        opt = shortOpt;
        for (auto* s : {"throughput", "fanout", "concurrent", "reqrep", "churn", "soak"}) {
            Result r = runOne(s);
            report(r);
            results.push_back(r);
        }
        opt = saved;
    } else {
        Result r = runOne(scenario);
        report(r);
        results.push_back(r);
    }

    int failed = 0;
    for (auto& r : results) if (!r.passed) ++failed;
    std::printf("\n=== %zu scenario(s): %d passed, %d failed ===\n",
                results.size(), (int)results.size() - failed, failed);
    return failed == 0 ? 0 : 1;
}
