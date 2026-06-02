/**
 * emqtop —— EmbedMQ 命令行监控 / 诊断工具（Phase 5）
 *
 * 作为一个普通参与者加入网络，用于实时观察总线流量、对端拓扑，
 * 以及手动收发消息进行联调。类似 `mosquitto_sub/pub` 的角色。
 *
 * 子命令：
 *   emqtop monitor [topic]   订阅 topic（默认 "#" 全量），实时打印消息，
 *                            并定期刷新对端列表与速率统计。
 *   emqtop sub <topic>       仅订阅并打印消息（monitor 的精简版）。
 *   emqtop pub <topic> <msg> [-n N] [-i ms]
 *                            向 topic 发布消息；可重复 N 次、间隔 i 毫秒。
 *   emqtop req <service> <msg> [-t ms]
 *                            向 service 发送请求并打印响应。
 *   emqtop echo <service>    注册一个 echo 服务（回显请求载荷），便于测试 req。
 *   emqtop peers             列出当前发现的对端后退出（等待数秒发现）。
 *
 * 通用选项：
 *   --name <n>     指定节点名（默认 emqtop-<pid>）
 *   --domain <d>   通信域（默认 0）
 *   --no-udp       禁用 UDP（仅本机/SHM 场景）
 *   --shm          启用共享内存传输
 *   -h, --help     显示帮助
 */
#include "embedmq/embedmq.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
#  include <process.h>
#else
#  include <unistd.h>
#endif

namespace {

std::atomic<bool> g_running{true};

void onSignal(int) { g_running.store(false); }

struct Options {
    std::string name;
    int         domain   = 0;
    bool        enableUdp = true;
    bool        enableShm = false;
    bool        enableMulticast = true;
};

void printHelp() {
    std::printf(
        "emqtop %s —— EmbedMQ 监控/诊断工具\n\n"
        "用法: emqtop <command> [args] [options]\n\n"
        "命令:\n"
        "  monitor [topic]            订阅 topic(默认 #)，实时打印消息 + 拓扑统计\n"
        "  sub <topic>                仅订阅并打印消息\n"
        "  pub <topic> <msg> [-n N] [-i ms]\n"
        "                             发布消息，可重复 N 次、间隔 i 毫秒\n"
        "  req <service> <msg> [-t ms]  发送请求并打印响应(默认超时 5000ms)\n"
        "  echo <service>             注册回显服务，便于测试 req\n"
        "  peers                      等待发现后列出对端\n\n"
        "选项:\n"
        "  --name <n>     节点名(默认 emqtop-<pid>)\n"
        "  --domain <d>   通信域(默认 0)\n"
        "  --no-udp       禁用 UDP\n"
        "  --shm          启用共享内存传输\n"
        "  -h, --help     显示本帮助\n",
        EMQ_VERSION_STRING);
}

// 把任意二进制载荷以可读形式打印：可打印则原样，否则转十六进制
void printPayload(const embedmq::Payload& p) {
    const uint8_t* d = p.data();
    size_t n = p.size();
    bool printable = true;
    for (size_t i = 0; i < n; ++i) {
        if (d[i] < 0x09 || (d[i] > 0x0d && d[i] < 0x20) || d[i] == 0x7f) {
            printable = false;
            break;
        }
    }
    if (printable) {
        std::fwrite(d, 1, n, stdout);
    } else {
        std::printf("<bin %zu B: ", n);
        size_t show = n < 32 ? n : 32;
        for (size_t i = 0; i < show; ++i) std::printf("%02x", d[i]);
        if (show < n) std::printf("...");
        std::printf(">");
    }
}

std::unique_ptr<embedmq::Participant> makeParticipant(const Options& opt) {
    embedmq::ParticipantConfig cfg;
    cfg.nodeName                  = opt.name;
    cfg.domainId                  = static_cast<uint8_t>(opt.domain);
    cfg.transport.enableUdp       = opt.enableUdp;
    cfg.transport.enableShm       = opt.enableShm;
    cfg.discovery.enableMulticast = opt.enableMulticast;
    return embedmq::Participant::create(cfg);
}

std::string timeStamp() {
    using namespace std::chrono;
    auto now = system_clock::now();
    auto t   = system_clock::to_time_t(now);
    auto ms  = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
    char buf[32];
    std::tm tmv{};
#if defined(_WIN32)
    localtime_s(&tmv, &t);
#else
    localtime_r(&t, &tmv);
#endif
    std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d",
                  tmv.tm_hour, tmv.tm_min, tmv.tm_sec, static_cast<int>(ms));
    return buf;
}

// ---- 子命令实现 ----

int cmdMonitor(const Options& opt, const std::string& topic, bool withStats) {
    auto p = makeParticipant(opt);
    if (!p) { std::fprintf(stderr, "创建参与者失败\n"); return 1; }

    std::atomic<uint64_t> total{0};
    auto sub = p->createSubscriber(topic,
        [&](const embedmq::ReceivedMessage& msg) {
            total.fetch_add(1, std::memory_order_relaxed);
            std::printf("[%s] src=%-5u seq=%-6u %s | ",
                        timeStamp().c_str(), msg.sourceId, msg.sequenceId,
                        msg.topic.c_str());
            printPayload(msg.payload);
            std::printf("\n");
            std::fflush(stdout);
        });

    std::printf("emqtop 监听中: node=%s id=%u topic=\"%s\"  (Ctrl-C 退出)\n",
                p->name().c_str(), p->id(), topic.c_str());

    uint64_t lastCount = 0;
    auto     lastTick  = std::chrono::steady_clock::now();
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (!withStats) continue;
        auto now = std::chrono::steady_clock::now();
        double dt = std::chrono::duration<double>(now - lastTick).count();
        if (dt < 2.0) continue;
        uint64_t cur  = total.load();
        double   rate = (cur - lastCount) / dt;
        auto peers = p->discoveredPeers();
        std::printf("---- [stats] msgs=%llu (%.1f msg/s) | peers(%zu):",
                    static_cast<unsigned long long>(cur), rate, peers.size());
        for (auto& n : peers) std::printf(" %s", n.c_str());
        std::printf(" ----\n");
        std::fflush(stdout);
        lastCount = cur;
        lastTick  = now;
    }
    std::printf("\n总计收到 %llu 条消息\n",
                static_cast<unsigned long long>(total.load()));
    return 0;
}

int cmdPub(const Options& opt, const std::string& topic, const std::string& msg,
           int count, int intervalMs) {
    auto p = makeParticipant(opt);
    if (!p) { std::fprintf(stderr, "创建参与者失败\n"); return 1; }
    auto pub = p->createPublisher(topic);
    // 给发现一点时间，避免首包还没建立路由
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    int sent = 0;
    for (int i = 0; i < count && g_running.load(); ++i) {
        if (pub->publish(std::string_view(msg))) ++sent;
        if (i + 1 < count) std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    std::printf("已发布 %d/%d 条到 \"%s\"（匹配订阅者: %zu）\n",
                sent, count, topic.c_str(), pub->subscriberCount());
    return sent == count ? 0 : 2;
}

int cmdReq(const Options& opt, const std::string& service, const std::string& msg,
           int timeoutMs) {
    auto p = makeParticipant(opt);
    if (!p) { std::fprintf(stderr, "创建参与者失败\n"); return 1; }
    auto req = p->createRequester(service, embedmq::QoSProfile::reliable());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    auto result = req->request(embedmq::Payload(std::string_view(msg)),
                               std::chrono::milliseconds(timeoutMs));
    if (result.has_value()) {
        std::printf("响应: ");
        printPayload(*result);
        std::printf("\n");
        return 0;
    }
    std::fprintf(stderr, "请求超时(%dms)\n", timeoutMs);
    return 2;
}

int cmdEcho(const Options& opt, const std::string& service) {
    auto p = makeParticipant(opt);
    if (!p) { std::fprintf(stderr, "创建参与者失败\n"); return 1; }
    std::atomic<uint64_t> handled{0};
    auto rep = p->createReplier(service,
        [&](const embedmq::ReceivedMessage& req) -> embedmq::Payload {
            handled.fetch_add(1, std::memory_order_relaxed);
            std::printf("[%s] echo <- ", timeStamp().c_str());
            printPayload(req.payload);
            std::printf("\n");
            std::fflush(stdout);
            return req.payload;
        },
        embedmq::QoSProfile::reliable());
    std::printf("emqtop echo 服务就绪: node=%s service=\"%s\" (Ctrl-C 退出)\n",
                p->name().c_str(), service.c_str());
    while (g_running.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::printf("\n共处理 %llu 个请求\n",
                static_cast<unsigned long long>(handled.load()));
    return 0;
}

int cmdPeers(const Options& opt) {
    auto p = makeParticipant(opt);
    if (!p) { std::fprintf(stderr, "创建参与者失败\n"); return 1; }
    std::printf("等待发现对端(约 3s)...\n");
    for (int i = 0; i < 6 && g_running.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    auto peers = p->discoveredPeers();
    std::printf("本节点: %s (id=%u)\n", p->name().c_str(), p->id());
    std::printf("发现对端 %zu 个:\n", peers.size());
    for (auto& n : peers) std::printf("  - %s\n", n.c_str());
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    if (argc < 2) { printHelp(); return 1; }

    std::string cmd = argv[1];
    if (cmd == "-h" || cmd == "--help") { printHelp(); return 0; }

    Options opt;
    opt.name = "emqtop-" + std::to_string(
#if defined(_WIN32)
        static_cast<int>(_getpid())
#else
        static_cast<int>(getpid())
#endif
    );

    // 位置参数与选项分离
    std::vector<std::string> pos;
    int    pubCount   = 1;
    int    pubIntv    = 1000;
    int    reqTimeout = 5000;
    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--name" && i + 1 < argc)         opt.name = argv[++i];
        else if (a == "--domain" && i + 1 < argc)  opt.domain = std::atoi(argv[++i]);
        else if (a == "--no-udp")                  opt.enableUdp = false;
        else if (a == "--shm")                     opt.enableShm = true;
        else if (a == "-n" && i + 1 < argc)        pubCount = std::atoi(argv[++i]);
        else if (a == "-i" && i + 1 < argc)        pubIntv = std::atoi(argv[++i]);
        else if (a == "-t" && i + 1 < argc)        reqTimeout = std::atoi(argv[++i]);
        else if (a == "-h" || a == "--help")       { printHelp(); return 0; }
        else                                       pos.push_back(a);
    }

    if (cmd == "monitor") {
        std::string topic = pos.empty() ? "#" : pos[0];
        return cmdMonitor(opt, topic, /*withStats=*/true);
    }
    if (cmd == "sub") {
        if (pos.empty()) { std::fprintf(stderr, "用法: emqtop sub <topic>\n"); return 1; }
        return cmdMonitor(opt, pos[0], /*withStats=*/false);
    }
    if (cmd == "pub") {
        if (pos.size() < 2) { std::fprintf(stderr, "用法: emqtop pub <topic> <msg> [-n N] [-i ms]\n"); return 1; }
        if (pubCount < 1) pubCount = 1;
        if (pubIntv < 0)  pubIntv = 0;
        return cmdPub(opt, pos[0], pos[1], pubCount, pubIntv);
    }
    if (cmd == "req") {
        if (pos.size() < 2) { std::fprintf(stderr, "用法: emqtop req <service> <msg> [-t ms]\n"); return 1; }
        return cmdReq(opt, pos[0], pos[1], reqTimeout);
    }
    if (cmd == "echo") {
        if (pos.empty()) { std::fprintf(stderr, "用法: emqtop echo <service>\n"); return 1; }
        return cmdEcho(opt, pos[0]);
    }
    if (cmd == "peers") {
        return cmdPeers(opt);
    }

    std::fprintf(stderr, "未知命令: %s\n\n", cmd.c_str());
    printHelp();
    return 1;
}
