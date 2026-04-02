/**
 * EmbedMQ 请求-响应示例
 *
 * 演示同进程内的 Req/Rep 通信：
 *   - Replier 注册 "calc" 服务，接收表达式并返回结果
 *   - Requester 发送请求并等待响应
 *
 * 运行: xmake run example_req_rep
 */

#include "embedmq/embedmq.h"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

int main() {
    std::printf("=== EmbedMQ Req/Rep Example ===\n");

    embedmq::ParticipantConfig cfg;
    cfg.nodeName                  = "calc_node";
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto participant = embedmq::Participant::create(cfg);
    std::printf("[Node] Started: %s (id=%u)\n",
                participant->name().c_str(), participant->id());

    // 注册计算服务
    auto replier = participant->createReplier(
        "multiply",
        [](const embedmq::ReceivedMessage& req) -> embedmq::Payload {
            std::string input = std::string(req.payload.asText());
            // 格式: "A B"
            int a = 0, b = 0;
            if (std::sscanf(input.c_str(), "%d %d", &a, &b) == 2) {
                std::string result = std::to_string(a * b);
                std::printf("  [Replier] %d * %d = %s\n", a, b, result.c_str());
                return embedmq::Payload(std::string_view(result));
            }
            return embedmq::Payload("ERROR");
        },
        embedmq::QoSProfile::reliable());

    // 发送请求
    auto requester = participant->createRequester(
        "multiply",
        embedmq::QoSProfile::reliable());

    struct TestCase { int a; int b; };
    TestCase tests[] = {{3, 7}, {12, 5}, {100, 0}, {-2, 8}};

    for (auto& tc : tests) {
        std::string query = std::to_string(tc.a) + " " + std::to_string(tc.b);
        std::printf("[Requester] %d * %d = ?", tc.a, tc.b);
        std::fflush(stdout);

        auto result = requester->request(
            embedmq::Payload(std::string_view(query)),
            std::chrono::milliseconds(2000));

        if (result.has_value()) {
            std::printf("  -> %s\n", std::string(result->asText()).c_str());
        } else {
            std::printf("  -> TIMEOUT\n");
        }
    }

    std::printf("\n[Stats] Requests handled: %llu\n",
                static_cast<unsigned long long>(replier->requestCount()));

    participant->shutdown();
    std::printf("=== Done ===\n");
    return 0;
}
