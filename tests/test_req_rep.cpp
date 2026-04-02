#define EMQ_TEST_MODULE "req_rep"
#include "test_framework.h"
#include "embedmq/embedmq.h"
#include <atomic>
#include <chrono>
#include <thread>

using namespace embedmq;

TEST(local_req_rep_basic) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);

    // 创建 Replier
    auto rep = p->createReplier("echo",
        [](const ReceivedMessage& req) -> Payload {
            std::string s = "echo:" + std::string(req.payload.asText());
            return Payload(std::string_view(s));
        });

    // 创建 Requester
    auto req = p->createRequester("echo");

    auto result = req->request(Payload("hello"), std::chrono::milliseconds(1000));
    CHECK_TRUE(result.has_value());
    CHECK_STR_EQ(std::string(result->asText()), "echo:hello");

    p->shutdown();
}

TEST(req_rep_multiple_requests) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);

    std::atomic<int> callCount{0};
    auto rep = p->createReplier("add1",
        [&](const ReceivedMessage& req) -> Payload {
            callCount++;
            std::string s = req.payload.asText().empty() ? "0" : std::string(req.payload.asText());
            int val = std::stoi(s) + 1;
            std::string result = std::to_string(val);
            return Payload(std::string_view(result));
        });

    auto requester = p->createRequester("add1");

    for (int i = 0; i < 3; i++) {
        std::string input = std::to_string(i);
        auto res = requester->request(Payload(std::string_view(input)),
                                       std::chrono::milliseconds(1000));
        CHECK_TRUE(res.has_value());
        std::string expected = std::to_string(i + 1);
        CHECK_STR_EQ(std::string(res->asText()), expected);
    }

    CHECK_EQ(callCount.load(), 3);
    p->shutdown();
}

TEST(req_rep_request_count) {
    ParticipantConfig cfg;
    cfg.transport.enableUdp       = false;
    cfg.discovery.enableMulticast = false;

    auto p = Participant::create(cfg);
    auto rep = p->createReplier("svc",
        [](const ReceivedMessage&) -> Payload { return Payload("ok"); });
    auto req = p->createRequester("svc");

    req->request(Payload("1"), std::chrono::milliseconds(500));
    req->request(Payload("2"), std::chrono::milliseconds(500));

    CHECK_EQ(rep->requestCount(), 2u);

    p->shutdown();
}
