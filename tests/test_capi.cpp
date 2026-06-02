// 模块: capi —— C ABI 包装层（Phase 5）
//
// 直接调用纯 C 接口，验证句柄生命周期、Pub/Sub、Req/Rep、对端查询、
// 错误码与 NULL 句柄健壮性等。
#define EMQ_TEST_MODULE "capi"
#include "test_framework.h"

#include "embedmq/embedmq_c.h"

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

namespace {

void sleepMs(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// 本地（同进程）参与者：禁用网络与多播以快速启动
emq_participant* makeLocal(const char* name) {
    return emq_participant_create_ex(name, 0,
                                     /*udp=*/0, /*shm=*/0, /*multicast=*/0);
}

struct SubCtx {
    std::atomic<int> count{0};
    std::string      lastTopic;
    std::string      lastPayload;
    uint16_t         lastSource{0};
};

void onSub(const emq_message* msg, void* ud) {
    auto* ctx = static_cast<SubCtx*>(ud);
    ctx->lastTopic.assign(msg->topic ? msg->topic : "");
    ctx->lastPayload.assign(reinterpret_cast<const char*>(msg->payload),
                            msg->payload_len);
    ctx->lastSource = msg->source_id;
    ctx->count.fetch_add(1, std::memory_order_relaxed);
}

// echo replier：把请求载荷原样返回，并统计调用次数
void onEcho(const emq_message* req, uint8_t** out, size_t* outLen, void* ud) {
    auto* calls = static_cast<std::atomic<int>*>(ud);
    calls->fetch_add(1, std::memory_order_relaxed);
    uint8_t* buf = emq_alloc(req->payload_len);
    if (req->payload_len) std::memcpy(buf, req->payload, req->payload_len);
    *out = buf;
    *outLen = req->payload_len;
}

} // namespace


TEST(capi_version_and_status) {
    CHECK(emq_version() != nullptr);
    CHECK(std::strlen(emq_version()) > 0);
    CHECK_STR_EQ(emq_status_str(EMQ_OK), "ok");
    CHECK_STR_EQ(emq_status_str(EMQ_ERR_TIMEOUT), "timeout");
    CHECK(std::strlen(emq_status_str(-999)) > 0); // 未知码也返回非空
}

TEST(capi_null_handle_safety) {
    // NULL 句柄不得崩溃，返回错误码 / 安全默认值
    CHECK_EQ(emq_participant_id(nullptr), 0);
    CHECK_STR_EQ(emq_participant_name(nullptr), "");
    CHECK_EQ(emq_participant_is_running(nullptr), 0);
    CHECK_EQ(emq_participant_peer_count(nullptr), EMQ_ERR_INVALID_ARG);
    CHECK_EQ(emq_publisher_publish(nullptr, "x", 1), EMQ_ERR_INVALID_ARG);
    CHECK_EQ(emq_subscriber_message_count(nullptr), 0u);
    CHECK_EQ(emq_replier_request_count(nullptr), 0u);
    // destroy(NULL) 安全
    emq_participant_destroy(nullptr);
    emq_publisher_destroy(nullptr);
    emq_subscriber_destroy(nullptr);
    emq_requester_destroy(nullptr);
    emq_replier_destroy(nullptr);
    CHECK(emq_publisher_create(nullptr, "t", EMQ_QOS_BEST_EFFORT) == nullptr);
    CHECK(emq_subscriber_create(nullptr, "t", EMQ_QOS_BEST_EFFORT, onSub, nullptr) == nullptr);
}

TEST(capi_participant_lifecycle) {
    emq_participant* p = makeLocal("capi_node");
    CHECK(p != nullptr);
    CHECK(emq_participant_id(p) != 0);
    CHECK_STR_EQ(emq_participant_name(p), "capi_node");
    CHECK_EQ(emq_participant_is_running(p), 1);
    CHECK_EQ(emq_participant_peer_count(p), 0); // 本地无对端
    emq_participant_shutdown(p);
    CHECK_EQ(emq_participant_is_running(p), 0);
    emq_participant_destroy(p);
}

TEST(capi_pub_sub_roundtrip) {
    emq_participant* p = makeLocal("capi_ps");
    CHECK(p != nullptr);

    SubCtx ctx;
    emq_subscriber* sub = emq_subscriber_create(p, "sensor/#",
                                                EMQ_QOS_BEST_EFFORT, onSub, &ctx);
    CHECK(sub != nullptr);

    emq_publisher* pub = emq_publisher_create(p, "sensor/temp", EMQ_QOS_BEST_EFFORT);
    CHECK(pub != nullptr);

    for (int i = 0; i < 5; ++i) {
        CHECK_EQ(emq_publisher_publish_str(pub, "hello"), EMQ_OK);
    }
    sleepMs(200);

    CHECK_EQ(ctx.count.load(), 5);
    CHECK_STR_EQ(ctx.lastTopic, "sensor/temp");
    CHECK_STR_EQ(ctx.lastPayload, "hello");
    CHECK_EQ(emq_subscriber_message_count(sub), 5u);
    CHECK_EQ(ctx.lastSource, emq_participant_id(p));

    emq_publisher_destroy(pub);
    emq_subscriber_destroy(sub);
    emq_participant_destroy(p);
}

TEST(capi_pub_sub_binary) {
    emq_participant* p = makeLocal("capi_bin");
    SubCtx ctx;
    emq_subscriber* sub = emq_subscriber_create(p, "raw",
                                                EMQ_QOS_BEST_EFFORT, onSub, &ctx);
    emq_publisher* pub = emq_publisher_create(p, "raw", EMQ_QOS_BEST_EFFORT);

    std::vector<uint8_t> blob(256);
    for (int i = 0; i < 256; ++i) blob[i] = static_cast<uint8_t>(i);
    CHECK_EQ(emq_publisher_publish(pub, blob.data(), blob.size()), EMQ_OK);
    sleepMs(150);

    CHECK_EQ(ctx.count.load(), 1);
    CHECK_EQ(ctx.lastPayload.size(), 256u);
    bool same = ctx.lastPayload.size() == 256;
    for (size_t i = 0; same && i < 256; ++i)
        same = (static_cast<uint8_t>(ctx.lastPayload[i]) == blob[i]);
    CHECK_TRUE(same);

    emq_publisher_destroy(pub);
    emq_subscriber_destroy(sub);
    emq_participant_destroy(p);
}

TEST(capi_subscriber_pause_resume) {
    emq_participant* p = makeLocal("capi_pause");
    SubCtx ctx;
    emq_subscriber* sub = emq_subscriber_create(p, "x",
                                                EMQ_QOS_BEST_EFFORT, onSub, &ctx);
    emq_publisher* pub = emq_publisher_create(p, "x", EMQ_QOS_BEST_EFFORT);

    CHECK_EQ(emq_subscriber_pause(sub), EMQ_OK);
    emq_publisher_publish_str(pub, "a");
    sleepMs(100);
    CHECK_EQ(ctx.count.load(), 0); // 暂停期间不投递

    CHECK_EQ(emq_subscriber_resume(sub), EMQ_OK);
    emq_publisher_publish_str(pub, "b");
    sleepMs(100);
    CHECK_EQ(ctx.count.load(), 1);

    emq_publisher_destroy(pub);
    emq_subscriber_destroy(sub);
    emq_participant_destroy(p);
}

TEST(capi_req_rep_roundtrip) {
    emq_participant* p = makeLocal("capi_rr");
    std::atomic<int> calls{0};

    emq_replier* rep = emq_replier_create(p, "echo", EMQ_QOS_RELIABLE,
                                          onEcho, &calls);
    CHECK(rep != nullptr);
    emq_requester* req = emq_requester_create(p, "echo", EMQ_QOS_RELIABLE);
    CHECK(req != nullptr);

    uint8_t* out = nullptr;
    size_t   outLen = 0;
    int rc = emq_requester_request(req, "ping", 4, 2000, &out, &outLen);
    CHECK_EQ(rc, EMQ_OK);
    CHECK_EQ(outLen, 4u);
    CHECK(out != nullptr);
    if (out) {
        CHECK_TRUE(std::memcmp(out, "ping", 4) == 0);
        emq_free(out);
    }
    CHECK_EQ(emq_replier_request_count(rep), 1u);
    CHECK_EQ(calls.load(), 1);

    emq_requester_destroy(req);
    emq_replier_destroy(rep);
    emq_participant_destroy(p);
}

TEST(capi_req_timeout_no_provider) {
    emq_participant* p = makeLocal("capi_to");
    emq_requester* req = emq_requester_create(p, "nobody", EMQ_QOS_RELIABLE);
    CHECK(req != nullptr);

    uint8_t* out = reinterpret_cast<uint8_t*>(0x1); // 故意非空，验证被清零
    size_t   outLen = 99;
    int rc = emq_requester_request(req, "x", 1, 200, &out, &outLen);
    CHECK_EQ(rc, EMQ_ERR_TIMEOUT);
    CHECK(out == nullptr);   // 超时不应分配，且应清空出参
    CHECK_EQ(outLen, 0u);

    emq_requester_destroy(req);
    emq_participant_destroy(p);
}

TEST(capi_publisher_subscriber_count) {
    // 注：核心层 subscriberCount() 当前统计远端匹配订阅者（本地订阅不计入），
    // 这里验证 C ABI 忠实转发该值且不返回错误码（>=0）。
    emq_participant* p = makeLocal("capi_cnt");
    emq_publisher* pub = emq_publisher_create(p, "topic/a", EMQ_QOS_BEST_EFFORT);
    CHECK_GE(emq_publisher_subscriber_count(pub), 0);

    emq_subscriber* s1 = emq_subscriber_create(p, "topic/a",
                                               EMQ_QOS_BEST_EFFORT, onSub, nullptr);
    sleepMs(50);
    CHECK_GE(emq_publisher_subscriber_count(pub), 0);

    emq_subscriber_destroy(s1);
    emq_publisher_destroy(pub);
    emq_participant_destroy(p);
}
