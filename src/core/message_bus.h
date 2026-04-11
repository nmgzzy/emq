#pragma once
#include "embedmq/embedmq.h"
#include "topic_router.h"
#include "qos_engine.h"
#include "message_codec.h"
#include "retained_store.h"
#include "../util/timer_wheel.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <future>

namespace embedmq {

class ITransport;
class TransportManager;

struct PeerInfo {
    uint16_t    id;
    std::string name;
    std::string hostName;
    std::vector<Endpoint> endpoints;
    std::vector<std::string> publishedTopics;
    std::vector<std::string> subscribedTopics;
};

/// 消息总线 —— 中间件核心
class MessageBus {
public:
    explicit MessageBus(uint16_t nodeId, TransportManager* tm);
    ~MessageBus();

    void start();
    void stop();

    // ---- Pub/Sub ----
    std::unique_ptr<Publisher>  createPublisher(const std::string& topic,
                                                const QoSProfile& qos);
    std::unique_ptr<Subscriber> createSubscriber(const std::string& topic,
                                                  SubscribeCallback cb,
                                                  const QoSProfile& qos);

    // ---- Req/Rep ----
    std::unique_ptr<Requester> createRequester(const std::string& service,
                                               const QoSProfile& qos);
    std::unique_ptr<Replier>   createReplier(const std::string& service,
                                              RequestHandler handler,
                                              const QoSProfile& qos);

    // ---- Serializer ----
    void registerSerializer(std::shared_ptr<ISerializer> s);

    // ---- Discovery 回调 ----
    void onPeerDiscovered(const PeerInfo& peer);
    void onPeerLost(uint16_t peerId);

    // ---- 内部：传输层收到数据时调用 ----
    void onReceived(const Endpoint& from, const uint8_t* data, size_t size);

    // ---- 内部：发布到本地 + 远端 ----
    bool publish(const std::string& topic,
                 const Payload& payload,
                 const QoSProfile& qos,
                 uint8_t flags = 0);

    // ---- 内部：发起请求 ----
    std::future<Payload> sendRequest(const std::string& service,
                                      const Payload& payload,
                                      const QoSProfile& qos,
                                      uint32_t corrId);

    void cancelPendingRequest(uint32_t corrId);

    uint16_t nodeId() const { return nodeId_; }

    // ---- 状态 ----
    std::vector<std::string> localTopics() const;

    // ---- 内部辅助（供 Subscriber/Replier 析构使用）----
    void removeLocalSubscription(uint64_t subId);
    void removeServiceHandler(const std::string& service);

    // corrCounter_ 供 Requester 使用（通过 sendRequest 访问）
    std::atomic<uint32_t> corrCounter_{1};

private:
    void sendAck(const Endpoint& to, uint32_t seqId);
    void sendNack(const Endpoint& to, uint32_t seqId);
    void sendReply(const Endpoint& to, uint32_t correlationId,
                   const Payload& payload, const QoSProfile& qos);

    uint16_t nodeId_;
    TransportManager* transportMgr_;

    TopicRouter    router_;
    QoSEngine      qosEngine_;
    RetainedStore  retainedStore_;
    util::TimerWheel timerWheel_;

    std::atomic<uint32_t> seqCounter_{1};

    // 等待中的 request：correlationId -> promise
    std::mutex pendingReqMutex_;
    std::unordered_map<uint32_t, std::promise<Payload>> pendingRequests_;

    // service handler 注册
    mutable std::mutex serviceMutex_;
    std::unordered_map<std::string, RequestHandler> serviceHandlers_;

    // 对端信息 -> endpoint 映射
    std::mutex peerMutex_;
    std::unordered_map<uint16_t, PeerInfo> peers_;

    std::atomic<bool> running_{false};
    util::TimerId retryTimerId_{0};
};

} // namespace embedmq
