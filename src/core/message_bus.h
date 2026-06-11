#pragma once
#include "embedmq/embedmq.h"
#include "topic_router.h"
#include "qos_engine.h"
#include "message_codec.h"
#include "retained_store.h"
#include "../util/timer_wheel.h"
#include <atomic>
#include <chrono>
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

    // 遗嘱消息（Last Will）：节点异常掉线时由发现该掉线的对端代为发布
    bool        hasWill{false};
    std::string willTopic;
    Payload     willPayload;
    bool        willRetain{false};
    uint8_t     willQos{0};
};

/// 消息总线 —— 中间件核心
class MessageBus {
public:
    explicit MessageBus(uint16_t nodeId, TransportManager* tm);
    ~MessageBus();

    void start(int cpuAffinity = -1);
    void stop();

    // 线缆 CRC 开关（默认开启）。可信链路/高频小包可关闭以省 CPU。
    void setChecksumEnabled(bool on) { crcEnabled_ = on; }

    // 保留消息约束：默认生存期（ms，0=永不过期）与条目数上限（0=不限制）。
    // 过期清理挂载在 start() 的周期任务上。
    void setRetainedLimits(uint32_t ttlMs, uint32_t maxCount) {
        retainedStore_.configure(ttlMs, maxCount);
    }

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
    void onPeerUpdated(const PeerInfo& peer);
    void onPeerLost(uint16_t peerId);

    // ---- 遗嘱消息：由 DiscoveryAgent 在检测到对端异常掉线时调用 ----
    // 将掉线对端的遗嘱消息投递到本地订阅者（并按需存为保留消息）
    void deliverWill(const std::string& topic, const Payload& payload,
                     bool retain, uint16_t sourceId);

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

    // 本地订阅/服务集合变化时回调（创建与销毁都会触发），由 Participant 接到
    // discovery->announceTopics，使对端及时获知本地不再持有的 topic/service。
    void setTopicsChangedCallback(std::function<void()> cb);

    // corrCounter_ 供 Requester 使用（通过 sendRequest 访问）
    std::atomic<uint32_t> corrCounter_{1};

private:
    void sendAck(const Endpoint& to, uint32_t seqId);
    void sendNack(const Endpoint& to, uint32_t seqId);
    // QoS2 两阶段握手控制包
    void sendCtrl(const Endpoint& to, MessageType type, uint32_t seqId);
    void sendReply(const Endpoint& to, uint32_t correlationId,
                   const Payload& payload, const QoSProfile& qos);

    // 在对端宣布的多个端点中按本地可用传输与能力优选数据面：
    // SHM（仅同主机）> TCP > UDP。返回选定端点（无可用时返回首个/空端点）。
    Endpoint selectEndpoint(const PeerInfo& peer) const;

    uint16_t nodeId_;
    TransportManager* transportMgr_;
    std::string       localHostName_;

    TopicRouter    router_;
    QoSEngine      qosEngine_;
    RetainedStore  retainedStore_;
    util::TimerWheel timerWheel_;

    std::atomic<uint32_t> seqCounter_{1};
    std::atomic<bool>     crcEnabled_{true};

    // 等待中的 request：correlationId -> (promise + 截止时间)
    // 注意：请求用 correlationId 索引，与发布可靠消息用的 seqId 是不同命名空间，
    // 二者不可混用（历史 bug）。deadline 用于在无响应时结束 future，避免永久挂起。
    struct PendingRequest {
        std::promise<Payload>                 promise;
        std::chrono::steady_clock::time_point deadline;
    };
    std::mutex pendingReqMutex_;
    std::unordered_map<uint32_t, PendingRequest> pendingRequests_;

    // service handler 注册
    mutable std::mutex serviceMutex_;
    std::unordered_map<std::string, RequestHandler> serviceHandlers_;

    // 本地 topic/service 集合变化通知（见 setTopicsChangedCallback）
    std::mutex             topicsCbMutex_;
    std::function<void()>  topicsChangedCb_;
    void notifyTopicsChanged();

    // 对端信息 -> endpoint 映射
    std::mutex peerMutex_;
    std::unordered_map<uint16_t, PeerInfo> peers_;

    std::atomic<bool> running_{false};
    util::TimerId retryTimerId_{0};
};

} // namespace embedmq
