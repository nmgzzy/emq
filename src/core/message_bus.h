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
    // timeoutOverride>0 时作为该请求的内部截止时间（供 request(payload,timeout)
    // 透传调用方超时）；为 0 时回退到按 QoS 推导（ackTimeoutMs*(maxRetries+1)）。
    std::future<Payload> sendRequest(const std::string& service,
                                      const Payload& payload,
                                      const QoSProfile& qos,
                                      uint32_t corrId,
                                      std::chrono::milliseconds timeoutOverride =
                                          std::chrono::milliseconds(0));

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
    // registerRetransmit=true 时（首次应答）对可靠请求登记 REPLY 重传至收到 ACK；
    // false 时仅单发一次（用于对重复 REQUEST 重发已缓存的应答，不再叠加重传项）。
    void sendReply(const Endpoint& to, uint32_t correlationId,
                   const Payload& payload, const QoSProfile& qos,
                   bool registerRetransmit = true);

    // 在对端宣布的多个端点中按本地可用传输与能力优选数据面：
    // SHM（仅同主机）> TCP > UDP。返回选定端点（无可用时返回首个/空端点）。
    Endpoint selectEndpoint(const PeerInfo& peer) const;

    // 解析当前可达的服务提供方并发送一次 REQUEST；返回是否至少发往一个对端。
    // 每次调用都重新读取 peers_ 解析路由，供首发与周期重传共用。
    bool dispatchRequest(const std::string& service, const Payload& payload,
                         const QoSProfile& qos, uint32_t corrId);

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
        // —— 远端请求重传上下文（本地服务直接应答，不登记 pending，下列字段不用）——
        // 提供方的 $SVC 通告可能晚于请求发出，故保留载荷在截止时间内反复重传，
        // 每次重传都重新解析路由，使提供方一旦可达即可送达（best-effort 单发会丢）。
        bool                                  remote{false};
        std::string                           service;
        Payload                               payload;
        QoSProfile                            qos;
        uint32_t                              corrId{0};
        std::chrono::steady_clock::time_point nextRetry;
        std::chrono::milliseconds             retryInterval{0};
    };
    std::mutex pendingReqMutex_;
    std::unordered_map<uint32_t, PendingRequest> pendingRequests_;

    // service handler 注册
    mutable std::mutex serviceMutex_;
    std::unordered_map<std::string, RequestHandler> serviceHandlers_;

    // 请求去重 / 应答缓存（replier 侧）：requester 在截止时间内会重传同一 REQUEST
    // （携带相同的 correlationId，但每次 seqId 不同），若每次都执行 handler 会导致
    // 重复副作用。按 (sourceId, correlationId) 缓存首个 REQUEST 的应答：命中即重发
    // 缓存的应答而不再执行 handler。条目由周期任务按 TTL 回收（见 expireReplyCache）。
    struct CachedReply {
        Payload                               response;
        QoSProfile                            qos;
        std::chrono::steady_clock::time_point cachedAt;
    };
    static uint64_t replyKey(uint16_t sourceId, uint32_t corrId) {
        return (static_cast<uint64_t>(sourceId) << 32) | corrId;
    }
    std::mutex replyCacheMutex_;
    std::unordered_map<uint64_t, CachedReply> replyCache_;
    void expireReplyCache();

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
