#include "message_bus.h"
#include "../transport/transport_manager.h"
#include "../platform/process.h"
#include "../util/logger.h"

namespace embedmq {

// ===================== Publisher::Impl =====================

struct Publisher::Impl {
    std::string  topic;
    QoSProfile   qos;
    MessageBus*  bus{nullptr};
    std::atomic<size_t> subCount{0};
    std::atomic<bool>   paused{false};
};

Publisher::Publisher(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Publisher::~Publisher() = default;

bool Publisher::publish(const void* data, size_t size) {
    return publish(Payload(data, size));
}
bool Publisher::publish(std::string_view text) {
    return publish(Payload(text));
}
bool Publisher::publish(const Payload& payload) {
    if (!impl_ || !impl_->bus) return false;
    uint8_t flags = impl_->qos.retain ? MsgFlags::RETAIN : 0;
    return impl_->bus->publish(impl_->topic, payload, impl_->qos, flags);
}
size_t             Publisher::subscriberCount() const { return impl_->subCount.load(); }
const std::string& Publisher::topic()           const { return impl_->topic; }


// ===================== Subscriber::Impl =====================

struct Subscriber::Impl {
    // 共享状态：被路由回调以 weak_ptr 持有，使得 Subscriber 析构后仍在执行的
    // 在途回调不会访问已释放对象（避免 UAF）。
    struct State {
        std::atomic<uint64_t> msgCount{0};
        std::atomic<bool>     paused{false};
    };
    std::string      topic;
    QoSProfile       qos;
    uint64_t         subId{0};
    MessageBus*      bus{nullptr};
    std::shared_ptr<State> state{std::make_shared<State>()};
};

Subscriber::Subscriber(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Subscriber::~Subscriber() {
    if (impl_ && impl_->bus && impl_->subId) {
        impl_->bus->removeLocalSubscription(impl_->subId);
    }
}
const std::string& Subscriber::topic()        const { return impl_->topic; }
void               Subscriber::pause()              { impl_->state->paused = true; }
void               Subscriber::resume()             { impl_->state->paused = false; }
uint64_t           Subscriber::messageCount() const { return impl_->state->msgCount.load(); }


// ===================== Requester::Impl =====================

struct Requester::Impl {
    std::string  service;
    QoSProfile   qos;
    MessageBus*  bus{nullptr};
    uint32_t     lastCorrId{0};
};

Requester::Requester(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Requester::~Requester() = default;
const std::string& Requester::service() const { return impl_->service; }

std::optional<Payload> Requester::request(
    const Payload& payload, std::chrono::milliseconds timeout)
{
    // 把调用方超时透传为内部截止时间，否则 sendRequest 只按 QoS 推导出较短的
    // 截止时间（如 reliable 仅 2s），-t 指定的更长超时形同虚设。
    impl_->lastCorrId = impl_->bus->corrCounter_++;
    auto fut = impl_->bus->sendRequest(impl_->service, payload, impl_->qos,
                                       impl_->lastCorrId, timeout);
    if (fut.wait_for(timeout) == std::future_status::ready) {
        // future 可能携带异常（无服务提供方 / 请求超时），同步接口统一返回 nullopt
        try { return fut.get(); }
        catch (...) { return std::nullopt; }
    }
    if (impl_->lastCorrId != 0) {
        impl_->bus->cancelPendingRequest(impl_->lastCorrId);
    }
    return std::nullopt;
}

std::future<Payload> Requester::requestAsync(const Payload& payload) {
    impl_->lastCorrId = impl_->bus->corrCounter_++;
    return impl_->bus->sendRequest(impl_->service, payload, impl_->qos,
                                    impl_->lastCorrId);
}


// ===================== Replier::Impl =====================

struct Replier::Impl {
    struct State {
        std::atomic<uint64_t> reqCount{0};
    };
    std::string    service;
    QoSProfile     qos;
    MessageBus*    bus{nullptr};
    std::shared_ptr<State> state{std::make_shared<State>()};
};

Replier::Replier(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Replier::~Replier() {
    if (impl_ && impl_->bus) {
        impl_->bus->removeServiceHandler(impl_->service);
    }
}
const std::string& Replier::service()      const { return impl_->service; }
uint64_t           Replier::requestCount() const { return impl_->state->reqCount.load(); }


// ===================== MessageBus =====================

MessageBus::MessageBus(uint16_t nodeId, TransportManager* tm)
    : nodeId_(nodeId), transportMgr_(tm), localHostName_(platform::getHostName())
{}

Endpoint MessageBus::selectEndpoint(const PeerInfo& peer) const {
    const Endpoint* shm = nullptr;
    const Endpoint* tcp = nullptr;
    const Endpoint* udp = nullptr;
    for (const auto& ep : peer.endpoints) {
        if      (ep.transportType == "shm" && !shm) shm = &ep;
        else if (ep.transportType == "tcp" && !tcp) tcp = &ep;
        else if (ep.transportType == "udp" && !udp) udp = &ep;
    }

    if (transportMgr_) {
        // SHM 仅在「同主机」且本地 SHM 传输可用时选用：跨主机段名指向的是本地的
        // 另一段（无人消费），会静默丢消息，故必须用主机名作硬性约束。
        if (shm && !shm->address.empty() &&
            !localHostName_.empty() && peer.hostName == localHostName_ &&
            transportMgr_->isActive("shm")) {
            return *shm;
        }
        // TCP 可路由（地址已由发现层用收包来源 IP 补全），优于无连接的 UDP。
        if (tcp && !tcp->address.empty() && transportMgr_->isActive("tcp")) {
            return *tcp;
        }
    }
    if (udp) return *udp;
    return peer.endpoints.empty() ? Endpoint{} : peer.endpoints.front();
}

MessageBus::~MessageBus() { stop(); }

void MessageBus::start(int cpuAffinity) {
    running_ = true;
    timerWheel_.setAffinity(cpuAffinity);
    timerWheel_.start();

    // 定期处理重传超时
    retryTimerId_ = timerWheel_.addPeriodic(200, [this]() {
        // 发布可靠消息(QoS1/2)的重传/放弃：放弃的是发布 seqId，与请求的
        // correlationId 属于不同命名空间，绝不能用它去索引 pendingRequests_。
        qosEngine_.processTimeouts();
        // 周期清理 QoS2 去重窗口，避免长期运行内存无界增长。
        qosEngine_.cleanupDedupWindow();
        // 丢弃过期的保留消息，使保留消息内存占用有上界（TTL 默认关闭）。
        retainedStore_.expire();

        // 请求超时与重传：截止则结束 future；未截止的远端请求按 retryInterval
        // 重新解析路由并重传——提供方上线后即可送达，修复"请求发出过早即丢失"。
        auto now = std::chrono::steady_clock::now();
        std::vector<std::promise<Payload>> expired;
        struct RetryJob { std::string service; Payload payload; QoSProfile qos; uint32_t corrId; };
        std::vector<RetryJob> retries;
        {
            std::lock_guard<std::mutex> lock(pendingReqMutex_);
            for (auto it = pendingRequests_.begin(); it != pendingRequests_.end(); ) {
                if (now >= it->second.deadline) {
                    expired.push_back(std::move(it->second.promise));
                    it = pendingRequests_.erase(it);
                } else {
                    auto& pr = it->second;
                    if (pr.remote && now >= pr.nextRetry) {
                        retries.push_back(RetryJob{ pr.service, pr.payload, pr.qos, pr.corrId });
                        pr.nextRetry = now + pr.retryInterval;
                    }
                    ++it;
                }
            }
        }
        // 锁外重传：dispatchRequest 会取 peerMutex_，必须避免与 pendingReqMutex_ 嵌套。
        for (auto& j : retries) {
            dispatchRequest(j.service, j.payload, j.qos, j.corrId);
        }
        // 锁外设置异常，避免在持锁状态下执行 future 续延逻辑
        for (auto& p : expired) {
            try {
                p.set_exception(std::make_exception_ptr(
                    std::runtime_error("request timeout")));
            } catch (...) {}
        }
    });
}

void MessageBus::stop() {
    if (running_.exchange(false)) {
        timerWheel_.stop();
        // 放弃所有待处理请求
        std::lock_guard<std::mutex> lock(pendingReqMutex_);
        for (auto& [id, pr] : pendingRequests_) {
            try {
                pr.promise.set_exception(std::make_exception_ptr(
                    std::runtime_error("bus stopped")));
            } catch (...) {}
        }
        pendingRequests_.clear();
    }
}

std::unique_ptr<Publisher> MessageBus::createPublisher(
    const std::string& topic, const QoSProfile& qos)
{
    auto impl = std::make_unique<Publisher::Impl>();
    impl->topic = topic;
    impl->qos   = qos;
    impl->bus   = this;
    return std::unique_ptr<Publisher>(new Publisher(std::move(impl)));
}

std::unique_ptr<Subscriber> MessageBus::createSubscriber(
    const std::string& topic, SubscribeCallback cb, const QoSProfile& qos)
{
    auto impl = std::make_unique<Subscriber::Impl>();
    impl->topic = topic;
    impl->qos   = qos;
    impl->bus   = this;

    std::weak_ptr<Subscriber::Impl::State> wstate = impl->state;
    uint64_t subId = router_.addSubscription(topic, [wstate, cb](const ReceivedMessage& msg) {
        auto s = wstate.lock();
        if (!s) return; // Subscriber 已销毁，安全跳过
        if (!s->paused.load()) {
            s->msgCount++;
            // 用户回调异常绝不能逃逸到传输/分发线程，否则纯 C/C++ 使用者会
            // 越过库边界触发 std::terminate。
            try { cb(msg); } catch (...) {}
        }
    }, qos);
    impl->subId = subId;

    // 检查保留消息
    if (qos.durability == DurabilityKind::TransientLocal) {
        auto retained = retainedStore_.get(topic);
        if (retained) {
            if (!impl->state->paused.load()) {
                impl->state->msgCount++;
                try { cb(*retained); } catch (...) {}
            }
        }
    }

    return std::unique_ptr<Subscriber>(new Subscriber(std::move(impl)));
}

std::unique_ptr<Requester> MessageBus::createRequester(
    const std::string& service, const QoSProfile& qos)
{
    auto impl = std::make_unique<Requester::Impl>();
    impl->service = service;
    impl->qos     = qos;
    impl->bus     = this;
    return std::unique_ptr<Requester>(new Requester(std::move(impl)));
}

std::unique_ptr<Replier> MessageBus::createReplier(
    const std::string& service, RequestHandler handler, const QoSProfile& qos)
{
    auto impl = std::make_unique<Replier::Impl>();
    impl->service = service;
    impl->qos     = qos;
    impl->bus     = this;

    std::weak_ptr<Replier::Impl::State> wstate = impl->state;
    {
        std::lock_guard<std::mutex> lock(serviceMutex_);
        serviceHandlers_[service] = [wstate, h = std::move(handler)](
            const ReceivedMessage& req) -> Payload {
            if (auto s = wstate.lock()) s->reqCount++;
            // 服务处理器抛出的异常不得逃逸到传输线程；失败回退为空响应。
            try { return h(req); } catch (...) { return Payload{}; }
        };
    }

    return std::unique_ptr<Replier>(new Replier(std::move(impl)));
}

void MessageBus::registerSerializer(std::shared_ptr<ISerializer> /*s*/) {
    // Phase 2: 序列化器注册
}

bool MessageBus::publish(const std::string& topic,
                          const Payload& payload,
                          const QoSProfile& qos,
                          uint8_t flags)
{
    uint32_t seqId = seqCounter_++;

    // 1. 存储保留消息
    if (flags & MsgFlags::RETAIN) {
        ReceivedMessage retained;
        retained.topic     = topic;
        retained.payload   = payload;
        retained.timestamp = 0;
        retained.sourceId  = nodeId_;
        retained.sequenceId = seqId;
        // QoSProfile.lifespanMs（>0）作为该条保留消息的 TTL，覆盖全局默认。
        retainedStore_.store(topic, retained, qos.lifespanMs);
    }

    // 2. 本地路由
    ReceivedMessage msg;
    msg.topic      = topic;
    msg.payload    = payload;
    msg.sequenceId = seqId;
    msg.sourceId   = nodeId_;
    router_.route(topic, msg);

    // 3. 远端发送
    std::vector<Endpoint> targets;
    {
        std::lock_guard<std::mutex> lock(peerMutex_);
        for (auto& [id, peer] : peers_) {
            for (auto& subTopic : peer.subscribedTopics) {
                if (TopicRouter::matchWildcard(subTopic, topic) ||
                    subTopic == topic) {
                    if (!peer.endpoints.empty())
                        targets.push_back(selectEndpoint(peer));
                    break;
                }
            }
        }
    }

    for (auto& ep : targets) {
        uint32_t epSeqId = seqCounter_++;

        if (qos.level >= QoSLevel::Reliable) {
            // 可靠传输：需保留完整缓冲以便重传
            auto data = MessageCodec::encode(
                MessageType::PUBLISH, nodeId_, 0xFFFF,
                topic, payload, qos, epSeqId, 0, flags, 0, crcEnabled_);
            if (transportMgr_) transportMgr_->send(ep, data.data(), data.size());
            qosEngine_.addPending(epSeqId, data, qos,
                [this, ep](const std::vector<uint8_t>& d) {
                    if (transportMgr_)
                        transportMgr_->send(ep, d.data(), d.size());
                });
        } else if (transportMgr_) {
            // BestEffort：零拷贝 scatter/gather 发送 {header, topic, payload}
            auto header = MessageCodec::encodeHeader(
                MessageType::PUBLISH, nodeId_, 0xFFFF,
                topic, payload, qos, epSeqId, 0, flags, 0, crcEnabled_);
            IoSlice slices[3];
            int n = 0;
            slices[n++] = IoSlice{ header.data(), header.size() };
            if (!topic.empty())
                slices[n++] = IoSlice{ topic.data(), topic.size() };
            if (payload.size() > 0)
                slices[n++] = IoSlice{ payload.data(), payload.size() };
            transportMgr_->sendv(ep, slices, static_cast<size_t>(n));
        }
    }

    return true;
}

void MessageBus::onReceived(const Endpoint& from,
                             const uint8_t* data, size_t size)
{
    auto result = MessageCodec::decode(data, size);
    if (!result.valid) {
        EMQ_LOG_W("MessageBus", "Invalid packet from %s", from.address.c_str());
        return;
    }

    auto msgType = static_cast<MessageType>(result.header.msgType);
    QoSLevel qosLevel = static_cast<QoSLevel>(result.header.qosLevel);

    if (msgType == MessageType::ACK) {
        qosEngine_.onAck(result.header.sequenceId);
        return;
    }

    if (msgType == MessageType::NACK) {
        qosEngine_.onNack(result.header.sequenceId);
        return;
    }

    // ---- QoS2 两阶段握手控制包 ----
    if (msgType == MessageType::PUBREC) {
        // 发送方：停止 PUBLISH 重传，转入 PUBREL 阶段（重传至 PUBCOMP）
        qosEngine_.onPubrec(result.header.sequenceId);
        QoSProfile relQos = QoSProfile::exactlyOnce();
        auto rel = MessageCodec::encode(MessageType::PUBREL, nodeId_, 0, "",
            Payload{}, relQos, result.header.sequenceId, 0, 0, 0, crcEnabled_);
        if (transportMgr_) transportMgr_->send(from, rel.data(), rel.size());
        Endpoint ep = from;
        uint32_t seq = result.header.sequenceId;
        qosEngine_.addPendingRel(seq, rel, relQos,
            [this, ep](const std::vector<uint8_t>& d) {
                if (transportMgr_) transportMgr_->send(ep, d.data(), d.size());
            });
        return;
    }
    if (msgType == MessageType::PUBREL) {
        // 接收方：回 PUBCOMP，完成握手
        sendCtrl(from, MessageType::PUBCOMP, result.header.sequenceId);
        return;
    }
    if (msgType == MessageType::PUBCOMP) {
        qosEngine_.onPubcomp(result.header.sequenceId);
        return;
    }

    // QoS1: 回 ACK；QoS2: 回 PUBREC（进入握手）
    if (msgType == MessageType::PUBLISH) {
        if (qosLevel == QoSLevel::ExactlyOnce)
            sendCtrl(from, MessageType::PUBREC, result.header.sequenceId);
        else if (qosLevel == QoSLevel::Reliable)
            sendAck(from, result.header.sequenceId);
    }

    // QoS2: 去重（保证仅投递一次）
    if (qosLevel == QoSLevel::ExactlyOnce) {
        if (qosEngine_.isDuplicate(result.header.sourceId,
                                    result.header.sequenceId)) {
            return;
        }
    }

    if (msgType == MessageType::PUBLISH) {
        // 保留消息处理
        if (result.header.flags & MsgFlags::RETAIN) {
            ReceivedMessage retained;
            retained.topic      = result.topic;
            retained.payload    = result.payload;
            retained.sourceId   = result.header.sourceId;
            retained.sequenceId = result.header.sequenceId;
            retainedStore_.store(result.topic, retained);
        }

        ReceivedMessage msg;
        msg.topic       = result.topic;
        msg.payload     = result.payload;
        msg.timestamp   = result.header.timestamp;
        msg.sourceId    = result.header.sourceId;
        msg.sequenceId  = result.header.sequenceId;
        msg.correlationId = result.header.correlationId;
        router_.route(result.topic, msg);
        return;
    }

    if (msgType == MessageType::REQUEST) {
        // 查找 service handler
        std::string svcName = result.topic;
        const std::string prefix = "$SVC/";
        if (svcName.substr(0, prefix.size()) == prefix)
            svcName = svcName.substr(prefix.size());

        RequestHandler handler;
        {
            std::lock_guard<std::mutex> lock(serviceMutex_);
            auto it = serviceHandlers_.find(svcName);
            if (it != serviceHandlers_.end()) handler = it->second;
        }

        if (handler) {
            ReceivedMessage req;
            req.topic       = svcName;
            req.payload     = result.payload;
            req.sourceId    = result.header.sourceId;
            req.sequenceId  = result.header.sequenceId;
            req.correlationId = result.header.correlationId;
            Payload response = handler(req);
            QoSProfile qos;
            qos.level = qosLevel;
            sendReply(from, result.header.correlationId, response, qos);
        }
        return;
    }

    if (msgType == MessageType::REPLY) {
        // 可靠 REPLY：回 ACK 以停止 replier 重传（与 QoS1 PUBLISH 对称）。
        // 即便是重复 REPLY 也回 ACK（幂等），以防先前的 ACK 在途中丢失。
        if (qosLevel >= QoSLevel::Reliable)
            sendAck(from, result.header.sequenceId);

        std::promise<Payload> p;
        bool found = false;
        {
            std::lock_guard<std::mutex> lock(pendingReqMutex_);
            auto it = pendingRequests_.find(result.header.correlationId);
            if (it != pendingRequests_.end()) {
                p = std::move(it->second.promise);
                pendingRequests_.erase(it);
                found = true;
            }
        }
        if (found) {
            try { p.set_value(result.payload); } catch (...) {}
        }
        return;
    }
}

void MessageBus::onPeerDiscovered(const PeerInfo& peer) {
    std::lock_guard<std::mutex> lock(peerMutex_);
    peers_[peer.id] = peer;
    EMQ_LOG_I("MessageBus", "Peer discovered: %s (id=%u)", peer.name.c_str(), peer.id);
}

void MessageBus::onPeerUpdated(const PeerInfo& peer) {
    // 对端订阅/端点变化：刷新路由表，避免已发现对端后续新增订阅时跨节点投递失败
    std::lock_guard<std::mutex> lock(peerMutex_);
    peers_[peer.id] = peer;
    EMQ_LOG_D("MessageBus", "Peer updated: %s (id=%u)", peer.name.c_str(), peer.id);
}

void MessageBus::onPeerLost(uint16_t peerId) {
    std::lock_guard<std::mutex> lock(peerMutex_);
    peers_.erase(peerId);
    EMQ_LOG_I("MessageBus", "Peer lost: id=%u", peerId);
}

void MessageBus::deliverWill(const std::string& topic, const Payload& payload,
                              bool retain, uint16_t sourceId)
{
    uint32_t seqId = seqCounter_++;

    if (retain) {
        ReceivedMessage retained;
        retained.topic      = topic;
        retained.payload    = payload;
        retained.timestamp  = 0;
        retained.sourceId   = sourceId;
        retained.sequenceId = seqId;
        retainedStore_.store(topic, retained);
    }

    ReceivedMessage msg;
    msg.topic      = topic;
    msg.payload    = payload;
    msg.sourceId   = sourceId;
    msg.sequenceId = seqId;
    router_.route(topic, msg);

    EMQ_LOG_I("MessageBus", "Last-Will delivered: topic=%s (src=%u)",
              topic.c_str(), sourceId);
}

void MessageBus::sendAck(const Endpoint& to, uint32_t seqId) {
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::ACK, nodeId_, 0, "", Payload{}, qos, seqId, 0, 0, 0, crcEnabled_);
    if (transportMgr_) transportMgr_->send(to, data.data(), data.size());
}

void MessageBus::sendNack(const Endpoint& to, uint32_t seqId) {
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::NACK, nodeId_, 0, "", Payload{}, qos, seqId, 0, 0, 0, crcEnabled_);
    if (transportMgr_) transportMgr_->send(to, data.data(), data.size());
}

void MessageBus::sendCtrl(const Endpoint& to, MessageType type, uint32_t seqId) {
    QoSProfile qos;
    auto data = MessageCodec::encode(
        type, nodeId_, 0, "", Payload{}, qos, seqId, 0, 0, 0, crcEnabled_);
    if (transportMgr_) transportMgr_->send(to, data.data(), data.size());
}

void MessageBus::sendReply(const Endpoint& to, uint32_t correlationId,
                            const Payload& payload, const QoSProfile& qos)
{
    uint32_t seqId = seqCounter_++;
    auto data = MessageCodec::encode(
        MessageType::REPLY, nodeId_, 0,
        "", payload, qos, seqId, correlationId, 0, 0, crcEnabled_);
    if (transportMgr_) transportMgr_->send(to, data.data(), data.size());

    // 可靠请求：REPLY 也必须可靠送达，否则丢失的 REPLY 只会让 requester 超时而不重试。
    // 登记 REPLY 重传，直到 requester 回 ACK（ACK 携带本 seqId，由 onReceived 的
    // ACK 分支经 qosEngine_.onAck 取消，与 QoS1 PUBLISH 完全对称）。处理器仅在首个
    // REQUEST 时执行一次，丢失的是 REPLY——故重传 REPLY 即可，无需缓存响应或对
    // REQUEST 去重。
    if (qos.level >= QoSLevel::Reliable) {
        Endpoint ep = to;
        qosEngine_.addPending(seqId, data, qos,
            [this, ep](const std::vector<uint8_t>& d) {
                if (transportMgr_) transportMgr_->send(ep, d.data(), d.size());
            });
    }
}

std::vector<std::string> MessageBus::localTopics() const {
    auto topics = router_.allTopics();
    {
        std::lock_guard<std::mutex> lock(serviceMutex_);
        for (auto& [svc, _] : serviceHandlers_) {
            topics.push_back("$SVC/" + svc);
        }
    }
    return topics;
}

void MessageBus::removeLocalSubscription(uint64_t subId) {
    router_.removeSubscription(subId);
    notifyTopicsChanged();
}

void MessageBus::removeServiceHandler(const std::string& service) {
    {
        std::lock_guard<std::mutex> lock(serviceMutex_);
        serviceHandlers_.erase(service);
    }
    notifyTopicsChanged();
}

void MessageBus::setTopicsChangedCallback(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(topicsCbMutex_);
    topicsChangedCb_ = std::move(cb);
}

void MessageBus::notifyTopicsChanged() {
    std::function<void()> cb;
    {
        std::lock_guard<std::mutex> lock(topicsCbMutex_);
        cb = topicsChangedCb_;
    }
    if (cb) cb();   // 锁外触发，避免在持锁状态下重入广播
}

void MessageBus::cancelPendingRequest(uint32_t corrId) {
    std::lock_guard<std::mutex> lock(pendingReqMutex_);
    pendingRequests_.erase(corrId);
}

bool MessageBus::dispatchRequest(const std::string& service, const Payload& payload,
                                 const QoSProfile& qos, uint32_t corrId)
{
    const std::string svcTopic = "$SVC/" + service;
    std::vector<Endpoint> targets;
    {
        std::lock_guard<std::mutex> lock(peerMutex_);
        for (auto& [id, peer] : peers_) {
            (void)id;
            for (auto& t : peer.subscribedTopics) {
                if (t == svcTopic) {
                    if (!peer.endpoints.empty())
                        targets.push_back(selectEndpoint(peer));
                    break;
                }
            }
        }
    }
    if (targets.empty()) return false;

    uint32_t seqId = seqCounter_++;
    auto data = MessageCodec::encode(
        MessageType::REQUEST, nodeId_, 0xFFFF,
        svcTopic, payload, qos, seqId, corrId, 0, 0, crcEnabled_);
    for (auto& ep : targets) {
        if (transportMgr_) transportMgr_->send(ep, data.data(), data.size());
    }
    return true;
}

std::future<Payload> MessageBus::sendRequest(const std::string& service,
                                               const Payload& payload,
                                               const QoSProfile& qos,
                                               uint32_t corrId,
                                               std::chrono::milliseconds timeoutOverride)
{
    std::promise<Payload> promise;
    auto future = promise.get_future();

    // 先找本地的 service handler：本地服务直接调用并返回，不进入 pending。
    RequestHandler handler;
    {
        std::lock_guard<std::mutex> lock(serviceMutex_);
        auto it = serviceHandlers_.find(service);
        if (it != serviceHandlers_.end()) handler = it->second;
    }
    if (handler) {
        ReceivedMessage req;
        req.topic         = service;
        req.payload       = payload;
        req.sourceId      = nodeId_;
        req.sequenceId    = seqCounter_++;
        req.correlationId = corrId;
        Payload response  = handler(req);
        try { promise.set_value(response); } catch (...) {}
        return future;
    }

    // 远端服务：先登记 pending（携带重传上下文，确保随后到达的 REPLY 必能匹配），
    // 再首发一次；其余交给周期任务重传。截止时间优先采用调用方显式 timeout，否则按
    // QoS 推导。提供方的 $SVC 通告可能晚于本次请求到达，故不再在"暂无提供方"时一律
    // 立即失败——只要还有活动传输能在后续发现提供方，就保留请求并反复重传，直到收到
    // REPLY 或截止时间到达（与发布路径每次重新解析对端的行为对齐）。
    uint64_t timeoutMs = timeoutOverride.count() > 0
        ? static_cast<uint64_t>(timeoutOverride.count())
        : (qos.ackTimeoutMs
            ? static_cast<uint64_t>(qos.ackTimeoutMs) * (qos.maxRetries + 1)
            : 5000);
    auto now      = std::chrono::steady_clock::now();
    auto deadline = now + std::chrono::milliseconds(timeoutMs);
    std::chrono::milliseconds retryInterval(qos.ackTimeoutMs ? qos.ackTimeoutMs : 200);
    {
        std::lock_guard<std::mutex> lock(pendingReqMutex_);
        PendingRequest pr;
        pr.promise       = std::move(promise);
        pr.deadline      = deadline;
        pr.remote        = true;
        pr.service       = service;
        pr.payload       = payload;
        pr.qos           = qos;
        pr.corrId        = corrId;
        pr.retryInterval = retryInterval;
        pr.nextRetry     = now + retryInterval;
        pendingRequests_[corrId] = std::move(pr);
    }

    bool sent = dispatchRequest(service, payload, qos, corrId);

    // 既没有可达提供方，又没有任何活动传输能在后续发现提供方（如禁用 UDP/组播且
    // 无 TCP/SHM），则等下去也不会有结果——立即结束，避免空等到调用方超时。
    const bool canDiscover = transportMgr_ &&
        (transportMgr_->isActive("udp") || transportMgr_->isActive("tcp") ||
         transportMgr_->isActive("shm"));
    if (!sent && !canDiscover) {
        std::promise<Payload> p;
        bool found = false;
        {
            std::lock_guard<std::mutex> lock(pendingReqMutex_);
            auto it = pendingRequests_.find(corrId);
            if (it != pendingRequests_.end()) {
                p = std::move(it->second.promise);
                pendingRequests_.erase(it);
                found = true;
            }
        }
        if (found) {
            try {
                p.set_exception(std::make_exception_ptr(
                    std::runtime_error("no provider for service: " + service)));
            } catch (...) {}
        }
    }
    return future;
}

} // namespace embedmq
