#include "message_bus.h"
#include "../transport/transport_manager.h"
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
    std::string      topic;
    QoSProfile       qos;
    uint64_t         subId{0};
    MessageBus*      bus{nullptr};
    std::atomic<uint64_t> msgCount{0};
    std::atomic<bool>     paused{false};
};

Subscriber::Subscriber(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Subscriber::~Subscriber() {
    if (impl_ && impl_->bus && impl_->subId) {
        impl_->bus->removeLocalSubscription(impl_->subId);
    }
}
const std::string& Subscriber::topic()        const { return impl_->topic; }
void               Subscriber::pause()              { impl_->paused = true; }
void               Subscriber::resume()             { impl_->paused = false; }
uint64_t           Subscriber::messageCount() const { return impl_->msgCount.load(); }


// ===================== Requester::Impl =====================

struct Requester::Impl {
    std::string  service;
    QoSProfile   qos;
    MessageBus*  bus{nullptr};
};

Requester::Requester(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Requester::~Requester() = default;
const std::string& Requester::service() const { return impl_->service; }

std::optional<Payload> Requester::request(
    const Payload& payload, std::chrono::milliseconds timeout)
{
    auto fut = requestAsync(payload);
    if (fut.wait_for(timeout) == std::future_status::ready) {
        return fut.get();
    }
    return std::nullopt;
}

std::future<Payload> Requester::requestAsync(const Payload& payload) {
    return impl_->bus->sendRequest(impl_->service, payload, impl_->qos);
}


// ===================== Replier::Impl =====================

struct Replier::Impl {
    std::string    service;
    QoSProfile     qos;
    MessageBus*    bus{nullptr};
    std::atomic<uint64_t> reqCount{0};
};

Replier::Replier(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Replier::~Replier() {
    if (impl_ && impl_->bus) {
        impl_->bus->removeServiceHandler(impl_->service);
    }
}
const std::string& Replier::service()      const { return impl_->service; }
uint64_t           Replier::requestCount() const { return impl_->reqCount.load(); }


// ===================== MessageBus =====================

MessageBus::MessageBus(uint16_t nodeId, TransportManager* tm)
    : nodeId_(nodeId), transportMgr_(tm)
{}

MessageBus::~MessageBus() { stop(); }

void MessageBus::start() {
    running_ = true;
    timerWheel_.start();

    // 定期处理重传超时
    retryTimerId_ = timerWheel_.addPeriodic(200, [this]() {
        auto abandoned = qosEngine_.processTimeouts();
        for (auto seqId : abandoned) {
            std::lock_guard<std::mutex> lock(pendingReqMutex_);
            auto it = pendingRequests_.find(seqId);
            if (it != pendingRequests_.end()) {
                it->second.set_exception(std::make_exception_ptr(
                    std::runtime_error("request timeout")));
                pendingRequests_.erase(it);
            }
        }
    });
}

void MessageBus::stop() {
    if (running_.exchange(false)) {
        timerWheel_.stop();
        // 放弃所有待处理请求
        std::lock_guard<std::mutex> lock(pendingReqMutex_);
        for (auto& [id, promise] : pendingRequests_) {
            try {
                promise.set_exception(std::make_exception_ptr(
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

    auto* implPtr = impl.get();
    uint64_t subId = router_.addSubscription(topic, [implPtr, cb](const ReceivedMessage& msg) {
        if (!implPtr->paused) {
            implPtr->msgCount++;
            cb(msg);
        }
    }, qos);
    impl->subId = subId;

    // 检查保留消息
    if (qos.durability == DurabilityKind::TransientLocal) {
        auto retained = retainedStore_.get(topic);
        if (retained) {
            if (!impl->paused) {
                impl->msgCount++;
                cb(*retained);
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
    {
        std::lock_guard<std::mutex> lock(serviceMutex_);
        serviceHandlers_[service] = std::move(handler);
    }

    auto impl = std::make_unique<Replier::Impl>();
    impl->service = service;
    impl->qos     = qos;
    impl->bus     = this;
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
        retainedStore_.store(topic, retained);
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
                        targets.push_back(peer.endpoints.front());
                    break;
                }
            }
        }
    }

    if (!targets.empty()) {
        auto data = MessageCodec::encode(
            MessageType::PUBLISH, nodeId_, 0xFFFF,
            topic, payload, qos, seqId, 0, flags);

        for (auto& ep : targets) {
            if (transportMgr_) transportMgr_->send(ep, data.data(), data.size());

            if (qos.level >= QoSLevel::Reliable) {
                qosEngine_.addPending(seqId, data, qos,
                    [this, ep](const std::vector<uint8_t>& d) {
                        if (transportMgr_)
                            transportMgr_->send(ep, d.data(), d.size());
                    });
            }
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

    // QoS 1: 发送 ACK
    if (qosLevel >= QoSLevel::Reliable && msgType == MessageType::PUBLISH) {
        sendAck(from, result.header.sequenceId);
    }

    // QoS 2: 去重
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
        std::lock_guard<std::mutex> lock(pendingReqMutex_);
        auto it = pendingRequests_.find(result.header.correlationId);
        if (it != pendingRequests_.end()) {
            it->second.set_value(result.payload);
            pendingRequests_.erase(it);
        }
        return;
    }
}

void MessageBus::onPeerDiscovered(const PeerInfo& peer) {
    std::lock_guard<std::mutex> lock(peerMutex_);
    peers_[peer.id] = peer;
    EMQ_LOG_I("MessageBus", "Peer discovered: %s (id=%u)", peer.name.c_str(), peer.id);
}

void MessageBus::onPeerLost(uint16_t peerId) {
    std::lock_guard<std::mutex> lock(peerMutex_);
    peers_.erase(peerId);
    EMQ_LOG_I("MessageBus", "Peer lost: id=%u", peerId);
}

void MessageBus::sendAck(const Endpoint& to, uint32_t seqId) {
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::ACK, nodeId_, 0, "", Payload{}, qos, seqId);
    if (transportMgr_) transportMgr_->send(to, data.data(), data.size());
}

void MessageBus::sendNack(const Endpoint& to, uint32_t seqId) {
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::NACK, nodeId_, 0, "", Payload{}, qos, seqId);
    if (transportMgr_) transportMgr_->send(to, data.data(), data.size());
}

void MessageBus::sendReply(const Endpoint& to, uint32_t correlationId,
                            const Payload& payload, const QoSProfile& qos)
{
    auto data = MessageCodec::encode(
        MessageType::REPLY, nodeId_, 0,
        "", payload, qos, seqCounter_++, correlationId);
    if (transportMgr_) transportMgr_->send(to, data.data(), data.size());
}

std::vector<std::string> MessageBus::localTopics() const {
    return router_.allTopics();
}

void MessageBus::removeLocalSubscription(uint64_t subId) {
    router_.removeSubscription(subId);
}

void MessageBus::removeServiceHandler(const std::string& service) {
    std::lock_guard<std::mutex> lock(serviceMutex_);
    serviceHandlers_.erase(service);
}

std::future<Payload> MessageBus::sendRequest(const std::string& service,
                                               const Payload& payload,
                                               const QoSProfile& qos)
{
    uint32_t corrId = corrCounter_++;

    std::promise<Payload> promise;
    auto future = promise.get_future();

    {
        std::lock_guard<std::mutex> lock(pendingReqMutex_);
        pendingRequests_[corrId] = std::move(promise);
    }

    // 先找本地的 service handler
    RequestHandler handler;
    {
        std::lock_guard<std::mutex> lock(serviceMutex_);
        auto it = serviceHandlers_.find(service);
        if (it != serviceHandlers_.end()) handler = it->second;
    }

    if (handler) {
        // 本地服务：直接调用
        ReceivedMessage req;
        req.topic        = service;
        req.payload      = payload;
        req.sourceId     = nodeId_;
        req.sequenceId   = seqCounter_++;
        req.correlationId = corrId;
        Payload response  = handler(req);

        std::lock_guard<std::mutex> lock(pendingReqMutex_);
        auto it = pendingRequests_.find(corrId);
        if (it != pendingRequests_.end()) {
            it->second.set_value(response);
            pendingRequests_.erase(it);
        }
    } else {
        // 远端服务：通过网络发送
        publish("$SVC/" + service, payload, qos, 0);
        // 等待回复，通过 onReceived 触发 promise
    }

    return future;
}

} // namespace embedmq
