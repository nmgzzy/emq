#include "embedmq/embedmq.h"
#include "message_bus.h"
#include "../discovery/discovery_agent.h"
#include "../transport/transport_manager.h"
#include "../platform/process.h"
#include "../util/logger.h"
#include <chrono>
#include <functional>

namespace embedmq {

struct Participant::Impl {
    ParticipantConfig              config;
    uint16_t                       id{0};
    std::string                    name;
    std::unique_ptr<TransportManager> transportMgr;
    std::unique_ptr<MessageBus>    messageBus;
    std::unique_ptr<DiscoveryAgent> discovery;
    std::atomic<bool>              running{false};
    PeerEventCallback              peerEventCb;

    uint16_t generateNodeId() {
        auto pid = platform::getProcessId();
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        std::hash<uint64_t> hasher;
        return static_cast<uint16_t>(hasher(pid ^ static_cast<uint64_t>(now)) & 0xFFFF);
    }

    void init();
};

void Participant::Impl::init() {
    id = generateNodeId();

    transportMgr = std::make_unique<TransportManager>();

    transportMgr->registerDefaultTransports(config);

    transportMgr->initAll(config);

    messageBus = std::make_unique<MessageBus>(id, transportMgr.get());

    discovery = std::make_unique<DiscoveryAgent>(
        id, name, config, transportMgr.get());

    discovery->setOnPeerDiscovered([this](const PeerInfo& peer) {
        messageBus->onPeerDiscovered(peer);
        if (peerEventCb) peerEventCb(peer.id, peer.name, true);
    });

    discovery->setOnPeerLost([this](uint16_t peerId, const std::string& peerName) {
        messageBus->onPeerLost(peerId);
        if (peerEventCb) peerEventCb(peerId, peerName, false);
    });

    // 将传输层接收数据按消息类型分发给 DiscoveryAgent 或 MessageBus
    transportMgr->setRecvCallback([this](const Endpoint& from,
                                          const uint8_t* data, size_t size) {
        if (size >= 4) {
            auto msgType = static_cast<MessageType>(data[3]);
            if (msgType == MessageType::ANNOUNCE   ||
                msgType == MessageType::HEARTBEAT  ||
                msgType == MessageType::FAREWELL   ||
                msgType == MessageType::DISCOVER_REQ ||
                msgType == MessageType::DISCOVER_RSP) {
                discovery->onDiscoveryMessage(from, data, size);
                return;
            }
        }
        messageBus->onReceived(from, data, size);
    });

    discovery->start();
    messageBus->start();
    running = true;

    EMQ_LOG_I("Participant", "Started: %s (id=%u)", name.c_str(), id);
}


// ===================== Factory =====================

std::unique_ptr<Participant> Participant::create(const std::string& name) {
    ParticipantConfig cfg;
    cfg.nodeName = name;
    return create(cfg);
}

std::unique_ptr<Participant> Participant::create(const ParticipantConfig& config) {
    auto impl = std::make_unique<Impl>();
    impl->config = config;
    impl->name   = config.nodeName.empty()
        ? "node_" + std::to_string(platform::getProcessId())
        : config.nodeName;
    impl->init();
    return std::unique_ptr<Participant>(new Participant(std::move(impl)));
}

Participant::Participant(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
Participant::~Participant() { shutdown(); }

// ===================== Pub/Sub =====================

std::unique_ptr<Publisher> Participant::createPublisher(
    const std::string& topic, const QoSProfile& qos)
{
    return impl_->messageBus->createPublisher(topic, qos);
}

std::unique_ptr<Subscriber> Participant::createSubscriber(
    const std::string& topic, SubscribeCallback callback, const QoSProfile& qos)
{
    auto sub = impl_->messageBus->createSubscriber(topic, std::move(callback), qos);
    impl_->discovery->announceTopics(impl_->messageBus->localTopics());
    return sub;
}

// ===================== Req/Rep =====================

std::unique_ptr<Requester> Participant::createRequester(
    const std::string& service, const QoSProfile& qos)
{
    return impl_->messageBus->createRequester(service, qos);
}

std::unique_ptr<Replier> Participant::createReplier(
    const std::string& service, RequestHandler handler, const QoSProfile& qos)
{
    auto rep = impl_->messageBus->createReplier(service, std::move(handler), qos);
    impl_->discovery->announceTopics(impl_->messageBus->localTopics());
    return rep;
}

// ===================== Other =====================

void Participant::registerSerializer(std::shared_ptr<ISerializer> s) {
    impl_->messageBus->registerSerializer(std::move(s));
}

void Participant::registerTransport(const std::string& name,
                                     std::shared_ptr<ITransport> transport)
{
    impl_->transportMgr->registerTransport(name, std::move(transport));
}

void Participant::onPeerEvent(PeerEventCallback callback) {
    impl_->peerEventCb = std::move(callback);
}

void Participant::shutdown() {
    if (impl_ && impl_->running.exchange(false)) {
        impl_->discovery->sendFarewell();
        impl_->discovery->stop();
        impl_->messageBus->stop();
        impl_->transportMgr->shutdownAll();
        EMQ_LOG_I("Participant", "Stopped: %s", impl_->name.c_str());
    }
}

uint16_t           Participant::id()      const { return impl_->id; }
const std::string& Participant::name()    const { return impl_->name; }
bool               Participant::isRunning() const { return impl_->running; }

std::vector<std::string> Participant::discoveredPeers() const {
    return impl_->discovery->peerNames();
}

} // namespace embedmq
