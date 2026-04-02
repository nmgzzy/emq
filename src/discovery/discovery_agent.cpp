#include "discovery_agent.h"
#include "../transport/transport_manager.h"
#include "../core/message_codec.h"
#include "../util/logger.h"
#include <cstring>

namespace embedmq {

// Announce payload 格式（简化版）：
// [2] participantId
// [1] domainId
// [64] nodeName (null-terminated)
// [1] topicCount
// topicCount * [1 role + 256 topicName]

static constexpr size_t ANNOUNCE_NODENAME_LEN = 64;
static constexpr size_t ANNOUNCE_TOPIC_LEN    = 128;

DiscoveryAgent::DiscoveryAgent(uint16_t nodeId,
                                const std::string& nodeName,
                                const ParticipantConfig& config,
                                TransportManager* tm)
    : nodeId_(nodeId), nodeName_(nodeName), config_(config), transportMgr_(tm)
{
    registry_.setOnDiscovered([this](const PeerInfo& p) {
        if (onPeerDiscovered_) onPeerDiscovered_(p);
    });
    registry_.setOnLost([this](uint16_t id, const std::string& name) {
        if (onPeerLost_) onPeerLost_(id, name);
    });
}

DiscoveryAgent::~DiscoveryAgent() { stop(); }

void DiscoveryAgent::setOnPeerDiscovered(std::function<void(const PeerInfo&)> cb) {
    onPeerDiscovered_ = std::move(cb);
}

void DiscoveryAgent::setOnPeerLost(std::function<void(uint16_t, const std::string&)> cb) {
    onPeerLost_ = std::move(cb);
}

void DiscoveryAgent::start() {
    running_ = true;
    timerWheel_.start();

    // 立即发送一次 Announce
    sendAnnounce();

    announceTimerId_ = timerWheel_.addPeriodic(
        config_.discovery.announceIntervalMs,
        [this]() { sendAnnounce(); });

    heartbeatTimerId_ = timerWheel_.addPeriodic(
        config_.discovery.heartbeatIntervalMs,
        [this]() { sendHeartbeat(); });

    timeoutTimerId_ = timerWheel_.addPeriodic(
        config_.discovery.heartbeatIntervalMs,
        [this]() { checkPeerTimeouts(); });
}

void DiscoveryAgent::stop() {
    if (running_.exchange(false)) {
        timerWheel_.stop();
    }
}

void DiscoveryAgent::announceTopics(const std::vector<std::string>& topics) {
    std::lock_guard<std::mutex> lock(topicMutex_);
    localTopics_ = topics;
}

void DiscoveryAgent::sendFarewell() {
    if (!transportMgr_) return;
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::FAREWELL, nodeId_, 0xFFFF,
        "", Payload{}, qos, 0);
    transportMgr_->broadcast(data.data(), data.size());
    EMQ_LOG_I("Discovery", "Farewell sent (id=%u)", nodeId_);
}

std::vector<std::string> DiscoveryAgent::peerNames() const {
    return registry_.peerNames();
}

void DiscoveryAgent::sendAnnounce() {
    if (!transportMgr_) return;
    auto payload = buildAnnouncePayload();
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::ANNOUNCE, nodeId_, 0xFFFF,
        "", Payload(payload.data(), payload.size()), qos, 0);
    transportMgr_->broadcast(data.data(), data.size());
    EMQ_LOG_D("Discovery", "Announce sent (id=%u name=%s)", nodeId_, nodeName_.c_str());
}

void DiscoveryAgent::sendHeartbeat() {
    if (!transportMgr_) return;
    QoSProfile qos;
    auto data = MessageCodec::encode(
        MessageType::HEARTBEAT, nodeId_, 0xFFFF,
        "", Payload{}, qos, 0);
    transportMgr_->broadcast(data.data(), data.size());
}

void DiscoveryAgent::checkPeerTimeouts() {
    registry_.checkTimeouts(config_.discovery.peerTimeoutMs);
}

void DiscoveryAgent::onDiscoveryMessage(const Endpoint& from,
                                         const uint8_t* data, size_t size)
{
    auto result = MessageCodec::decode(data, size);
    if (!result.valid) return;

    auto msgType = static_cast<MessageType>(result.header.msgType);
    uint16_t senderId = result.header.sourceId;

    if (senderId == nodeId_) return; // 忽略自己的消息

    if (msgType == MessageType::ANNOUNCE) {
        PeerInfo peer;
        if (parseAnnouncePayload(result.payload.data(),
                                  result.payload.size(), peer)) {
            peer.endpoints.push_back(from);
            registry_.addOrUpdate(peer);
        }
        return;
    }

    if (msgType == MessageType::HEARTBEAT) {
        registry_.heartbeat(senderId);
        return;
    }

    if (msgType == MessageType::FAREWELL) {
        registry_.remove(senderId);
        return;
    }
}

std::vector<uint8_t> DiscoveryAgent::buildAnnouncePayload() const {
    std::vector<uint8_t> buf;

    // participantId (2 bytes)
    buf.push_back(static_cast<uint8_t>(nodeId_ & 0xFF));
    buf.push_back(static_cast<uint8_t>((nodeId_ >> 8) & 0xFF));

    // domainId (1 byte)
    buf.push_back(config_.domainId);

    // nodeName (ANNOUNCE_NODENAME_LEN bytes, null-padded)
    buf.resize(buf.size() + ANNOUNCE_NODENAME_LEN, 0);
    size_t copyLen = std::min(nodeName_.size(), ANNOUNCE_NODENAME_LEN - 1);
    std::memcpy(buf.data() + 3, nodeName_.c_str(), copyLen);

    // topicCount (1 byte)
    std::vector<std::string> topics;
    {
        std::lock_guard<std::mutex> lock(topicMutex_);
        topics = localTopics_;
    }
    uint8_t topicCount = static_cast<uint8_t>(std::min<size_t>(topics.size(), 255));
    buf.push_back(topicCount);

    for (uint8_t i = 0; i < topicCount; i++) {
        // role = 0x01 (PUBLISHER or SUBSCRIBER, simplified)
        buf.push_back(0x01);
        // topic name (ANNOUNCE_TOPIC_LEN bytes, null-padded)
        size_t oldSize = buf.size();
        buf.resize(oldSize + ANNOUNCE_TOPIC_LEN, 0);
        size_t tlen = std::min(topics[i].size(), ANNOUNCE_TOPIC_LEN - 1);
        std::memcpy(buf.data() + oldSize, topics[i].c_str(), tlen);
    }

    return buf;
}

bool DiscoveryAgent::parseAnnouncePayload(const uint8_t* data, size_t size,
                                           PeerInfo& out) const
{
    size_t minSize = 2 + 1 + ANNOUNCE_NODENAME_LEN + 1;
    if (size < minSize) return false;

    size_t offset = 0;
    out.id = static_cast<uint16_t>(data[offset]) |
             (static_cast<uint16_t>(data[offset+1]) << 8);
    offset += 2;

    // domainId
    uint8_t domainId = data[offset++];
    if (domainId != config_.domainId) return false;

    // nodeName
    out.name = std::string(reinterpret_cast<const char*>(data + offset),
                           strnlen(reinterpret_cast<const char*>(data + offset),
                                   ANNOUNCE_NODENAME_LEN));
    offset += ANNOUNCE_NODENAME_LEN;

    if (offset >= size) return false;
    uint8_t topicCount = data[offset++];

    for (uint8_t i = 0; i < topicCount; i++) {
        if (offset + 1 + ANNOUNCE_TOPIC_LEN > size) break;
        // uint8_t role = data[offset];
        offset++;
        std::string topic(reinterpret_cast<const char*>(data + offset),
                          strnlen(reinterpret_cast<const char*>(data + offset),
                                  ANNOUNCE_TOPIC_LEN));
        offset += ANNOUNCE_TOPIC_LEN;
        out.subscribedTopics.push_back(topic);
        out.publishedTopics.push_back(topic);
    }

    return true;
}

} // namespace embedmq
