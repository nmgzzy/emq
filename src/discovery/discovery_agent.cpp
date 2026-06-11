#include "discovery_agent.h"
#include "../transport/transport_manager.h"
#include "../core/message_codec.h"
#include "../platform/process.h"
#include "../util/logger.h"
#include <algorithm>
#include <cstring>

namespace embedmq {

// Announce payload 格式（TLV / 变长，显式小端）：
//   u16 nodeId
//   u8  domainId
//   u8  flags            (bit0 = hasWill)
//   u8  nameLen ; [name]
//   u8  hostLen ; [host]   // 用于同主机判定（SHM 仅在同主机可用）
//   u8  topicCount ; 每项: u8 role(0x01=PUB,0x02=SUB), u16 topicLen, [topic]
//   u8  endpointCount ; 每项: u8 typeLen,[type], u8 addrLen,[addr], u16 port
//   if hasWill: u8 willQos, u8 willRetain, u16 willTopicLen,[willTopic],
//               u32 willPayloadLen,[willPayload]
// 相比旧的定长 64B 名 + 128B/topic，短字符串场景显著更省。

static constexpr uint8_t ROLE_SUB = 0x02;

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
    registry_.setOnWill([this](const PeerInfo& p) {
        if (onPeerWill_) onPeerWill_(p);
    });
    registry_.setOnUpdated([this](const PeerInfo& p) {
        if (onPeerUpdated_) onPeerUpdated_(p);
    });
}

DiscoveryAgent::~DiscoveryAgent() { stop(); }

void DiscoveryAgent::setOnPeerDiscovered(std::function<void(const PeerInfo&)> cb) {
    onPeerDiscovered_ = std::move(cb);
}

void DiscoveryAgent::setOnPeerLost(std::function<void(uint16_t, const std::string&)> cb) {
    onPeerLost_ = std::move(cb);
}

void DiscoveryAgent::setOnPeerWill(std::function<void(const PeerInfo&)> cb) {
    onPeerWill_ = std::move(cb);
}

void DiscoveryAgent::setOnPeerUpdated(std::function<void(const PeerInfo&)> cb) {
    onPeerUpdated_ = std::move(cb);
}

void DiscoveryAgent::start() {
    // 关闭本地自动发现：不启动 announce/heartbeat/timeout，节点仅依赖静态配置或不互联
    if (!config_.discovery.enableLocalDiscovery) {
        EMQ_LOG_I("Discovery", "Local discovery disabled (id=%u)", nodeId_);
        return;
    }

    running_ = true;
    if (config_.threading.pinCpu)
        timerWheel_.setAffinity(config_.threading.cpuAffinity);
    timerWheel_.start();

    // 立即发送一次 Announce
    sendAnnounce();

    // 合并心跳：ANNOUNCE 周期性广播即兼任保活信标（接收方 addOrUpdate 刷新 lastSeen），
    // 不再单独发送 HEARTBEAT，减少嵌入式无线链路上的冗余流量。
    announceTimerId_ = timerWheel_.addPeriodic(
        config_.discovery.announceIntervalMs,
        [this]() { sendAnnounce(); });

    timeoutTimerId_ = timerWheel_.addPeriodic(
        config_.discovery.announceIntervalMs,
        [this]() { checkPeerTimeouts(); });
}

void DiscoveryAgent::stop() {
    if (running_.exchange(false)) {
        timerWheel_.stop();
    }
}

void DiscoveryAgent::announceTopics(const std::vector<std::string>& topics) {
    {
        std::lock_guard<std::mutex> lock(topicMutex_);
        localTopics_ = topics;
    }
    // 订阅集合变化时立即宣布，缩短对端建立路由的时延（稳定期仍按周期广播）
    if (running_) sendAnnounce();
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
            // 端点解析：UDP 以收包来源 from 为准（advertised 多为 0.0.0.0 不可路由）；
            // TCP advertised 地址为 0.0.0.0 时用 from 的 IP 补全；SHM 段名直接采用。
            std::vector<Endpoint> resolved;
            resolved.push_back(from);
            for (auto& e : peer.endpoints) {
                if (e.transportType == "udp") continue; // 用 from 替代
                Endpoint ep = e;
                if (e.transportType == "tcp" &&
                    (e.address == "0.0.0.0" || e.address.empty()))
                    ep.address = from.address;
                resolved.push_back(ep);
            }
            peer.endpoints = std::move(resolved);
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
    std::vector<uint8_t> b;
    auto putU16 = [&](uint16_t v) {
        b.push_back(static_cast<uint8_t>(v & 0xFF));
        b.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    };
    auto putU32 = [&](uint32_t v) {
        for (int i = 0; i < 4; ++i) b.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
    };
    auto putStr8 = [&](const std::string& s) {
        size_t n = std::min<size_t>(s.size(), 0xFF);
        b.push_back(static_cast<uint8_t>(n));
        b.insert(b.end(), s.begin(), s.begin() + n);
    };
    auto putStr16 = [&](const std::string& s) {
        size_t n = std::min<size_t>(s.size(), 0xFFFF);
        putU16(static_cast<uint16_t>(n));
        b.insert(b.end(), s.begin(), s.begin() + n);
    };

    putU16(nodeId_);
    b.push_back(config_.domainId);

    const auto& will = config_.lastWill;
    bool hasWill = will.enabled && !will.topic.empty();
    b.push_back(hasWill ? 0x01 : 0x00);

    putStr8(nodeName_);
    putStr8(platform::getHostName()); // 主机名：供对端判定是否同主机（决定 SHM 可用性）

    std::vector<std::string> topics;
    {
        std::lock_guard<std::mutex> lock(topicMutex_);
        topics = localTopics_;
    }
    uint8_t topicCount = static_cast<uint8_t>(std::min<size_t>(topics.size(), 255));
    b.push_back(topicCount);
    for (uint8_t i = 0; i < topicCount; ++i) {
        b.push_back(ROLE_SUB);   // 仅订阅/服务端点会被宣布
        putStr16(topics[i]);
    }

    // 本地端点列表（供对端按能力选择 SHM/TCP/UDP 数据面）
    std::vector<Endpoint> eps;
    if (transportMgr_) eps = transportMgr_->allLocalEndpoints();
    uint8_t epCount = static_cast<uint8_t>(std::min<size_t>(eps.size(), 255));
    b.push_back(epCount);
    for (uint8_t i = 0; i < epCount; ++i) {
        putStr8(eps[i].transportType);
        putStr8(eps[i].address);
        putU16(eps[i].port);
    }

    if (hasWill) {
        b.push_back(static_cast<uint8_t>(will.qos));
        b.push_back(will.retain ? 1 : 0);
        putStr16(will.topic);
        putU32(static_cast<uint32_t>(will.payload.size()));
        const uint8_t* pd = will.payload.data();
        if (pd) b.insert(b.end(), pd, pd + will.payload.size());
    }
    return b;
}

namespace {
// 带边界检查的小端读取游标
struct Reader {
    const uint8_t* p; size_t size; size_t off{0}; bool ok{true};
    // 防溢出：用减法比较替代 off+n（off+n 在 32 位 size_t 上对攻击者可控的 n 会回绕，
    // 绕过边界检查导致越界读——与 message_codec.h 的 64 位累加同样的防护意图）。
    bool need(size_t n) { if (n > size || off > size - n) { ok = false; return false; } return true; }
    uint8_t  u8()  { if (!need(1)) return 0; return p[off++]; }
    uint16_t u16() { if (!need(2)) return 0; uint16_t v = uint16_t(p[off]) | (uint16_t(p[off+1])<<8); off += 2; return v; }
    uint32_t u32() { if (!need(4)) return 0; uint32_t v = 0; for (int i=0;i<4;i++) v |= uint32_t(p[off+i])<<(8*i); off += 4; return v; }
    std::string str8()  { uint8_t n = u8();  if (!need(n)) return {}; std::string s(reinterpret_cast<const char*>(p+off), n); off += n; return s; }
    std::string str16() { uint16_t n = u16(); if (!need(n)) return {}; std::string s(reinterpret_cast<const char*>(p+off), n); off += n; return s; }
};
} // namespace

bool DiscoveryAgent::parseAnnouncePayload(const uint8_t* data, size_t size,
                                           PeerInfo& out) const
{
    Reader r{data, size};
    out.id = r.u16();
    uint8_t domainId = r.u8();
    if (!r.ok || domainId != config_.domainId) return false;
    uint8_t flags = r.u8();
    bool hasWill = (flags & 0x01) != 0;

    out.name     = r.str8();
    out.hostName = r.str8();

    uint8_t topicCount = r.u8();
    for (uint8_t i = 0; i < topicCount && r.ok; ++i) {
        uint8_t role = r.u8();
        std::string topic = r.str16();
        if (!r.ok) break;
        if (role == ROLE_SUB) out.subscribedTopics.push_back(topic);
        else                  out.publishedTopics.push_back(topic);
    }

    uint8_t epCount = r.u8();
    for (uint8_t i = 0; i < epCount && r.ok; ++i) {
        Endpoint ep;
        ep.transportType = r.str8();
        ep.address       = r.str8();
        ep.port          = r.u16();
        if (r.ok) out.endpoints.push_back(ep);
    }

    if (hasWill) {
        uint8_t  q   = r.u8();
        uint8_t  ret = r.u8();
        std::string wt = r.str16();
        uint32_t plen  = r.u32();
        if (r.ok && r.need(plen)) {
            out.willQos    = q;
            out.willRetain = (ret != 0);
            out.willTopic  = wt;
            if (plen > 0) out.willPayload = Payload(data + r.off, plen);
            out.hasWill = true;
        }
    }

    return r.ok;
}

} // namespace embedmq
