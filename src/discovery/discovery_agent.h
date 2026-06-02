#pragma once
#include "embedmq/config.h"
#include "../core/message_bus.h"
#include "peer_registry.h"
#include "../util/timer_wheel.h"
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace embedmq {

class TransportManager;

class DiscoveryAgent {
public:
    DiscoveryAgent(uint16_t nodeId,
                   const std::string& nodeName,
                   const ParticipantConfig& config,
                   TransportManager* tm);
    ~DiscoveryAgent();

    void setOnPeerDiscovered(std::function<void(const PeerInfo&)> cb);
    void setOnPeerLost(std::function<void(uint16_t, const std::string&)> cb);
    // 对端异常掉线时触发其遗嘱（携带遗嘱主题/载荷的对端信息）
    void setOnPeerWill(std::function<void(const PeerInfo&)> cb);
    // 已知对端的订阅/端点发生变化时触发（用于刷新路由表）
    void setOnPeerUpdated(std::function<void(const PeerInfo&)> cb);

    void start();
    void stop();

    void announceTopics(const std::vector<std::string>& topics);
    void sendFarewell();

    std::vector<std::string> peerNames() const;

    // 处理发现消息（由 TransportManager 接收后转发）
    void onDiscoveryMessage(const Endpoint& from, const uint8_t* data, size_t size);

private:
    void sendAnnounce();
    void sendHeartbeat();
    void checkPeerTimeouts();

    // 序列化/反序列化 Announce payload
    std::vector<uint8_t> buildAnnouncePayload() const;
    bool                 parseAnnouncePayload(const uint8_t* data, size_t size,
                                              PeerInfo& out) const;

    uint16_t               nodeId_;
    std::string            nodeName_;
    ParticipantConfig      config_;
    TransportManager*      transportMgr_;
    PeerRegistry           registry_;
    util::TimerWheel       timerWheel_;
    std::atomic<bool>      running_{false};
    std::vector<std::string> localTopics_;
    mutable std::mutex       topicMutex_;

    util::TimerId announceTimerId_{0};
    util::TimerId heartbeatTimerId_{0};
    util::TimerId timeoutTimerId_{0};

    std::function<void(const PeerInfo&)>              onPeerDiscovered_;
    std::function<void(uint16_t, const std::string&)> onPeerLost_;
    std::function<void(const PeerInfo&)>              onPeerWill_;
    std::function<void(const PeerInfo&)>              onPeerUpdated_;
};

} // namespace embedmq
