#pragma once
#include "../core/message_bus.h"
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace embedmq {

struct PeerRecord {
    PeerInfo   info;
    std::chrono::steady_clock::time_point lastSeen;
    bool       alive{true};
};

class PeerRegistry {
public:
    using PeerDiscoveredCb = std::function<void(const PeerInfo&)>;
    using PeerLostCb       = std::function<void(uint16_t, const std::string&)>;
    using PeerWillCb       = std::function<void(const PeerInfo&)>;

    void setOnDiscovered(PeerDiscoveredCb cb) { onDiscovered_ = std::move(cb); }
    void setOnLost(PeerLostCb cb)             { onLost_       = std::move(cb); }
    void setOnWill(PeerWillCb cb)             { onWill_       = std::move(cb); }

    void addOrUpdate(const PeerInfo& peer) {
        bool isNew = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            isNew = (records_.find(peer.id) == records_.end());
            records_[peer.id] = {peer, std::chrono::steady_clock::now(), true};
        }
        if (isNew && onDiscovered_) onDiscovered_(peer);
    }

    /// 移除对端。triggerWill=true 表示异常掉线（超时），需要代为发布其遗嘱；
    /// triggerWill=false 表示优雅退出（收到 FAREWELL），遗嘱被丢弃。
    void remove(uint16_t peerId, bool triggerWill = false) {
        std::string name;
        PeerInfo    info;
        bool        fireWill = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = records_.find(peerId);
            if (it == records_.end()) return;
            info     = it->second.info;
            name     = info.name;
            fireWill = triggerWill && info.hasWill;
            records_.erase(it);
        }
        if (fireWill && onWill_) onWill_(info);
        if (onLost_) onLost_(peerId, name);
    }

    /// 检查超时节点，返回已超时的 peer id 列表。超时属于异常掉线，触发遗嘱。
    std::vector<uint16_t> checkTimeouts(uint32_t timeoutMs) {
        std::vector<uint16_t> dead;
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& [id, rec] : records_) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - rec.lastSeen).count();
                if (elapsed > static_cast<long long>(timeoutMs)) {
                    dead.push_back(id);
                }
            }
        }
        for (auto id : dead) remove(id, /*triggerWill=*/true);
        return dead;
    }

    void heartbeat(uint16_t peerId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = records_.find(peerId);
        if (it != records_.end()) it->second.lastSeen = std::chrono::steady_clock::now();
    }

    std::vector<std::string> peerNames() const {
        std::vector<std::string> names;
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [id, rec] : records_) names.push_back(rec.info.name);
        return names;
    }

    std::vector<PeerInfo> allPeers() const {
        std::vector<PeerInfo> peers;
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [id, rec] : records_) peers.push_back(rec.info);
        return peers;
    }

    size_t count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return records_.size();
    }

private:
    mutable std::mutex                         mutex_;
    std::unordered_map<uint16_t, PeerRecord>   records_;
    PeerDiscoveredCb                           onDiscovered_;
    PeerLostCb                                 onLost_;
    PeerWillCb                                 onWill_;
};

} // namespace embedmq
