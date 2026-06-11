#pragma once
#include "embedmq/qos.h"
#include "embedmq/types.h"
#include "../util/logger.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <unordered_map>
#include <optional>

namespace embedmq {

/// QoS 引擎：管理消息确认/重传、QoS2 两阶段握手与滑动去重窗口。
///
/// QoS2 状态机（MQTT 风格）：
///   发送方 PUBLISH ──(重传至 PUBREC)──▶ 收 PUBREC ─▶ 发 PUBREL（重传至 PUBCOMP）─▶ 收 PUBCOMP 完成
///   接收方 收 PUBLISH ─▶ 去重并投递 + 发 PUBREC；收 PUBREL ─▶ 发 PUBCOMP
/// PUBLISH 阶段挂起在 pending_；PUBREL 阶段挂起在 pendingRel_，二者均参与超时重传。
class QoSEngine {
public:
    using SendCallback = std::function<void(const std::vector<uint8_t>& data)>;

    struct PendingMsg {
        uint32_t             seqId;
        std::vector<uint8_t> data;
        std::chrono::steady_clock::time_point lastSent;
        uint32_t             retryCount{0};
        uint32_t             maxRetries{0};
        uint32_t             retryIntervalMs{0};
        QoSLevel             qosLevel{QoSLevel::BestEffort};
        SendCallback         resend;
    };

    // ---- 发送方：PUBLISH 挂起 ----
    bool addPending(uint32_t seqId, std::vector<uint8_t> data,
                    const QoSProfile& qos, SendCallback resend) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pending_[seqId] = make(seqId, std::move(data), qos, std::move(resend));
        return true;
    }

    void onAck(uint32_t seqId) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pending_.erase(seqId);
    }

    void onNack(uint32_t seqId) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        auto it = pending_.find(seqId);
        if (it != pending_.end()) retry(it->second);
    }

    // QoS2 发送方收到 PUBREC：停止 PUBLISH 重传。返回该 PUBLISH 是否处于挂起态。
    bool onPubrec(uint32_t seqId) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        return pending_.erase(seqId) > 0;
    }

    // QoS2 发送方发出 PUBREL 后登记其重传（直到收到 PUBCOMP）
    void addPendingRel(uint32_t seqId, std::vector<uint8_t> data,
                       const QoSProfile& qos, SendCallback resend) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingRel_[seqId] = make(seqId, std::move(data), qos, std::move(resend));
    }

    // QoS2 发送方收到 PUBCOMP：握手完成
    void onPubcomp(uint32_t seqId) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingRel_.erase(seqId);
    }

    /// 检查并重传超时消息（PUBLISH 与 PUBREL 两阶段），返回已放弃的 seqId 列表
    std::vector<uint32_t> processTimeouts() {
        std::vector<uint32_t> abandoned;
        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(pendingMutex_);
        sweep(pending_,    now, abandoned);
        sweep(pendingRel_, now, abandoned);
        return abandoned;
    }

    // ---- 接收方：QoS2 滑动去重窗口 ----
    // 仅对“是否重复投递”给出判定，内存按窗口大小有界（不再无界增长）。
    bool isDuplicate(uint16_t sourceId, uint32_t seqId) {
        std::lock_guard<std::mutex> lock(dedupMutex_);
        auto& w = windows_[sourceId];
        w.lastTouch = std::chrono::steady_clock::now();
        return w.checkAndMark(seqId);
    }

    // 淘汰长时间无活动的 source 窗口，使 windows_ 内存有上界（即便 16 位 source
    // 滚动也不至于积累到 64K 条目）。空闲窗口被移除；该 source 之后再出现时重建窗口，
    // 最多导致一次跨窗口重复放行，可接受。
    void cleanupDedupWindow(size_t /*maxSize*/ = 0) {
        using namespace std::chrono;
        auto now = steady_clock::now();
        std::lock_guard<std::mutex> lock(dedupMutex_);
        for (auto it = windows_.begin(); it != windows_.end(); ) {
            if (now - it->second.lastTouch > seconds(DEDUP_IDLE_TTL_SEC))
                it = windows_.erase(it);
            else
                ++it;
        }
    }

    size_t pendingCount() const {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        return pending_.size();
    }
    size_t pendingRelCount() const {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        return pendingRel_.size();
    }

private:
    PendingMsg make(uint32_t seqId, std::vector<uint8_t> data,
                    const QoSProfile& qos, SendCallback resend) {
        PendingMsg pm;
        pm.seqId           = seqId;
        pm.data            = std::move(data);
        pm.lastSent        = std::chrono::steady_clock::now();
        pm.maxRetries      = qos.maxRetries;
        pm.retryIntervalMs = qos.retryIntervalMs;
        pm.qosLevel        = qos.level;
        pm.resend          = std::move(resend);
        return pm;
    }

    void retry(PendingMsg& pm) {
        pm.retryCount++;
        pm.lastSent = std::chrono::steady_clock::now();
        if (pm.resend) pm.resend(pm.data);
    }

    void sweep(std::unordered_map<uint32_t, PendingMsg>& m,
               std::chrono::steady_clock::time_point now,
               std::vector<uint32_t>& abandoned) {
        for (auto it = m.begin(); it != m.end(); ) {
            auto& pm = it->second;
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - pm.lastSent).count();
            if (elapsed >= static_cast<long long>(pm.retryIntervalMs)) {
                if (pm.retryCount >= pm.maxRetries) {
                    abandoned.push_back(pm.seqId);
                    it = m.erase(it);
                } else {
                    retry(pm);
                    ++it;
                }
            } else {
                ++it;
            }
        }
    }

    // 每个 source 维护一个有界滑动去重窗口
    static constexpr int DEDUP_IDLE_TTL_SEC = 30; // 窗口空闲多久后回收

    struct DedupWindow {
        static constexpr uint32_t WINDOW = 1024;
        bool     hasAny{false};
        uint32_t highWater{0};
        std::chrono::steady_clock::time_point lastTouch{};
        std::map<uint32_t, char> recent; // 有序，便于按下界滑动淘汰

        bool checkAndMark(uint32_t seq) {
            if (!hasAny) { hasAny = true; highWater = seq; recent[seq] = 1; return false; }
            if (seq < highWater && (highWater - seq) >= WINDOW)
                return true;                       // 太旧：视为已处理（重复）
            if (!recent.emplace(seq, 1).second)
                return true;                       // 窗口内已存在
            if (seq > highWater) highWater = seq;
            uint32_t low = (highWater >= WINDOW) ? (highWater - WINDOW) : 0;
            while (!recent.empty() && recent.begin()->first < low)
                recent.erase(recent.begin());
            return false;
        }
    };

    mutable std::mutex                       pendingMutex_;
    std::unordered_map<uint32_t, PendingMsg> pending_;
    std::unordered_map<uint32_t, PendingMsg> pendingRel_;

    mutable std::mutex                        dedupMutex_;
    std::unordered_map<uint16_t, DedupWindow> windows_;
};

} // namespace embedmq
