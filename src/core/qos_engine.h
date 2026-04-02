#pragma once
#include "embedmq/qos.h"
#include "embedmq/types.h"
#include "../util/logger.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <optional>

namespace embedmq {

/// QoS 引擎：管理消息确认、重传和去重
class QoSEngine {
public:
    using AckCallback  = std::function<void(uint32_t seqId)>;
    using SendCallback = std::function<void(const std::vector<uint8_t>& data)>;

    struct PendingMsg {
        uint32_t             seqId;
        std::vector<uint8_t> data;
        std::chrono::steady_clock::time_point lastSent;
        uint32_t             retryCount{0};
        uint32_t             maxRetries;
        uint32_t             retryIntervalMs;
        QoSLevel             qosLevel;
        SendCallback         resend;
    };

    void onAck(uint32_t seqId) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pending_.erase(seqId);
        EMQ_LOG_D("QoSEngine", "ACK received seqId=%u", seqId);
    }

    void onNack(uint32_t seqId) {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        auto it = pending_.find(seqId);
        if (it != pending_.end()) {
            retry(it->second);
        }
    }

    /// 注册待确认消息（QoS 1/2）
    bool addPending(uint32_t seqId,
                    std::vector<uint8_t> data,
                    const QoSProfile& qos,
                    SendCallback resend)
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        PendingMsg pm;
        pm.seqId          = seqId;
        pm.data           = std::move(data);
        pm.lastSent       = std::chrono::steady_clock::now();
        pm.maxRetries     = qos.maxRetries;
        pm.retryIntervalMs = qos.retryIntervalMs;
        pm.qosLevel       = qos.level;
        pm.resend         = std::move(resend);
        pending_[seqId]   = std::move(pm);
        return true;
    }

    /// 检查并重传超时消息，返回已放弃的 seqId 列表
    std::vector<uint32_t> processTimeouts() {
        std::vector<uint32_t> abandoned;
        auto now = std::chrono::steady_clock::now();

        std::lock_guard<std::mutex> lock(pendingMutex_);
        for (auto it = pending_.begin(); it != pending_.end(); ) {
            auto& pm = it->second;
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - pm.lastSent).count();
            if (elapsed >= static_cast<long long>(pm.retryIntervalMs)) {
                if (pm.retryCount >= pm.maxRetries) {
                    EMQ_LOG_W("QoSEngine", "Abandon msg seqId=%u after %u retries",
                              pm.seqId, pm.retryCount);
                    abandoned.push_back(pm.seqId);
                    it = pending_.erase(it);
                } else {
                    retry(pm);
                    ++it;
                }
            } else {
                ++it;
            }
        }
        return abandoned;
    }

    // ---- QoS 2 去重 ----
    bool isDuplicate(uint16_t sourceId, uint32_t seqId) {
        std::lock_guard<std::mutex> lock(dedupMutex_);
        auto key = (static_cast<uint64_t>(sourceId) << 32) | seqId;
        return !received_.insert(key).second;
    }

    void cleanupDedupWindow(size_t maxSize = 10000) {
        std::lock_guard<std::mutex> lock(dedupMutex_);
        if (received_.size() > maxSize) {
            received_.clear(); // 简化：生产环境应使用滑动窗口
        }
    }

    size_t pendingCount() const {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        return pending_.size();
    }

private:
    void retry(PendingMsg& pm) {
        pm.retryCount++;
        pm.lastSent = std::chrono::steady_clock::now();
        EMQ_LOG_D("QoSEngine", "Retry seqId=%u (attempt %u/%u)",
                  pm.seqId, pm.retryCount, pm.maxRetries);
        if (pm.resend) pm.resend(pm.data);
    }

    mutable std::mutex                         pendingMutex_;
    std::unordered_map<uint32_t, PendingMsg>   pending_;

    mutable std::mutex           dedupMutex_;
    std::unordered_set<uint64_t> received_;
};

} // namespace embedmq
