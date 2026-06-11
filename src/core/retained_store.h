#pragma once
#include "embedmq/types.h"
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace embedmq {

/// 保留消息存储（Phase 2）
///
/// 支持可配置的生存期（TTL）与条目数上限，避免长期运行时保留消息无界累积：
/// - TTL：每条记录存入时刻起超过 effectiveTtl 即被周期清理（expire）丢弃；
///   单条记录的 TTL = 该条 store() 传入的 lifespanMs（>0 时）否则用全局 defaultTtlMs_。
///   两者皆为 0 表示该条永不过期（保持原始 MQTT 语义）。
/// - maxCount：条目数（按主题计）超过上限时，于 store() 内驱逐最早存入的条目。
class RetainedStore {
public:
    /// 配置默认 TTL 与条目上限（0 表示不启用对应约束）。
    void configure(uint32_t defaultTtlMs, uint32_t maxCount) {
        std::lock_guard<std::mutex> lock(mutex_);
        defaultTtlMs_ = defaultTtlMs;
        maxCount_     = maxCount;
    }

    /// 存入/覆盖某主题的保留消息。
    /// lifespanMs > 0 时作为该条的 TTL 覆盖全局默认；0 表示沿用全局默认。
    void store(const std::string& topic, const ReceivedMessage& msg,
               uint32_t lifespanMs = 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        Entry& e   = retained_[topic];
        e.msg      = msg;
        e.storedAt = std::chrono::steady_clock::now();
        e.ttlMs    = lifespanMs ? lifespanMs : defaultTtlMs_;
        enforceMaxCountLocked();
    }

    void remove(const std::string& topic) {
        std::lock_guard<std::mutex> lock(mutex_);
        retained_.erase(topic);
    }

    std::optional<ReceivedMessage> get(const std::string& topic) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = retained_.find(topic);
        if (it != retained_.end()) return it->second.msg;
        return std::nullopt;
    }

    /// 获取所有匹配 pattern 的保留消息
    std::vector<ReceivedMessage> getMatching(
        const std::string& pattern,
        bool isWildcard) const
    {
        std::vector<ReceivedMessage> result;
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [topic, entry] : retained_) {
            if (!isWildcard && topic == pattern) {
                result.push_back(entry.msg);
            } else if (isWildcard) {
                // 通配符匹配由 TopicRouter 提供
                (void)topic;
            }
        }
        return result;
    }

    /// 清理已过期的保留消息（由 MessageBus 周期任务调用）。
    /// 返回被丢弃的条目数，便于上层记录/统计。
    size_t expire() {
        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(mutex_);
        size_t dropped = 0;
        for (auto it = retained_.begin(); it != retained_.end(); ) {
            if (it->second.ttlMs != 0 &&
                now - it->second.storedAt >=
                    std::chrono::milliseconds(it->second.ttlMs)) {
                it = retained_.erase(it);
                ++dropped;
            } else {
                ++it;
            }
        }
        return dropped;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return retained_.size();
    }

private:
    struct Entry {
        ReceivedMessage                       msg;
        std::chrono::steady_clock::time_point storedAt{};
        uint32_t                              ttlMs{0}; // 0 = 永不过期
    };

    // 条目数超过上限时，逐出最早存入的条目，直至满足上限。调用方须已持锁。
    void enforceMaxCountLocked() {
        if (maxCount_ == 0) return;
        while (retained_.size() > maxCount_) {
            auto oldest = retained_.begin();
            for (auto it = std::next(retained_.begin()); it != retained_.end(); ++it) {
                if (it->second.storedAt < oldest->second.storedAt) oldest = it;
            }
            retained_.erase(oldest);
        }
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> retained_;
    uint32_t defaultTtlMs_{0};
    uint32_t maxCount_{0};
};

} // namespace embedmq
