#pragma once
#include "embedmq/types.h"
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace embedmq {

/// 保留消息存储（Phase 2）
class RetainedStore {
public:
    void store(const std::string& topic, const ReceivedMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        retained_[topic] = msg;
    }

    void remove(const std::string& topic) {
        std::lock_guard<std::mutex> lock(mutex_);
        retained_.erase(topic);
    }

    std::optional<ReceivedMessage> get(const std::string& topic) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = retained_.find(topic);
        if (it != retained_.end()) return it->second;
        return std::nullopt;
    }

    /// 获取所有匹配 pattern 的保留消息
    std::vector<ReceivedMessage> getMatching(
        const std::string& pattern,
        bool isWildcard) const
    {
        std::vector<ReceivedMessage> result;
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [topic, msg] : retained_) {
            if (!isWildcard && topic == pattern) {
                result.push_back(msg);
            } else if (isWildcard) {
                // 通配符匹配由 TopicRouter 提供
                (void)topic;
            }
        }
        return result;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return retained_.size();
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ReceivedMessage> retained_;
};

} // namespace embedmq
