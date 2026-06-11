#pragma once
#include "embedmq/types.h"
#include "embedmq/qos.h"
#include <algorithm>
#include <atomic>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace embedmq {

using SubscribeCallback = std::function<void(const ReceivedMessage&)>;

struct Subscription {
    uint64_t         id;
    std::string      pattern;
    bool             isWildcard;
    SubscribeCallback callback;
    QoSProfile       qos;
};

class TopicRouter {
public:
    uint64_t addSubscription(const std::string& pattern,
                             SubscribeCallback cb,
                             const QoSProfile& qos)
    {
        uint64_t id = nextId_++;
        bool isWild = (pattern.find('*') != std::string::npos ||
                       pattern.find('#') != std::string::npos);
        std::unique_lock lock(mutex_);
        if (isWild) {
            wildcardSubs_.push_back({id, pattern, true, std::move(cb), qos});
        } else {
            exactSubs_[pattern].push_back({id, pattern, false, std::move(cb), qos});
        }
        return id;
    }

    void removeSubscription(uint64_t id) {
        std::unique_lock lock(mutex_);
        for (auto& [topic, subs] : exactSubs_) {
            subs.erase(
                std::remove_if(subs.begin(), subs.end(),
                    [id](const Subscription& s) { return s.id == id; }),
                subs.end());
        }
        wildcardSubs_.erase(
            std::remove_if(wildcardSubs_.begin(), wildcardSubs_.end(),
                [id](const Subscription& s) { return s.id == id; }),
            wildcardSubs_.end());
    }

    /// 将消息路由到所有匹配的订阅者，返回触发的回调数量
    size_t route(const std::string& topic, const ReceivedMessage& msg) {
        std::vector<SubscribeCallback> cbs;
        {
            std::shared_lock lock(mutex_);
            auto it = exactSubs_.find(topic);
            if (it != exactSubs_.end()) {
                for (auto& sub : it->second)
                    cbs.push_back(sub.callback);
            }
            for (auto& sub : wildcardSubs_) {
                if (matchWildcard(sub.pattern, topic))
                    cbs.push_back(sub.callback);
            }
        }
        for (auto& cb : cbs) cb(msg);
        return cbs.size();
    }

    bool hasSubscribers(const std::string& topic) const {
        std::shared_lock lock(mutex_);
        auto it = exactSubs_.find(topic);
        if (it != exactSubs_.end() && !it->second.empty()) return true;
        for (const auto& sub : wildcardSubs_)
            if (matchWildcard(sub.pattern, topic)) return true;
        return false;
    }

    /// 获取所有已订阅的精确 topic 列表
    std::vector<std::string> allTopics() const {
        std::shared_lock lock(mutex_);
        std::vector<std::string> topics;
        for (auto& [t, _] : exactSubs_)
            if (!_.empty()) topics.push_back(t);
        for (auto& s : wildcardSubs_)
            topics.push_back(s.pattern);
        return topics;
    }

    /// MQTT 风格通配符匹配
    static bool matchWildcard(const std::string& pattern,
                               const std::string& topic)
    {
        auto patParts = split(pattern, '/');
        auto topParts = split(topic,   '/');

        size_t pi = 0, ti = 0;
        while (pi < patParts.size() && ti < topParts.size()) {
            if (patParts[pi] == "#") return true;
            if (patParts[pi] == "*" || patParts[pi] == topParts[ti]) {
                pi++; ti++;
            } else {
                return false;
            }
        }
        // '#' at end also matches zero remaining segments
        if (pi < patParts.size() && patParts[pi] == "#") return true;
        return (pi == patParts.size() && ti == topParts.size());
    }

private:
    static std::vector<std::string> split(const std::string& s, char delim) {
        std::vector<std::string> result;
        std::string token;
        for (char c : s) {
            if (c == delim) { result.push_back(token); token.clear(); }
            else            { token += c; }
        }
        if (!token.empty()) result.push_back(token);
        return result;
    }

    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::vector<Subscription>> exactSubs_;
    std::vector<Subscription>  wildcardSubs_;
    std::atomic<uint64_t>      nextId_{1};
};

} // namespace embedmq
