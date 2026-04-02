#pragma once
#include <algorithm>
#include <chrono>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <atomic>
#include <thread>

// Windows min/max macro workaround
#ifdef max
#undef max
#endif
#ifdef min
#undef min
#endif

namespace embedmq {
namespace util {

using TimerCallback = std::function<void()>;
using TimerId = uint64_t;

/// 简单的时间轮定时器（单层，精度 10ms）
class TimerWheel {
public:
    static constexpr int    SLOT_COUNT   = 512;
    static constexpr int    TICK_MS      = 10;

    explicit TimerWheel() {
        slots_.resize(SLOT_COUNT);
    }

    ~TimerWheel() { stop(); }

    void start() {
        running_ = true;
        thread_  = std::thread([this]() { loop(); });
    }

    void stop() {
        running_ = false;
        if (thread_.joinable()) thread_.join();
    }

    /// 添加一次性定时器，delayMs 后触发
    TimerId addOnce(uint32_t delayMs, TimerCallback cb) {
        return addTimer(delayMs, 0, std::move(cb));
    }

    /// 添加周期性定时器
    TimerId addPeriodic(uint32_t intervalMs, TimerCallback cb) {
        return addTimer(intervalMs, intervalMs, std::move(cb));
    }

    /// 取消定时器
    void cancel(TimerId id) {
        std::lock_guard<std::mutex> lock(mutex_);
        cancelSet_.insert(id); // unordered_set<TimerId>
    }

private:
    struct Timer {
        TimerId      id;
        uint32_t     intervalMs; // 0 = one-shot
        uint32_t     ticksLeft;
        TimerCallback cb;
    };

    TimerId addTimer(uint32_t delayMs, uint32_t intervalMs, TimerCallback cb) {
        TimerId id = nextId_++;
        uint32_t ticks = std::max(1u, (delayMs + TICK_MS - 1) / TICK_MS);

        std::lock_guard<std::mutex> lock(mutex_);
        uint32_t slot = (currentSlot_ + ticks) % SLOT_COUNT;
        slots_[slot].push_back({id, intervalMs, ticks, std::move(cb)});
        return id;
    }

    void loop() {
        auto next = std::chrono::steady_clock::now();
        while (running_) {
            next += std::chrono::milliseconds(TICK_MS);
            std::this_thread::sleep_until(next);
            tick();
        }
    }

    void tick() {
        std::unique_lock<std::mutex> lock(mutex_);
        currentSlot_ = (currentSlot_ + 1) % SLOT_COUNT;
        auto timers   = std::move(slots_[currentSlot_]);
        slots_[currentSlot_].clear();
        auto cancelled = std::move(cancelSet_);
        cancelSet_.clear();
        lock.unlock();

        for (auto& t : timers) {
            if (cancelled.count(t.id)) continue;
            t.cb();
            if (t.intervalMs > 0) {
                addTimer(t.intervalMs, t.intervalMs, t.cb);
            }
        }
    }

    std::vector<std::list<Timer>> slots_;
    std::unordered_set<TimerId>   cancelSet_;
    std::mutex         mutex_;
    uint32_t           currentSlot_{0};
    std::atomic<TimerId> nextId_{1};
    std::atomic<bool>  running_{false};
    std::thread        thread_;
};

} // namespace util
} // namespace embedmq
