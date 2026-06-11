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
#include "../platform/process.h"

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

    /// 设置工作线程的 CPU 亲和性（cpu >= 0 时生效），需在 start() 前调用
    void setAffinity(int cpu) { affinityCpu_ = cpu; }

    void start() {
        running_ = true;
        thread_  = std::thread([this]() {
            if (affinityCpu_ >= 0)
                platform::setCurrentThreadAffinity(affinityCpu_);
            loop();
        });
    }

    void stop() {
        running_ = false;
        // 序列化 stop()：owner::stop()、~owner、~TimerWheel 可能多次/并发调用，
        // 不加锁的 joinable()+join() 会在并发下重复 join 同一线程（UB）。
        // join 同时是“在飞回调已完成”的屏障——返回后保证无回调再触碰 owner 成员。
        std::lock_guard<std::mutex> lk(stopMutex_);
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
        TimerId       id;
        uint32_t      intervalMs; // 0 = one-shot
        uint64_t      fireTick;   // 绝对触发 tick（支持任意长延时，不受单层轮长度限制）
        TimerCallback cb;
    };

    TimerId addTimer(uint32_t delayMs, uint32_t intervalMs, TimerCallback cb) {
        TimerId id = nextId_++;
        std::lock_guard<std::mutex> lock(mutex_);
        addTimerLocked(id, delayMs, intervalMs, std::move(cb));
        return id;
    }

    // 必须持有 mutex_ 调用
    void addTimerLocked(TimerId id, uint32_t delayMs, uint32_t intervalMs,
                        TimerCallback cb) {
        uint64_t ticks = std::max<uint64_t>(1u, (delayMs + TICK_MS - 1) / TICK_MS);
        uint64_t fireTick = curTick_ + ticks;
        uint32_t slot = static_cast<uint32_t>(fireTick % SLOT_COUNT);
        slots_[slot].push_back({id, intervalMs, fireTick, std::move(cb)});
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
        std::vector<Timer> due;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            ++curTick_;
            uint32_t slot = static_cast<uint32_t>(curTick_ % SLOT_COUNT);

            // 同一 slot 内可能混有需更晚触发的定时器（fireTick 相差 SLOT_COUNT 的倍数），
            // 仅取出 fireTick <= curTick_ 的，其余保留在槽中等待下一轮。
            auto& bucket = slots_[slot];
            for (auto it = bucket.begin(); it != bucket.end(); ) {
                if (it->fireTick <= curTick_) {
                    // 已取消的定时器：丢弃并清除 cancel 记录（cancelSet_ 持久保存
                    // 直到对应定时器被处理，避免“取消未来定时器”被提前清空而失效）
                    if (cancelSet_.erase(it->id) == 0)
                        due.push_back(std::move(*it));
                    it = bucket.erase(it);
                } else {
                    ++it;
                }
            }
        }

        for (auto& t : due) {
            // 关停过程中不再触发回调，避免与 shutdown 顺序竞态
            if (!running_) break;
            t.cb();
            if (t.intervalMs > 0 && running_) {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!cancelSet_.count(t.id))
                    addTimerLocked(t.id, t.intervalMs, t.intervalMs, t.cb);
            }
        }
    }

    std::vector<std::list<Timer>> slots_;
    std::unordered_set<TimerId>   cancelSet_;
    std::mutex         mutex_;
    std::mutex         stopMutex_;
    uint64_t           curTick_{0};
    std::atomic<TimerId> nextId_{1};
    std::atomic<bool>  running_{false};
    std::thread        thread_;
    int                affinityCpu_{-1};
};

} // namespace util
} // namespace embedmq
