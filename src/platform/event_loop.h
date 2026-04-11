#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <atomic>
#include "embedmq/platform.h"

namespace embedmq {
namespace platform {

enum class IoEvent : uint8_t {
    Readable = 0x01,
    Writable = 0x02,
    Error    = 0x04,
};

inline IoEvent operator|(IoEvent a, IoEvent b) {
    return static_cast<IoEvent>(
        static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}
inline bool operator&(IoEvent a, IoEvent b) {
    return (static_cast<uint8_t>(a) & static_cast<uint8_t>(b)) != 0;
}

using IoCallback = std::function<void(IoHandle handle, IoEvent event)>;

/// 跨平台事件循环接口
class EventLoop {
public:
    /// 工厂方法：自动创建当前平台最优实现
    static std::unique_ptr<EventLoop> create();

    virtual ~EventLoop() = default;

    virtual void addHandle(IoHandle handle, IoEvent interest, IoCallback cb) = 0;
    virtual void removeHandle(IoHandle handle) = 0;
    virtual void modifyHandle(IoHandle handle, IoEvent interest) = 0;

    virtual void runOnce(int timeoutMs = -1) = 0;
    virtual void start() = 0;
    virtual void stop()  = 0;
    virtual void wakeup() = 0;

    virtual bool isRunning() const = 0;
};

} // namespace platform
} // namespace embedmq
