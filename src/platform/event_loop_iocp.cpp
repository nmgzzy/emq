#include "event_loop.h"
#include "embedmq/platform.h"

#ifdef EMQ_PLATFORM_WINDOWS

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winsock2.h>
#include <unordered_map>
#include <mutex>
#include <thread>

namespace embedmq {
namespace platform {

// Windows 平台上 EventLoop 使用简化的基于 WSAWaitForMultipleEvents 的实现
// 完整 IOCP 用于 TCP 流式传输，此处为 Reactor 风格事件分发
class IocpEventLoop : public EventLoop {
public:
    IocpEventLoop() {
        wakeupEvent_ = ::CreateEventW(nullptr, FALSE, FALSE, nullptr);
    }

    ~IocpEventLoop() override {
        stop();
        if (wakeupEvent_ != INVALID_HANDLE_VALUE) {
            ::CloseHandle(wakeupEvent_);
        }
    }

    void addHandle(IoHandle handle, IoEvent interest, IoCallback cb) override {
        std::lock_guard<std::mutex> lock(mutex_);
        handlers_[handle] = {interest, std::move(cb)};
    }

    void removeHandle(IoHandle handle) override {
        std::lock_guard<std::mutex> lock(mutex_);
        handlers_.erase(handle);
    }

    void modifyHandle(IoHandle handle, IoEvent interest) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = handlers_.find(handle);
        if (it != handlers_.end()) {
            it->second.interest = interest;
        }
    }

    void runOnce(int timeoutMs = -1) override {
        std::vector<HANDLE> handles;
        std::vector<IoHandle> ioHandles;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handles.push_back(wakeupEvent_);
            ioHandles.push_back(-1);
            for (auto& [h, info] : handlers_) {
                handles.push_back(reinterpret_cast<HANDLE>(h));
                ioHandles.push_back(h);
            }
        }

        if (handles.empty()) {
            ::Sleep(timeoutMs > 0 ? timeoutMs : 10);
            return;
        }

        DWORD count = static_cast<DWORD>(handles.size());
        DWORD ms = (timeoutMs < 0) ? INFINITE : static_cast<DWORD>(timeoutMs);
        DWORD result = ::WaitForMultipleObjects(count, handles.data(), FALSE, ms);

        if (result == WAIT_FAILED || result == WAIT_TIMEOUT) return;

        DWORD idx = result - WAIT_OBJECT_0;
        if (idx == 0) return; // wakeup

        if (idx < count) {
            IoHandle h = ioHandles[idx];
            IoCallback cb;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = handlers_.find(h);
                if (it != handlers_.end()) cb = it->second.cb;
            }
            if (cb) cb(h, IoEvent::Readable);
        }
    }

    void start() override {
        running_ = true;
        thread_ = std::thread([this]() {
            while (running_) {
                runOnce(100);
            }
        });
    }

    void stop() override {
        running_ = false;
        wakeup();
        if (thread_.joinable()) thread_.join();
    }

    void wakeup() override {
        if (wakeupEvent_ != INVALID_HANDLE_VALUE) {
            ::SetEvent(wakeupEvent_);
        }
    }

    bool isRunning() const override { return running_; }

private:
    struct HandlerInfo {
        IoEvent    interest;
        IoCallback cb;
    };

    HANDLE                                    wakeupEvent_{INVALID_HANDLE_VALUE};
    std::unordered_map<IoHandle, HandlerInfo> handlers_;
    std::mutex                                mutex_;
    std::atomic<bool>                         running_{false};
    std::thread                               thread_;
};

std::unique_ptr<EventLoop> EventLoop::create() {
    return std::make_unique<IocpEventLoop>();
}

} // namespace platform
} // namespace embedmq

#endif // EMQ_PLATFORM_WINDOWS
