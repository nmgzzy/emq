#include "event_loop.h"
#include "embedmq/platform.h"

#ifdef EMQ_PLATFORM_LINUX

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <cerrno>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <cstring>

namespace embedmq {
namespace platform {

class EpollEventLoop : public EventLoop {
public:
    EpollEventLoop() {
        epollFd_  = ::epoll_create1(EPOLL_CLOEXEC);
        wakeupFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

        epoll_event ev{};
        ev.events  = EPOLLIN;
        ev.data.fd = wakeupFd_;
        ::epoll_ctl(epollFd_, EPOLL_CTL_ADD, wakeupFd_, &ev);
    }

    ~EpollEventLoop() override {
        stop();
        if (epollFd_  >= 0) ::close(epollFd_);
        if (wakeupFd_ >= 0) ::close(wakeupFd_);
    }

    void addHandle(IoHandle handle, IoEvent interest, IoCallback cb) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handlers_[handle] = {interest, std::move(cb)};
        }
        epoll_event ev{};
        ev.events = toEpollEvents(interest);
        ev.data.fd = static_cast<int>(handle);
        ::epoll_ctl(epollFd_, EPOLL_CTL_ADD, static_cast<int>(handle), &ev);
    }

    void removeHandle(IoHandle handle) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handlers_.erase(handle);
        }
        ::epoll_ctl(epollFd_, EPOLL_CTL_DEL, static_cast<int>(handle), nullptr);
    }

    void modifyHandle(IoHandle handle, IoEvent interest) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = handlers_.find(handle);
            if (it != handlers_.end()) it->second.interest = interest;
        }
        epoll_event ev{};
        ev.events  = toEpollEvents(interest);
        ev.data.fd = static_cast<int>(handle);
        ::epoll_ctl(epollFd_, EPOLL_CTL_MOD, static_cast<int>(handle), &ev);
    }

    void runOnce(int timeoutMs = -1) override {
        constexpr int MAX_EVENTS = 64;
        epoll_event events[MAX_EVENTS];

        int n = ::epoll_wait(epollFd_, events, MAX_EVENTS, timeoutMs);
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if (fd == wakeupFd_) {
                uint64_t val;
                ssize_t nread = ::read(wakeupFd_, &val, sizeof(val));
                if (nread < 0 && errno != EAGAIN) {
                    // eventfd 非阻塞读；除 EAGAIN 外错误无需中断事件循环
                }
                continue;
            }
            IoCallback cb;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = handlers_.find(static_cast<IoHandle>(fd));
                if (it != handlers_.end()) cb = it->second.cb;
            }
            if (cb) {
                // 合并而非覆盖：可读/可写/错误是各自独立的位标志（IoEvent 支持 |/&）。
                // 半关闭连接常同时携带 EPOLLIN+EPOLLHUP——错误不得掩盖可读，
                // 否则缓冲区里尚未读取的入站数据会被丢弃。错误时仍置可读，
                // 让上层有机会把残留数据 drain 完。
                uint8_t bits = 0;
                if (events[i].events & EPOLLIN)  bits |= static_cast<uint8_t>(IoEvent::Readable);
                if (events[i].events & EPOLLOUT) bits |= static_cast<uint8_t>(IoEvent::Writable);
                if (events[i].events & (EPOLLERR | EPOLLHUP))
                    bits |= static_cast<uint8_t>(IoEvent::Error) |
                            static_cast<uint8_t>(IoEvent::Readable);
                cb(static_cast<IoHandle>(fd), static_cast<IoEvent>(bits));
            }
        }
    }

    void start() override {
        running_ = true;
        thread_ = std::thread([this]() {
            while (running_) runOnce(100);
        });
    }

    void stop() override {
        running_ = false;
        wakeup();
        if (thread_.joinable()) thread_.join();
    }

    void wakeup() override {
        if (wakeupFd_ >= 0) {
            uint64_t val = 1;
            ssize_t nwritten = ::write(wakeupFd_, &val, sizeof(val));
            if (nwritten < 0 && errno != EAGAIN) {
                // eventfd 计数达到上限时可能返回 EAGAIN，可安全忽略
            }
        }
    }

    bool isRunning() const override { return running_; }

private:
    static uint32_t toEpollEvents(IoEvent e) {
        uint32_t ev = 0;
        if (static_cast<uint8_t>(e) & static_cast<uint8_t>(IoEvent::Readable))
            ev |= EPOLLIN;
        if (static_cast<uint8_t>(e) & static_cast<uint8_t>(IoEvent::Writable))
            ev |= EPOLLOUT;
        return ev;
    }

    struct HandlerInfo {
        IoEvent    interest;
        IoCallback cb;
    };

    int                                       epollFd_{-1};
    int                                       wakeupFd_{-1};
    std::unordered_map<IoHandle, HandlerInfo> handlers_;
    std::mutex                                mutex_;
    std::atomic<bool>                         running_{false};
    std::thread                               thread_;
};

// 当启用 io_uring（实验性）时，create() 由 event_loop_io_uring.cpp 提供
#ifndef EMBEDMQ_ENABLE_IO_URING
std::unique_ptr<EventLoop> EventLoop::create() {
    return std::make_unique<EpollEventLoop>();
}
#endif

} // namespace platform
} // namespace embedmq

#endif // EMQ_PLATFORM_LINUX
