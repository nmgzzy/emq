#include "event_loop.h"
#include "embedmq/platform.h"

#ifdef EMQ_PLATFORM_LINUX

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
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
                ::read(wakeupFd_, &val, sizeof(val));
                continue;
            }
            IoCallback cb;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = handlers_.find(static_cast<IoHandle>(fd));
                if (it != handlers_.end()) cb = it->second.cb;
            }
            if (cb) {
                IoEvent ev = IoEvent::Readable;
                if (events[i].events & EPOLLOUT) ev = IoEvent::Writable;
                if (events[i].events & (EPOLLERR | EPOLLHUP)) ev = IoEvent::Error;
                cb(static_cast<IoHandle>(fd), ev);
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
            ::write(wakeupFd_, &val, sizeof(val));
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

std::unique_ptr<EventLoop> EventLoop::create() {
    return std::make_unique<EpollEventLoop>();
}

} // namespace platform
} // namespace embedmq

#endif // EMQ_PLATFORM_LINUX
