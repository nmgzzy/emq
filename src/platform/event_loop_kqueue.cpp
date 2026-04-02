#include "event_loop.h"
#include "embedmq/platform.h"

#ifdef EMQ_PLATFORM_MACOS

#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <unordered_map>
#include <mutex>
#include <thread>

namespace embedmq {
namespace platform {

class KqueueEventLoop : public EventLoop {
public:
    KqueueEventLoop() {
        kqFd_ = ::kqueue();
        ::pipe(wakeupPipe_);
        // 设置非阻塞
        ::fcntl(wakeupPipe_[0], F_SETFL, O_NONBLOCK);
        ::fcntl(wakeupPipe_[1], F_SETFL, O_NONBLOCK);

        struct kevent kev{};
        EV_SET(&kev, wakeupPipe_[0], EVFILT_READ, EV_ADD, 0, 0, nullptr);
        ::kevent(kqFd_, &kev, 1, nullptr, 0, nullptr);
    }

    ~KqueueEventLoop() override {
        stop();
        if (kqFd_ >= 0)         ::close(kqFd_);
        if (wakeupPipe_[0] >= 0) ::close(wakeupPipe_[0]);
        if (wakeupPipe_[1] >= 0) ::close(wakeupPipe_[1]);
    }

    void addHandle(IoHandle handle, IoEvent interest, IoCallback cb) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handlers_[handle] = {interest, std::move(cb)};
        }
        struct kevent kev{};
        if (static_cast<uint8_t>(interest) & static_cast<uint8_t>(IoEvent::Readable))
            EV_SET(&kev, handle, EVFILT_READ,  EV_ADD, 0, 0, nullptr);
        if (static_cast<uint8_t>(interest) & static_cast<uint8_t>(IoEvent::Writable))
            EV_SET(&kev, handle, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
        ::kevent(kqFd_, &kev, 1, nullptr, 0, nullptr);
    }

    void removeHandle(IoHandle handle) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handlers_.erase(handle);
        }
        struct kevent kev{};
        EV_SET(&kev, handle, EVFILT_READ,  EV_DELETE, 0, 0, nullptr);
        ::kevent(kqFd_, &kev, 1, nullptr, 0, nullptr);
        EV_SET(&kev, handle, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        ::kevent(kqFd_, &kev, 1, nullptr, 0, nullptr);
    }

    void modifyHandle(IoHandle handle, IoEvent interest) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = handlers_.find(handle);
        if (it != handlers_.end()) it->second.interest = interest;
    }

    void runOnce(int timeoutMs = -1) override {
        constexpr int MAX_EVENTS = 64;
        struct kevent events[MAX_EVENTS];
        struct timespec ts{0, 100000000L}; // 100ms
        struct timespec* pts = (timeoutMs < 0) ? nullptr : &ts;
        if (timeoutMs >= 0) {
            ts.tv_sec  = timeoutMs / 1000;
            ts.tv_nsec = (timeoutMs % 1000) * 1000000L;
        }

        int n = ::kevent(kqFd_, nullptr, 0, events, MAX_EVENTS, pts);
        for (int i = 0; i < n; i++) {
            auto fd = static_cast<IoHandle>(events[i].ident);
            if (fd == static_cast<IoHandle>(wakeupPipe_[0])) {
                char buf[64];
                ::read(wakeupPipe_[0], buf, sizeof(buf));
                continue;
            }
            IoCallback cb;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = handlers_.find(fd);
                if (it != handlers_.end()) cb = it->second.cb;
            }
            if (cb) {
                IoEvent ev = IoEvent::Readable;
                if (events[i].filter == EVFILT_WRITE) ev = IoEvent::Writable;
                if (events[i].flags & EV_ERROR)        ev = IoEvent::Error;
                cb(fd, ev);
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
        if (wakeupPipe_[1] >= 0) {
            char c = 1;
            ::write(wakeupPipe_[1], &c, 1);
        }
    }

    bool isRunning() const override { return running_; }

private:
    struct HandlerInfo {
        IoEvent    interest;
        IoCallback cb;
    };

    int                                       kqFd_{-1};
    int                                       wakeupPipe_[2]{-1, -1};
    std::unordered_map<IoHandle, HandlerInfo> handlers_;
    std::mutex                                mutex_;
    std::atomic<bool>                         running_{false};
    std::thread                               thread_;
};

std::unique_ptr<EventLoop> EventLoop::create() {
    return std::make_unique<KqueueEventLoop>();
}

} // namespace platform
} // namespace embedmq

#endif // EMQ_PLATFORM_MACOS
