// io_uring 事件循环（Linux 可选，实验性）
//
// 仅当构建时启用 enable_io_uring 选项（定义 EMBEDMQ_ENABLE_IO_URING）且链接 liburing
// 时才会编译进库。需要 Linux 内核 5.1+（推荐 5.6+）。
//
// 该实现使用 io_uring 的 IORING_OP_POLL_ADD 对一组 fd 做就绪通知，语义上等价于
// epoll，但通过单次 io_uring_enter 系统调用提交/收割，减少系统调用次数；可作为
// 高并发连接场景下 epoll 的替代。默认关闭，未充分性能调优，标记为实验性。
#include "event_loop.h"
#include "embedmq/platform.h"

#if defined(EMQ_PLATFORM_LINUX) && defined(EMBEDMQ_ENABLE_IO_URING)

#include <liburing.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <poll.h>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <cstring>

namespace embedmq {
namespace platform {

class IoUringEventLoop : public EventLoop {
public:
    IoUringEventLoop() {
        io_uring_queue_init(QUEUE_DEPTH, &ring_, 0);
        wakeupFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        armPoll(wakeupFd_, POLLIN);
    }

    ~IoUringEventLoop() override {
        stop();
        io_uring_queue_exit(&ring_);
        if (wakeupFd_ >= 0) ::close(wakeupFd_);
    }

    void addHandle(IoHandle handle, IoEvent interest, IoCallback cb) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            handlers_[handle] = {interest, std::move(cb)};
        }
        armPoll(static_cast<int>(handle), toPollEvents(interest));
    }

    void removeHandle(IoHandle handle) override {
        std::lock_guard<std::mutex> lock(mutex_);
        handlers_.erase(handle);
        // 下一次该 fd 的事件到来时不再重新武装即可自然移除
    }

    void modifyHandle(IoHandle handle, IoEvent interest) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = handlers_.find(handle);
        if (it != handlers_.end()) it->second.interest = interest;
    }

    void runOnce(int timeoutMs = -1) override {
        io_uring_submit(&ring_);

        io_uring_cqe* cqe = nullptr;
        if (timeoutMs >= 0) {
            __kernel_timespec ts{};
            ts.tv_sec  = timeoutMs / 1000;
            ts.tv_nsec = (timeoutMs % 1000) * 1000000L;
            int r = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
            if (r < 0 || !cqe) return;
        } else {
            if (io_uring_wait_cqe(&ring_, &cqe) < 0 || !cqe) return;
        }

        int fd = static_cast<int>(io_uring_cqe_get_data64(cqe));
        int res = cqe->res;
        io_uring_cqe_seen(&ring_, cqe);

        if (fd == wakeupFd_) {
            uint64_t val;
            (void)::read(wakeupFd_, &val, sizeof(val));
            armPoll(wakeupFd_, POLLIN); // 重新武装
            return;
        }

        IoCallback cb;
        IoEvent interest = IoEvent::Readable;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = handlers_.find(static_cast<IoHandle>(fd));
            if (it != handlers_.end()) { cb = it->second.cb; interest = it->second.interest; }
        }
        if (cb && res >= 0) {
            IoEvent ev = IoEvent::Readable;
            if (res & POLLOUT) ev = IoEvent::Writable;
            if (res & (POLLERR | POLLHUP)) ev = IoEvent::Error;
            cb(static_cast<IoHandle>(fd), ev);
            armPoll(fd, toPollEvents(interest)); // 重新武装（多次触发）
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
            (void)::write(wakeupFd_, &val, sizeof(val));
        }
    }

    bool isRunning() const override { return running_; }

private:
    static constexpr unsigned QUEUE_DEPTH = 256;

    static short toPollEvents(IoEvent e) {
        short ev = 0;
        if (static_cast<uint8_t>(e) & static_cast<uint8_t>(IoEvent::Readable)) ev |= POLLIN;
        if (static_cast<uint8_t>(e) & static_cast<uint8_t>(IoEvent::Writable)) ev |= POLLOUT;
        return ev;
    }

    void armPoll(int fd, short events) {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return;
        io_uring_prep_poll_add(sqe, fd, events);
        io_uring_sqe_set_data64(sqe, static_cast<__u64>(fd));
        io_uring_submit(&ring_);
    }

    struct HandlerInfo { IoEvent interest; IoCallback cb; };

    io_uring                                  ring_{};
    int                                       wakeupFd_{-1};
    std::unordered_map<IoHandle, HandlerInfo> handlers_;
    std::mutex                                mutex_;
    std::atomic<bool>                         running_{false};
    std::thread                               thread_;
};

std::unique_ptr<EventLoop> EventLoop::create() {
    return std::make_unique<IoUringEventLoop>();
}

} // namespace platform
} // namespace embedmq

#endif // EMQ_PLATFORM_LINUX && EMBEDMQ_ENABLE_IO_URING
