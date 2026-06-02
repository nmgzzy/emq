#pragma once
#include "embedmq/transport/itransport.h"
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace embedmq {

/// 共享内存传输（同主机进程间高速通道）
///
/// - POSIX：shm_open + mmap；Windows：CreateFileMapping + MapViewOfFile。
/// - 每个实例拥有一个「收件箱」共享内存段，内部是一个有界 MPSC 槽位环：
///   多个远端进程作为生产者写入，本实例的轮询线程作为唯一消费者读取。
/// - 寻址：Endpoint.address = 实例名（共享段名）。向某对端发送即打开其收件箱写入。
///
/// 适用：同主机、低延迟、零网络栈开销的本地 IPC。跨主机请使用 UDP/TCP。
class ShmTransport : public ITransport {
public:
    // 单条消息上限（含 40 字节线缆头），超过则该传输拒绝发送
    static constexpr uint32_t DEFAULT_SLOT_SIZE  = 4096;
    static constexpr uint32_t DEFAULT_SLOT_COUNT = 256;

    ShmTransport() = default;
    ~ShmTransport() override { shutdown(); }

    std::string         typeName()   const override { return "shm"; }
    TransportCapability capability() const override;

    bool init(const std::string& config) override;
    void shutdown()                      override;

    bool send(const Endpoint& to, const uint8_t* data, size_t size) override;
    bool sendv(const Endpoint& to, const IoSlice* slices, size_t count) override;
    bool broadcast(const uint8_t* data, size_t size)                 override;

    void setRecvCallback(TransportRecvCallback cb)   override;
    void setEventCallback(TransportEventCallback cb) override;

    std::vector<Endpoint> localEndpoints() const override;
    bool isActive() const override { return active_; }

    const std::string& instanceName() const { return name_; }

    // 因收件箱满或槽位竞争而丢弃的消息数（可观测背压）
    uint64_t droppedCount() const { return dropped_.load(std::memory_order_relaxed); }

private:
    struct Region;             // 平台相关的映射句柄（pImpl）
    struct ShmHeader;

    void   recvLoop();
    Region* openInbox(const std::string& name, bool create);
    void    closeRegion(Region* r);
    bool    writeToRegion(Region* r, const uint8_t* data, size_t size);
    bool    writevToRegion(Region* r, const IoSlice* slices, size_t count);
    void    wakeConsumer(ShmHeader* h);
    Region* resolvePeer(const std::string& target);

    std::string  name_;
    uint32_t     slotSize_{DEFAULT_SLOT_SIZE};
    uint32_t     slotCount_{DEFAULT_SLOT_COUNT};

    Region*      inbox_{nullptr};     // 本实例收件箱（消费者）
    std::atomic<bool>      active_{false};
    std::atomic<uint64_t>  dropped_{0};
    std::thread            recvThread_;
    TransportRecvCallback  recvCb_;
    TransportEventCallback eventCb_;
    mutable std::mutex     mutex_;

    // 已打开的对端收件箱缓存（生产者侧）
    std::mutex   peerMutex_;
    std::unordered_map<std::string, Region*> peerRegions_;
};

} // namespace embedmq
