#pragma once
#include "embedmq/transport/itransport.h"
#include "../platform/socket_api.h"
#include "../platform/event_loop.h"
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace embedmq {

class UdpTransport : public ITransport {
public:
    UdpTransport() = default;
    ~UdpTransport() override { shutdown(); }

    std::string         typeName()   const override { return "udp"; }
    TransportCapability capability() const override;

    bool init(const std::string& config) override;
    void shutdown()                      override;

    bool send(const Endpoint& to, const uint8_t* data, size_t size) override;
    bool broadcast(const uint8_t* data, size_t size)                 override;
    bool sendv(const Endpoint& to, const IoSlice* slices, size_t count) override;

    void setRecvCallback(TransportRecvCallback cb)   override;
    void setEventCallback(TransportEventCallback cb) override;

    std::vector<Endpoint> localEndpoints() const override;
    bool isActive() const override { return active_; }

private:
    void recvLoop();
    void processFd(SockFd fd);
    void parseConfig(const std::string& jsonConfig);

    SockFd    unicastFd_{INVALID_SOCK};
    SockFd    multicastFd_{INVALID_SOCK};
    std::unique_ptr<platform::EventLoop> eventLoop_; // Linux: epoll 反应堆，替代 select
    std::vector<uint8_t>                 recvBuf_;
    // 单数据报接收缓冲上限（可由 config.transport.udpRecvBufferSize 配置）。
    // 注意：UDP 语义下超过该长度的数据报会被内核截断。
    uint32_t  recvBufferSize_{65536};
    uint16_t  localPort_{0};
    std::string multicastGroup_{"239.255.0.1"};
    uint16_t    multicastPort_{19900};
    bool        multicastEnabled_{true};

    std::atomic<bool>     active_{false};
    std::thread           recvThread_;
    TransportRecvCallback recvCb_;
    TransportEventCallback eventCb_;
    mutable std::mutex    mutex_;
};

} // namespace embedmq
