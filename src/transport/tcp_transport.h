#pragma once
#include "embedmq/transport/itransport.h"
#include "../platform/socket_api.h"
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace embedmq {

/// TCP Transport（Phase 2）
class TcpTransport : public ITransport {
public:
    TcpTransport() = default;
    ~TcpTransport() override { shutdown(); }

    std::string         typeName()   const override { return "tcp"; }
    TransportCapability capability() const override;

    bool init(const std::string& config) override;
    void shutdown() override;

    bool send(const Endpoint& to, const uint8_t* data, size_t size) override;
    bool broadcast(const uint8_t* data, size_t size) override { return false; }

    void setRecvCallback(TransportRecvCallback cb)   override;
    void setEventCallback(TransportEventCallback cb) override;

    std::vector<Endpoint> localEndpoints() const override;
    bool isActive() const override { return active_; }

private:
    void acceptLoop();
    void clientLoop(SockFd clientFd, const Endpoint& peerEp);
    SockFd getOrConnect(const Endpoint& ep);
    bool sendAll(SockFd fd, const uint8_t* data, size_t size);

    SockFd                                           listenFd_{INVALID_SOCK};
    uint16_t                                         localPort_{0};
    std::atomic<bool>                                active_{false};
    std::thread                                      acceptThread_;
    std::unordered_map<std::string, SockFd>          connections_;
    std::vector<std::thread>                         clientThreads_;
    mutable std::mutex                               mutex_;
    TransportRecvCallback                            recvCb_;
    TransportEventCallback                           eventCb_;
};

} // namespace embedmq
