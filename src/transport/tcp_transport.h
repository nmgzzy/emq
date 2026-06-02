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
    bool sendv(const Endpoint& to, const IoSlice* slices, size_t count) override;
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
    bool recvAll(SockFd fd, uint8_t* buf, size_t size);
    void removeConnection(const Endpoint& ep);

    SockFd                                           listenFd_{INVALID_SOCK};
    uint16_t                                         localPort_{0};
    std::atomic<bool>                                active_{false};
    std::thread                                      acceptThread_;
    std::unordered_map<std::string, SockFd>          connections_;
    std::vector<std::thread>                         clientThreads_;
    mutable std::mutex                               mutex_;
    std::mutex                                       sendMutex_; // 串行化写，防止帧交错
    TransportRecvCallback                            recvCb_;
    TransportEventCallback                           eventCb_;
};

} // namespace embedmq
