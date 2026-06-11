#pragma once
#include "embedmq/transport/itransport.h"
#include "../platform/socket_api.h"
#include <atomic>
#include <condition_variable>
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
    // 仅当 connections_[key] 仍指向 fd 时关闭并移除——保证每个 fd 恰好 close 一次，
    // 杜绝 clientLoop 与 shutdown() 重复 close 同一 fd（及 fd 复用竞态）。
    void closeOwnedConnection(const Endpoint& ep, SockFd fd);
    // 启动一个分离的 client 线程并登记计数（供 shutdown 等待全部退出）
    void spawnClientThread(SockFd fd, const Endpoint& peerEp);

    SockFd                                           listenFd_{INVALID_SOCK};
    uint16_t                                         localPort_{0};
    std::atomic<bool>                                active_{false};
    std::thread                                      acceptThread_;
    std::unordered_map<std::string, SockFd>          connections_;
    mutable std::mutex                               mutex_;
    std::mutex                                       sendMutex_; // 串行化写，防止帧交错
    // client 线程改为 detach + 计数：避免 clientThreads_ vector 在连接 churn 下无界
    // 增长，也消除原先 acceptLoop 不持锁 push_back 的数据竞争。shutdown 等待归零。
    std::mutex                                       clientCvMutex_;
    std::condition_variable                          clientCv_;
    std::atomic<int>                                 clientThreadCount_{0};
    TransportRecvCallback                            recvCb_;
    TransportEventCallback                           eventCb_;
};

} // namespace embedmq
