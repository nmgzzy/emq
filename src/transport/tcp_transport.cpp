#include "tcp_transport.h"
#include "../util/logger.h"
#include <cstring>

namespace embedmq {

TransportCapability TcpTransport::capability() const {
    TransportCapability cap;
    cap.supportsMulticast       = false;
    cap.supportsBroadcast       = false;
    cap.supportsReliable        = true;
    cap.supportsStreaming       = true;
    cap.maxPayloadSize         = 1048576;
    cap.estimatedLatencyUs     = 200;
    cap.estimatedBandwidthKbps = 1000000;
    return cap;
}

bool TcpTransport::init(const std::string& /*config*/) {
    platform::SocketApi::initialize();

    listenFd_ = platform::SocketApi::createTcp();
    if (listenFd_ == INVALID_SOCK) return false;

    platform::SocketApi::setReuseAddr(listenFd_);
    if (!platform::SocketApi::bindAny(listenFd_, localPort_)) {
        EMQ_LOG_E("TCP", "Failed to bind");
        return false;
    }
    localPort_ = platform::SocketApi::getLocalPort(listenFd_);
    if (!platform::SocketApi::listen(listenFd_)) {
        EMQ_LOG_E("TCP", "Failed to listen");
        return false;
    }

    active_ = true;
    acceptThread_ = std::thread([this]() { acceptLoop(); });
    EMQ_LOG_I("TCP", "Listening on port %u", localPort_);
    return true;
}

void TcpTransport::shutdown() {
    if (active_.exchange(false)) {
        if (listenFd_ != INVALID_SOCK) {
            platform::SocketApi::close(listenFd_);
            listenFd_ = INVALID_SOCK;
        }
        // 关闭所有连接 fd 并清空表：这会让阻塞在 recv 的 clientLoop 立即返回。
        // 由本处统一 close 并移除条目后，clientLoop 的 closeOwnedConnection 将
        // 找不到匹配条目，从而不会重复 close（避免 fd 复用竞态）。
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& [ep, fd] : connections_)
                platform::SocketApi::close(fd);
            connections_.clear();
        }
        if (acceptThread_.joinable()) acceptThread_.join();
        // 等待所有（已 detach 的）client 线程退出后再返回，确保它们不再触碰 this。
        std::unique_lock<std::mutex> lk(clientCvMutex_);
        clientCv_.wait(lk, [this]() { return clientThreadCount_.load() == 0; });
    }
}

void TcpTransport::spawnClientThread(SockFd fd, const Endpoint& peerEp) {
    clientThreadCount_.fetch_add(1, std::memory_order_acq_rel);
    std::thread([this, fd, peerEp]() {
        clientLoop(fd, peerEp);
        if (clientThreadCount_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lk(clientCvMutex_);
            clientCv_.notify_all();
        }
    }).detach();
}

bool TcpTransport::sendAll(SockFd fd, const uint8_t* data, size_t size) {
    size_t sent = 0;
    while (sent < size) {
        int n = platform::SocketApi::send(fd, data + sent,
                                           static_cast<int>(size - sent));
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// TCP 是字节流，单次 recv 可能返回不足请求的字节数，必须循环读满。
bool TcpTransport::recvAll(SockFd fd, uint8_t* buf, size_t size) {
    size_t got = 0;
    while (got < size) {
        int r = platform::SocketApi::recv(fd, buf + got,
                                          static_cast<int>(size - got));
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

void TcpTransport::closeOwnedConnection(const Endpoint& ep, SockFd fd) {
    std::string key = ep.address + ":" + std::to_string(ep.port);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = connections_.find(key);
    if (it != connections_.end() && it->second == fd) {
        platform::SocketApi::close(fd);   // 本线程仍“拥有”该 fd：唯一 close 点
        connections_.erase(it);
    }
    // 否则该 fd 已被 shutdown() 关闭并移除，或已被新连接替换——不得再次 close
}

static inline void encodeLen(uint32_t len, uint8_t lenBuf[4]) {
    lenBuf[0] = (len >> 24) & 0xFF;
    lenBuf[1] = (len >> 16) & 0xFF;
    lenBuf[2] = (len >>  8) & 0xFF;
    lenBuf[3] =  len        & 0xFF;
}

bool TcpTransport::send(const Endpoint& to, const uint8_t* data, size_t size) {
    if (!active_) return false;
    SockFd fd = getOrConnect(to);
    if (fd == INVALID_SOCK) return false;

    uint8_t lenBuf[4];
    encodeLen(static_cast<uint32_t>(size), lenBuf);

    // 串行化整帧写入，避免并发发送导致长度前缀与负载交错
    std::lock_guard<std::mutex> lock(sendMutex_);
    if (!sendAll(fd, lenBuf, 4)) return false;
    return sendAll(fd, data, size);
}

// 原生 scatter/gather：一次长度前缀 + 逐分片写入，避免调用方拼接缓冲
bool TcpTransport::sendv(const Endpoint& to, const IoSlice* slices, size_t count) {
    if (!active_) return false;
    SockFd fd = getOrConnect(to);
    if (fd == INVALID_SOCK) return false;

    size_t total = 0;
    for (size_t i = 0; i < count; ++i) total += slices[i].len;

    uint8_t lenBuf[4];
    encodeLen(static_cast<uint32_t>(total), lenBuf);

    std::lock_guard<std::mutex> lock(sendMutex_);
    if (!sendAll(fd, lenBuf, 4)) return false;
    for (size_t i = 0; i < count; ++i) {
        if (slices[i].len &&
            !sendAll(fd, static_cast<const uint8_t*>(slices[i].data), slices[i].len))
            return false;
    }
    return true;
}

void TcpTransport::setRecvCallback(TransportRecvCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    recvCb_ = std::move(cb);
}

void TcpTransport::setEventCallback(TransportEventCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    eventCb_ = std::move(cb);
}

std::vector<Endpoint> TcpTransport::localEndpoints() const {
    Endpoint ep;
    ep.transportType = "tcp";
    ep.port          = localPort_;
    ep.address       = "0.0.0.0";
    return {ep};
}

void TcpTransport::acceptLoop() {
    while (active_) {
        std::string peerIp;
        uint16_t    peerPort = 0;

        SockFd clientFd = platform::SocketApi::accept(listenFd_, peerIp, peerPort);
        if (clientFd == INVALID_SOCK) {
            if (!active_) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        Endpoint peerEp;
        peerEp.address       = peerIp;
        peerEp.port          = peerPort;
        peerEp.transportType = "tcp";

        {
            std::lock_guard<std::mutex> lock(mutex_);
            connections_[peerIp + ":" + std::to_string(peerPort)] = clientFd;
        }

        spawnClientThread(clientFd, peerEp);
    }
}

void TcpTransport::clientLoop(SockFd clientFd, const Endpoint& peerEp) {
    std::vector<uint8_t> buf;

    while (active_) {
        uint8_t lenBuf[4];
        // 长度前缀可能被拆包，必须读满 4 字节
        if (!recvAll(clientFd, lenBuf, 4)) break;

        uint32_t msgLen = (static_cast<uint32_t>(lenBuf[0]) << 24) |
                          (static_cast<uint32_t>(lenBuf[1]) << 16) |
                          (static_cast<uint32_t>(lenBuf[2]) <<  8) |
                          static_cast<uint32_t>(lenBuf[3]);

        if (msgLen == 0 || msgLen > 10 * 1024 * 1024) break;

        buf.resize(msgLen);
        if (!recvAll(clientFd, buf.data(), msgLen)) break;

        TransportRecvCallback cb;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cb = recvCb_;
        }
        if (cb) cb(peerEp, buf.data(), msgLen);
    }

    // 连接结束：仅当本线程仍拥有该 fd 时关闭并移除（避免与 shutdown 重复 close）
    closeOwnedConnection(peerEp, clientFd);
}

SockFd TcpTransport::getOrConnect(const Endpoint& ep) {
    std::string key = ep.address + ":" + std::to_string(ep.port);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = connections_.find(key);
    if (it != connections_.end()) return it->second;

    SockFd fd = platform::SocketApi::createTcp();
    if (fd == INVALID_SOCK) return INVALID_SOCK;

    if (!platform::SocketApi::connect(fd, ep.address, ep.port)) {
        platform::SocketApi::close(fd);
        return INVALID_SOCK;
    }
    connections_[key] = fd;

    spawnClientThread(fd, ep);
    return fd;
}

} // namespace embedmq
