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
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& [ep, fd] : connections_)
                platform::SocketApi::close(fd);
            connections_.clear();
        }
        if (acceptThread_.joinable()) acceptThread_.join();
        for (auto& t : clientThreads_)
            if (t.joinable()) t.join();
    }
}

bool TcpTransport::send(const Endpoint& to, const uint8_t* data, size_t size) {
    if (!active_) return false;
    SockFd fd = getOrConnect(to);
    if (fd == INVALID_SOCK) return false;

    // 4-byte length prefix
    uint32_t len = static_cast<uint32_t>(size);
    uint8_t lenBuf[4];
    lenBuf[0] = (len >> 24) & 0xFF;
    lenBuf[1] = (len >> 16) & 0xFF;
    lenBuf[2] = (len >>  8) & 0xFF;
    lenBuf[3] =  len        & 0xFF;

    platform::SocketApi::send(fd, lenBuf, 4);
    return platform::SocketApi::send(fd, data, static_cast<int>(size)) ==
           static_cast<int>(size);
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

        clientThreads_.emplace_back([this, clientFd, peerEp]() {
            clientLoop(clientFd, peerEp);
        });
    }
}

void TcpTransport::clientLoop(SockFd clientFd, const Endpoint& peerEp) {
    std::vector<uint8_t> buf;

    while (active_) {
        uint8_t lenBuf[4];
        int n = platform::SocketApi::recv(clientFd, lenBuf, 4);
        if (n != 4) break;

        uint32_t msgLen = (static_cast<uint32_t>(lenBuf[0]) << 24) |
                          (static_cast<uint32_t>(lenBuf[1]) << 16) |
                          (static_cast<uint32_t>(lenBuf[2]) <<  8) |
                          static_cast<uint32_t>(lenBuf[3]);

        if (msgLen == 0 || msgLen > 10 * 1024 * 1024) break;

        buf.resize(msgLen);
        size_t received = 0;
        while (received < msgLen) {
            int r = platform::SocketApi::recv(clientFd,
                        buf.data() + received,
                        static_cast<int>(msgLen - received));
            if (r <= 0) goto done;
            received += r;
        }

        {
            TransportRecvCallback cb;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                cb = recvCb_;
            }
            if (cb) cb(peerEp, buf.data(), msgLen);
        }
    }
done:
    platform::SocketApi::close(clientFd);
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

    Endpoint peerEp = ep;
    clientThreads_.emplace_back([this, fd, peerEp]() {
        clientLoop(fd, peerEp);
    });
    return fd;
}

} // namespace embedmq
