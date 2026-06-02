#include "udp_transport.h"
#include "../util/logger.h"
#include <cstring>
#include <sstream>

namespace embedmq {

TransportCapability UdpTransport::capability() const {
    TransportCapability cap;
    cap.supportsMulticast       = true;
    cap.supportsBroadcast       = true;
    cap.supportsReliable        = false;
    cap.supportsStreaming       = false;
    cap.maxPayloadSize         = 65507;
    cap.estimatedLatencyUs     = 50;
    cap.estimatedBandwidthKbps = 1000000;
    return cap;
}

void UdpTransport::parseConfig(const std::string& cfg) {
    // 简单 JSON 解析（避免第三方依赖）
    auto extract = [&](const std::string& key) -> std::string {
        auto pos = cfg.find("\"" + key + "\"");
        if (pos == std::string::npos) return "";
        pos = cfg.find(':', pos);
        if (pos == std::string::npos) return "";
        pos++;
        while (pos < cfg.size() && (cfg[pos] == ' ' || cfg[pos] == '"')) pos++;
        size_t end = pos;
        while (end < cfg.size() && cfg[end] != '"' && cfg[end] != ',' && cfg[end] != '}')
            end++;
        return cfg.substr(pos, end - pos);
    };

    std::string port = extract("port");
    if (!port.empty()) {
        try { localPort_ = static_cast<uint16_t>(std::stoi(port)); } catch(...) {}
    }
    std::string mg = extract("multicast_group");
    if (!mg.empty()) multicastGroup_ = mg;
    std::string mp = extract("multicast_port");
    if (!mp.empty()) {
        try { multicastPort_ = static_cast<uint16_t>(std::stoi(mp)); } catch(...) {}
    }
    std::string me = extract("multicast_enabled");
    if (!me.empty()) multicastEnabled_ = (me != "0");
    std::string rb = extract("udp_recv_buffer_size");
    if (!rb.empty()) {
        try {
            uint32_t v = static_cast<uint32_t>(std::stoul(rb));
            if (v >= 64) recvBufferSize_ = v; // 设下界，避免连固定头都放不下
        } catch (...) {}
    }
}

bool UdpTransport::init(const std::string& config) {
    parseConfig(config);

    platform::SocketApi::initialize();

    // 单播接收 socket
    unicastFd_ = platform::SocketApi::createUdp();
    if (unicastFd_ == INVALID_SOCK) {
        EMQ_LOG_E("UDP", "Failed to create unicast socket");
        return false;
    }
    platform::SocketApi::setReuseAddr(unicastFd_);
    platform::SocketApi::setNonBlocking(unicastFd_);
    if (!platform::SocketApi::bindAny(unicastFd_, localPort_)) {
        EMQ_LOG_E("UDP", "Failed to bind unicast socket");
        return false;
    }
    localPort_ = platform::SocketApi::getLocalPort(unicastFd_);

    // 多播接收 socket（仅在启用多播时创建并加入多播组）
    multicastFd_ = multicastEnabled_ ? platform::SocketApi::createUdp() : INVALID_SOCK;
    if (multicastFd_ != INVALID_SOCK) {
        platform::SocketApi::setReuseAddr(multicastFd_);
#ifdef EMQ_PLATFORM_POSIX
        platform::SocketApi::setReusePort(multicastFd_);
#endif
        if (platform::SocketApi::bindAny(multicastFd_, multicastPort_)) {
            platform::SocketApi::joinMulticast(multicastFd_, multicastGroup_);
        } else {
            EMQ_LOG_W("UDP", "Failed to bind multicast socket, disabling multicast recv");
            platform::SocketApi::close(multicastFd_);
            multicastFd_ = INVALID_SOCK;
        }
    }

    recvBuf_.resize(recvBufferSize_);

#if defined(EMQ_PLATFORM_LINUX)
    // 线程收敛：UDP 接收接入 epoll 反应堆，取代 select() 的 1024 fd 限制与定时轮询。
    eventLoop_ = platform::EventLoop::create();
    if (eventLoop_) {
        auto cb = [this](platform::IoHandle h, platform::IoEvent) {
            processFd(static_cast<SockFd>(platform::fromIoHandle(h)));
        };
        eventLoop_->addHandle(platform::toIoHandle(unicastFd_),
                              platform::IoEvent::Readable, cb);
        if (multicastFd_ != INVALID_SOCK)
            eventLoop_->addHandle(platform::toIoHandle(multicastFd_),
                                  platform::IoEvent::Readable, cb);
    }
#endif

    active_ = true;
    recvThread_ = std::thread([this]() { recvLoop(); });
    EMQ_LOG_I("UDP", "Initialized on port %u, multicast=%s:%u",
              localPort_, multicastGroup_.c_str(), multicastPort_);
    return true;
}

void UdpTransport::shutdown() {
    if (active_.exchange(false)) {
        if (eventLoop_) eventLoop_->wakeup();
        if (recvThread_.joinable()) recvThread_.join();
        if (eventLoop_) {
            if (unicastFd_   != INVALID_SOCK)
                eventLoop_->removeHandle(platform::toIoHandle(unicastFd_));
            if (multicastFd_ != INVALID_SOCK)
                eventLoop_->removeHandle(platform::toIoHandle(multicastFd_));
            eventLoop_.reset();
        }
        if (unicastFd_   != INVALID_SOCK) platform::SocketApi::close(unicastFd_);
        if (multicastFd_ != INVALID_SOCK) platform::SocketApi::close(multicastFd_);
        unicastFd_   = INVALID_SOCK;
        multicastFd_ = INVALID_SOCK;
        platform::SocketApi::cleanup();
    }
}

bool UdpTransport::send(const Endpoint& to, const uint8_t* data, size_t size) {
    if (!active_ || unicastFd_ == INVALID_SOCK) return false;
    int n = platform::SocketApi::sendTo(unicastFd_, data, static_cast<int>(size),
                                         to.address, to.port);
    return n == static_cast<int>(size);
}

bool UdpTransport::sendv(const Endpoint& to, const IoSlice* slices, size_t count) {
    if (!active_ || unicastFd_ == INVALID_SOCK) return false;
    // 转换为平台层 IoSlice 并通过 sendmsg/WSASendTo 零拷贝发送
    std::vector<platform::IoSlice> pslices(count);
    size_t total = 0;
    for (size_t i = 0; i < count; ++i) {
        pslices[i].data = slices[i].data;
        pslices[i].len  = slices[i].len;
        total += slices[i].len;
    }
    int n = platform::SocketApi::sendToV(unicastFd_, pslices.data(),
                                          static_cast<int>(count),
                                          to.address, to.port);
    return n == static_cast<int>(total);
}

bool UdpTransport::broadcast(const uint8_t* data, size_t size) {
    if (!active_ || unicastFd_ == INVALID_SOCK) return false;
    if (!multicastEnabled_) return false; // 多播关闭时不做组播广播
    int n = platform::SocketApi::sendTo(unicastFd_, data, static_cast<int>(size),
                                         multicastGroup_, multicastPort_);
    return n == static_cast<int>(size);
}

void UdpTransport::setRecvCallback(TransportRecvCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    recvCb_ = std::move(cb);
}

void UdpTransport::setEventCallback(TransportEventCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    eventCb_ = std::move(cb);
}

std::vector<Endpoint> UdpTransport::localEndpoints() const {
    Endpoint ep;
    ep.transportType = "udp";
    ep.port          = localPort_;
    ep.address       = "0.0.0.0";
    return {ep};
}

void UdpTransport::processFd(SockFd fd) {
    std::string srcIp;
    uint16_t    srcPort = 0;
    int n = platform::SocketApi::recvFrom(fd, recvBuf_.data(),
                                          static_cast<int>(recvBuf_.size()),
                                          srcIp, srcPort);
    if (n > 0) {
        Endpoint from;
        from.address       = srcIp;
        from.port          = srcPort;
        from.transportType = "udp";
        TransportRecvCallback cb;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cb = recvCb_;
        }
        if (cb) cb(from, recvBuf_.data(), static_cast<size_t>(n));
    }
}

void UdpTransport::recvLoop() {
#if defined(EMQ_PLATFORM_LINUX)
    // epoll 反应堆：runOnce 带 100ms 超时，关停时由 wakeup() + active_ 立即退出
    if (eventLoop_) {
        while (active_) eventLoop_->runOnce(100);
        return;
    }
#endif
    // 非 Linux（或无事件循环）回退：select() 等待两个 socket
    while (active_) {
        fd_set fds;
        FD_ZERO(&fds);

        SockFd maxFd = INVALID_SOCK;
        if (unicastFd_ != INVALID_SOCK) {
            FD_SET(unicastFd_, &fds);
            if (unicastFd_ > maxFd) maxFd = unicastFd_;
        }
        if (multicastFd_ != INVALID_SOCK) {
            FD_SET(multicastFd_, &fds);
            if (multicastFd_ > maxFd) maxFd = multicastFd_;
        }

        if (maxFd == INVALID_SOCK) break;

        struct timeval tv{0, 100000}; // 100ms
        int ret = ::select(static_cast<int>(maxFd + 1), &fds, nullptr, nullptr, &tv);
        if (ret <= 0) continue;

        if (unicastFd_   != INVALID_SOCK && FD_ISSET(unicastFd_,   &fds)) processFd(unicastFd_);
        if (multicastFd_ != INVALID_SOCK && FD_ISSET(multicastFd_, &fds)) processFd(multicastFd_);
    }
}

} // namespace embedmq
