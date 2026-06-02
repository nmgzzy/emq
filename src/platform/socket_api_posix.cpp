#include "socket_api.h"
#include "embedmq/platform.h"

#ifdef EMQ_PLATFORM_POSIX

#include <cstring>
#include <errno.h>
#include <vector>
#include <sys/uio.h>

namespace embedmq {
namespace platform {

bool SocketApi::initialize() { return true; }
void SocketApi::cleanup()    {}

SockFd SocketApi::createUdp(bool /*ipv6*/) {
    return ::socket(AF_INET, SOCK_DGRAM, 0);
}

SockFd SocketApi::createTcp(bool /*ipv6*/) {
    return ::socket(AF_INET, SOCK_STREAM, 0);
}

bool SocketApi::setNonBlocking(SockFd sock) {
    int flags = ::fcntl(sock, F_GETFL, 0);
    if (flags < 0) return false;
    return ::fcntl(sock, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool SocketApi::setReuseAddr(SockFd sock) {
    int opt = 1;
    return ::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
                        &opt, sizeof(opt)) == 0;
}

bool SocketApi::setReusePort(SockFd sock) {
#ifdef SO_REUSEPORT
    int opt = 1;
    return ::setsockopt(sock, SOL_SOCKET, SO_REUSEPORT,
                        &opt, sizeof(opt)) == 0;
#else
    (void)sock; return true;
#endif
}

bool SocketApi::setBroadcast(SockFd sock) {
    int opt = 1;
    return ::setsockopt(sock, SOL_SOCKET, SO_BROADCAST,
                        &opt, sizeof(opt)) == 0;
}

bool SocketApi::setRecvBuf(SockFd sock, int size) {
    return ::setsockopt(sock, SOL_SOCKET, SO_RCVBUF,
                        &size, sizeof(size)) == 0;
}

bool SocketApi::setSendBuf(SockFd sock, int size) {
    return ::setsockopt(sock, SOL_SOCKET, SO_SNDBUF,
                        &size, sizeof(size)) == 0;
}

bool SocketApi::setTtl(SockFd sock, int ttl) {
    return ::setsockopt(sock, IPPROTO_IP, IP_TTL,
                        &ttl, sizeof(ttl)) == 0;
}

bool SocketApi::bindAddr(SockFd sock, uint32_t ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(ip);
    addr.sin_port        = htons(port);
    return ::bind(sock, reinterpret_cast<sockaddr*>(&addr),
                  sizeof(addr)) == 0;
}

bool SocketApi::bindAny(SockFd sock, uint16_t port) {
    return bindAddr(sock, INADDR_ANY, port);
}

bool SocketApi::joinMulticast(SockFd sock,
                               const std::string& group,
                               const std::string& /*iface*/) {
    ip_mreq mreq{};
    ::inet_pton(AF_INET, group.c_str(), &mreq.imr_multiaddr);
    mreq.imr_interface.s_addr = INADDR_ANY;
    return ::setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP,
                        &mreq, sizeof(mreq)) == 0;
}

bool SocketApi::leaveMulticast(SockFd sock,
                                const std::string& group,
                                const std::string& /*iface*/) {
    ip_mreq mreq{};
    ::inet_pton(AF_INET, group.c_str(), &mreq.imr_multiaddr);
    mreq.imr_interface.s_addr = INADDR_ANY;
    return ::setsockopt(sock, IPPROTO_IP, IP_DROP_MEMBERSHIP,
                        &mreq, sizeof(mreq)) == 0;
}

bool SocketApi::connect(SockFd sock, const std::string& ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    ::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
    addr.sin_port = htons(port);
    return ::connect(sock, reinterpret_cast<sockaddr*>(&addr),
                     sizeof(addr)) == 0;
}

bool SocketApi::listen(SockFd sock, int backlog) {
    return ::listen(sock, backlog) == 0;
}

SockFd SocketApi::accept(SockFd sock, std::string& peerIp, uint16_t& peerPort) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    SockFd client = ::accept(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    if (client < 0) return INVALID_SOCK;
    char buf[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
    peerIp   = buf;
    peerPort = ntohs(addr.sin_port);
    return client;
}

int SocketApi::sendTo(SockFd sock, const void* data, int len,
                      const std::string& ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    ::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
    addr.sin_port = htons(port);
    return static_cast<int>(::sendto(sock, data, len, 0,
                                     reinterpret_cast<sockaddr*>(&addr),
                                     sizeof(addr)));
}

int SocketApi::recvFrom(SockFd sock, void* buf, int bufLen,
                        std::string& srcIp, uint16_t& srcPort) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    int n = static_cast<int>(::recvfrom(sock, buf, bufLen, 0,
                                         reinterpret_cast<sockaddr*>(&addr),
                                         &len));
    if (n > 0) {
        char ibuf[INET_ADDRSTRLEN];
        ::inet_ntop(AF_INET, &addr.sin_addr, ibuf, sizeof(ibuf));
        srcIp   = ibuf;
        srcPort = ntohs(addr.sin_port);
    }
    return n;
}

int SocketApi::send(SockFd sock, const void* data, int len) {
    return static_cast<int>(::send(sock, data, len, 0));
}

int SocketApi::sendToV(SockFd sock, const IoSlice* slices, int count,
                       const std::string& ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    ::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
    addr.sin_port = htons(port);

    std::vector<iovec> iov(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        iov[i].iov_base = const_cast<void*>(slices[i].data);
        iov[i].iov_len  = slices[i].len;
    }
    msghdr msg{};
    msg.msg_name    = &addr;
    msg.msg_namelen = sizeof(addr);
    msg.msg_iov     = iov.data();
    msg.msg_iovlen  = static_cast<size_t>(count);
    return static_cast<int>(::sendmsg(sock, &msg, 0));
}

int SocketApi::sendV(SockFd sock, const IoSlice* slices, int count) {
    std::vector<iovec> iov(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        iov[i].iov_base = const_cast<void*>(slices[i].data);
        iov[i].iov_len  = slices[i].len;
    }
    return static_cast<int>(::writev(sock, iov.data(), count));
}

int SocketApi::recv(SockFd sock, void* buf, int bufLen) {
    return static_cast<int>(::recv(sock, buf, bufLen, 0));
}

void SocketApi::close(SockFd sock) {
    ::close(sock);
}

uint16_t SocketApi::getLocalPort(SockFd sock) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    return ntohs(addr.sin_port);
}

std::string SocketApi::getLocalIp(SockFd sock) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    char buf[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
    return buf;
}

int  SocketApi::lastError()         { return errno; }
bool SocketApi::wouldBlock(int err) { return err == EAGAIN || err == EWOULDBLOCK; }

} // namespace platform
} // namespace embedmq

#endif // EMQ_PLATFORM_POSIX
