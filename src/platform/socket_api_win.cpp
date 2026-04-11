#include "socket_api.h"
#include "embedmq/platform.h"

#ifdef EMQ_PLATFORM_WINDOWS

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <cstring>

#pragma comment(lib, "ws2_32.lib")

namespace embedmq {
namespace platform {

bool SocketApi::initialize() {
    WSADATA wsaData;
    return ::WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;
}

void SocketApi::cleanup() {
    ::WSACleanup();
}

SockFd SocketApi::createUdp(bool /*ipv6*/) {
    return ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
}

SockFd SocketApi::createTcp(bool /*ipv6*/) {
    return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
}

bool SocketApi::setNonBlocking(SockFd sock) {
    u_long mode = 1;
    return ::ioctlsocket(sock, FIONBIO, &mode) == 0;
}

bool SocketApi::setReuseAddr(SockFd sock) {
    int opt = 1;
    return ::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
                        reinterpret_cast<const char*>(&opt), sizeof(opt)) == 0;
}

bool SocketApi::setReusePort(SockFd /*sock*/) {
    return true; // Windows 不需要 SO_REUSEPORT
}

bool SocketApi::setBroadcast(SockFd sock) {
    int opt = 1;
    return ::setsockopt(sock, SOL_SOCKET, SO_BROADCAST,
                        reinterpret_cast<const char*>(&opt), sizeof(opt)) == 0;
}

bool SocketApi::setRecvBuf(SockFd sock, int size) {
    return ::setsockopt(sock, SOL_SOCKET, SO_RCVBUF,
                        reinterpret_cast<const char*>(&size), sizeof(size)) == 0;
}

bool SocketApi::setSendBuf(SockFd sock, int size) {
    return ::setsockopt(sock, SOL_SOCKET, SO_SNDBUF,
                        reinterpret_cast<const char*>(&size), sizeof(size)) == 0;
}

bool SocketApi::setTtl(SockFd sock, int ttl) {
    return ::setsockopt(sock, IPPROTO_IP, IP_TTL,
                        reinterpret_cast<const char*>(&ttl), sizeof(ttl)) == 0;
}

bool SocketApi::bindAddr(SockFd sock, uint32_t ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(ip);
    addr.sin_port        = htons(port);
    return ::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
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
                        reinterpret_cast<const char*>(&mreq), sizeof(mreq)) == 0;
}

bool SocketApi::leaveMulticast(SockFd sock,
                                const std::string& group,
                                const std::string& /*iface*/) {
    ip_mreq mreq{};
    ::inet_pton(AF_INET, group.c_str(), &mreq.imr_multiaddr);
    mreq.imr_interface.s_addr = INADDR_ANY;
    return ::setsockopt(sock, IPPROTO_IP, IP_DROP_MEMBERSHIP,
                        reinterpret_cast<const char*>(&mreq), sizeof(mreq)) == 0;
}

bool SocketApi::connect(SockFd sock, const std::string& ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    ::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
    addr.sin_port = htons(port);
    return ::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
}

bool SocketApi::listen(SockFd sock, int backlog) {
    return ::listen(sock, backlog) == 0;
}

SockFd SocketApi::accept(SockFd sock, std::string& peerIp, uint16_t& peerPort) {
    sockaddr_in addr{};
    int len = sizeof(addr);
    SockFd client = ::accept(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    if (client == INVALID_SOCKET) return INVALID_SOCK;
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
    return ::sendto(sock, static_cast<const char*>(data), len, 0,
                    reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
}

int SocketApi::recvFrom(SockFd sock, void* buf, int bufLen,
                        std::string& srcIp, uint16_t& srcPort) {
    sockaddr_in addr{};
    int len = sizeof(addr);
    int n = ::recvfrom(sock, static_cast<char*>(buf), bufLen, 0,
                       reinterpret_cast<sockaddr*>(&addr), &len);
    if (n > 0) {
        char ibuf[INET_ADDRSTRLEN];
        ::inet_ntop(AF_INET, &addr.sin_addr, ibuf, sizeof(ibuf));
        srcIp   = ibuf;
        srcPort = ntohs(addr.sin_port);
    }
    return n;
}

int SocketApi::send(SockFd sock, const void* data, int len) {
    return ::send(sock, static_cast<const char*>(data), len, 0);
}

int SocketApi::recv(SockFd sock, void* buf, int bufLen) {
    return ::recv(sock, static_cast<char*>(buf), bufLen, 0);
}

void SocketApi::close(SockFd sock) {
    ::closesocket(sock);
}

uint16_t SocketApi::getLocalPort(SockFd sock) {
    sockaddr_in addr{};
    int len = sizeof(addr);
    ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    return ntohs(addr.sin_port);
}

std::string SocketApi::getLocalIp(SockFd sock) {
    sockaddr_in addr{};
    int len = sizeof(addr);
    ::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len);
    char buf[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
    return buf;
}

int  SocketApi::lastError()         { return ::WSAGetLastError(); }
bool SocketApi::wouldBlock(int err) { return err == WSAEWOULDBLOCK; }

} // namespace platform
} // namespace embedmq

#endif // EMQ_PLATFORM_WINDOWS
