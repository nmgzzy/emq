#pragma once
#include "embedmq/platform.h"
#include <cstdint>
#include <string>

#ifdef EMQ_PLATFORM_WINDOWS
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
using SockFd = SOCKET;
constexpr SockFd INVALID_SOCK = INVALID_SOCKET;
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
using SockFd = int;
constexpr SockFd INVALID_SOCK = -1;
#endif

namespace embedmq {
namespace platform {

/// 跨平台 Socket 工具
class SocketApi {
public:
    /// 平台初始化（Windows 需要 WSAStartup）
    static bool initialize();
    static void cleanup();

    static SockFd createUdp(bool ipv6 = false);
    static SockFd createTcp(bool ipv6 = false);

    static bool   setNonBlocking(SockFd sock);
    static bool   setReuseAddr(SockFd sock);
    static bool   setReusePort(SockFd sock);
    static bool   setBroadcast(SockFd sock);
    static bool   setRecvBuf(SockFd sock, int size);
    static bool   setSendBuf(SockFd sock, int size);
    static bool   setTtl(SockFd sock, int ttl);

    static bool   bindAddr(SockFd sock, uint32_t ip, uint16_t port);
    static bool   bindAny(SockFd sock, uint16_t port);

    static bool   joinMulticast(SockFd sock,
                                const std::string& group,
                                const std::string& iface = "");
    static bool   leaveMulticast(SockFd sock,
                                 const std::string& group,
                                 const std::string& iface = "");

    static bool   connect(SockFd sock, const std::string& ip, uint16_t port);
    static bool   listen(SockFd sock, int backlog = 10);
    static SockFd accept(SockFd sock, std::string& peerIp, uint16_t& peerPort);

    static int    sendTo(SockFd sock, const void* data, int len,
                         const std::string& ip, uint16_t port);
    static int    recvFrom(SockFd sock, void* buf, int bufLen,
                           std::string& srcIp, uint16_t& srcPort);
    static int    send(SockFd sock, const void* data, int len);
    static int    recv(SockFd sock, void* buf, int bufLen);

    static void   close(SockFd sock);

    static uint16_t getLocalPort(SockFd sock);
    static std::string getLocalIp(SockFd sock);

    static int      lastError();
    static bool     wouldBlock(int err);
};

} // namespace platform
} // namespace embedmq
