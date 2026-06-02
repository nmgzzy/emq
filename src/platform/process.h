#pragma once
#include "embedmq/platform.h"
#include <cstdint>
#include <string>

#ifdef EMQ_PLATFORM_WINDOWS
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <pwd.h>
#include <pthread.h>
#if defined(EMQ_PLATFORM_LINUX)
#include <sched.h>
#endif
#endif

#include <thread>

namespace embedmq {
namespace platform {

inline uint64_t getProcessId() {
#ifdef EMQ_PLATFORM_WINDOWS
    return static_cast<uint64_t>(::GetCurrentProcessId());
#else
    return static_cast<uint64_t>(::getpid());
#endif
}

inline std::string getHostName() {
    char buf[256] = {};
#ifdef EMQ_PLATFORM_WINDOWS
    DWORD size = sizeof(buf);
    ::GetComputerNameA(buf, &size);
#else
    ::gethostname(buf, sizeof(buf));
#endif
    return buf;
}

inline std::string getEmbedMqTempDir() {
#ifdef EMQ_PLATFORM_WINDOWS
    char buf[MAX_PATH] = {};
    ::GetTempPathA(sizeof(buf), buf);
    return std::string(buf) + "embedmq\\";
#elif defined(EMQ_PLATFORM_MACOS)
    const char* home = ::getenv("HOME");
    if (!home) {
        struct passwd* pw = ::getpwuid(::getuid());
        home = pw ? pw->pw_dir : "/tmp";
    }
    return std::string(home) + "/Library/Caches/embedmq/";
#else
    return "/tmp/embedmq/";
#endif
}

inline void ensureDirectory(const std::string& path) {
#ifdef EMQ_PLATFORM_WINDOWS
    ::CreateDirectoryA(path.c_str(), nullptr);
#else
    ::mkdir(path.c_str(), 0755);
#endif
}

/// 返回逻辑 CPU 核心数
inline unsigned hardwareConcurrency() {
    unsigned n = std::thread::hardware_concurrency();
    return n == 0 ? 1 : n;
}

/// 将指定 std::thread 绑定到给定 CPU 核心（cpu >= 0 时生效）。
/// 返回是否成功；不支持的平台（如 macOS）返回 false 但不视为错误。
inline bool setThreadAffinity(std::thread& t, int cpu) {
    if (cpu < 0) return false;
#if defined(EMQ_PLATFORM_LINUX)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<unsigned>(cpu) % hardwareConcurrency(), &set);
    return ::pthread_setaffinity_np(t.native_handle(),
                                    sizeof(cpu_set_t), &set) == 0;
#elif defined(EMQ_PLATFORM_WINDOWS)
    DWORD_PTR mask = (static_cast<DWORD_PTR>(1)
                      << (static_cast<unsigned>(cpu) % hardwareConcurrency()));
    return ::SetThreadAffinityMask(
               reinterpret_cast<HANDLE>(t.native_handle()), mask) != 0;
#else
    (void)t;
    return false; // macOS 无公开的硬亲和性 API
#endif
}

/// 绑定调用线程到给定 CPU 核心。
inline bool setCurrentThreadAffinity(int cpu) {
    if (cpu < 0) return false;
#if defined(EMQ_PLATFORM_LINUX)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(static_cast<unsigned>(cpu) % hardwareConcurrency(), &set);
    return ::pthread_setaffinity_np(::pthread_self(),
                                    sizeof(cpu_set_t), &set) == 0;
#elif defined(EMQ_PLATFORM_WINDOWS)
    DWORD_PTR mask = (static_cast<DWORD_PTR>(1)
                      << (static_cast<unsigned>(cpu) % hardwareConcurrency()));
    return ::SetThreadAffinityMask(::GetCurrentThread(), mask) != 0;
#else
    return false;
#endif
}

} // namespace platform
} // namespace embedmq
