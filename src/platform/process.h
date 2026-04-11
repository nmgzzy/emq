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
#endif

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

} // namespace platform
} // namespace embedmq
