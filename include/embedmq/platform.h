#pragma once
#include <cstdint>
#include <string>

// ---- 平台检测 ----
#if defined(_WIN32) || defined(_WIN64)
    #define EMQ_PLATFORM_WINDOWS 1
    #define EMQ_PLATFORM_NAME "windows"
#elif defined(__APPLE__) && defined(__MACH__)
    #include <TargetConditionals.h>
    #define EMQ_PLATFORM_MACOS 1
    #define EMQ_PLATFORM_NAME "macos"
#elif defined(__linux__)
    #define EMQ_PLATFORM_LINUX 1
    #define EMQ_PLATFORM_NAME "linux"
#else
    #error "Unsupported platform"
#endif

#if defined(EMQ_PLATFORM_LINUX) || defined(EMQ_PLATFORM_MACOS)
    #define EMQ_PLATFORM_POSIX 1
#endif

// ---- 编译器检测 ----
#if defined(_MSC_VER)
    #define EMQ_COMPILER_MSVC 1
#elif defined(__clang__)
    #define EMQ_COMPILER_CLANG 1
#elif defined(__GNUC__)
    #define EMQ_COMPILER_GCC 1
#endif

// ---- DLL 导出宏 ----
#ifdef EMQ_PLATFORM_WINDOWS
    #ifdef EMBEDMQ_EXPORTS
        #define EMQ_API __declspec(dllexport)
    #else
        #define EMQ_API
    #endif
#else
    #define EMQ_API __attribute__((visibility("default")))
#endif

// ---- 统一句柄类型 ----
namespace embedmq {
namespace platform {

#ifdef EMQ_PLATFORM_WINDOWS
    using NativeHandle = void*;
    constexpr NativeHandle InvalidNativeHandle = nullptr;
#else
    using NativeHandle = int;
    constexpr NativeHandle InvalidNativeHandle = -1;
#endif

using IoHandle = std::intptr_t;

inline IoHandle toIoHandle(NativeHandle h) {
    return static_cast<IoHandle>(h);
}
inline NativeHandle fromIoHandle(IoHandle h) {
    return static_cast<NativeHandle>(h);
}

} // namespace platform
} // namespace embedmq
