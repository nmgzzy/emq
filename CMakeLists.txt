cmake_minimum_required(VERSION 3.16)
project(EmbedMQ VERSION 0.4.0 LANGUAGES CXX)

# ---- 全局 C++ 标准与默认构建类型 ----
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# 静态库以 PIC 编译，便于被 C ABI 共享库静态链接
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ---- 构建选项（对应 xmake option）----
set(EMBEDMQ_PROFILE "full" CACHE STRING "Build profile: full | embedded")
set_property(CACHE EMBEDMQ_PROFILE PROPERTY STRINGS "full" "embedded")

option(EMBEDMQ_BUILD_TESTS    "Build unit tests" ON)
option(EMBEDMQ_BUILD_EXAMPLES "Build examples" ON)
option(EMBEDMQ_BUILD_BENCH    "Build performance benchmarks (Phase 3)" ON)
option(EMBEDMQ_BUILD_CAPI     "Build C ABI shared library (Phase 5)" ON)
option(EMBEDMQ_BUILD_TOOLS    "Build CLI monitoring tools, e.g. emqtop (Phase 5)" ON)
option(EMBEDMQ_ENABLE_TCP     "Enable TCP transport (Phase 2)" OFF)
option(EMBEDMQ_ENABLE_SHM     "Enable shared-memory transport (Phase 3)" ON)
option(EMBEDMQ_ENABLE_IO_URING "Enable io_uring event loop (Linux, experimental, Phase 3)" OFF)

# ---- 解析画像，得到各特性最终开关 ----
if(EMBEDMQ_PROFILE STREQUAL "embedded")
    set(EMBEDMQ_EMBEDDED ON)
else()
    set(EMBEDMQ_EMBEDDED OFF)
endif()

# embedded 优化取向（仅 GCC/Clang，与 xmake.lua 对应）：偏向 CPU/吞吐而非体积。
# -O2（此 I/O 密集型中间件的甜点，避免 -O3 的体积膨胀/i-cache 压力）+ LTO（跨模块
# 内联，利于 CPU 与体积）+ --gc-sections（仅删未引用段）+ strip。在所有后续 target 生效。
#
# 编译取向（codegen）与画像的特性裁剪（关 tools/bench/capi）解耦：默认随 embedded 画像
# 开启，但可单独用 -DEMBEDMQ_EMBEDDED_CODEGEN=ON 在 full 画像上启用 —— 这样既得 embedded
# 的代码生成取向，又保留 emqtop/emq_stress/emq_simnode 等工具（基准/对比 harness 需要）。
set(EMBEDMQ_EMBEDDED_CODEGEN ${EMBEDMQ_EMBEDDED}
    CACHE BOOL "Apply embedded compile orientation (-O2/LTO/gc-sections/strip) independently of profile feature trimming")
if(EMBEDMQ_EMBEDDED_CODEGEN AND NOT MSVC)
    add_compile_options(-O2 -ffunction-sections -fdata-sections)
    add_link_options(-Wl,--gc-sections -s)
    # CMake Release 默认含 -O3，改回 -O2 以免其覆盖上面的 -O2
    string(REPLACE "-O3" "-O2" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
    include(CheckIPOSupported)
    check_ipo_supported(RESULT EMQ_IPO_OK OUTPUT EMQ_IPO_MSG)
    if(EMQ_IPO_OK)
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
    endif()
endif()

if(EMBEDMQ_EMBEDDED)
    set(WANT_TCP OFF)
    set(WANT_IOURING OFF)
    set(WANT_BENCH OFF)
    set(WANT_EXAMPLES OFF)
    set(WANT_CAPI OFF)
    set(WANT_TOOLS OFF)
else()
    set(WANT_TCP ${EMBEDMQ_ENABLE_TCP})
    set(WANT_IOURING ${EMBEDMQ_ENABLE_IO_URING})
    set(WANT_BENCH ${EMBEDMQ_BUILD_BENCH})
    set(WANT_EXAMPLES ${EMBEDMQ_BUILD_EXAMPLES})
    set(WANT_CAPI ${EMBEDMQ_BUILD_CAPI})
    set(WANT_TOOLS ${EMBEDMQ_BUILD_TOOLS})
endif()
set(WANT_SHM ${EMBEDMQ_ENABLE_SHM})

# ---- 平台检测：PAL 源码 / 编译定义 / 系统链接库 ----
set(PAL_SOURCES "")
set(EMQ_PLATFORM_DEFINES "")
set(EMQ_SYSLIBS "")

if(WIN32)
    list(APPEND EMQ_PLATFORM_DEFINES EMQ_PLATFORM_WINDOWS)
    set(PAL_SOURCES
        src/platform/event_loop_iocp.cpp
        src/platform/socket_api_win.cpp)
    set(EMQ_SYSLIBS ws2_32 mswsock advapi32)
    # MSVC: UTF-8 源码 + 禁用安全函数废弃警告
    if(MSVC)
        add_compile_options(/utf-8 /wd4819)
        add_compile_definitions(_CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
    endif()
elseif(APPLE)
    list(APPEND EMQ_PLATFORM_DEFINES EMQ_PLATFORM_MACOS)
    set(PAL_SOURCES
        src/platform/event_loop_kqueue.cpp
        src/platform/socket_api_posix.cpp)
    set(EMQ_SYSLIBS pthread)
elseif(UNIX)
    list(APPEND EMQ_PLATFORM_DEFINES EMQ_PLATFORM_LINUX)
    set(PAL_SOURCES
        src/platform/event_loop_epoll.cpp
        src/platform/socket_api_posix.cpp)
    set(EMQ_SYSLIBS pthread rt)
endif()

# ---- 公共编译告警 ----
if(MSVC)
    add_compile_options(/W3)
else()
    add_compile_options(-Wall)
endif()

# =====================================================================
# 主静态库 embedmq
# =====================================================================
set(EMBEDMQ_SOURCES
    src/core/participant.cpp
    src/core/message_bus.cpp
    src/discovery/discovery_agent.cpp
    src/transport/transport_manager.cpp
    src/transport/udp_transport.cpp
    ${PAL_SOURCES})

if(WANT_TCP)
    list(APPEND EMBEDMQ_SOURCES src/transport/tcp_transport.cpp)
endif()
if(WANT_SHM)
    list(APPEND EMBEDMQ_SOURCES src/transport/shm_transport.cpp)
endif()
if(WANT_IOURING AND UNIX AND NOT APPLE)
    list(APPEND EMBEDMQ_SOURCES src/platform/event_loop_io_uring.cpp)
endif()

add_library(embedmq STATIC ${EMBEDMQ_SOURCES})

target_include_directories(embedmq
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_compile_definitions(embedmq PRIVATE ${EMQ_PLATFORM_DEFINES})
if(EMBEDMQ_EMBEDDED)
    target_compile_definitions(embedmq PRIVATE EMBEDMQ_EMBEDDED_PROFILE)
endif()
if(WANT_TCP)
    target_compile_definitions(embedmq PRIVATE EMBEDMQ_ENABLE_TCP)
endif()
if(WANT_SHM)
    # public：依赖方需感知 SHM 已启用
    target_compile_definitions(embedmq PUBLIC EMBEDMQ_ENABLE_SHM)
endif()
if(WANT_IOURING AND UNIX AND NOT APPLE)
    target_compile_definitions(embedmq PRIVATE EMBEDMQ_ENABLE_IO_URING)
    target_link_libraries(embedmq PUBLIC uring)
endif()

target_link_libraries(embedmq PUBLIC ${EMQ_SYSLIBS})

# 便于内部 target 复用：include 目录 helper
function(emq_target_setup tgt)
    target_include_directories(${tgt} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_compile_definitions(${tgt} PRIVATE ${EMQ_PLATFORM_DEFINES})
    target_link_libraries(${tgt} PRIVATE embedmq ${EMQ_SYSLIBS})
endfunction()

# =====================================================================
# 主入口（仅供演示）
# =====================================================================
add_executable(emq src/main.cpp)
emq_target_setup(emq)

# =====================================================================
# 单元测试（统一可执行文件）
# =====================================================================
if(EMBEDMQ_BUILD_TESTS)
    add_executable(emq_tests
        tests/test_main.cpp
        tests/test_topic_router.cpp
        tests/test_message_codec.cpp
        tests/test_qos_engine.cpp
        tests/test_retained_store.cpp
        tests/test_pal.cpp
        tests/test_pub_sub.cpp
        tests/test_req_rep.cpp
        tests/test_last_will.cpp
        tests/test_phase3.cpp
        tests/test_review_fixes.cpp
        tests/test_refactor_v2.cpp
        tests/test_capi.cpp
        src/capi/embedmq_c.cpp)
    emq_target_setup(emq_tests)
    # Phase 5：C ABI 测试，直接把包装层源码编入测试可执行文件
    target_compile_definitions(emq_tests PRIVATE EMBEDMQ_C_EXPORTS)

    enable_testing()
    add_test(NAME emq_tests COMMAND emq_tests)
endif()

# =====================================================================
# 示例程序
# =====================================================================
if(WANT_EXAMPLES)
    add_executable(example_pub_sub examples/pub_sub/main.cpp)
    emq_target_setup(example_pub_sub)

    add_executable(example_req_rep examples/req_rep/main.cpp)
    emq_target_setup(example_req_rep)
endif()

# =====================================================================
# C ABI 共享库（Phase 5：供 Python ctypes / 其他 FFI 调用）
# =====================================================================
if(WANT_CAPI)
    add_library(embedmq_c SHARED src/capi/embedmq_c.cpp)
    emq_target_setup(embedmq_c)
    target_compile_definitions(embedmq_c PRIVATE EMBEDMQ_C_EXPORTS)
endif()

# =====================================================================
# CLI 监控工具（Phase 5：emqtop / emq_stress）
# =====================================================================
if(WANT_TOOLS)
    add_executable(emqtop tools/emqtop/main.cpp)
    emq_target_setup(emqtop)

    add_executable(emq_stress tools/emq_stress/main.cpp)
    emq_target_setup(emq_stress)

    add_executable(emq_simnode tools/emq_simnode/main.cpp)
    emq_target_setup(emq_simnode)
endif()

# =====================================================================
# 性能基准测试
# =====================================================================
if(WANT_BENCH)
    add_executable(emq_bench bench/bench_main.cpp)
    emq_target_setup(emq_bench)
endif()

# =====================================================================
# 安装规则（供打包 / Buildroot 等下游使用；随各 WANT_ 开关生效）
# =====================================================================
include(GNUInstallDirs)

# 静态库 + 公共头文件（开发期：供 SDK / 其它项目链接）
install(TARGETS embedmq
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/embedmq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# 演示主程序
install(TARGETS emq
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# C ABI 共享库（运行时 / FFI）
if(WANT_CAPI)
    install(TARGETS embedmq_c
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

# CLI 监控工具
if(WANT_TOOLS)
    install(TARGETS emqtop emq_stress emq_simnode
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

# 示例 / 基准（仅在对应开关打开时安装）
if(WANT_EXAMPLES)
    install(TARGETS example_pub_sub example_req_rep
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
if(WANT_BENCH)
    install(TARGETS emq_bench
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
