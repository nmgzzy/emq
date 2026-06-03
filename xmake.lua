set_project("EmbedMQ")
set_version("0.4.0")
set_xmakever("2.7.0")

add_rules("mode.debug", "mode.release")
set_languages("c++17")
set_warnings("all")

-- MSVC: UTF-8 源码 + 禁用安全函数废弃警告
if is_plat("windows") then
    add_cxflags("/utf-8", "/wd4819", { force = true })
    add_defines("_CRT_SECURE_NO_WARNINGS", "NOMINMAX", "WIN32_LEAN_AND_MEAN")
end

-- ---- 选项 ----
-- 构建画像：full（默认，开发/桌面，全特性）/ embedded（瘦身：关闭 TCP、示例、基准、io_uring）
option("profile",        { default = "full", values = {"full", "embedded"},
                           description = "Build profile: full | embedded" })
option("build_tests",    { default = true,  description = "Build unit tests" })
option("build_examples", { default = true,  description = "Build examples" })
option("build_bench",    { default = true,  description = "Build performance benchmarks (Phase 3)" })
-- Phase 5：C ABI 共享库（供 Python/FFI）与命令行监控工具
option("build_capi",     { default = true,  description = "Build C ABI shared library (Phase 5)" })
option("build_tools",    { default = true,  description = "Build CLI monitoring tools, e.g. emqtop (Phase 5)" })
-- TCP 默认关闭：嵌入式以 UDP/SHM 为主，按需显式开启 --enable_tcp=y
option("enable_tcp",     { default = false, description = "Enable TCP transport (Phase 2)" })
option("enable_shm",     { default = true,  description = "Enable shared-memory transport (Phase 3)" })
option("enable_io_uring",{ default = false, description = "Enable io_uring event loop (Linux, experimental, Phase 3)" })

-- 解析画像，得到各特性最终开关（embedded 为强约束的瘦身画像）
local embedded     = (get_config("profile") == "embedded")
local want_tcp     = has_config("enable_tcp")     and not embedded
local want_shm     = has_config("enable_shm")
local want_iouring = has_config("enable_io_uring") and not embedded
local want_bench   = has_config("build_bench")    and not embedded
local want_examples= has_config("build_examples") and not embedded
-- C ABI / CLI 工具：嵌入式画像默认不构建（按需 --build_capi=y / --build_tools=y）
local want_capi    = has_config("build_capi")     and not embedded
local want_tools   = has_config("build_tools")    and not embedded
if embedded then
    add_defines("EMBEDMQ_EMBEDDED_PROFILE")
end

-- ---- 平台 PAL 源码选择 ----
local pal_sources = {}

-- 使用 is_plat（目标平台）而非 is_os（构建主机），保证交叉编译时按目标选择 PAL
if is_plat("linux") then
    add_defines("EMQ_PLATFORM_LINUX")
    pal_sources = { "src/platform/event_loop_epoll.cpp",
                    "src/platform/socket_api_posix.cpp" }
elseif is_plat("macosx") then
    add_defines("EMQ_PLATFORM_MACOS")
    pal_sources = { "src/platform/event_loop_kqueue.cpp",
                    "src/platform/socket_api_posix.cpp" }
elseif is_plat("windows") then
    add_defines("EMQ_PLATFORM_WINDOWS")
    pal_sources = { "src/platform/event_loop_iocp.cpp",
                    "src/platform/socket_api_win.cpp" }
end

-- ---- 主入口（仅供演示）----
target("emq")
    set_kind("binary")
    add_deps("embedmq")
    add_includedirs("include", "src")
    add_files("src/main.cpp")
    if is_plat("linux") then
        add_syslinks("pthread", "rt")
    elseif is_plat("macosx") then
        add_syslinks("pthread")
    elseif is_plat("windows") then
        add_syslinks("ws2_32", "mswsock", "advapi32")
    end

-- ---- 主静态库 ----
target("embedmq")
    set_kind("static")
    add_includedirs("include", { public = true })
    add_includedirs("src")
    -- 以 PIC 编译，便于被 C ABI 共享库（embedmq_c）静态链接
    if not is_plat("windows") then
        add_cxflags("-fPIC")
    end

    -- core
    add_files("src/core/participant.cpp")
    add_files("src/core/message_bus.cpp")

    -- discovery
    add_files("src/discovery/discovery_agent.cpp")

    -- transport
    add_files("src/transport/transport_manager.cpp")
    add_files("src/transport/udp_transport.cpp")
    if want_tcp then
        add_defines("EMBEDMQ_ENABLE_TCP")
        add_files("src/transport/tcp_transport.cpp")
    end
    if want_shm then
        add_defines("EMBEDMQ_ENABLE_SHM", { public = true })
        add_files("src/transport/shm_transport.cpp")
    end
    -- io_uring（Linux 可选，实验性）：默认关闭，需内核 5.1+ 与 liburing
    if want_iouring and is_plat("linux") then
        add_defines("EMBEDMQ_ENABLE_IO_URING")
        add_files("src/platform/event_loop_io_uring.cpp")
        add_syslinks("uring")
    end

    -- PAL
    for _, f in ipairs(pal_sources) do
        add_files(f)
    end

    -- 平台链接库
    if is_plat("linux") then
        add_syslinks("pthread", "rt")
    elseif is_plat("macosx") then
        add_syslinks("pthread")
    elseif is_plat("windows") then
        add_syslinks("ws2_32", "mswsock", "advapi32")
    end

-- ---- 单元测试（统一可执行文件）----
if has_config("build_tests") then
    target("emq_tests")
        set_kind("binary")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_files("tests/test_main.cpp")
        add_files("tests/test_topic_router.cpp")
        add_files("tests/test_message_codec.cpp")
        add_files("tests/test_qos_engine.cpp")
        add_files("tests/test_pal.cpp")
        add_files("tests/test_pub_sub.cpp")
        add_files("tests/test_req_rep.cpp")
        add_files("tests/test_last_will.cpp")
        add_files("tests/test_phase3.cpp")
        add_files("tests/test_review_fixes.cpp")
        add_files("tests/test_refactor_v2.cpp")
        -- Phase 5：C ABI 测试，直接把包装层源码编入测试可执行文件
        add_defines("EMBEDMQ_C_EXPORTS")
        add_files("tests/test_capi.cpp")
        add_files("src/capi/embedmq_c.cpp")
        if is_plat("linux") then
            add_syslinks("pthread", "rt")
        elseif is_plat("macosx") then
            add_syslinks("pthread")
        elseif is_plat("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("tests")
end

-- ---- 示例程序 ----
if want_examples then
    target("example_pub_sub")
        set_kind("binary")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_files("examples/pub_sub/main.cpp")
        if is_plat("linux") then
            add_syslinks("pthread", "rt")
        elseif is_plat("macosx") then
            add_syslinks("pthread")
        elseif is_plat("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("examples")

    target("example_req_rep")
        set_kind("binary")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_files("examples/req_rep/main.cpp")
        if is_plat("linux") then
            add_syslinks("pthread", "rt")
        elseif is_plat("macosx") then
            add_syslinks("pthread")
        elseif is_plat("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("examples")
end

-- ---- C ABI 共享库（Phase 5：供 Python ctypes / 其他 FFI 调用）----
if want_capi then
    target("embedmq_c")
        set_kind("shared")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_defines("EMBEDMQ_C_EXPORTS")
        add_files("src/capi/embedmq_c.cpp")
        if is_plat("linux") then
            add_syslinks("pthread", "rt")
        elseif is_plat("macosx") then
            add_syslinks("pthread")
        elseif is_plat("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
end

-- ---- CLI 监控工具（Phase 5：emqtop）----
if want_tools then
    target("emqtop")
        set_kind("binary")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_files("tools/emqtop/main.cpp")
        if is_plat("linux") then
            add_syslinks("pthread", "rt")
        elseif is_plat("macosx") then
            add_syslinks("pthread")
        elseif is_plat("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("tools")

    -- 压力 / 稳定性测试工具
    target("emq_stress")
        set_kind("binary")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_files("tools/emq_stress/main.cpp")
        if is_plat("linux") then
            add_syslinks("pthread", "rt")
        elseif is_plat("macosx") then
            add_syslinks("pthread")
        elseif is_plat("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("tools")
end

-- ---- 性能基准测试 ----
if want_bench then
    target("emq_bench")
        set_kind("binary")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_files("bench/bench_main.cpp")
        if is_plat("linux") then
            add_syslinks("pthread", "rt")
        elseif is_plat("macosx") then
            add_syslinks("pthread")
        elseif is_plat("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("bench")
end
