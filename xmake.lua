set_project("EmbedMQ")
set_version("0.2.0")
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
option("build_tests",    { default = true,  description = "Build unit tests" })
option("build_examples", { default = true,  description = "Build examples" })
option("enable_tcp",     { default = true,  description = "Enable TCP transport (Phase 2)" })

-- ---- 平台 PAL 源码选择 ----
local pal_sources = {}

if is_os("linux") then
    add_defines("EMQ_PLATFORM_LINUX")
    pal_sources = { "src/platform/event_loop_epoll.cpp",
                    "src/platform/socket_api_posix.cpp" }
elseif is_os("macosx") then
    add_defines("EMQ_PLATFORM_MACOS")
    pal_sources = { "src/platform/event_loop_kqueue.cpp",
                    "src/platform/socket_api_posix.cpp" }
elseif is_os("windows") then
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
    if is_os("linux") then
        add_syslinks("pthread", "rt")
    elseif is_os("macosx") then
        add_syslinks("pthread")
    elseif is_os("windows") then
        add_syslinks("ws2_32", "mswsock", "advapi32")
    end

-- ---- 主静态库 ----
target("embedmq")
    set_kind("static")
    add_includedirs("include", { public = true })
    add_includedirs("src")

    -- core
    add_files("src/core/participant.cpp")
    add_files("src/core/message_bus.cpp")

    -- discovery
    add_files("src/discovery/discovery_agent.cpp")

    -- transport
    add_files("src/transport/transport_manager.cpp")
    add_files("src/transport/udp_transport.cpp")
    if has_config("enable_tcp") then
        add_defines("EMBEDMQ_ENABLE_TCP")
        add_files("src/transport/tcp_transport.cpp")
    end

    -- PAL
    for _, f in ipairs(pal_sources) do
        add_files(f)
    end

    -- 平台链接库
    if is_os("linux") then
        add_syslinks("pthread", "rt")
    elseif is_os("macosx") then
        add_syslinks("pthread")
    elseif is_os("windows") then
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
        if is_os("linux") then
            add_syslinks("pthread", "rt")
        elseif is_os("macosx") then
            add_syslinks("pthread")
        elseif is_os("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("tests")
end

-- ---- 示例程序 ----
if has_config("build_examples") then
    target("example_pub_sub")
        set_kind("binary")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_files("examples/pub_sub/main.cpp")
        if is_os("linux") then
            add_syslinks("pthread", "rt")
        elseif is_os("macosx") then
            add_syslinks("pthread")
        elseif is_os("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("examples")

    target("example_req_rep")
        set_kind("binary")
        add_deps("embedmq")
        add_includedirs("include", "src")
        add_files("examples/req_rep/main.cpp")
        if is_os("linux") then
            add_syslinks("pthread", "rt")
        elseif is_os("macosx") then
            add_syslinks("pthread")
        elseif is_os("windows") then
            add_syslinks("ws2_32", "mswsock", "advapi32")
        end
        set_group("examples")
end
