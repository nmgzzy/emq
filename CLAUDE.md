# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目简介

EmbedMQ 是一个去中心化（无 Broker）的 C++17 通信中间件，受 DDS / ZMQ / MQTT 启发。它在可插拔的传输层之上提供发布-订阅（1:N）与请求-响应（1:1）两种模式，并支持 UDP 多播自发现、分级 QoS、保留消息与遗嘱消息。零第三方依赖——仅依赖 C++17 标准库与平台原生 API。目标平台为嵌入式 Linux 及桌面（Windows / Linux / macOS）。

代码注释与文档以中文为主；编辑既有文件时请与上下文保持同一语言。

## 构建

仓库中并存两套构建系统，新增/删除源文件或构建选项时**必须同步修改**：**`xmake.lua`**（主）与 **`CMakeLists.txt`**（兼容）。改动其一的源文件/目标列表时，请在另一个里同步。

xmake（主）：
```bash
xmake f -m debug && xmake           # 或 -m release
xmake f --profile=embedded && xmake # 瘦身画像：关闭 TCP/示例/基准/io_uring/capi/工具
xmake build embedmq_c               # 仅构建 C ABI 共享库（Python 绑定需要）
```

CMake（输出已配置于 `build-cmake/`）：
```bash
cmake -S . -B build-cmake && cmake --build build-cmake
ctest --test-dir build-cmake
```

主要特性开关（xmake `--name=y/n`，CMake `-DEMBEDMQ_NAME=ON/OFF`）：`enable_tcp`（默认**关闭**——TCP 传输为实验性）、`enable_shm`（默认开）、`enable_io_uring`（Linux，实验性，默认关）、`build_capi`、`build_tools`、`build_bench`。`embedded` 画像会强制关闭 TCP/io_uring/bench/examples/capi/tools，覆盖各独立开关。

构建产物输出至 `build/<plat>/<arch>/<mode>/`（xmake）——例如 `build/linux/x86_64/release/`。

## 测试

统一为单一测试可执行文件 `emq_tests`，使用 `tests/test_framework.h` 中的自研框架（无外部测试库）。每个测试文件声明 `#define EMQ_TEST_MODULE "name"`，并用 `TEST(name) { ... }` 宏；测试通过静态初始化自动注册。

```bash
xmake run emq_tests                 # 全部模块
xmake run emq_tests pub_sub req_rep # 仅运行指定模块
xmake run emq_tests --list          # 列出模块名
```
粒度最细到模块——要“跑单个测试”就跑它所在的模块。模块名即各 `tests/test_*.cpp` 中的 `EMQ_TEST_MODULE` 字符串。

## 工具

```bash
xmake run emqtop monitor [topic]        # 订阅 + 实时拓扑/速率统计
xmake run emqtop pub <topic> <msg> [-n N] [-i ms]
xmake run emqtop req <service> <msg>    # / emqtop echo <service> 注册回显服务
xmake run emq_stress all                # 吞吐/扇出/并发/请求响应/churn/soak
python3 bindings/python/example.py      # Python ctypes 绑定（需先构建 embedmq_c）
python3 bindings/python/stress.py all   # 经由 Python 绑定的等价压力套件
```
Python 绑定（`bindings/python/embedmq.py`）通过 `$EMBEDMQ_LIB` 或扫描 `build/` 定位共享库；运行前需先构建 `embedmq_c`。

## 架构

严格分层，自顶向下（完整设计见 `docs/architecture.md`，以其为准——其“实现现状/v0.4”前言在与正文冲突时优先）：

- **User API**（`include/embedmq/embedmq.h`、`src/core/participant.cpp`）——一切实体均经由 `Participant` 创建：`createPublisher/Subscriber/Requester/Replier`。公开类采用 pImpl 惯用法（`struct Impl`）；唯一的公开头文件汇入 `types.h`、`qos.h`、`config.h`、`platform.h`、`transport/itransport.h`。
- **Core**（`src/core/`）——`MessageBus`（`message_bus.cpp`，核心：本地分发 + 远程发送、请求关联、重传定时器）驱动 `TopicRouter`（通配符 `*`/`#` 匹配）、`QoSEngine`（QoS 0/1/2 状态机，含 PUBLISH→PUBREC→PUBREL→PUBCOMP 握手与按 source 的去重窗口）、`message_codec.h`（线缆协议）与 `retained_store.h`。
- **Discovery**（`src/discovery/`）——`DiscoveryAgent` + `PeerRegistry`。UDP 多播 `239.255.0.1:19900`。发现 **v2** 使用 TLV/变长 ANNOUNCE 携带 endpoint 列表；周期 ANNOUNCE 兼任心跳（无单独 HEARTBEAT 包）。
- **Transport 插件**（`src/transport/`）——均实现 `ITransport`（`include/embedmq/transport/itransport.h`），由 `TransportManager` 管理。已实现：UDP（默认，Linux 下有自己的 epoll 反应堆）、SHM（同主机 IPC，含 layout/几何校验 + futex 唤醒）、TCP（实验性，默认关）。串口/BLE 仅为占位。配置项 `enableLocalIpc/enableSerial/enableBle` 为保留/空操作。
- **平台抽象层（PAL）**（`src/platform/`）——每个 OS 原语一个源文件，按目标平台在构建期选择：事件循环（`event_loop_epoll`/`_kqueue`/`_iocp`/`_io_uring`）、套接字 API（`socket_api_posix`/`_win`）。`EMQ_PLATFORM_LINUX/MACOS/WINDOWS` 宏与 `pal_sources` 列表在构建脚本中经 `is_plat`（目标平台而非构建主机——故交叉编译能正确选择）确定。

**线缆协议 v2**（`EMBEDMQ_VERSION = 2`）：紧凑变长头（基础 26 字节 + 数据包可选 8 字节 timestamp + 可选 4 字节 CRC），所有字段显式小端（不 `memcpy` 打包结构体——跨架构安全）。校验和可选，由 `config.enableChecksum` 控制并经 `hdrFlags` 自描述。

**跨语言**：稳定 C ABI 位于 `src/capi/embedmq_c.cpp`（`include/embedmq/embedmq_c.h`），构建为 `embedmq_c` 共享库；其上为 Python `ctypes` 绑定。该 C ABI 源文件也直接编入 `emq_tests`（带 `EMBEDMQ_C_EXPORTS`），供 `test_capi` 测试。

## 性能敏感原语

`src/util/` 存放无锁/零拷贝构件：`mpsc_queue.h`、`ring_buffer.h`（SPSC）、`memory_pool.h`、`timer_wheel.h`、`crc32.h`、`logger.h`。各传输统一依赖 `sendv` scatter/gather 实现零拷贝发送——改动传输代码时请保持这一点。

## 已知遗留（见 `docs/todo.md`）

`Publisher::subscriberCount()` 恒返回 0（未维护计数）；各传输各有自己的接收循环，尚未收敛到单一 reactor（仅 UDP 接入共享 epoll）；协议 v2 的分片/压缩/加密 flag 已定义但尚未实际编解码。
