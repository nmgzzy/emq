# EmbedMQ

**跨平台轻量级通信中间件** | Embedded Message Queue / Middleware

[![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)]()
[![Language](https://img.shields.io/badge/language-C%2B%2B17-orange)]()
[![Build](https://img.shields.io/badge/build-xmake-green)]()
[![Phase](https://img.shields.io/badge/phase-5%20done-brightgreen)]()
[![Tests](https://img.shields.io/badge/tests-2277%20passed-brightgreen)]()

---

## 简介

EmbedMQ 是一个**去中心化、插件化传输层、支持发布-订阅与请求-响应双模式**的轻量级 C++ 通信中间件。

受 DDS / ZMQ / MQTT 启发，专为**嵌入式 Linux 和桌面平台（Windows / Linux / macOS）**设计。

| 特性 | 说明 |
|------|------|
| 去中心化 P2P | 无需 Broker/服务器，节点间对等通信 |
| 双通信模式 | 发布-订阅（1:N） + 请求-响应（1:1） |
| 传输插件化 | UDP / TCP（实验性）/ 共享内存（同主机 IPC，已实现）；串口·BLE 规划中，均以插件形式接入 |
| 自发现 | UDP 多播（239.255.0.1:19900），无需手动配置对端地址 |
| 可靠传输 | QoS 0/1/2：BestEffort / ACK重传 / 恰好一次 |
| 遗嘱消息 | 节点异常掉线时由对端代为发布 Last Will |
| 性能优化 | 零拷贝 scatter/gather、内存池、无锁 MPSC/SPSC 队列、CPU 亲和性 |
| 跨平台 PAL | 通过平台抽象层统一 Windows / Linux / macOS 差异 |
| 零第三方依赖 | 仅 C++17 标准库 + 平台原生 API（io_uring 为可选实验特性）|
| 简洁 API | 16 个核心调用，5 分钟上手 |
| 多语言接入 | 稳定 C ABI + Python 绑定（ctypes，零依赖）|
| 命令行工具 | `emqtop` 监控/收发/诊断（类 `mosquitto_pub/sub`）|

---

## 快速开始

### 环境要求

| 组件 | 最低版本 |
|------|---------|
| C++ 编译器 | MSVC 2019+ / GCC 7+ / Clang 8+ / AppleClang 12+ |
| 构建工具 | [xmake](https://xmake.io) 2.7+ |
| 操作系统 | Windows 10+ / Linux 4.x+ / macOS 11+ |

### 构建

```bash
# 克隆仓库
git clone https://github.com/<your-org>/embedmq.git
cd embedmq

# 构建（Debug）
xmake f -m debug
xmake

# 构建（Release）
xmake f -m release
xmake
```

### 运行示例

```bash
# 发布-订阅示例
xmake run example_pub_sub

# 请求-响应示例
xmake run example_req_rep
```

### 运行单元测试

```bash
# 运行全部测试（11 个模块，2277 个断言）
xmake run emq_tests

# 运行指定模块
xmake run emq_tests pal
xmake run emq_tests pub_sub req_rep
xmake run emq_tests capi

# 列出所有可用模块
xmake run emq_tests --list

# 查看帮助
xmake run emq_tests --help
```

---

## 核心 API

只需包含一个头文件：

```cpp
#include "embedmq/embedmq.h"
```

### 发布-订阅

```cpp
// 创建参与者（节点）
auto p = embedmq::Participant::create("my_node");

// 订阅主题（支持通配符 * 和 #）
auto sub = p->createSubscriber("sensor/#",
    [](const embedmq::ReceivedMessage& msg) {
        std::cout << msg.topic << ": "
                  << msg.payload.asText() << "\n";
    });

// 发布数据
auto pub = p->createPublisher("sensor/temperature");
pub->publish("25.6°C");
pub->publish(rawBytes, size);  // 二进制
```

### 请求-响应

```cpp
// 服务端：注册处理器
auto rep = p->createReplier("echo",
    [](const embedmq::ReceivedMessage& req) -> embedmq::Payload {
        return embedmq::Payload("echo:" + std::string(req.payload.asText()));
    });

// 客户端：同步请求（带超时）
auto req = p->createRequester("echo");
auto result = req->request(embedmq::Payload("hello"),
                            std::chrono::milliseconds(5000));
if (result) {
    std::cout << result->asText() << "\n";  // "echo:hello"
}
```

### QoS 配置

```cpp
// QoS Level 0：BestEffort（默认，高频数据）
auto pub = p->createPublisher("sensor/temp", embedmq::QoSProfile::bestEffort());

// QoS Level 1：Reliable（ACK + 重传）
auto pub = p->createPublisher("ctrl/cmd", embedmq::QoSProfile::reliable());

// QoS Level 2：ExactlyOnce（恰好一次）
auto pub = p->createPublisher("config", embedmq::QoSProfile::exactlyOnce());

// 保留消息（新订阅者立即收到最新值）
embedmq::QoSProfile retainQos;
retainQos.retain     = true;
retainQos.durability = embedmq::DurabilityKind::TransientLocal;
auto pub = p->createPublisher("status/online", retainQos);
pub->publish("true");
```

### 完整节点配置

```cpp
embedmq::ParticipantConfig cfg;
cfg.nodeName                  = "sensor_node";
cfg.domainId                  = 0;                // 隔离通信域
cfg.discovery.multicastGroup  = "239.255.0.1";
cfg.discovery.multicastPort   = 19900;
cfg.discovery.heartbeatIntervalMs = 2000;
cfg.discovery.peerTimeoutMs   = 10000;
cfg.transport.enableUdp       = true;
cfg.transport.enableTcp       = false;

// 遗嘱消息：节点异常掉线时自动发布
cfg.lastWill.topic   = "status/sensor_node";
cfg.lastWill.payload = embedmq::Payload("offline");
cfg.lastWill.retain  = true;
cfg.lastWill.enabled = true;

auto p = embedmq::Participant::create(cfg);

// 对端连接/断开回调
p->onPeerEvent([](uint16_t id, const std::string& name, bool connected) {
    std::cout << name << (connected ? " connected" : " disconnected") << "\n";
});
```

---

## 主题命名规范

```
格式:    segment/segment/segment
示例:    sensor/temperature/room1
         vehicle/can/engine/rpm
         $SYS/node/status        # 系统保留前缀

通配符:
  *   匹配单级    sensor/*/room1   → 匹配 sensor/temperature/room1
  #   匹配多级    sensor/#         → 匹配 sensor/temp/room1/detail
```

---

## 架构概览

```
┌─────────────────────────────────────────────┐
│               Application                   │
│  Publisher  Subscriber  Requester  Replier  │
├─────────────────────────────────────────────┤
│         Participant (User API)               │
├──────────────┬──────────────────────────────┤
│  MessageBus  │  DiscoveryAgent               │
│  TopicRouter │  PeerRegistry (heartbeat)     │
│  QoSEngine   │  Announce / Farewell          │
│  RetainedStore│                              │
├──────────────┴──────────────────────────────┤
│         Transport Plugin Layer               │
│   UDP Transport   TCP Transport   (...)      │
├─────────────────────────────────────────────┤
│    Platform Abstraction Layer (PAL)          │
│  EventLoop  SocketApi  Process  Timer        │
│  epoll(Linux) kqueue(macOS) IOCP(Windows)   │
└─────────────────────────────────────────────┘
```

---

## 项目结构

```
embedmq/
├── include/embedmq/              # 公共 API（仅需包含此目录）
│   ├── embedmq.h                 # C++ 主头文件（唯一入口）
│   ├── embedmq_c.h               # C ABI 头文件（Phase 5：跨语言/FFI 稳定接口）
│   ├── platform.h                # 平台检测宏
│   ├── types.h                   # Payload / ReceivedMessage
│   ├── qos.h                     # QoSProfile
│   ├── config.h                  # ParticipantConfig
│   └── transport/itransport.h    # Transport 插件接口
│
├── src/
│   ├── capi/                     # C ABI 包装层（Phase 5）
│   │   └── embedmq_c.cpp         # 不透明句柄封装 + 异常隔离
│   │
│   ├── platform/                 # 平台抽象层 (PAL)
│   │   ├── event_loop.h          # 接口
│   │   ├── event_loop_epoll.cpp  # Linux
│   │   ├── event_loop_kqueue.cpp # macOS
│   │   ├── event_loop_iocp.cpp   # Windows
│   │   ├── socket_api.h + *.cpp  # 跨平台 Socket
│   │   └── process.h             # PID / hostname / 临时目录
│   │
│   ├── util/                     # 工具库（纯 C++17，无平台依赖）
│   │   ├── crc32.h               # CRC32 校验
│   │   ├── ring_buffer.h         # 无锁 SPSC 环形缓冲区
│   │   ├── mpsc_queue.h          # 无锁 MPSC 队列（Vyukov）
│   │   ├── memory_pool.h         # 固定块内存池
│   │   ├── logger.h              # 轻量级日志
│   │   └── timer_wheel.h         # 时间轮定时器（支持 CPU 亲和性）
│   │
│   ├── core/                     # 核心层
│   │   ├── message_codec.h       # Wire Format 编解码
│   │   ├── topic_router.h        # 主题路由与通配符匹配
│   │   ├── qos_engine.h          # QoS 策略引擎
│   │   ├── retained_store.h      # 保留消息
│   │   ├── message_bus.h/cpp     # 消息总线
│   │   └── participant.cpp       # Participant 实现
│   │
│   ├── discovery/                # 自发现层
│   │   ├── peer_registry.h       # 对端注册表
│   │   └── discovery_agent.h/cpp # Announce / Heartbeat / Farewell
│   │
│   └── transport/                # 传输插件
│       ├── transport_manager.h/cpp
│       ├── udp_transport.h/cpp   # UDP 单播 + 多播（零拷贝 sendmsg）
│       ├── tcp_transport.h/cpp   # TCP（长度前缀帧化）
│       └── shm_transport.h/cpp   # 共享内存（POSIX shm / Win FileMapping）
│
├── tests/                        # 单元测试（统一可执行文件 emq_tests）
│   ├── test_framework.h          # 轻量框架（模块注册 + 命令行过滤）
│   ├── test_main.cpp             # 统一入口
│   ├── test_topic_router.cpp     # 模块: topic_router
│   ├── test_message_codec.cpp    # 模块: message_codec
│   ├── test_qos_engine.cpp       # 模块: qos_engine
│   ├── test_pal.cpp              # 模块: pal
│   ├── test_pub_sub.cpp          # 模块: pub_sub
│   ├── test_req_rep.cpp          # 模块: req_rep
│   ├── test_last_will.cpp        # 模块: last_will（遗嘱消息）
│   ├── test_phase3.cpp           # 模块: phase3（内存池/MPSC/SHM/亲和性/零拷贝）
│   ├── test_review_fixes.cpp     # 模块: review_fixes（审查修复回归）
│   └── test_capi.cpp             # 模块: capi（C ABI 句柄/收发/错误码）
│
├── examples/
│   ├── pub_sub/main.cpp          # 传感器数据发布订阅
│   └── req_rep/main.cpp          # 计算服务请求响应
│
├── bindings/                     # 语言绑定（Phase 5）
│   └── python/
│       ├── embedmq.py            # Python ctypes 绑定（零依赖）
│       └── example.py            # Python 自测 / 示例
│
├── tools/                        # 命令行工具（Phase 5）
│   └── emqtop/main.cpp           # 监控/收发/诊断 CLI（emqtop）
│
├── bench/
│   └── bench_main.cpp            # 性能基准（emq_bench）
│
├── docs/
│   └── architecture.md           # 完整设计文档
│
└── xmake.lua                     # 构建脚本
```

---

## Wire Format（线缆格式）

EmbedMQ 使用固定头部 + 可变载荷的二进制协议：

```
┌────────────────────────────────────────────────┐
│ magic(2) | version(1) | msgType(1)             │  4 B
│ qosLevel(1) | flags(1) | sourceId(2)           │  4 B
│ destId(2) | topicLen(2)                        │  4 B
│ sequenceId(4)                                  │  4 B
│ correlationId(4)                               │  4 B
│ timestamp(8)                                   │  8 B
│ serializerId(1) | reserved(3)                  │  4 B
│ payloadLen(4)                                  │  4 B
│ checksum / CRC32(4)                            │  4 B
├────────────────────────────────────────────────┤
│ topic (variable, topicLen bytes)               │
│ payload (variable, payloadLen bytes)           │
└────────────────────────────────────────────────┘
  固定头部：40 bytes
```

**Magic Number：** `0xEBDC`（EmbedMQ Data Channel）

---

## QoS 说明

| 级别 | 名称 | 语义 | 适用场景 |
|------|------|------|---------|
| 0 | BestEffort | 最多一次，不确认 | 高频传感器、日志流 |
| 1 | Reliable | 至少一次，ACK + 重传 | 控制指令、状态上报 |
| 2 | ExactlyOnce | 恰好一次，去重保证 | 关键配置、交易数据 |

---

## 自发现机制

节点启动后自动通过 **UDP 多播**互相发现，无需手动配置对端地址：

```
Node A 启动
  └── 发送 ANNOUNCE (239.255.0.1:19900)
        { id, name, topics, endpoints }

Node B 启动
  ├── 收到 A 的 ANNOUNCE → 发现 A
  └── 发送自己的 ANNOUNCE → A 发现 B

双方定期发送 HEARTBEAT (默认 2s)
  └── 超时未收到 (默认 10s) → 标记离线
```

同设备内进程间通信可启用**共享内存传输**（见下文）以获得最低延迟。

---

## 性能优化（Phase 3）

### 共享内存传输

同主机进程间可启用零网络栈开销的共享内存通道（POSIX `shm_open`+`mmap` / Windows `CreateFileMapping`）：

```cpp
embedmq::ParticipantConfig cfg;
cfg.transport.enableShm = true;   // 启用共享内存收件箱
auto p = embedmq::Participant::create(cfg);
```

每个节点拥有一个有界的「收件箱」槽位环（默认 256 槽 × 4 KB），多个进程作为生产者并发写入（CAS 预留槽位），节点自身的轮询线程作为唯一消费者读取。

### 零拷贝 scatter/gather

BestEffort（QoS 0）发布路径使用 `encodeHeader` 仅生成紧凑线缆头（协议 v2：基础 26 字节 + 可选 timestamp/CRC），配合 `sendmsg`/`WSASendTo` 的 iovec 分片发送 `{header, topic, payload}`，避免将载荷再拷贝进单一缓冲。

### 内存池与无锁队列

以下为库内置的**可选工具组件**（已在 `tests`/`bench` 中验证），当前核心收发路径
默认仍使用 `std::vector`/标准容器，尚未默认接入；可按需在自定义传输/队列中使用：

- `util::FixedBlockPool`：固定块内存池，O(1) 分配/归还，提供确定性延迟，避免碎片。
- `util::MpscQueue`：无锁多生产者单消费者队列（Vyukov 算法）。
- `util::SPSCRingBuffer`：无锁单生产者单消费者环形缓冲。

### CPU 亲和性

```cpp
cfg.threading.pinCpu      = true;
cfg.threading.cpuAffinity = 2;   // 将内部工作线程绑定到 2 号核心
```

### 性能基准（Linux x64, Release 参考值）

| 项目 | 吞吐 / 延迟 |
|------|------------|
| 本地 Pub/Sub | ~11 M msg/s（平均发布延迟 ~0.09 µs）|
| SPSC 环形缓冲 | ~270 M msg/s |
| MPSC 队列（4→1）| ~16 M msg/s |
| 内存池 vs malloc | 约 1.06×（单线程）|

> 运行 `xmake f -m release && xmake run emq_bench` 复现。具体数值随硬件而异。

---

## 多语言与工具（Phase 5）

### C ABI（跨语言稳定接口）

`include/embedmq/embedmq_c.h` 提供一套纯 C 的扁平接口，对 C++ 核心库做不透明句柄封装：
所有错误以返回码 / NULL 暴露，C++ 异常绝不跨越 ABI 边界，便于其他语言通过 FFI 调用。

```c
#include "embedmq/embedmq_c.h"

static void on_msg(const emq_message* m, void* ud) {
    printf("%s: %.*s\n", m->topic, (int)m->payload_len, m->payload);
}

int main(void) {
    emq_participant* p   = emq_participant_create("c_node");
    emq_subscriber*  sub = emq_subscriber_create(p, "sensor/#",
                                                 EMQ_QOS_BEST_EFFORT, on_msg, NULL);
    emq_publisher*   pub = emq_publisher_create(p, "sensor/temp", EMQ_QOS_BEST_EFFORT);
    emq_publisher_publish_str(pub, "25.6");
    /* ... */
    emq_publisher_destroy(pub);
    emq_subscriber_destroy(sub);
    emq_participant_destroy(p);
    return 0;
}
```

构建产物：共享库 `libembedmq_c.so` / `.dylib` / `embedmq_c.dll`（`xmake build embedmq_c`）。

### Python 绑定

基于 ctypes 封装 C ABI，**零第三方依赖**，提供 Pythonic 接口：

```python
import embedmq

with embedmq.Participant("py_node") as p:
    sub = p.create_subscriber("sensor/#", lambda m: print(m.topic, m.text))
    pub = p.create_publisher("sensor/temp")
    pub.publish("25.6")

    # 请求-响应
    rep = p.create_replier("multiply",
        lambda m: str(eval(m.text.replace(" ", "*"))))
    req = p.create_requester("multiply")
    print(req.request("6 7"))   # b'42'
```

运行自测：先 `xmake build embedmq_c`，再 `python3 bindings/python/example.py`。
库定位顺序：环境变量 `EMBEDMQ_LIB` → 仓库 `build/` 目录扫描 → 系统库路径。

### emqtop —— 命令行监控/诊断工具

类似 `mosquitto_pub/sub` 的网络瑞士军刀，作为普通节点加入网络：

```bash
emqtop monitor [topic]              # 订阅(默认 #)，实时打印消息 + 拓扑速率统计
emqtop sub <topic>                  # 仅订阅并打印
emqtop pub <topic> <msg> [-n N] [-i ms]   # 发布(可重复 N 次、间隔 i 毫秒)
emqtop req <service> <msg> [-t ms]  # 发送请求并打印响应
emqtop echo <service>               # 注册回显服务(便于测试 req)
emqtop peers                        # 列出已发现对端

# 通用选项：--name <n>  --domain <d>  --no-udp  --shm
```

---

## 自定义 Transport 插件

实现 `ITransport` 接口即可接入任意传输通道：

```cpp
class MyCanTransport : public embedmq::ITransport {
public:
    std::string typeName() const override { return "can"; }

    TransportCapability capability() const override {
        TransportCapability cap;
        cap.maxPayloadSize = 8;          // CAN 帧 8 字节
        cap.estimatedLatencyUs = 500;
        return cap;
    }

    bool init(const std::string& config) override {
        // 初始化 CAN socket...
        return true;
    }

    bool send(const Endpoint& to,
              const uint8_t* data, size_t size) override {
        // 发送 CAN 帧...
        return true;
    }

    // ... 实现其余纯虚函数
};

// 注册
participant->registerTransport("can",
    std::make_shared<MyCanTransport>());
```

---

## 与同类方案对比

| 特性 | EmbedMQ | MQTT | ZeroMQ | DDS |
|------|---------|------|--------|-----|
| 需要服务器 | ❌ | ✅ Broker | ❌ | ❌ |
| 自发现 | ✅ | ❌ | ❌ | ✅ |
| Pub/Sub | ✅ | ✅ | ✅ | ✅ |
| Req/Rep | ✅ | ❌ | ✅ | ❌ |
| 传输插件化 | ✅ | ❌ | ❌ | 有限 |
| 串口/BLE 支持 | 📋 规划中 | ❌ | ❌ | ❌ |
| 保留消息 | ✅ | ✅ | ❌ | ✅ |
| 遗嘱消息 | ✅ | ✅ | ❌ | ✅ |
| 共享内存 IPC | ✅ | ❌ | 有限 | 有限 |
| 通配符 | ✅ `* #` | ✅ | 前缀 | 有限 |
| 库大小 | ~300 KB | ~200 KB | ~500 KB | ~5 MB |
| 第三方依赖 | **零** | 有 | 有 | 有 |
| 嵌入式适配 | ★★★★★ | ★★★★ | ★★★ | ★★ |

---

## 开发路线图

| 阶段 | 版本 | 状态 | 内容 |
|------|------|------|------|
| Phase 1 | v0.1 | ✅ **已完成** | PAL 层、UDP、Pub/Sub、Req/Rep、QoS 0、自发现 |
| Phase 2 | v0.2 | ✅ **已完成** | QoS 1/2、通配符、保留消息、遗嘱消息、心跳、TCP（实验性）|
| Phase 3 | v0.3 | ✅ **已完成** | 共享内存 Transport、零拷贝 scatter/gather、内存池、无锁 MPSC 队列、CPU 亲和性、io_uring（实验性）、性能基准 |
| Phase 4 | v0.4 | 📋 规划中 | 串口 Transport、BLE Transport、大消息分片 |
| Phase 5 | v0.5 | ✅ **已完成** | C ABI（稳定跨语言接口）、Python 绑定（ctypes，零依赖）、命令行监控工具 `emqtop` |
| Phase 6 | v1.0 | 📋 规划中 | TLS 加密、LZ4 压缩、CI/CD、正式发布 |

---

## 构建选项

```bash
# 禁用 TCP（仅 UDP）
xmake f --enable_tcp=n

# 禁用共享内存传输
xmake f --enable_shm=n

# 启用 io_uring 事件循环（Linux 实验性，需内核 5.1+ 与 liburing）
xmake f --enable_io_uring=y

# 禁用测试 / 示例 / 基准
xmake f --build_tests=n
xmake f --build_examples=n
xmake f --build_bench=n

# 禁用 C ABI 共享库 / CLI 工具（Phase 5；embedded 画像下默认不构建）
xmake f --build_capi=n
xmake f --build_tools=n

# Release 模式
xmake f -m release

# 运行性能基准
xmake run emq_bench
```

---

## 测试覆盖

> **平台：** Linux x64 (GCC) / Windows 10 x64 (MSVC 2022) | **总计：2277 assertions / 81 tests，全部通过**

| 测试套件 | 覆盖内容 | 断言数 |
|---------|---------|--------|
| `test_topic_router` | 精确匹配、`*`/`#` 通配符、取消订阅、多订阅者 | 20 |
| `test_message_codec` | 编解码正确性、CRC32 完整性、边界条件 | 23 |
| `test_qos_engine` | ACK 确认、超时重传、放弃机制、QoS 2 去重 | 14 |
| `test_pal` | 进程工具、CRC32、无锁环形缓冲、时间轮定时器 | 24 |
| `test_pub_sub` | 本地 Pub/Sub、通配符路由、保留消息、暂停/恢复 | 10 |
| `test_req_rep` | 同步/异步请求、多请求、请求计数 | 10 |
| `test_last_will` | 超时触发遗嘱、优雅退出丢弃、本地投递、保留遗嘱 | 15 |
| `test_phase3` | 内存池、无锁 MPSC、CPU 亲和性、共享内存收发、零拷贝编码 | 49 |
| `test_review_fixes` | 编解码加固、时间轮长定时器/取消、对端更新、无服务请求不挂起 | 15 |
| `test_refactor_v2` | Payload SBO、协议 v2 小端/紧凑头/可选 CRC、QoS2 滑动窗口与握手 | ~2040 |
| `test_capi` | C ABI 句柄生命周期、Pub/Sub 与 Req/Rep 往返、二进制载荷、超时、NULL 健壮性 | 55 |

---

## 设计文档

详细的架构设计、时序图、协议格式、QoS 状态机等请参阅：

**[docs/architecture.md](docs/architecture.md)**

---

## License

MIT License — 详见 [LICENSE](LICENSE) 文件。

---

*EmbedMQ v0.4.0 — Phase 1 + Phase 2 + Phase 3 + Phase 5 已实现；协议 v2（紧凑头/显式小端/可选 CRC）、QoS2 完整状态机、TLV 发现、嵌入式构建 profile，以及 C ABI / Python 绑定 / `emqtop` CLI 已落地*
