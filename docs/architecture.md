# EmbedMQ —— 跨平台轻量级通信中间件设计文档

---

> ## ⚙️ 实现现状（v0.4）与协议版本——请先阅读
>
> 本设计文档同时记录“总体设计”与部分“路线图/aspirational”内容。**以下为 v0.4 代码实测现状**，
> 当文档正文与此块不一致时，**以此块为准**（正文中较早的协议字段/时序描述属历史设计稿）。
>
> - **线缆协议 v2（`EMBEDMQ_VERSION = 2`）**：紧凑变长头——基础 **26 字节** + 可选
>   `timestamp(8)`（仅数据包 PUBLISH/REQUEST/REPLY）+ 可选 `checksum(4)`（由 `hdrFlags` 标识）。
>   所有字段**显式小端**读写（跨架构一致），不再 `memcpy` 打包结构体。正文 §6 中的 40 字节定长头为旧版（v1）。
> - **CRC 可选**：`config.enableChecksum` 控制；解码依据 `hdrFlags` 自描述。
> - **QoS2 完整状态机**：PUBLISH→PUBREC→PUBREL→PUBCOMP 两阶段握手 + **每 source 有界滑动去重窗口**。
> - **发现 v2**：ANNOUNCE 为 **TLV/变长**，携带 endpoint 列表；**已合并心跳**——周期 ANNOUNCE 兼任保活，
>   不再单独发送 HEARTBEAT，订阅变化时立即重播（正文 §7 的独立 HEARTBEAT 时序属旧设计）。
> - **传输**：UDP 接收在 Linux 下接入 **epoll 反应堆**（替代 select）；UDP/TCP/SHM 均原生实现 `sendv`；
>   SHM 增加 layout 版本/几何校验、失效段回收与 futex 唤醒。
> - **构建画像**：`xmake f --profile=embedded` 瘦身（关闭 TCP/示例/基准/io_uring）；**TCP 默认关闭**。
> - **保留（reserved）配置**：`enableLocalIpc`/`enableSerial`/`enableBle`/`serialDevice` 当前**未实现对应 Transport**，
>   默认关闭，仅作占位（正文与配置示例中出现仅表意图，置位无效果）。

---

## 目录

1. [项目概述](#1-项目概述)
2. [设计目标与约束](#2-设计目标与约束)
3. [总体架构](#3-总体架构)
4. [分层设计详解](#4-分层设计详解)
5. [核心概念与数据模型](#5-核心概念与数据模型)
6. [协议设计](#6-协议设计)
7. [自发现机制](#7-自发现机制)
8. [QoS 配置体系](#8-qos-配置体系)
9. [API 设计](#9-api-设计)
10. [Transport 插件体系](#10-transport-插件体系)
11. [跨语言扩展方案](#11-跨语言扩展方案)
12. [关键流程时序](#12-关键流程时序)
13. [可靠性与容错](#13-可靠性与容错)
14. [性能设计](#14-性能设计)
15. [目录结构与构建](#15-目录结构与构建)
16. [核心代码骨架](#16-核心代码骨架)
17. [配置示例](#17-配置示例)
18. [典型用例](#18-典型用例)
19. [与 MQTT/ZMQ/DDS 对比](#19-与-mqttzmqdds-对比)
20. [路线图](#20-路线图)

---

## 1. 项目概述

### 1.1 项目名称

**EmbedMQ** （Embedded Message Queue / Middleware）

### 1.2 一句话定义

> 一个**跨平台（Windows / Linux / macOS）的去中心化、插件化传输层、支持发布-订阅与请求-响应双模式**的轻量级 C++ 通信中间件。

### 1.3 设计哲学

| 关键词 | 说明 |
|--------|------|
| **去中心化** | 受 DDS 启发，无需 Broker/服务器，节点间对等通信 |
| **低延迟** | 受 ZMQ 启发，零拷贝消息传递、无锁队列、内存池 |
| **可靠可控** | 受 MQTT 启发，分级 QoS、保留消息、遗嘱消息 |
| **传输无关** | Socket/串口/蓝牙/共享内存均以插件形式接入 |
| **跨平台** | 通过平台抽象层(PAL)统一 Windows / Linux / macOS 差异 |
| **简单为先** | API 行数 < 20 个核心调用，5 分钟上手 |

---

## 2. 设计目标与约束

### 2.1 功能性需求

| ID | 需求 | 优先级 |
|----|------|--------|
| F1 | 发布-订阅模式（1:N） | P0 |
| F2 | 请求-响应模式（1:1） | P0 |
| F3 | 支持二进制与文本数据 | P0 |
| F4 | 自定义序列化格式 | P0 |
| F5 | 自发现（无需手动配置对端地址） | P0 |
| F6 | 可配置 QoS（至少3级） | P0 |
| F7 | Transport 插件化 | P0 |
| F8 | 跨进程通信 | P0 |
| F9 | 跨设备通信 | P0 |
| F10 | 保留消息(Retained) | P1 |
| F11 | 遗嘱消息(Last Will) | P1 |
| F12 | 主题通配符匹配 | P1 |
| F13 | 跨语言支持（C / Python） | P2 |

### 2.2 非功能性需求

| ID | 需求 | 目标值 |
|----|------|--------|
| NF1 | 本地 IPC 延迟 | < 50 μs（P99） |
| NF2 | 跨设备 UDP 延迟 | < 1 ms（P99，千兆局域网） |
| NF3 | 内存占用 | 静态库 < 512 KB，运行时 RSS < 2 MB |
| NF4 | 依赖 | 仅 C++17 标准库 + 平台原生 API，零第三方强依赖 |
| NF5 | 编译目标 | Windows (x64/ARM64)、Linux (x86_64/armv7/aarch64)、macOS (x86_64/arm64) |
| NF6 | 线程安全 | 所有公开 API 线程安全 |

### 2.3 设计约束

- 目标 OS：Windows 10+、Linux（内核 4.x+，含嵌入式 Linux）、macOS 11+
- 编译器：GCC 7+ / Clang 8+ / MSVC 2019+ (v142+) / AppleClang 12+
- 构建系统：xmake 2.7+（主构建）/ CMake 3.14+（兼容）
- 不依赖 Boost、Protobuf 等重量级库
- 可选依赖（按平台）：
  - Linux：`liburing`（io_uring 加速）、`bluez`（蓝牙）
  - macOS：CoreBluetooth.framework（蓝牙）
  - Windows：WinRT BLE API（蓝牙）

---

## 3. 总体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Application Layer                          │
│  ┌───────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │ Publisher  │  │  Subscriber  │  │   Requester  │  │  Replier  │ │
│  └─────┬─────┘  └──────┬───────┘  └──────┬───────┘  └─────┬─────┘ │
├────────┼───────────────┼────────────────┼────────────────┼────────┤
│        │     User API Layer (embedmq.h) │                │        │
│        ▼               ▼                ▼                ▼        │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                      Participant                            │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │   │
│  │  │ TopicRouter  │  │  QoSEngine   │  │  SerializerMgr   │  │   │
│  │  └──────┬──────┘  └──────┬───────┘  └────────┬──────────┘  │   │
│  │         │                │                    │             │   │
│  │  ┌──────┴────────────────┴────────────────────┴──────────┐  │   │
│  │  │                   MessageBus                          │  │   │
│  │  │  ┌────────────┐  ┌────────────┐  ┌────────────────┐   │  │   │
│  │  │  │ MsgEncoder │  │ MsgDecoder │  │ RetransmitMgr  │   │  │   │
│  │  │  └─────┬──────┘  └─────┬──────┘  └───────┬────────┘   │  │   │
│  │  └────────┼───────────────┼──────────────────┼────────────┘  │   │
│  └───────────┼───────────────┼──────────────────┼───────────────┘   │
├──────────────┼───────────────┼──────────────────┼───────────────────┤
│              │    Discovery Layer                │                   │
│  ┌───────────┴───────────────────────────────────┴───────────────┐  │
│  │              DiscoveryAgent (SSDP-like)                       │  │
│  │  ┌──────────┐  ┌───────────┐  ┌──────────────────────────┐   │  │
│  │  │ Announcer│  │ Listener  │  │ PeerRegistry (heartbeat) │   │  │
│  │  └──────────┘  └───────────┘  └──────────────────────────┘   │  │
│  └───────────────────────────────┬───────────────────────────────┘  │
├──────────────────────────────────┼──────────────────────────────────┤
│              Transport Plugin Layer                                 │
│  ┌───────────────────────────────┴───────────────────────────────┐  │
│  │                   TransportManager                            │  │
│  │  ┌──────────┐ ┌──────────┐ ┌────────┐ ┌────────┐ ┌────────┐  │  │
│  │  │ LocalIPC │ │  UDP     │ │  TCP   │ │ Serial │ │  BLE   │  │  │
│  │  │(UDS/     │ │(Unicast/ │ │        │ │ (UART) │ │        │  │  │
│  │  │NamedPipe)│ │Multicast)│ │        │ │        │ │        │  │  │
│  │  └──────────┘ └──────────┘ └────────┘ └────────┘ └────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│              Platform Abstraction Layer (PAL)                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐  │  │
│  │  │EventLoop │ │SharedMem │ │  Socket  │ │  Thread/Timer   │  │  │
│  │  │(epoll/   │ │(POSIX/   │ │(BSD/     │ │  (std::thread/  │  │  │
│  │  │ kqueue/  │ │ Win32    │ │ Winsock) │ │   platform)     │  │  │
│  │  │ IOCP)    │ │ FileMap) │ │          │ │                 │  │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └─────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────┤
│                      OS: Windows / Linux / macOS                    │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.1 架构层次说明

| 层次 | 职责 | 关键类 |
|------|------|--------|
| **User API** | 提供极简的面向应用的接口 | `Participant`, `Publisher`, `Subscriber`, `Requester`, `Replier` |
| **Core（消息总线）** | 消息路由、编解码、QoS、序列化 | `MessageBus`, `TopicRouter`, `QoSEngine`, `SerializerMgr` |
| **Discovery** | 节点自发现、心跳保活、拓扑维护 | `DiscoveryAgent`, `PeerRegistry` |
| **Transport Plugin** | 抽象传输接口，各协议插件实现 | `ITransport`, `LocalIpcTransport`, `UdpTransport`, ... |
| **PAL（平台抽象层）** | 封装 OS 差异，提供统一的底层原语 | `EventLoop`, `SharedMemory`, `Socket`, `Timer` |

---

## 4. 分层设计详解

### 4.1 User API Layer

**设计原则：** 用户只需与 `Participant` 交互，通过它创建所有通信实体。

```
Participant
 ├── createPublisher(topic, qos, serializer) -> Publisher
 ├── createSubscriber(topic, qos, callback)  -> Subscriber
 ├── createRequester(service, qos)           -> Requester
 └── createReplier(service, handler)         -> Replier
```

- `Participant` 代表网络中的一个参与者节点（类比 DDS 的 DomainParticipant）
- 一个进程可以有多个 `Participant`（不同 domain），通常一个即可
- 所有通信实体的生命周期由 `Participant` 管理

### 4.2 Core Layer — MessageBus

MessageBus 是中间件的心脏：

```
MessageBus
 ├── publish(topic, message)           // 本地分发 + 远程发送
 ├── subscribe(topic, callback)        // 注册本地回调
 ├── request(service, msg) -> Future   // 发起请求
 ├── reply(service, handler)           // 注册服务处理
 ├── TopicRouter                       // 主题匹配与路由表
 │    ├── exactMatch(topic)
 │    ├── wildcardMatch(pattern)       // 支持 * 和 # 通配符
 │    └── routeTable: Map<Topic, Set<Endpoint>>
 ├── QoSEngine                         // QoS 策略执行
 │    ├── applyQoS(message, level)
 │    ├── ackManager
 │    └── retransmitManager
 └── SerializerMgr                     // 序列化管理
      ├── registerSerializer(id, impl)
      └── getSerializer(id) -> ISerializer
```

### 4.3 Discovery Layer

```
DiscoveryAgent
 ├── Announcer                         // 周期性广播自身信息
 │    ├── announceParticipant()
 │    └── announceTopics()
 ├── Listener                          // 监听其他节点广播
 │    └── onDiscoveryMessage(msg)
 └── PeerRegistry                      // 维护已知对端
      ├── addPeer(peerInfo)
      ├── removePeer(peerId)
      ├── getPeersForTopic(topic) -> vector<Peer>
      └── heartbeat timer
```

**发现协议选择：**
- **跨进程（同设备）：**
  - Linux/macOS：Unix Domain Socket 多播 或 共享文件
  - Windows：Named Pipe 轮询 或 本地 UDP 回环（localhost multicast）
- **跨设备（局域网）：** UDP 多播（类似 SSDP/mDNS），默认组 `239.255.0.1:19900`（全平台通用）
- **串口/蓝牙：** 点对点握手协议

### 4.4 Transport Plugin Layer

```
ITransport (接口)
 ├── init(config) -> Status
 ├── send(endpoint, data, len) -> Status
 ├── recv(callback)                    // 异步接收
 ├── getEndpoints() -> vector<Endpoint>
 ├── getType() -> TransportType
 ├── shutdown()
 └── supportsMulticast() -> bool

TransportManager
 ├── registerTransport(name, factory)
 ├── getTransport(name) -> ITransport*
 ├── selectTransport(peer) -> ITransport*  // 智能选择最优传输
 └── transports_: Map<string, ITransport*>
```

### 4.5 Platform Abstraction Layer (PAL)

PAL 层是 EmbedMQ 实现跨平台的关键基础设施，封装所有操作系统差异。

```
platform::EventLoop (接口)
 ├── create() -> unique_ptr<EventLoop>   // 工厂：自动选择平台实现
 ├── addHandle(handle, interest, cb)     // 注册 IO 句柄
 ├── removeHandle(handle)
 ├── start() / stop() / wakeup()
 │
 ├── [Linux]   EpollEventLoop            // epoll + eventfd
 ├── [macOS]   KqueueEventLoop           // kqueue + pipe
 └── [Windows] IocpEventLoop             // IOCP + Event

platform::SocketApi
 ├── createSocket(domain, type) -> IoHandle
 ├── bind / listen / accept / connect
 ├── send / recv / sendTo / recvFrom
 ├── setNonBlocking / setReuseAddr
 ├── joinMulticast / leaveMulticast
 │
 ├── [POSIX]   BSD Socket 实现
 └── [Windows] Winsock2 实现 (需 WSAStartup)

platform::LocalIpc
 ├── createServer(path) -> IoHandle
 ├── connect(path) -> IoHandle
 ├── accept() -> IoHandle
 │
 ├── [POSIX]   Unix Domain Socket 实现
 └── [Windows] Named Pipe 实现

platform::SharedMemory
 ├── create(name, size) -> void*
 ├── open(name) -> void*
 ├── close()
 │
 ├── [POSIX]   shm_open + mmap
 └── [Windows] CreateFileMapping + MapViewOfFile

platform::SerialPort
 ├── open(device, baudRate, ...) -> IoHandle
 ├── read / write / close
 │
 ├── [POSIX]   termios API
 └── [Windows] Win32 CommAPI

platform::Process
 ├── getProcessId() -> uint64_t
 ├── getHostName() -> string
 └── getTempDir() -> string             // 平台临时目录
```

**PAL 设计原则：**
- 接口使用 C++17 标准类型，不暴露平台原生类型
- 通过工厂方法或编译期条件选择实现
- `IoHandle` 统一抽象文件描述符(fd)和 Windows HANDLE
- 每个 PAL 组件可独立测试

---

## 5. 核心概念与数据模型

### 5.1 核心概念映射

| EmbedMQ 概念 | DDS 类比 | MQTT 类比 | ZMQ 类比 | 说明 |
|--------------|----------|-----------|----------|------|
| Domain | Domain | Broker namespace | — | 隔离的通信域 |
| Participant | DomainParticipant | Client | Context | 通信节点 |
| Topic | Topic | Topic | — | 发布订阅主题 |
| Service | — | — | REQ/REP addr | 请求响应服务名 |
| Publisher | DataWriter | Publisher | PUB socket | 数据发布者 |
| Subscriber | DataReader | Subscriber | SUB socket | 数据订阅者 |
| Requester | — | — | REQ socket | 请求者 |
| Replier | — | — | REP socket | 应答者 |
| Message | Sample | MQTT Payload | zmq_msg_t | 消息体 |
| QoSProfile | QoSPolicy | QoS Level | HWM/etc | 服务质量配置 |

### 5.2 Message 数据结构

```cpp
struct Message {
    MessageHeader header;
    Payload       payload;
};

struct MessageHeader {
    uint32_t magic;           // 0xEBDC (EmbedMQ Data Channel)
    uint8_t  version;         // 协议版本
    uint8_t  msgType;         // PUBLISH / SUBSCRIBE / REQUEST / REPLY / ACK / DISCOVER / HEARTBEAT
    uint8_t  qosLevel;        // 0, 1, 2
    uint8_t  flags;           // RETAIN | WILL | COMPRESSED | ENCRYPTED
    uint32_t sequenceId;      // 消息序号
    uint64_t timestamp;       // 纳秒级时间戳
    uint16_t topicLen;        // 主题长度
    uint32_t payloadLen;      // 载荷长度
    uint32_t correlationId;   // 请求-响应关联ID
    uint8_t  serializerId;    // 序列化器标识
    uint16_t sourceId;        // 源节点ID
    uint16_t destId;          // 目标节点ID (0xFFFF = broadcast)
    uint32_t checksum;        // CRC32 校验
    // --- 变长部分 ---
    char     topic[];         // 主题字符串 (topicLen bytes)
};

struct Payload {
    uint8_t* data;
    uint32_t size;
    bool     isOwned;         // 是否拥有内存所有权
};
```

### 5.3 消息类型枚举

```cpp
enum class MessageType : uint8_t {
    // 数据通道
    PUBLISH       = 0x01,
    SUBSCRIBE     = 0x02,
    UNSUBSCRIBE   = 0x03,
    REQUEST       = 0x04,
    REPLY         = 0x05,
    
    // 控制通道
    ACK           = 0x10,
    NACK          = 0x11,
    
    // 发现通道
    ANNOUNCE      = 0x20,
    DISCOVER_REQ  = 0x21,
    DISCOVER_RSP  = 0x22,
    HEARTBEAT     = 0x23,
    FAREWELL      = 0x24,  // 优雅退出
    
    // 系统
    PING          = 0x30,
    PONG          = 0x31,
};
```

---

## 6. 协议设计

### 6.1 Wire Format（线缆格式）

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Magic (0xEBDC)        |   Version     |   MsgType     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  QoS  | Flags |   Reserved    |       Source ID               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|        Dest ID                |       Topic Length            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Sequence ID                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Correlation ID                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Timestamp (high)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Timestamp (low)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| SerializerID  |              Payload Length                    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Checksum (CRC32)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Topic (variable)                       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Payload (variable)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**固定头部大小：36 bytes**（精简于 DDS RTPS 的 ~60 bytes）

### 6.2 Flags 位定义

```
Bit 0: RETAIN     - 保留消息
Bit 1: WILL       - 遗嘱消息
Bit 2: COMPRESSED - 载荷已压缩 (LZ4)
Bit 3: ENCRYPTED  - 载荷已加密
Bit 4: FRAGMENT   - 分片消息
Bit 5: LAST_FRAG  - 最后一个分片
Bit 6-7: Reserved
```

### 6.3 主题命名规范

```
格式:  segment/segment/segment
示例:  sensor/temperature/room1
       vehicle/can/engine/rpm

通配符:
  *  匹配单级   sensor/*/room1  匹配 sensor/temperature/room1
  #  匹配多级   sensor/#        匹配 sensor/temperature/room1/detail
```

**规则：**
- 主题为 UTF-8 字符串
- 最大长度 256 bytes
- 分隔符为 `/`
- 不允许以 `/` 开头或结尾
- `$SYS/` 前缀保留给系统主题

---

## 7. 自发现机制

### 7.1 发现流程

```
┌─────────┐                              ┌─────────┐
│  Node A  │                              │  Node B  │
│(启动)    │                              │(已运行)  │
└────┬─────┘                              └────┬─────┘
     │                                         │
     │  ──── ANNOUNCE (multicast) ──────────>  │
     │  Participant info + Topic list          │
     │                                         │
     │  <──── ANNOUNCE (multicast) ──────────  │
     │  Participant info + Topic list          │
     │                                         │
     │  (Topic 匹配发现)                        │
     │                                         │
     │  ──── DISCOVER_REQ (unicast) ────────>  │
     │  请求详细端点信息                         │
     │                                         │
     │  <──── DISCOVER_RSP (unicast) ────────  │
     │  返回传输端点详情                         │
     │                                         │
     │  ═══════ DATA CHANNEL ESTABLISHED ═════ │
     │                                         │
     │  ──── HEARTBEAT (periodic) ──────────>  │
     │  <──── HEARTBEAT (periodic) ──────────  │
     │                                         │
```

### 7.2 Announce 报文

```cpp
struct AnnouncePayload {
    uint16_t participantId;
    uint8_t  domainId;
    char     nodeName[64];          // 人类可读名称
    uint8_t  transportCount;        // 支持的传输数量
    struct TransportEndpoint {
        uint8_t  type;              // UDP/TCP/UDS/Serial/BLE
        char     address[128];      // 地址字符串
        uint16_t port;
    } endpoints[];
    uint16_t topicCount;
    struct TopicInfo {
        uint8_t  role;              // PUBLISHER / SUBSCRIBER / REQUESTER / REPLIER
        uint8_t  qosLevel;
        char     topicName[256];
    } topics[];
};
```

### 7.3 发现参数配置

```cpp
struct DiscoveryConfig {
    std::string multicastGroup = "239.255.0.1";
    uint16_t    multicastPort  = 19900;
    uint32_t    announceIntervalMs = 1000;     // 启动时快速广播
    uint32_t    announceSlowIntervalMs = 5000;  // 稳定后慢速
    uint32_t    heartbeatIntervalMs = 2000;
    uint32_t    peerTimeoutMs = 10000;          // 超时判定离线
    uint8_t     domainId = 0;                   // 域隔离
};
```

### 7.4 同设备发现优化

对于同设备跨进程场景，各平台采用不同的本地发现策略：

| 平台 | 本地发现机制 | 发现目录/路径 |
|------|-------------|--------------|
| **Linux** | UDS 广播 | `/tmp/embedmq/domain_{id}/` |
| **macOS** | UDS 广播 | `~/Library/Caches/embedmq/domain_{id}/` |
| **Windows** | Named Pipe 枚举 + 本地 UDP | `\\.\pipe\embedmq_domain_{id}_*` |

- 自动检测对端是否在同一设备，优先使用共享内存或本地 IPC 传输
- PAL 层提供统一的 `LocalDiscovery` 接口，屏蔽平台差异

---

## 8. QoS 配置体系

### 8.1 QoS Level

| Level | 名称 | 语义 | 实现 | 适用场景 |
|-------|------|------|------|----------|
| **0** | BestEffort | 最多一次，无确认 | 直接发送，不等待ACK | 高频传感器数据，丢失可接受 |
| **1** | Reliable | 至少一次，有确认 | 发送后等ACK，超时重传 | 控制指令，状态上报 |
| **2** | ExactlyOnce | 恰好一次 | 两阶段确认+去重 | 关键交易、配置变更 |

### 8.2 扩展 QoS 策略

```cpp
struct QoSProfile {
    // 基本
    QoSLevel    level = QoSLevel::BestEffort;
    
    // 可靠性
    uint32_t    maxRetries = 3;
    uint32_t    retryIntervalMs = 100;
    uint32_t    ackTimeoutMs = 500;
    
    // 历史
    HistoryKind history = HistoryKind::KeepLast;
    uint32_t    historyDepth = 1;             // 保留最近N条
    
    // 持久性
    DurabilityKind durability = DurabilityKind::Volatile;
    //   Volatile       - 不保存历史
    //   TransientLocal  - 保存在内存，新订阅者可收到最近消息
    
    // 生命期
    uint32_t    lifespanMs = 0;               // 0 = 永不过期
    
    // 流量控制
    uint32_t    maxRateHz = 0;                // 0 = 不限制
    uint32_t    sendQueueSize = 1024;
    uint32_t    recvQueueSize = 1024;
    
    // 优先级
    uint8_t     priority = 0;                 // 0-255, 0最高
    
    // 大消息分片
    uint32_t    maxFragmentSize = 65000;       // 自动分片阈值
};
```

### 8.3 QoS 兼容性矩阵

发布者和订阅者 QoS 不匹配时的行为：

| Publisher QoS | Subscriber QoS | 结果 |
|--------------|----------------|------|
| 0 | 0 | ✅ BestEffort |
| 1 | 0 | ✅ 降级为 BestEffort |
| 0 | 1 | ⚠️ 警告，按 BestEffort 工作 |
| 1 | 1 | ✅ Reliable |
| 2 | 2 | ✅ ExactlyOnce |
| 2 | 1 | ✅ 降级为 Reliable |

---

## 9. API 设计

### 9.1 设计原则

- **Builder Pattern** 用于构造配置
- **RAII** 管理生命周期
- **回调/Future** 两种异步模式
- **类型安全** 通过模板支持自动序列化
- **总 API < 20 个核心调用**

### 9.2 核心 API 头文件

```cpp
// embedmq/embedmq.h — 唯一需要包含的头文件

#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <chrono>

namespace embedmq {

// ===================== 前置声明 =====================
class Participant;
class Publisher;
class Subscriber;
class Requester;
class Replier;

// ===================== 基础类型 =====================

/// 消息载荷 —— 零拷贝设计
class Payload {
public:
    Payload() = default;
    
    /// 从二进制数据构造（拷贝）
    Payload(const void* data, size_t size);
    
    /// 从字符串构造
    explicit Payload(std::string_view text);
    
    /// 从移动语义构造（零拷贝）
    Payload(std::vector<uint8_t>&& data);
    
    const uint8_t* data() const;
    size_t size() const;
    
    /// 视为字符串
    std::string_view asText() const;
    
    /// 视为二进制
    std::span<const uint8_t> asBinary() const;
    
    /// 判断是否为空
    bool empty() const;
    
private:
    std::shared_ptr<std::vector<uint8_t>> data_;
};


/// 接收到的消息
struct ReceivedMessage {
    std::string topic;          // 消息主题
    Payload     payload;        // 消息载荷
    uint64_t    timestamp;      // 发送时间戳(ns)
    uint16_t    sourceId;       // 源节点ID
    uint32_t    sequenceId;     // 序列号
};


/// QoS 级别
enum class QoSLevel : uint8_t {
    BestEffort  = 0,
    Reliable    = 1,
    ExactlyOnce = 2,
};

/// 历史策略
enum class HistoryKind : uint8_t {
    KeepLast = 0,
    KeepAll  = 1,
};

/// 持久性
enum class DurabilityKind : uint8_t {
    Volatile       = 0,
    TransientLocal = 1,
};


/// QoS 配置
struct QoSProfile {
    QoSLevel       level           = QoSLevel::BestEffort;
    uint32_t       maxRetries      = 3;
    uint32_t       retryIntervalMs = 100;
    uint32_t       ackTimeoutMs    = 500;
    HistoryKind    history         = HistoryKind::KeepLast;
    uint32_t       historyDepth    = 1;
    DurabilityKind durability      = DurabilityKind::Volatile;
    uint32_t       lifespanMs      = 0;
    uint32_t       maxRateHz       = 0;
    uint32_t       sendQueueSize   = 1024;
    uint32_t       recvQueueSize   = 1024;
    uint8_t        priority        = 128;
    
    /// 预设配置
    static QoSProfile bestEffort();
    static QoSProfile reliable();
    static QoSProfile exactlyOnce();
    static QoSProfile sensorData();     // 高频、可丢失
    static QoSProfile controlCommand(); // 可靠、低延迟
};


/// 参与者配置
struct ParticipantConfig {
    std::string nodeName    = "";       // 节点名称，空则自动生成
    uint8_t     domainId    = 0;        // 通信域ID
    
    // 发现配置
    struct {
        std::string multicastGroup   = "239.255.0.1";
        uint16_t    multicastPort    = 19900;
        uint32_t    announceIntervalMs = 1000;
        uint32_t    heartbeatIntervalMs = 2000;
        uint32_t    peerTimeoutMs    = 10000;
        bool        enableMulticast  = true;
        bool        enableLocalDiscovery = true;  // 本地发现 (UDS/Named Pipe)
    } discovery;
    
    // 传输配置
    struct {
        bool enableUdp      = true;
        bool enableTcp      = false;
        bool enableLocalIpc = true;     // 本地 IPC (Linux/macOS: UDS, Windows: Named Pipe)
        bool enableShm      = false;    // 共享内存
        bool enableSerial   = false;
        bool enableBle      = false;
        
        uint16_t udpPort    = 0;       // 0 = 自动分配
        uint16_t tcpPort    = 0;
        std::string serialDevice = "";  // Linux/macOS: "/dev/ttyS0", Windows: "COM1"（空则使用平台默认值）
        uint32_t    serialBaud   = 115200;
    } transport;
    
    // 线程配置
    struct {
        uint32_t ioThreads = 1;
        uint32_t workerThreads = 1;
        bool     pinCpu = false;
        int      cpuAffinity = -1;
    } threading;
};


// ===================== 序列化接口 =====================

/// 自定义序列化器接口
class ISerializer {
public:
    virtual ~ISerializer() = default;
    virtual uint8_t id() const = 0;                          // 唯一ID
    virtual std::string name() const = 0;                    // 名称
    virtual Payload serialize(const void* obj) const = 0;    // 序列化
    virtual bool deserialize(const Payload& data, 
                            void* obj) const = 0;            // 反序列化
};


// ===================== 回调类型 =====================

/// 订阅回调
using SubscribeCallback = std::function<void(const ReceivedMessage& msg)>;

/// 请求处理器
using RequestHandler = std::function<Payload(const ReceivedMessage& request)>;

/// 连接事件回调
using PeerEventCallback = std::function<void(uint16_t peerId, 
                                              const std::string& peerName,
                                              bool connected)>;


// ===================== Publisher =====================

class Publisher {
public:
    ~Publisher();
    
    /// 发布二进制数据
    bool publish(const void* data, size_t size);
    
    /// 发布 Payload
    bool publish(const Payload& payload);
    
    /// 发布字符串
    bool publish(std::string_view text);
    
    /// 模板：发布自定义类型（需注册序列化器）
    template<typename T>
    bool publish(const T& obj);
    
    /// 获取匹配的订阅者数量
    size_t subscriberCount() const;
    
    /// 获取主题
    const std::string& topic() const;
    
private:
    friend class Participant;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    Publisher(std::unique_ptr<Impl> impl);
};


// ===================== Subscriber =====================

class Subscriber {
public:
    ~Subscriber();
    
    /// 获取主题
    const std::string& topic() const;
    
    /// 暂停/恢复接收
    void pause();
    void resume();
    
    /// 获取已接收消息数
    uint64_t messageCount() const;
    
private:
    friend class Participant;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    Subscriber(std::unique_ptr<Impl> impl);
};


// ===================== Requester =====================

class Requester {
public:
    ~Requester();
    
    /// 同步请求（带超时）
    std::optional<Payload> request(
        const Payload& payload,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));
    
    /// 异步请求
    std::future<Payload> requestAsync(const Payload& payload);
    
    /// 获取服务名
    const std::string& service() const;
    
private:
    friend class Participant;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    Requester(std::unique_ptr<Impl> impl);
};


// ===================== Replier =====================

class Replier {
public:
    ~Replier();
    
    /// 获取服务名
    const std::string& service() const;
    
    /// 获取已处理请求数
    uint64_t requestCount() const;
    
private:
    friend class Participant;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    Replier(std::unique_ptr<Impl> impl);
};


// ===================== Participant =====================

class Participant {
public:
    /// 使用默认配置创建
    static std::unique_ptr<Participant> create(
        const std::string& name = "");
    
    /// 使用自定义配置创建
    static std::unique_ptr<Participant> create(
        const ParticipantConfig& config);
    
    /// 从配置文件创建
    static std::unique_ptr<Participant> createFromFile(
        const std::string& configPath);
    
    ~Participant();
    
    // ---- 发布订阅 ----
    
    std::unique_ptr<Publisher> createPublisher(
        const std::string& topic,
        const QoSProfile& qos = QoSProfile::bestEffort());
    
    std::unique_ptr<Subscriber> createSubscriber(
        const std::string& topic,
        SubscribeCallback callback,
        const QoSProfile& qos = QoSProfile::bestEffort());
    
    // ---- 请求响应 ----
    
    std::unique_ptr<Requester> createRequester(
        const std::string& service,
        const QoSProfile& qos = QoSProfile::reliable());
    
    std::unique_ptr<Replier> createReplier(
        const std::string& service,
        RequestHandler handler,
        const QoSProfile& qos = QoSProfile::reliable());
    
    // ---- 序列化器注册 ----
    
    void registerSerializer(std::shared_ptr<ISerializer> serializer);
    
    // ---- 传输插件注册 ----
    
    void registerTransport(const std::string& name,
                           std::shared_ptr<class ITransport> transport);
    
    // ---- 事件回调 ----
    
    void onPeerEvent(PeerEventCallback callback);
    
    // ---- 状态查询 ----
    
    uint16_t id() const;
    const std::string& name() const;
    std::vector<std::string> discoveredPeers() const;
    bool isRunning() const;
    
    // ---- 生命周期 ----
    
    void shutdown();
    
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    Participant(std::unique_ptr<Impl> impl);
};

} // namespace embedmq
```

### 9.3 API 数量统计

| 类 | 核心方法数 | 说明 |
|----|-----------|------|
| Participant | 8 | 创建、注册、查询、关闭 |
| Publisher | 3 | publish 重载 |
| Subscriber | 2 | pause/resume |
| Requester | 2 | 同步/异步请求 |
| Replier | 1 | 构造时传入 handler |
| **合计** | **16** | **< 20，满足简洁要求** |

---

## 10. Transport 插件体系

### 10.1 Transport 接口

```cpp
// embedmq/transport/itransport.h

#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace embedmq {

/// 传输层端点标识
struct Endpoint {
    std::string address;        // 地址 (IP, path, device, ...)
    uint16_t    port = 0;       // 端口 (如适用)
    std::string transportType;  // 传输类型名称
    
    std::string toString() const;
    bool operator==(const Endpoint& o) const;
};

/// 传输事件类型
enum class TransportEvent {
    Connected,
    Disconnected,
    Error,
};

/// 接收回调
using TransportRecvCallback = std::function<void(
    const Endpoint& from,
    const uint8_t* data,
    size_t size)>;

/// 事件回调
using TransportEventCallback = std::function<void(
    const Endpoint& peer,
    TransportEvent event,
    const std::string& detail)>;

/// 传输层能力描述
struct TransportCapability {
    bool supportsMulticast  = false;
    bool supportsBroadcast  = false;
    bool supportsReliable   = false;    // 底层是否支持可靠传输
    bool supportsStreaming   = false;    // 流式传输
    uint32_t maxPayloadSize = 65535;    // 单次发送最大载荷
    uint32_t estimatedLatencyUs = 100;  // 预估延迟(微秒)
    uint32_t estimatedBandwidthKbps = 100000; // 预估带宽(kbps)
};


/// 传输层插件接口
class ITransport {
public:
    virtual ~ITransport() = default;
    
    /// 获取传输类型名称
    virtual std::string typeName() const = 0;
    
    /// 获取能力描述
    virtual TransportCapability capability() const = 0;
    
    /// 初始化
    /// @param config JSON格式配置字符串
    virtual bool init(const std::string& config) = 0;
    
    /// 关闭
    virtual void shutdown() = 0;
    
    /// 发送数据到指定端点
    virtual bool send(const Endpoint& to,
                      const uint8_t* data, size_t size) = 0;
    
    /// 广播/多播发送（如支持）
    virtual bool broadcast(const uint8_t* data, size_t size) = 0;
    
    /// 设置接收回调
    virtual void setRecvCallback(TransportRecvCallback cb) = 0;
    
    /// 设置事件回调
    virtual void setEventCallback(TransportEventCallback cb) = 0;
    
    /// 获取本地端点列表
    virtual std::vector<Endpoint> localEndpoints() const = 0;
    
    /// 是否已初始化
    virtual bool isActive() const = 0;
};


/// Transport 工厂函数类型
using TransportFactory = std::function<std::shared_ptr<ITransport>()>;

} // namespace embedmq
```

### 10.2 内置 Transport 插件

#### 10.2.1 UDP Transport

```cpp
// embedmq/transport/udp_transport.h

class UdpTransport : public ITransport {
public:
    std::string typeName() const override { return "udp"; }
    
    TransportCapability capability() const override {
        return {
            .supportsMulticast = true,
            .supportsBroadcast = true,
            .supportsReliable = false,
            .supportsStreaming = false,
            .maxPayloadSize = 65507,
            .estimatedLatencyUs = 50,
            .estimatedBandwidthKbps = 1000000,
        };
    }
    
    // ... 实现细节
    
private:
    int unicastFd_  = -1;
    int multicastFd_ = -1;
    std::thread recvThread_;
    // 使用 epoll 事件循环
};
```

#### 10.2.2 本地 IPC Transport（跨平台）

```cpp
/// 跨平台本地 IPC 传输
/// Linux/macOS: Unix Domain Socket
/// Windows: Named Pipe
class LocalIpcTransport : public ITransport {
public:
    std::string typeName() const override { return "local_ipc"; }
    
    TransportCapability capability() const override {
        return {
            .supportsMulticast = false,
            .supportsBroadcast = false,
            .supportsReliable = true,
            .supportsStreaming = true,
#ifdef _WIN32
            .maxPayloadSize = 65536,        // Named Pipe 缓冲区
#else
            .maxPayloadSize = 212992,       // UDS 默认缓冲区
#endif
            .estimatedLatencyUs = 5,
            .estimatedBandwidthKbps = 5000000,
        };
    }
    
private:
#ifdef _WIN32
    // Windows Named Pipe
    void* pipeHandle_ = nullptr;           // HANDLE
    std::string pipeName_;                  // \\.\pipe\embedmq_xxx
#else
    // POSIX Unix Domain Socket
    int fd_ = -1;
    std::string socketPath_;
#endif
};
```

#### 10.2.3 共享内存 Transport（跨平台）

```cpp
/// 跨平台共享内存传输
/// Linux/macOS: POSIX shm_open + mmap
/// Windows: CreateFileMapping + MapViewOfFile
class ShmTransport : public ITransport {
public:
    std::string typeName() const override { return "shm"; }
    
    TransportCapability capability() const override {
        return {
            .supportsMulticast = false,
            .supportsBroadcast = false,
            .supportsReliable = true,
            .supportsStreaming = false,
            .maxPayloadSize = 1048576,      // 1MB
            .estimatedLatencyUs = 1,        // 极低延迟
            .estimatedBandwidthKbps = 10000000,
        };
    }
    
private:
    void* shmPtr_ = nullptr;
#ifdef _WIN32
    void* hMapFile_ = nullptr;             // HANDLE (CreateFileMapping)
    void* hEvent_   = nullptr;             // HANDLE (CreateEvent 通知)
#else
    int shmFd_ = -1;                       // shm_open fd
    int notifyFd_ = -1;                    // eventfd (Linux) / pipe (macOS) 通知
#endif
    // 环形缓冲区（跨平台）
};
```

#### 10.2.4 Serial Transport（跨平台）

```cpp
/// 跨平台串口传输
/// Linux/macOS: termios API
/// Windows: Win32 CommAPI (CreateFile + ReadFile/WriteFile)
class SerialTransport : public ITransport {
public:
    std::string typeName() const override { return "serial"; }
    
    TransportCapability capability() const override {
        return {
            .supportsMulticast = false,
            .supportsBroadcast = false,
            .supportsReliable = false,
            .supportsStreaming = true,
            .maxPayloadSize = 4096,
            .estimatedLatencyUs = 1000,
            .estimatedBandwidthKbps = 115,  // 115200 baud
        };
    }
    
private:
#ifdef _WIN32
    void* hSerial_ = nullptr;              // HANDLE (CreateFile)
#else
    int fd_ = -1;                          // open() fd
#endif
    std::string device_;                    // "/dev/ttyS0" 或 "COM1"
    uint32_t baudRate_;
    // SLIP/COBS 帧化
};
```

#### 10.2.5 BLE Transport（跨平台）

```cpp
/// 跨平台蓝牙低功耗传输
/// Linux: BlueZ D-Bus API 或 直接 HCI
/// macOS: CoreBluetooth.framework
/// Windows: WinRT BLE API (Windows.Devices.Bluetooth)
class BleTransport : public ITransport {
public:
    std::string typeName() const override { return "ble"; }
    
    TransportCapability capability() const override {
        return {
            .supportsMulticast = false,
            .supportsBroadcast = true,      // BLE 广播
            .supportsReliable = false,
            .supportsStreaming = false,
            .maxPayloadSize = 244,          // BLE 5.0 MTU
            .estimatedLatencyUs = 7500,     // ~7.5ms连接间隔
            .estimatedBandwidthKbps = 250,
        };
    }
    
private:
    struct PlatformImpl;
    std::unique_ptr<PlatformImpl> platformImpl_;
};
```

### 10.3 Transport 选择策略

```cpp
class TransportSelector {
public:
    /// 根据对端和消息特征选择最优传输
    ITransport* select(const Endpoint& peer, 
                       const MessageHeader& header) {
        // 1. 同一设备 -> 优先 SHM > LocalIPC(UDS/NamedPipe) > UDP loopback
        // 2. 局域网   -> UDP (BestEffort) 或 TCP (Reliable)
        // 3. 串口连接 -> Serial
        // 4. 蓝牙设备 -> BLE
        // 5. 根据 QoS 需求微调
        //    - QoS 2 且底层不可靠 -> 应用层可靠性
        //    - 大消息 -> 选带宽高的传输
        //    - 低延迟要求 -> 选延迟低的传输
    }
};
```

### 10.4 自定义 Transport 插件开发

用户只需实现 `ITransport` 接口并注册：

```cpp
class MyCanTransport : public embedmq::ITransport {
    // 实现所有纯虚函数
    std::string typeName() const override { return "can"; }
    // ...
};

// 注册
participant->registerTransport("can", std::make_shared<MyCanTransport>());
```

---

## 11. 跨语言扩展方案

### 11.1 架构

```
┌──────────────────────────────────────────┐
│           Language Bindings              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│  │  C++ API │ │  C API   │ │ Python   │ │
│  │ (native) │ │ (FFI)    │ │ (ctypes/ │ │
│  │          │ │          │ │  pybind) │ │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ │
│       │            │            │        │
│  ┌────┴────────────┴────────────┴─────┐  │
│  │         C ABI Layer                │  │
│  │     embedmq_c.h (extern "C")      │  │
│  └────────────────┬───────────────────┘  │
│                   │                      │
│  ┌────────────────┴───────────────────┐  │
│  │       C++ Core Library             │  │
│  │  libembedmq.so/.dylib/.dll         │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
```

### 11.2 C ABI 接口

```c
// embedmq/embedmq_c.h

#ifndef EMBEDMQ_C_H
#define EMBEDMQ_C_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ---- 句柄类型 ----
typedef void* emq_participant_t;
typedef void* emq_publisher_t;
typedef void* emq_subscriber_t;
typedef void* emq_requester_t;
typedef void* emq_replier_t;

// ---- 返回码 ----
typedef enum {
    EMQ_OK              = 0,
    EMQ_ERROR           = -1,
    EMQ_TIMEOUT         = -2,
    EMQ_INVALID_ARG     = -3,
    EMQ_NO_MEMORY       = -4,
    EMQ_NOT_FOUND       = -5,
} emq_status_t;

// ---- QoS ----
typedef enum {
    EMQ_QOS_BEST_EFFORT  = 0,
    EMQ_QOS_RELIABLE     = 1,
    EMQ_QOS_EXACTLY_ONCE = 2,
} emq_qos_t;

// ---- 回调 ----
typedef void (*emq_recv_callback_t)(
    const char* topic,
    const uint8_t* data,
    size_t size,
    void* userdata);

typedef void (*emq_request_handler_t)(
    const char* service,
    const uint8_t* req_data,
    size_t req_size,
    uint8_t** resp_data,
    size_t* resp_size,
    void* userdata);

// ---- Participant ----
emq_participant_t emq_create(const char* name);
emq_participant_t emq_create_with_config(const char* config_json);
void              emq_destroy(emq_participant_t p);

// ---- Pub/Sub ----
emq_publisher_t   emq_create_publisher(emq_participant_t p,
                                        const char* topic,
                                        emq_qos_t qos);
emq_status_t      emq_publish(emq_publisher_t pub,
                               const uint8_t* data,
                               size_t size);
void              emq_destroy_publisher(emq_publisher_t pub);

emq_subscriber_t  emq_create_subscriber(emq_participant_t p,
                                          const char* topic,
                                          emq_qos_t qos,
                                          emq_recv_callback_t cb,
                                          void* userdata);
void              emq_destroy_subscriber(emq_subscriber_t sub);

// ---- Req/Rep ----
emq_requester_t   emq_create_requester(emq_participant_t p,
                                        const char* service,
                                        emq_qos_t qos);
emq_status_t      emq_request(emq_requester_t req,
                               const uint8_t* data, size_t size,
                               uint8_t** resp, size_t* resp_size,
                               uint32_t timeout_ms);
void              emq_free_response(uint8_t* resp);
void              emq_destroy_requester(emq_requester_t req);

emq_replier_t     emq_create_replier(emq_participant_t p,
                                      const char* service,
                                      emq_qos_t qos,
                                      emq_request_handler_t handler,
                                      void* userdata);
void              emq_destroy_replier(emq_replier_t rep);

#ifdef __cplusplus
}
#endif

#endif // EMBEDMQ_C_H
```

### 11.3 Python 绑定示例

```python
# embedmq/bindings/python/embedmq.py (基于 ctypes)

import ctypes
from ctypes import c_void_p, c_char_p, c_uint8, c_size_t, c_int, CFUNCTYPE, POINTER

import sys
if sys.platform == 'win32':
    lib = ctypes.CDLL('embedmq.dll')
elif sys.platform == 'darwin':
    lib = ctypes.CDLL('libembedmq.dylib')
else:
    lib = ctypes.CDLL('libembedmq.so')

RECV_CALLBACK = CFUNCTYPE(None, c_char_p, POINTER(c_uint8), c_size_t, c_void_p)

class Participant:
    def __init__(self, name=""):
        self._handle = lib.emq_create(name.encode())
    
    def __del__(self):
        lib.emq_destroy(self._handle)
    
    def create_publisher(self, topic, qos=0):
        return Publisher(lib.emq_create_publisher(
            self._handle, topic.encode(), qos))
    
    def create_subscriber(self, topic, callback, qos=0):
        return Subscriber(lib.emq_create_subscriber(
            self._handle, topic.encode(), qos,
            RECV_CALLBACK(callback), None))

class Publisher:
    def __init__(self, handle):
        self._handle = handle
    
    def publish(self, data):
        if isinstance(data, str):
            data = data.encode()
        buf = (c_uint8 * len(data))(*data)
        return lib.emq_publish(self._handle, buf, len(data))
```

---

## 12. 关键流程时序

### 12.1 发布-订阅流程

```
 Publisher App        Participant A          Network          Participant B        Subscriber App
      │                    │                    │                    │                    │
      │  publish(topic,    │                    │                    │                    │
      │   payload)         │                    │                    │                    │
      ├──────────────────>│                    │                    │                    │
      │                    │                    │                    │                    │
      │               [1] TopicRouter:         │                    │                    │
      │                   查找本地订阅者        │                    │                    │
      │                   查找远程订阅者        │                    │                    │
      │                    │                    │                    │                    │
      │               [2] QoSEngine:           │                    │                    │
      │                   应用 QoS 策略        │                    │                    │
      │                    │                    │                    │                    │
      │               [3] MsgEncoder:          │                    │                    │
      │                   序列化 + 组帧        │                    │                    │
      │                    │                    │                    │                    │
      │               [4] Transport:           │                    │                    │
      │                   选择传输通道          │                    │                    │
      │                    │                    │                    │                    │
      │                    ├──── PUBLISH ──────>├─────────────────>│                    │
      │                    │    (wire format)   │                    │                    │
      │                    │                    │               [5] MsgDecoder:          │
      │                    │                    │                   解帧 + 反序列化      │
      │                    │                    │                    │                    │
      │                    │                    │               [6] QoSEngine:           │
      │                    │                    │                   去重、ACK            │
      │                    │                    │                    │                    │
      │                    │                    │               [7] TopicRouter:          │
      │                    │                    │                   匹配本地订阅          │
      │                    │                    │                    │                    │
      │                    │                    │                    ├──────────────────>│
      │                    │                    │                    │  callback(msg)     │
      │                    │                    │                    │                    │
      │                    │  (QoS ≥ 1)         │                    │                    │
      │                    │<──── ACK ──────────├<───────────────── │                    │
      │                    │                    │                    │                    │
      │  return OK         │                    │                    │                    │
      │<──────────────────│                    │                    │                    │
```

### 12.2 请求-响应流程

```
 Requester App        Participant A          Network          Participant B        Replier App
      │                    │                    │                    │                    │
      │  request(service,  │                    │                    │                    │
      │   payload, timeout)│                    │                    │                    │
      ├──────────────────>│                    │                    │                    │
      │                    │                    │                    │                    │
      │               [1] 生成 correlationId   │                    │                    │
      │               [2] 注册等待回调          │                    │                    │
      │                    │                    │                    │                    │
      │                    ├──── REQUEST ──────>├─────────────────>│                    │
      │                    │  (correlationId=X) │                    │                    │
      │                    │                    │                    │                    │
      │                    │                    │               [3] 匹配 service handler │
      │                    │                    │                    ├──────────────────>│
      │                    │                    │                    │  handler(request)  │
      │                    │                    │                    │                    │
      │                    │                    │                    │<──────────────────│
      │                    │                    │                    │  return response   │
      │                    │                    │                    │                    │
      │                    │<──── REPLY ────────├<───────────────── │                    │
      │                    │  (correlationId=X) │                    │                    │
      │                    │                    │                    │                    │
      │               [4] 匹配 correlationId   │                    │                    │
      │               [5] 唤醒等待线程          │                    │                    │
      │                    │                    │                    │                    │
      │  return response   │                    │                    │                    │
      │<──────────────────│                    │                    │                    │
```

### 12.3 自发现与连接建立

```
timeline (ms):

0     Node A 启动
      ├── 创建 Participant
      ├── 启动 DiscoveryAgent
      ├── 绑定本地 Transport (UDP:random, LocalIPC:平台路径)
      └── 发送 ANNOUNCE (multicast, 239.255.0.1:19900)
           { id:A, name:"node_a", topics:[pub:"sensor/temp"], 
             endpoints:[udp:192.168.1.10:45000, local_ipc:平台路径] }

100   Node B 启动（已订阅 "sensor/temp"）
      ├── 发送 ANNOUNCE
      └── 收到 Node A 的 ANNOUNCE
           → 发现 A 发布 "sensor/temp"，本地有订阅
           → 发送 DISCOVER_REQ (unicast to A)

150   Node A 收到 DISCOVER_REQ from B
      ├── 检查 B 是否同设备（比较 hostname/IP）
      │   YES → 建议使用 LocalIPC transport
      │   NO  → 建议使用 UDP transport
      └── 回复 DISCOVER_RSP 
           { preferred_transport: "local_ipc", endpoint: "平台路径" }

200   Node B 收到 DISCOVER_RSP
      ├── 注册 Peer A
      ├── 建立到 A 的数据通道 (LocalIPC)
      └── 开始心跳检测

1000  Node A 发布 sensor/temp 数据
      → MessageBus 查找订阅者
      → 发现 Peer B 通过 LocalIPC 连接
      → 通过 LocalIPC 发送 PUBLISH 消息

2000  定期 HEARTBEAT 交换
      → 超时未收到 → 标记 peer 离线 → 通知应用层
```

---

## 13. 可靠性与容错

### 13.1 消息可靠性

```
┌─────────────────────────────────────────────────────────┐
│                  QoS Engine 状态机                       │
│                                                         │
│  QoS 0 (BestEffort):                                    │
│     SEND ──────────────> DONE                           │
│     (fire and forget)                                   │
│                                                         │
│  QoS 1 (Reliable):                                      │
│     SEND ──> WAIT_ACK ──> DONE                         │
│                  │                                      │
│                  ├── timeout → RETRY (up to maxRetries) │
│                  └── NACK   → RETRY                    │
│                                                         │
│  QoS 2 (ExactlyOnce):                                   │
│     SEND ──> WAIT_REC ──> SEND_REL ──> WAIT_COMP ──> DONE │
│     (类似 MQTT QoS 2 四步握手，但简化为三步)              │
│                                                         │
│  Receiver Side:                                         │
│     QoS 1: 记录 seqId，收到重复消息时去重               │
│     QoS 2: 维护 seqId 集合，确保恰好一次                │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 13.2 连接容错

| 场景 | 处理策略 |
|------|----------|
| 对端进程崩溃 | 心跳超时检测 → 清理路由表 → 通知应用层 → 对端重启后自动重新发现 |
| 网络临时中断 | 重传队列保留未确认消息 → 连接恢复后自动重传 |
| Transport 故障 | 自动切换到备选 Transport（如 UDP 失败切 TCP） |
| 消息积压 | 达到队列上限时：根据策略丢弃旧消息或阻塞发送 |
| 分片丢失 | 分片超时定时器 → 请求重传丢失分片 或 丢弃整个消息 |

### 13.3 保留消息 (Retained)

```cpp
// 发布保留消息
pub->publish(payload);  // QoSProfile 中设置 retain = true

// 新订阅者连接时自动收到最新的保留消息
// 实现：每个 topic 维护最新一条 retained message
// 存储在 Participant 的 RetainedStore 中
```

### 13.4 遗嘱消息 (Last Will)

```cpp
ParticipantConfig config;
config.lastWill.topic   = "status/node_a";
config.lastWill.payload = Payload("offline");
config.lastWill.qos     = QoSLevel::Reliable;
config.lastWill.retain   = true;

// 当节点异常断开时，其他节点会自动发布遗嘱消息
// 实现：在 ANNOUNCE 中携带遗嘱信息，Peer 超时后由本地触发
```

---

## 14. 性能设计

### 14.1 零拷贝

```
应用层                   中间件                     传输层
  │                        │                         │
  │  publish(data, len)    │                         │
  ├───────────────────────>│                         │
  │                        │                         │
  │  [1] 如果 QoS=0 且    │                         │
  │      传输支持 scatter/ │                         │
  │      gather I/O:       │                         │
  │                        │                         │
  │      Header 和 Payload │                         │
  │      分别存放，不合并   │                         │
  │      拷贝              │                         │
  │                        ├──── scatter write ─────>│
  │                        │    [header][payload]    │
  │                        │    Linux: writev(iov)   │
  │                        │    Win: WSASend(bufs)   │
  │                        │    (2次DMA，0次拷贝)     │
  │                        │                         │
  │  [2] 共享内存传输:     │                         │
  │      直接写入共享环形   │                         │
  │      缓冲区，通知对端   │                         │
  │      Linux: eventfd    │                         │
  │      macOS: pipe       │                         │
  │      Win:   Event      │                         │
```

### 14.2 内存管理

```cpp
/// 内存池 —— 减少 malloc/free 开销
class MessagePool {
    // 预分配固定大小块（64B, 256B, 1KB, 4KB, 64KB）
    // 使用 free-list 管理
    // 线程安全：每线程本地缓存 + 全局池
    
    void* allocate(size_t size);
    void  deallocate(void* ptr, size_t size);
};

/// 环形缓冲区 —— 无锁队列
template<typename T, size_t Capacity>
class SPSCRingBuffer {
    // 单生产者单消费者无锁环形缓冲区
    // 用于 IO 线程 → 工作线程 的消息传递
    
    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
    T buffer_[Capacity];
    
    bool push(T&& item);
    bool pop(T& item);
};
```

### 14.3 IO 模型

IO 模型通过 PAL 层的 `EventLoop` 抽象统一各平台差异：

| 平台 | IO 多路复用 | 通知机制 | 定时器 |
|------|-----------|---------|--------|
| **Linux** | epoll (默认) / io_uring (可选) | eventfd | timerfd / 时间轮 |
| **macOS** | kqueue | pipe (self-pipe trick) | kqueue EVFILT_TIMER / 时间轮 |
| **Windows** | IOCP (IO Completion Ports) | CreateEvent / WSAEvent | CreateTimerQueueTimer / 时间轮 |

```
┌──────────────────────────────────────────────────┐
│                   IO Thread                       │
│                                                  │
│  ┌──────────────────────────────────────────┐    │
│  │     PAL::EventLoop (平台自适应)           │    │
│  │     Linux: epoll / io_uring              │    │
│  │     macOS: kqueue                        │    │
│  │     Windows: IOCP                        │    │
│  │                                          │    │
│  │  handle1 (UDP socket) ──> recv + decode  │    │
│  │  handle2 (LocalIPC)   ──> recv + decode  │    │
│  │  handle3 (serial)     ──> recv + decode  │    │
│  │  handle4 (timer)      ──> heartbeat/retry│    │
│  │  handle5 (wakeup)     ──> wakeup signal  │    │
│  │                                          │    │
│  └──────────────────┬───────────────────────┘    │
│                     │                            │
│                     ▼                            │
│  ┌──────────────────────────────────────────┐    │
│  │         SPSC Ring Buffer                  │    │
│  └──────────────────┬───────────────────────┘    │
│                     │                            │
├─────────────────────┼────────────────────────────┤
│                     ▼                            │
│               Worker Thread(s)                   │
│  ┌──────────────────────────────────────────┐    │
│  │  TopicRouter → callback dispatch         │    │
│  │  QoS processing                          │    │
│  │  User callback invocation                │    │
│  └──────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
```

### 14.4 预期性能指标

| 场景 | 延迟 (P50) | 延迟 (P99) | 吞吐量 |
|------|-----------|-----------|--------|
| 同进程 (inproc) | ~1 μs | ~5 μs | > 5M msg/s |
| 共享内存 (shm) | ~3 μs | ~10 μs | > 2M msg/s |
| Unix Domain Socket | ~10 μs | ~50 μs | > 500K msg/s |
| UDP (同机loopback) | ~20 μs | ~100 μs | > 300K msg/s |
| UDP (千兆局域网) | ~100 μs | ~500 μs | > 200K msg/s |
| 串口 (115200 baud) | ~5 ms | ~20 ms | ~1K msg/s |

---

## 15. 目录结构与构建

### 15.1 项目目录结构

```
embedmq/
├── xmake.lua                         # 主构建脚本 (xmake)
├── CMakeLists.txt                    # 兼容构建 (CMake)
├── README.md
├── LICENSE
├── docs/
│   ├── design.md                     # 本设计文档
│   ├── api_reference.md
│   └── getting_started.md
│
├── include/
│   └── embedmq/
│       ├── embedmq.h                 # C++ 主头文件
│       ├── embedmq_c.h               # C ABI 头文件
│       ├── types.h                   # 基础类型定义
│       ├── platform.h                # 平台检测宏与统一类型
│       ├── qos.h                     # QoS 相关
│       ├── config.h                  # 配置结构
│       └── transport/
│           └── itransport.h          # Transport 接口
│
├── src/
│   ├── core/
│   │   ├── participant.cpp
│   │   ├── publisher.cpp
│   │   ├── subscriber.cpp
│   │   ├── requester.cpp
│   │   ├── replier.cpp
│   │   ├── message_bus.cpp           # 消息总线核心
│   │   ├── message_bus.h
│   │   ├── topic_router.cpp          # 主题路由与匹配
│   │   ├── topic_router.h
│   │   ├── qos_engine.cpp            # QoS 策略引擎
│   │   ├── qos_engine.h
│   │   ├── message_codec.cpp         # 消息编解码
│   │   ├── message_codec.h
│   │   ├── serializer_mgr.cpp
│   │   └── serializer_mgr.h
│   │
│   ├── discovery/
│   │   ├── discovery_agent.cpp       # 发现代理
│   │   ├── discovery_agent.h
│   │   ├── peer_registry.cpp         # 对端注册表
│   │   ├── peer_registry.h
│   │   ├── announcer.cpp
│   │   └── announcer.h
│   │
│   ├── transport/
│   │   ├── transport_manager.cpp     # 传输管理器
│   │   ├── transport_manager.h
│   │   ├── transport_selector.cpp    # 智能选择
│   │   ├── transport_selector.h
│   │   ├── udp_transport.cpp         # UDP (跨平台 BSD/Winsock)
│   │   ├── udp_transport.h
│   │   ├── local_ipc_transport.cpp   # 本地 IPC (UDS / Named Pipe)
│   │   ├── local_ipc_transport.h
│   │   ├── tcp_transport.cpp         # TCP (跨平台)
│   │   ├── tcp_transport.h
│   │   ├── shm_transport.cpp         # 共享内存 (POSIX / Win32)
│   │   ├── shm_transport.h
│   │   ├── serial_transport.cpp      # 串口 (termios / Win32 Comm)
│   │   ├── serial_transport.h
│   │   ├── ble_transport.cpp         # 蓝牙 (BlueZ/CoreBT/WinRT)
│   │   └── ble_transport.h
│   │
│   ├── platform/                     # 平台抽象层 (PAL)
│   │   ├── platform_defs.h           # 平台检测、统一宏定义
│   │   ├── event_loop.h              # 事件循环接口
│   │   ├── event_loop_epoll.cpp      # Linux: epoll 实现
│   │   ├── event_loop_kqueue.cpp     # macOS: kqueue 实现
│   │   ├── event_loop_iocp.cpp       # Windows: IOCP 实现
│   │   ├── socket_api.h              # 统一 socket 接口
│   │   ├── socket_api_posix.cpp      # POSIX socket 实现
│   │   ├── socket_api_win.cpp        # Winsock 实现
│   │   ├── shared_memory.h           # 共享内存接口
│   │   ├── shared_memory_posix.cpp   # POSIX shm 实现
│   │   ├── shared_memory_win.cpp     # Win32 FileMapping 实现
│   │   ├── serial_port.h             # 串口接口
│   │   ├── serial_port_posix.cpp     # termios 实现
│   │   ├── serial_port_win.cpp       # Win32 CommAPI 实现
│   │   ├── local_ipc.h               # 本地 IPC 接口
│   │   ├── local_ipc_uds.cpp         # Unix Domain Socket 实现
│   │   ├── local_ipc_namedpipe.cpp   # Windows Named Pipe 实现
│   │   ├── clock.h                   # 高精度时钟 (std::chrono 统一)
│   │   └── process.h                 # 进程工具 (PID, hostname 等)
│   │
│   ├── util/
│   │   ├── ring_buffer.h             # 无锁环形缓冲 (纯C++, 跨平台)
│   │   ├── memory_pool.h             # 内存池
│   │   ├── memory_pool.cpp
│   │   ├── timer_wheel.h             # 时间轮定时器 (纯C++, 跨平台)
│   │   ├── timer_wheel.cpp
│   │   ├── crc32.h                   # CRC32 校验
│   │   ├── logger.h                  # 日志
│   │   └── logger.cpp
│   │
│   └── bindings/
│       └── c_api.cpp                 # C ABI 实现
│
├── bindings/
│   └── python/
│       ├── embedmq.py                # Python ctypes 绑定
│       └── setup.py
│
├── examples/
│   ├── pub_sub/
│   │   ├── publisher.cpp
│   │   └── subscriber.cpp
│   ├── req_rep/
│   │   ├── client.cpp
│   │   └── server.cpp
│   ├── sensor_demo/
│   │   ├── sensor_node.cpp
│   │   └── display_node.cpp
│   └── serial_bridge/
│       ├── device_a.cpp
│       └── device_b.cpp
│
├── tests/                               # 统一可执行文件 emq_tests
│   ├── test_framework.h                 # 轻量框架（模块注册 + 命令行过滤）
│   ├── test_main.cpp                    # 统一入口（支持 --list / --help / 模块过滤）
│   ├── test_topic_router.cpp            # 模块: topic_router
│   ├── test_qos_engine.cpp              # 模块: qos_engine
│   ├── test_message_codec.cpp           # 模块: message_codec
│   ├── test_pub_sub.cpp                 # 模块: pub_sub
│   ├── test_req_rep.cpp                 # 模块: req_rep
│   ├── test_pal.cpp                     # 模块: pal (进程工具/CRC32/环形缓冲/定时器)
│   ├── test_discovery.cpp
│   ├── test_transport_udp.cpp
│   ├── test_transport_local_ipc.cpp
│   └── benchmark/
│       ├── bench_latency.cpp
│       └── bench_throughput.cpp
│
├── tools/
│   ├── emq_monitor.cpp               # 命令行监控工具
│   └── emq_bridge.cpp                # 跨网络桥接工具
│
└── cmake/
    ├── EmbedMQConfig.cmake.in
    ├── PlatformSetup.cmake            # 平台检测与配置
    ├── FindBluez.cmake
    └── cross_compile/
        ├── armv7.cmake
        └── aarch64.cmake
```

### 15.2 xmake 构建（主构建系统）

```lua
-- xmake.lua (顶层)

set_project("EmbedMQ")
set_version("1.0.0")
set_xmakever("2.7.0")

add_rules("mode.debug", "mode.release")
set_languages("c++17")

-- ---- 选项 ----
option("enable_shm",    { default = true,  description = "Enable shared memory transport" })
option("enable_serial", { default = true,  description = "Enable serial transport" })
option("enable_ble",    { default = false, description = "Enable BLE transport" })
option("enable_iouring",{ default = false, description = "Enable io_uring (Linux only)" })
option("build_examples",{ default = true,  description = "Build examples" })
option("build_tests",   { default = true,  description = "Build tests" })
option("build_tools",   { default = true,  description = "Build tools" })
option("build_c_api",   { default = true,  description = "Build C ABI" })

-- ---- 平台检测与 PAL 源码 ----
local pal_sources = { "src/platform/clock.h", "src/platform/process.h" }

if is_os("linux") then
    add_defines("EMQ_PLATFORM_LINUX")
    pal_sources = {
        "src/platform/event_loop_epoll.cpp",
        "src/platform/socket_api_posix.cpp",
        "src/platform/shared_memory_posix.cpp",
        "src/platform/serial_port_posix.cpp",
        "src/platform/local_ipc_uds.cpp",
    }
elseif is_os("macosx") then
    add_defines("EMQ_PLATFORM_MACOS")
    pal_sources = {
        "src/platform/event_loop_kqueue.cpp",
        "src/platform/socket_api_posix.cpp",
        "src/platform/shared_memory_posix.cpp",
        "src/platform/serial_port_posix.cpp",
        "src/platform/local_ipc_uds.cpp",
    }
elseif is_os("windows") then
    add_defines("EMQ_PLATFORM_WINDOWS")
    pal_sources = {
        "src/platform/event_loop_iocp.cpp",
        "src/platform/socket_api_win.cpp",
        "src/platform/shared_memory_win.cpp",
        "src/platform/serial_port_win.cpp",
        "src/platform/local_ipc_namedpipe.cpp",
    }
end

-- ---- 主库 ----
target("embedmq")
    set_kind("static")
    add_headerfiles("include/(embedmq/*.h)", "include/(embedmq/transport/*.h)")
    add_includedirs("include", { public = true })
    add_includedirs("src")
    add_files("src/core/*.cpp")
    add_files("src/discovery/*.cpp")
    add_files("src/util/*.cpp")
    add_files("src/transport/transport_manager.cpp")
    add_files("src/transport/transport_selector.cpp")
    add_files("src/transport/udp_transport.cpp")
    add_files("src/transport/local_ipc_transport.cpp")
    add_files("src/transport/tcp_transport.cpp")
    add_files(table.unpack(pal_sources))

    if has_config("enable_shm") then
        add_defines("EMBEDMQ_ENABLE_SHM")
        add_files("src/transport/shm_transport.cpp")
    end
    if has_config("enable_serial") then
        add_defines("EMBEDMQ_ENABLE_SERIAL")
        add_files("src/transport/serial_transport.cpp")
    end
    if has_config("enable_ble") then
        add_defines("EMBEDMQ_ENABLE_BLE")
        add_files("src/transport/ble_transport.cpp")
    end
    if has_config("build_c_api") then
        add_files("src/bindings/c_api.cpp")
    end

    -- 平台链接库
    if is_os("linux") then
        add_syslinks("pthread", "rt")
        if has_config("enable_iouring") then
            add_defines("EMBEDMQ_ENABLE_IOURING")
            add_syslinks("uring")
        end
    elseif is_os("macosx") then
        add_syslinks("pthread")
    elseif is_os("windows") then
        add_syslinks("ws2_32", "mswsock", "advapi32")
    end

-- ---- 动态库 ----
target("embedmq_shared")
    set_kind("shared")
    add_deps("embedmq", { inherit = true })
    -- 继承 embedmq 的所有源码和配置

-- ---- 示例 / 测试 / 工具 (略，结构同上) ----
```

### 15.3 CMake 构建（兼容）

```cmake
# CMakeLists.txt (顶层)

cmake_minimum_required(VERSION 3.14)
project(EmbedMQ VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---- 平台检测 ----
include(cmake/PlatformSetup.cmake)

# ---- 选项 ----
option(EMBEDMQ_BUILD_SHARED   "Build shared library"          ON)
option(EMBEDMQ_BUILD_STATIC   "Build static library"          ON)
option(EMBEDMQ_BUILD_EXAMPLES "Build examples"                ON)
option(EMBEDMQ_BUILD_TESTS    "Build tests"                   ON)
option(EMBEDMQ_BUILD_TOOLS    "Build tools"                   ON)
option(EMBEDMQ_BUILD_C_API    "Build C ABI"                   ON)
option(EMBEDMQ_ENABLE_SHM     "Enable shared memory transport" ON)
option(EMBEDMQ_ENABLE_SERIAL  "Enable serial transport"       ON)
option(EMBEDMQ_ENABLE_BLE     "Enable BLE transport"          OFF)
option(EMBEDMQ_ENABLE_IOURING "Enable io_uring (Linux only)"  OFF)

# ---- 源码收集 ----
file(GLOB_RECURSE CORE_SOURCES    "src/core/*.cpp")
file(GLOB_RECURSE DISC_SOURCES    "src/discovery/*.cpp")
file(GLOB_RECURSE UTIL_SOURCES    "src/util/*.cpp")

set(TRANSPORT_SOURCES
    src/transport/transport_manager.cpp
    src/transport/transport_selector.cpp
    src/transport/udp_transport.cpp
    src/transport/local_ipc_transport.cpp
    src/transport/tcp_transport.cpp
)

# ---- PAL 源码（按平台选择） ----
if(WIN32)
    add_definitions(-DEMQ_PLATFORM_WINDOWS)
    set(PAL_SOURCES
        src/platform/event_loop_iocp.cpp
        src/platform/socket_api_win.cpp
        src/platform/shared_memory_win.cpp
        src/platform/serial_port_win.cpp
        src/platform/local_ipc_namedpipe.cpp
    )
    set(PLATFORM_LIBS ws2_32 mswsock advapi32)
elseif(APPLE)
    add_definitions(-DEMQ_PLATFORM_MACOS)
    set(PAL_SOURCES
        src/platform/event_loop_kqueue.cpp
        src/platform/socket_api_posix.cpp
        src/platform/shared_memory_posix.cpp
        src/platform/serial_port_posix.cpp
        src/platform/local_ipc_uds.cpp
    )
    set(PLATFORM_LIBS pthread)
else()  # Linux
    add_definitions(-DEMQ_PLATFORM_LINUX)
    set(PAL_SOURCES
        src/platform/event_loop_epoll.cpp
        src/platform/socket_api_posix.cpp
        src/platform/shared_memory_posix.cpp
        src/platform/serial_port_posix.cpp
        src/platform/local_ipc_uds.cpp
    )
    set(PLATFORM_LIBS pthread rt)
    if(EMBEDMQ_ENABLE_IOURING)
        add_definitions(-DEMBEDMQ_ENABLE_IOURING)
        list(APPEND PLATFORM_LIBS uring)
    endif()
endif()

if(EMBEDMQ_ENABLE_SHM)
    list(APPEND TRANSPORT_SOURCES src/transport/shm_transport.cpp)
    add_definitions(-DEMBEDMQ_ENABLE_SHM)
endif()

if(EMBEDMQ_ENABLE_SERIAL)
    list(APPEND TRANSPORT_SOURCES src/transport/serial_transport.cpp)
    add_definitions(-DEMBEDMQ_ENABLE_SERIAL)
endif()

if(EMBEDMQ_ENABLE_BLE)
    if(UNIX AND NOT APPLE)
        find_package(Bluez REQUIRED)
    endif()
    list(APPEND TRANSPORT_SOURCES src/transport/ble_transport.cpp)
    add_definitions(-DEMBEDMQ_ENABLE_BLE)
endif()

set(ALL_SOURCES
    ${CORE_SOURCES}
    ${DISC_SOURCES}
    ${TRANSPORT_SOURCES}
    ${PAL_SOURCES}
    ${UTIL_SOURCES}
)

if(EMBEDMQ_BUILD_C_API)
    list(APPEND ALL_SOURCES src/bindings/c_api.cpp)
endif()

# ---- 库构建 ----
if(EMBEDMQ_BUILD_SHARED)
    add_library(embedmq SHARED ${ALL_SOURCES})
    target_include_directories(embedmq PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    target_include_directories(embedmq PRIVATE src)
    target_link_libraries(embedmq PRIVATE ${PLATFORM_LIBS})
    set_target_properties(embedmq PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION 1
    )
    if(WIN32)
        target_compile_definitions(embedmq PRIVATE EMBEDMQ_EXPORTS)
    endif()
endif()

if(EMBEDMQ_BUILD_STATIC)
    add_library(embedmq_static STATIC ${ALL_SOURCES})
    target_include_directories(embedmq_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    target_include_directories(embedmq_static PRIVATE src)
    target_link_libraries(embedmq_static PRIVATE ${PLATFORM_LIBS})
endif()

# ---- 安装 ----
install(TARGETS embedmq embedmq_static
    EXPORT EmbedMQTargets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin      # Windows DLL
)
install(DIRECTORY include/embedmq DESTINATION include)

# ---- 子目录 ----
if(EMBEDMQ_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(EMBEDMQ_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(EMBEDMQ_BUILD_TOOLS)
    add_subdirectory(tools)
endif()
```

### 15.4 交叉编译与多平台构建

**xmake 方式（推荐）：**

```bash
# Linux 本地构建
xmake f -m release && xmake -j$(nproc)

# macOS 本地构建
xmake f -m release && xmake -j$(sysctl -n hw.ncpu)

# Windows 本地构建 (MSVC)
xmake f -m release && xmake -j%NUMBER_OF_PROCESSORS%

# Linux ARM64 交叉编译
xmake f -p linux -a arm64 --cross=aarch64-linux-gnu- && xmake

# Windows ARM64 交叉编译 (MSVC)
xmake f -p windows -a arm64 && xmake
```

**CMake 方式（兼容）：**

```cmake
# cmake/cross_compile/aarch64.cmake

set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)

set(CMAKE_C_COMPILER   aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)

set(CMAKE_FIND_ROOT_PATH /opt/sysroot/aarch64)
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
```

```bash
# CMake 交叉编译
mkdir build-arm && cd build-arm
cmake .. -DCMAKE_TOOLCHAIN_FILE=../cmake/cross_compile/aarch64.cmake \
         -DEMBEDMQ_ENABLE_BLE=OFF \
         -DEMBEDMQ_BUILD_TESTS=OFF
make -j$(nproc)

# Windows (Visual Studio)
mkdir build-win && cd build-win
cmake .. -G "Visual Studio 17 2022" -A x64
cmake --build . --config Release
```

---

## 16. 核心代码骨架

### 16.1 Participant 实现

```cpp
// src/core/participant.cpp

#include "embedmq/embedmq.h"
#include "message_bus.h"
#include "../discovery/discovery_agent.h"
#include "../transport/transport_manager.h"
#include "../platform/process.h"      // 跨平台进程工具

namespace embedmq {

struct Participant::Impl {
    ParticipantConfig config;
    uint16_t id;
    std::string name;
    
    std::unique_ptr<MessageBus>       messageBus;
    std::unique_ptr<DiscoveryAgent>   discovery;
    std::unique_ptr<TransportManager> transportMgr;
    
    std::atomic<bool> running{false};
    
    PeerEventCallback peerEventCb;
    
    void init() {
        // 1. 生成唯一节点 ID（跨平台）
        id = generateNodeId();
        
        // 2. 初始化传输管理器
        transportMgr = std::make_unique<TransportManager>();
        
        // 注册默认传输（全平台可用）
        if (config.transport.enableUdp)
            transportMgr->registerTransport("udp", 
                std::make_shared<UdpTransport>());
        if (config.transport.enableLocalIpc)
            transportMgr->registerTransport("local_ipc",
                std::make_shared<LocalIpcTransport>());
        if (config.transport.enableTcp)
            transportMgr->registerTransport("tcp",
                std::make_shared<TcpTransport>());
#ifdef EMBEDMQ_ENABLE_SHM
        if (config.transport.enableShm)
            transportMgr->registerTransport("shm",
                std::make_shared<ShmTransport>());
#endif
#ifdef EMBEDMQ_ENABLE_SERIAL
        if (config.transport.enableSerial)
            transportMgr->registerTransport("serial",
                std::make_shared<SerialTransport>());
#endif
        
        // 初始化各传输
        transportMgr->initAll(config);
        
        // 3. 初始化消息总线
        messageBus = std::make_unique<MessageBus>(
            id, transportMgr.get());
        
        // 4. 初始化发现
        discovery = std::make_unique<DiscoveryAgent>(
            id, name, config.discovery, transportMgr.get());
        
        // 发现事件回调
        discovery->setOnPeerDiscovered([this](const PeerInfo& peer) {
            messageBus->onPeerDiscovered(peer);
            if (peerEventCb) 
                peerEventCb(peer.id, peer.name, true);
        });
        
        discovery->setOnPeerLost([this](uint16_t peerId, 
                                         const std::string& name) {
            messageBus->onPeerLost(peerId);
            if (peerEventCb)
                peerEventCb(peerId, name, false);
        });
        
        // 5. 启动
        discovery->start();
        messageBus->start();
        running = true;
    }
    
    uint16_t generateNodeId() {
        // 跨平台：使用 PAL 获取 PID + 高精度时间戳
        auto pid = platform::getProcessId();
        auto now = std::chrono::steady_clock::now()
                       .time_since_epoch().count();
        std::hash<uint64_t> hasher;
        return static_cast<uint16_t>(
            hasher(pid ^ now) & 0xFFFF);
    }
};

// static factory
std::unique_ptr<Participant> Participant::create(const std::string& name) {
    ParticipantConfig config;
    config.nodeName = name;
    return create(config);
}

std::unique_ptr<Participant> Participant::create(
    const ParticipantConfig& config) 
{
    auto impl = std::make_unique<Impl>();
    impl->config = config;
    impl->name = config.nodeName.empty() ? 
        "node_" + std::to_string(platform::getProcessId()) : config.nodeName;
    impl->init();
    return std::unique_ptr<Participant>(
        new Participant(std::move(impl)));
}

Participant::Participant(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

Participant::~Participant() { shutdown(); }

std::unique_ptr<Publisher> Participant::createPublisher(
    const std::string& topic,
    const QoSProfile& qos) 
{
    return impl_->messageBus->createPublisher(topic, qos);
}

std::unique_ptr<Subscriber> Participant::createSubscriber(
    const std::string& topic,
    SubscribeCallback callback,
    const QoSProfile& qos)
{
    auto sub = impl_->messageBus->createSubscriber(
        topic, std::move(callback), qos);
    
    // 通知发现层有新订阅，触发重新匹配
    impl_->discovery->announceSubscription(topic, qos);
    
    return sub;
}

std::unique_ptr<Requester> Participant::createRequester(
    const std::string& service,
    const QoSProfile& qos)
{
    return impl_->messageBus->createRequester(service, qos);
}

std::unique_ptr<Replier> Participant::createReplier(
    const std::string& service,
    RequestHandler handler,
    const QoSProfile& qos)
{
    auto rep = impl_->messageBus->createReplier(
        service, std::move(handler), qos);
    
    impl_->discovery->announceService(service, qos);
    
    return rep;
}

void Participant::registerSerializer(
    std::shared_ptr<ISerializer> serializer) 
{
    impl_->messageBus->registerSerializer(std::move(serializer));
}

void Participant::registerTransport(
    const std::string& name,
    std::shared_ptr<ITransport> transport) 
{
    impl_->transportMgr->registerTransport(name, std::move(transport));
}

void Participant::onPeerEvent(PeerEventCallback callback) {
    impl_->peerEventCb = std::move(callback);
}

void Participant::shutdown() {
    if (impl_ && impl_->running.exchange(false)) {
        // 发送 FAREWELL 消息
        impl_->discovery->sendFarewell();
        impl_->discovery->stop();
        impl_->messageBus->stop();
        impl_->transportMgr->shutdownAll();
    }
}

uint16_t Participant::id() const { return impl_->id; }
const std::string& Participant::name() const { return impl_->name; }
bool Participant::isRunning() const { return impl_->running; }

} // namespace embedmq
```

### 16.2 TopicRouter 实现

```cpp
// src/core/topic_router.h

#pragma once
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>

namespace embedmq {

struct Subscription {
    uint64_t            id;
    std::string         pattern;
    bool                isWildcard;
    SubscribeCallback   callback;
    QoSProfile          qos;
};

class TopicRouter {
public:
    uint64_t addSubscription(const std::string& pattern,
                             SubscribeCallback callback,
                             const QoSProfile& qos) 
    {
        auto id = nextId_++;
        
        bool isWild = (pattern.find('*') != std::string::npos ||
                       pattern.find('#') != std::string::npos);
        
        std::unique_lock lock(mutex_);
        
        if (isWild) {
            wildcardSubs_.push_back({id, pattern, true, 
                                     std::move(callback), qos});
        } else {
            exactSubs_[pattern].push_back({id, pattern, false,
                                           std::move(callback), qos});
        }
        
        return id;
    }
    
    void removeSubscription(uint64_t id) {
        std::unique_lock lock(mutex_);
        
        for (auto& [topic, subs] : exactSubs_) {
            subs.erase(
                std::remove_if(subs.begin(), subs.end(),
                    [id](const Subscription& s) { return s.id == id; }),
                subs.end());
        }
        
        wildcardSubs_.erase(
            std::remove_if(wildcardSubs_.begin(), wildcardSubs_.end(),
                [id](const Subscription& s) { return s.id == id; }),
            wildcardSubs_.end());
    }
    
    /// 路由消息到匹配的订阅者
    void route(const std::string& topic, const ReceivedMessage& msg) {
        std::shared_lock lock(mutex_);
        
        // 精确匹配
        auto it = exactSubs_.find(topic);
        if (it != exactSubs_.end()) {
            for (auto& sub : it->second) {
                sub.callback(msg);
            }
        }
        
        // 通配符匹配
        for (auto& sub : wildcardSubs_) {
            if (matchWildcard(sub.pattern, topic)) {
                sub.callback(msg);
            }
        }
    }
    
    /// 检查主题是否有订阅者
    bool hasSubscribers(const std::string& topic) const {
        std::shared_lock lock(mutex_);
        
        if (exactSubs_.count(topic) > 0 && 
            !exactSubs_.at(topic).empty()) {
            return true;
        }
        
        for (const auto& sub : wildcardSubs_) {
            if (matchWildcard(sub.pattern, topic)) {
                return true;
            }
        }
        
        return false;
    }
    
private:
    /// MQTT 风格通配符匹配
    /// '*' 匹配单级, '#' 匹配多级
    static bool matchWildcard(const std::string& pattern, 
                               const std::string& topic) 
    {
        auto patParts = split(pattern, '/');
        auto topParts = split(topic, '/');
        
        size_t pi = 0, ti = 0;
        
        while (pi < patParts.size() && ti < topParts.size()) {
            if (patParts[pi] == "#") {
                return true;  // # 匹配剩余所有
            }
            if (patParts[pi] == "*" || patParts[pi] == topParts[ti]) {
                pi++; ti++;
            } else {
                return false;
            }
        }
        
        // 检查是否都消耗完
        return (pi == patParts.size() && ti == topParts.size());
    }
    
    static std::vector<std::string> split(const std::string& s, char delim) {
        std::vector<std::string> result;
        std::string token;
        for (char c : s) {
            if (c == delim) {
                result.push_back(token);
                token.clear();
            } else {
                token += c;
            }
        }
        if (!token.empty()) result.push_back(token);
        return result;
    }
    
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::vector<Subscription>> exactSubs_;
    std::vector<Subscription> wildcardSubs_;
    std::atomic<uint64_t> nextId_{1};
};

} // namespace embedmq
```

### 16.3 MessageCodec 实现

```cpp
// src/core/message_codec.h

#pragma once
#include "embedmq/types.h"
#include <cstring>
#include <vector>

namespace embedmq {

constexpr uint16_t EMBEDMQ_MAGIC = 0xEBDC;
constexpr uint8_t  EMBEDMQ_VERSION = 1;
constexpr size_t   HEADER_FIXED_SIZE = 36;

#pragma pack(push, 1)
struct WireHeader {
    uint16_t magic;
    uint8_t  version;
    uint8_t  msgType;
    uint8_t  qosLevel;
    uint8_t  flags;
    uint16_t sourceId;
    uint16_t destId;
    uint16_t topicLen;
    uint32_t sequenceId;
    uint32_t correlationId;
    uint64_t timestamp;
    uint8_t  serializerId;
    uint8_t  reserved[3];
    uint32_t payloadLen;
    uint32_t checksum;
};
#pragma pack(pop)

static_assert(sizeof(WireHeader) == HEADER_FIXED_SIZE, 
              "WireHeader size mismatch");

class MessageCodec {
public:
    /// 编码消息为线缆格式
    static std::vector<uint8_t> encode(
        MessageType type,
        uint16_t sourceId,
        uint16_t destId,
        const std::string& topic,
        const Payload& payload,
        const QoSProfile& qos,
        uint32_t sequenceId,
        uint32_t correlationId = 0,
        uint8_t flags = 0,
        uint8_t serializerId = 0)
    {
        WireHeader header{};
        header.magic         = EMBEDMQ_MAGIC;
        header.version       = EMBEDMQ_VERSION;
        header.msgType       = static_cast<uint8_t>(type);
        header.qosLevel      = static_cast<uint8_t>(qos.level);
        header.flags         = flags;
        header.sourceId      = sourceId;
        header.destId        = destId;
        header.topicLen      = static_cast<uint16_t>(topic.size());
        header.sequenceId    = sequenceId;
        header.correlationId = correlationId;
        header.timestamp     = currentTimestampNs();
        header.serializerId  = serializerId;
        header.payloadLen    = static_cast<uint32_t>(payload.size());
        header.checksum      = 0;  // 先设为0
        
        // 组装
        size_t totalSize = HEADER_FIXED_SIZE + topic.size() + payload.size();
        std::vector<uint8_t> buffer(totalSize);
        
        std::memcpy(buffer.data(), &header, HEADER_FIXED_SIZE);
        std::memcpy(buffer.data() + HEADER_FIXED_SIZE, 
                    topic.data(), topic.size());
        if (payload.size() > 0) {
            std::memcpy(buffer.data() + HEADER_FIXED_SIZE + topic.size(),
                        payload.data(), payload.size());
        }
        
        // 计算 CRC32 校验
        uint32_t crc = crc32(buffer.data() + 4,  // 跳过 magic+version+type
                             totalSize - 4);
        auto* hdr = reinterpret_cast<WireHeader*>(buffer.data());
        hdr->checksum = crc;
        
        return buffer;
    }
    
    /// 解码线缆格式为消息
    struct DecodeResult {
        bool        valid = false;
        WireHeader  header;
        std::string topic;
        Payload     payload;
    };
    
    static DecodeResult decode(const uint8_t* data, size_t size) {
        DecodeResult result;
        
        // 基本长度检查
        if (size < HEADER_FIXED_SIZE) return result;
        
        // 解析头部
        std::memcpy(&result.header, data, HEADER_FIXED_SIZE);
        
        // Magic 校验
        if (result.header.magic != EMBEDMQ_MAGIC) return result;
        
        // 版本校验
        if (result.header.version != EMBEDMQ_VERSION) return result;
        
        // 长度校验
        size_t expectedSize = HEADER_FIXED_SIZE + 
                              result.header.topicLen + 
                              result.header.payloadLen;
        if (size < expectedSize) return result;
        
        // CRC32 校验
        uint32_t savedCrc = result.header.checksum;
        // ... 验证 CRC
        
        // 提取主题
        result.topic.assign(
            reinterpret_cast<const char*>(data + HEADER_FIXED_SIZE),
            result.header.topicLen);
        
        // 提取载荷
        if (result.header.payloadLen > 0) {
            result.payload = Payload(
                data + HEADER_FIXED_SIZE + result.header.topicLen,
                result.header.payloadLen);
        }
        
        result.valid = true;
        return result;
    }
    
private:
    static uint64_t currentTimestampNs() {
        // 跨平台：使用 C++17 标准库
        auto now = std::chrono::high_resolution_clock::now();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                now.time_since_epoch()).count());
    }
    
    static uint32_t crc32(const uint8_t* data, size_t len) {
        uint32_t crc = 0xFFFFFFFF;
        for (size_t i = 0; i < len; i++) {
            crc ^= data[i];
            for (int j = 0; j < 8; j++) {
                crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
            }
        }
        return ~crc;
    }
};

} // namespace embedmq
```

### 16.4 EventLoop 实现（跨平台抽象）

EventLoop 通过平台抽象层提供统一接口，底层按平台自动选择最优实现。

```cpp
// src/platform/event_loop.h — 跨平台事件循环接口

#pragma once
#include <functional>
#include <memory>
#include <atomic>
#include <thread>

namespace embedmq {
namespace platform {

/// 平台无关的句柄类型
using IoHandle = std::intptr_t;  // Linux/macOS: fd, Windows: HANDLE

/// 事件类型
enum class IoEvent : uint8_t {
    Readable  = 0x01,
    Writable  = 0x02,
    Error     = 0x04,
};

using IoCallback = std::function<void(IoHandle handle, IoEvent event)>;

/// 跨平台事件循环接口
class EventLoop {
public:
    /// 工厂方法：自动创建当前平台最优实现
    static std::unique_ptr<EventLoop> create();
    
    virtual ~EventLoop() = default;
    
    virtual void addHandle(IoHandle handle, IoEvent interest, IoCallback cb) = 0;
    virtual void removeHandle(IoHandle handle) = 0;
    virtual void modifyHandle(IoHandle handle, IoEvent interest) = 0;
    
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual void wakeup() = 0;
    
    virtual bool isRunning() const = 0;
};

} // namespace platform
} // namespace embedmq
```

**Linux epoll 实现：**

```cpp
// src/platform/event_loop_epoll.cpp

#include "event_loop.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace embedmq::platform {

class EpollEventLoop : public EventLoop {
public:
    EpollEventLoop() {
        epollFd_ = ::epoll_create1(0);
        wakeupFd_ = ::eventfd(0, EFD_NONBLOCK);
        addHandle(wakeupFd_, IoEvent::Readable, [this](IoHandle, IoEvent) {
            uint64_t val;
            ::read(wakeupFd_, &val, sizeof(val));
        });
    }
    ~EpollEventLoop() override { stop(); ::close(epollFd_); ::close(wakeupFd_); }
    
    void addHandle(IoHandle handle, IoEvent interest, IoCallback cb) override;
    void removeHandle(IoHandle handle) override;
    void start() override;
    void stop() override;
    void wakeup() override {
        uint64_t val = 1;
        ::write(wakeupFd_, &val, sizeof(val));
    }
    // ...
private:
    int epollFd_ = -1;
    int wakeupFd_ = -1;
};

std::unique_ptr<EventLoop> EventLoop::create() {
    return std::make_unique<EpollEventLoop>();
}

} // namespace embedmq::platform
```

**macOS kqueue 实现：**

```cpp
// src/platform/event_loop_kqueue.cpp

#include "event_loop.h"
#include <sys/event.h>
#include <unistd.h>

namespace embedmq::platform {

class KqueueEventLoop : public EventLoop {
public:
    KqueueEventLoop() {
        kqFd_ = ::kqueue();
        // self-pipe trick for wakeup
        ::pipe(wakeupPipe_);
    }
    ~KqueueEventLoop() override { stop(); ::close(kqFd_); }
    
    void wakeup() override {
        char c = 1;
        ::write(wakeupPipe_[1], &c, 1);
    }
    // ...
private:
    int kqFd_ = -1;
    int wakeupPipe_[2] = {-1, -1};
};

std::unique_ptr<EventLoop> EventLoop::create() {
    return std::make_unique<KqueueEventLoop>();
}

} // namespace embedmq::platform
```

**Windows IOCP 实现：**

```cpp
// src/platform/event_loop_iocp.cpp

#include "event_loop.h"
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winsock2.h>

namespace embedmq::platform {

class IocpEventLoop : public EventLoop {
public:
    IocpEventLoop() {
        hIocp_ = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
        hWakeup_ = ::CreateEvent(NULL, FALSE, FALSE, NULL);
    }
    ~IocpEventLoop() override {
        stop();
        ::CloseHandle(hIocp_);
        ::CloseHandle(hWakeup_);
    }
    
    void wakeup() override {
        ::PostQueuedCompletionStatus(hIocp_, 0, 0, nullptr);
    }
    // ...
private:
    HANDLE hIocp_ = INVALID_HANDLE_VALUE;
    HANDLE hWakeup_ = INVALID_HANDLE_VALUE;
};

std::unique_ptr<EventLoop> EventLoop::create() {
    return std::make_unique<IocpEventLoop>();
}

} // namespace embedmq::platform
```

### 16.5 无锁环形缓冲区

```cpp
// src/util/ring_buffer.h

#pragma once
#include <atomic>
#include <cstddef>
#include <new>
#include <optional>

namespace embedmq {

/// 单生产者单消费者无锁环形缓冲区
template<typename T, size_t Capacity>
class SPSCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, 
                  "Capacity must be power of 2");

public:
    bool push(const T& item) {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & (Capacity - 1);
        
        if (next == tail_.load(std::memory_order_acquire)) {
            return false;  // 满
        }
        
        buffer_[head] = item;
        head_.store(next, std::memory_order_release);
        return true;
    }
    
    bool push(T&& item) {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & (Capacity - 1);
        
        if (next == tail_.load(std::memory_order_acquire)) {
            return false;
        }
        
        buffer_[head] = std::move(item);
        head_.store(next, std::memory_order_release);
        return true;
    }
    
    std::optional<T> pop() {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        
        if (tail == head_.load(std::memory_order_acquire)) {
            return std::nullopt;  // 空
        }
        
        T item = std::move(buffer_[tail]);
        tail_.store((tail + 1) & (Capacity - 1), 
                    std::memory_order_release);
        return item;
    }
    
    bool empty() const {
        return head_.load(std::memory_order_acquire) == 
               tail_.load(std::memory_order_acquire);
    }
    
    size_t size() const {
        const size_t head = head_.load(std::memory_order_acquire);
        const size_t tail = tail_.load(std::memory_order_acquire);
        return (head - tail) & (Capacity - 1);
    }
    
private:
    // 避免 false sharing
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    T buffer_[Capacity];
};

} // namespace embedmq
```

### 16.6 平台检测与统一类型

```cpp
// include/embedmq/platform.h — 平台检测宏与统一类型定义

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

// ---- POSIX 类系统统一标记 ----
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
        #define EMQ_API __declspec(dllimport)
    #endif
#else
    #define EMQ_API __attribute__((visibility("default")))
#endif

// ---- 统一句柄类型 ----
namespace embedmq::platform {

#ifdef EMQ_PLATFORM_WINDOWS
    using NativeHandle = void*;     // Windows HANDLE
    constexpr NativeHandle InvalidHandle = nullptr;
#else
    using NativeHandle = int;        // POSIX file descriptor
    constexpr NativeHandle InvalidHandle = -1;
#endif

using IoHandle = std::intptr_t;     // 在接口层统一使用

inline IoHandle toIoHandle(NativeHandle h) {
    return reinterpret_cast<IoHandle>(h);
}
inline NativeHandle fromIoHandle(IoHandle h) {
    return reinterpret_cast<NativeHandle>(h);
}

} // namespace embedmq::platform
```

### 16.7 跨平台进程工具

```cpp
// src/platform/process.h

#pragma once
#include <cstdint>
#include <string>

namespace embedmq::platform {

/// 获取当前进程 ID
inline uint64_t getProcessId() {
#ifdef EMQ_PLATFORM_WINDOWS
    return static_cast<uint64_t>(::GetCurrentProcessId());
#else
    return static_cast<uint64_t>(::getpid());
#endif
}

/// 获取主机名
std::string getHostName();

/// 获取平台临时目录
/// Linux: /tmp/embedmq/
/// macOS: ~/Library/Caches/embedmq/
/// Windows: %LOCALAPPDATA%\embedmq\ 
std::string getEmbedMqTempDir();

/// 获取默认串口设备名
/// Linux: "/dev/ttyS0"
/// macOS: "/dev/cu.usbserial"
/// Windows: "COM1"
std::string getDefaultSerialDevice();

} // namespace embedmq::platform
```

---

## 17. 配置示例

### 17.1 YAML 配置文件

```yaml
# embedmq.yaml — 节点配置文件

node:
  name: "sensor_node_1"
  domain_id: 0

discovery:
  multicast_group: "239.255.0.1"
  multicast_port: 19900
  announce_interval_ms: 1000
  heartbeat_interval_ms: 2000
  peer_timeout_ms: 10000
  enable_multicast: true
  enable_local_discovery: true

transport:
  udp:
    enabled: true
    port: 0                    # 自动分配
    recv_buffer_size: 2097152  # 2MB
    send_buffer_size: 2097152
  
  local_ipc:
    enabled: true
    # Linux: "/tmp/embedmq", macOS: "~/Library/Caches/embedmq"
    # Windows: 自动使用 Named Pipe (\\.\pipe\embedmq_*)
    socket_dir: ""                 # 空则使用平台默认路径
  
  tcp:
    enabled: false
    port: 0
  
  shm:
    enabled: true
    segment_size: 4194304      # 4MB
  
  serial:
    enabled: false
    device: ""                     # 空则使用平台默认 (Linux: /dev/ttyS0, Windows: COM1)
    baud_rate: 115200
    data_bits: 8
    parity: "none"
    stop_bits: 1
  
  ble:
    enabled: false

threading:
  io_threads: 1
  worker_threads: 1
  pin_cpu: false

qos_profiles:
  sensor_data:
    level: 0                   # BestEffort
    max_rate_hz: 100
    history: "keep_last"
    history_depth: 1
  
  control:
    level: 1                   # Reliable
    max_retries: 5
    retry_interval_ms: 50
    ack_timeout_ms: 200
    send_queue_size: 256
  
  config_update:
    level: 2                   # ExactlyOnce
    max_retries: 10
    durability: "transient_local"

logging:
  level: "info"                # trace/debug/info/warn/error
  file: ""                     # 空则输出到 stderr
  max_size_mb: 10
```

### 17.2 代码中加载配置

```cpp
// 方式一：从文件加载
auto participant = Participant::createFromFile("embedmq.yaml");

// 方式二：代码配置
ParticipantConfig config;
config.nodeName = "sensor_node_1";
config.domainId = 0;
config.transport.enableUdp = true;
config.transport.enableLocalIpc = true;
config.transport.enableShm = true;
auto participant = Participant::create(config);

// 方式三：默认配置
auto participant = Participant::create("my_node");
```

---

## 18. 典型用例

### 18.1 发布-订阅：传感器数据

**发布者（sensor_publisher.cpp）：**

```cpp
#include <embedmq/embedmq.h>
#include <iostream>
#include <thread>
#include <cmath>

struct SensorData {
    float temperature;
    float humidity;
    uint64_t timestamp;
};

int main() {
    // 创建参与者
    auto participant = embedmq::Participant::create("sensor_node");
    
    // 配置 QoS：传感器数据高频、可丢失
    auto qos = embedmq::QoSProfile::bestEffort();
    qos.maxRateHz = 100;
    
    // 创建发布者
    auto pub = participant->createPublisher("sensor/temperature", qos);
    
    std::cout << "Sensor publisher started. ID: " 
              << participant->id() << std::endl;
    
    // 等待对端发现
    participant->onPeerEvent([](uint16_t id, const std::string& name, bool connected) {
        std::cout << "Peer " << name << (connected ? " connected" : " disconnected") 
                  << std::endl;
    });
    
    // 发布循环
    uint64_t seq = 0;
    while (true) {
        SensorData data{
            .temperature = 25.0f + std::sin(seq * 0.1f) * 5.0f,
            .humidity    = 60.0f + std::cos(seq * 0.05f) * 10.0f,
            .timestamp   = seq++,
        };
        
        // 发布二进制数据
        pub->publish(&data, sizeof(data));
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    return 0;
}
```

**订阅者（sensor_subscriber.cpp）：**

```cpp
#include <embedmq/embedmq.h>
#include <iostream>

struct SensorData {
    float temperature;
    float humidity;
    uint64_t timestamp;
};

int main() {
    auto participant = embedmq::Participant::create("display_node");
    
    auto qos = embedmq::QoSProfile::bestEffort();
    
    // 创建订阅者，带回调
    auto sub = participant->createSubscriber(
        "sensor/temperature",
        [](const embedmq::ReceivedMessage& msg) {
            if (msg.payload.size() == sizeof(SensorData)) {
                auto* data = reinterpret_cast<const SensorData*>(
                    msg.payload.data());
                
                std::cout << "Temp: " << data->temperature 
                         << "°C, Humidity: " << data->humidity 
                         << "%, seq=" << data->timestamp
                         << " (from node " << msg.sourceId << ")"
                         << std::endl;
            }
        },
        qos
    );
    
    std::cout << "Subscriber started. Waiting for data..." << std::endl;
    
    // 阻塞等待
    std::this_thread::sleep_for(std::chrono::hours(24));
    
    return 0;
}
```

### 18.2 通配符订阅

```cpp
// 订阅所有传感器数据
auto sub = participant->createSubscriber(
    "sensor/#",            // 匹配 sensor/temperature, sensor/humidity, etc.
    [](const embedmq::ReceivedMessage& msg) {
        std::cout << "Topic: " << msg.topic 
                 << ", Size: " << msg.payload.size() 
                 << " bytes" << std::endl;
    }
);

// 订阅特定房间的所有传感器
auto sub2 = participant->createSubscriber(
    "sensor/*/room1",     // 匹配 sensor/temperature/room1, sensor/humidity/room1
    callback
);
```

### 18.3 请求-响应：RPC 服务

**服务端（config_server.cpp）：**

```cpp
#include <embedmq/embedmq.h>
#include <iostream>
#include <map>

int main() {
    auto participant = embedmq::Participant::create("config_server");
    
    // 配置存储
    std::map<std::string, std::string> configStore = {
        {"log_level", "info"},
        {"sample_rate", "100"},
        {"device_name", "sensor_01"},
    };
    
    // 创建响应者
    auto replier = participant->createReplier(
        "config/get",
        [&configStore](const embedmq::ReceivedMessage& request) 
            -> embedmq::Payload 
        {
            std::string key(request.payload.asText());
            std::cout << "Config request: " << key << std::endl;
            
            auto it = configStore.find(key);
            if (it != configStore.end()) {
                return embedmq::Payload(it->second);
            } else {
                return embedmq::Payload("ERROR: key not found");
            }
        },
        embedmq::QoSProfile::reliable()
    );
    
    // 另一个服务：设置配置
    auto setReplier = participant->createReplier(
        "config/set",
        [&configStore](const embedmq::ReceivedMessage& request) 
            -> embedmq::Payload 
        {
            // 解析 "key=value" 格式
            auto text = std::string(request.payload.asText());
            auto pos = text.find('=');
            if (pos != std::string::npos) {
                auto key = text.substr(0, pos);
                auto value = text.substr(pos + 1);
                configStore[key] = value;
                return embedmq::Payload("OK");
            }
            return embedmq::Payload("ERROR: invalid format");
        },
        embedmq::QoSProfile::reliable()
    );
    
    std::cout << "Config server started." << std::endl;
    std::this_thread::sleep_for(std::chrono::hours(24));
    
    return 0;
}
```

**客户端（config_client.cpp）：**

```cpp
#include <embedmq/embedmq.h>
#include <iostream>

int main() {
    auto participant = embedmq::Participant::create("config_client");
    
    // 等待服务发现
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // 创建请求者
    auto requester = participant->createRequester(
        "config/get",
        embedmq::QoSProfile::reliable()
    );
    
    // 同步请求
    auto response = requester->request(
        embedmq::Payload("log_level"),
        std::chrono::milliseconds(3000)  // 3秒超时
    );
    
    if (response) {
        std::cout << "Config value: " << response->asText() << std::endl;
    } else {
        std::cout << "Request timeout!" << std::endl;
    }
    
    // 异步请求
    auto future = requester->requestAsync(
        embedmq::Payload("sample_rate")
    );
    
    // 做其他工作...
    
    auto result = future.get();
    std::cout << "Sample rate: " << result.asText() << std::endl;
    
    return 0;
}
```

### 18.4 串口跨设备通信

```cpp
// device_a.cpp — 运行在设备 A
#include <embedmq/embedmq.h>

int main() {
    embedmq::ParticipantConfig config;
    config.nodeName = "device_a";
    
    // 禁用网络传输，只用串口
    config.transport.enableUdp      = false;
    config.transport.enableLocalIpc = false;
    config.transport.enableSerial   = true;
#ifdef _WIN32
    config.transport.serialDevice = "COM3";
#else
    config.transport.serialDevice = "/dev/ttyS1";
#endif
    config.transport.serialBaud   = 115200;
    
    // 串口场景禁用多播发现，使用点对点握手
    config.discovery.enableMulticast = false;
    
    auto participant = embedmq::Participant::create(config);
    
    auto pub = participant->createPublisher(
        "device/status",
        embedmq::QoSProfile::reliable()
    );
    
    while (true) {
        pub->publish("alive");
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}
```

### 18.5 文本与 JSON 数据

```cpp
#include <embedmq/embedmq.h>

int main() {
    auto participant = embedmq::Participant::create("json_demo");
    
    auto pub = participant->createPublisher("data/json");
    
    // 发布 JSON 字符串
    std::string json = R"({
        "sensor": "temperature",
        "value": 25.5,
        "unit": "celsius",
        "timestamp": 1234567890
    })";
    
    pub->publish(json);  // 以文本发送
    
    // 订阅
    auto sub = participant->createSubscriber(
        "data/json",
        [](const embedmq::ReceivedMessage& msg) {
            std::cout << "Received JSON: " << msg.payload.asText() 
                     << std::endl;
            // 应用层自行解析 JSON
        }
    );
}
```

### 18.6 自定义序列化器

```cpp
// 自定义 MessagePack 序列化器示例
class MsgPackSerializer : public embedmq::ISerializer {
public:
    uint8_t id() const override { return 10; }  // 自定义ID
    std::string name() const override { return "msgpack"; }
    
    embedmq::Payload serialize(const void* obj) const override {
        // 使用 msgpack 库序列化
        // ...
        return embedmq::Payload(buffer.data(), buffer.size());
    }
    
    bool deserialize(const embedmq::Payload& data, 
                     void* obj) const override {
        // 使用 msgpack 库反序列化
        // ...
        return true;
    }
};

// 注册
participant->registerSerializer(std::make_shared<MsgPackSerializer>());
```

---

## 19. 与 MQTT/ZMQ/DDS 对比

| 特性 | EmbedMQ | MQTT | ZeroMQ | DDS (FastDDS) |
|------|---------|------|--------|---------------|
| **架构** | 去中心化 P2P | Broker中心 | P2P | 去中心化 P2P |
| **需要服务器** | ❌ | ✅ (Broker) | ❌ | ❌ |
| **自发现** | ✅ | ❌ | ❌ | ✅ |
| **Pub/Sub** | ✅ | ✅ | ✅ | ✅ |
| **Req/Rep** | ✅ | ❌ (需模拟) | ✅ | ❌ (需额外库) |
| **QoS 级别** | 3级 | 3级 | 有限 | 丰富(22种策略) |
| **传输插件化** | ✅ | ❌ | ❌ | 有限 |
| **串口支持** | ✅ 原生 | ❌ | ❌ | ❌ |
| **BLE 支持** | ✅ 插件 | ❌ | ❌ | ❌ |
| **共享内存** | ✅ | ❌ | ❌ | ✅ |
| **数据格式** | 自由 | 自由 | 自由 | IDL 强类型 |
| **通配符** | ✅ | ✅ | ✅ (前缀) | 有限 |
| **库大小** | ~300KB | ~200KB | ~500KB | ~5MB |
| **内存占用** | < 2MB | < 1MB | < 5MB | ~20MB |
| **延迟(IPC)** | < 50μs | N/A | ~30μs | ~50μs |
| **学习成本** | 低 | 低 | 中 | 高 |
| **嵌入式适配** | ★★★★★ | ★★★★ | ★★★ | ★★ |
| **跨平台** | ✅ Win/Linux/Mac | ✅ (客户端) | ✅ | ✅ |
| **跨语言** | C/Python(扩展) | 丰富 | 丰富 | 丰富 |
| **保留消息** | ✅ | ✅ | ❌ | ✅ (Durability) |
| **遗嘱消息** | ✅ | ✅ | ❌ | ✅ (Liveliness) |

### EmbedMQ 独特优势

1. **传输插件化**：唯一原生支持串口、蓝牙、共享内存等嵌入式常用通道的中间件
2. **无服务器 + 自发现**：结合 DDS 的去中心化和 MQTT 的简洁
3. **极致轻量**：为资源受限的设备优化，比 DDS 小一个数量级
4. **真正跨平台**：通过 PAL 层统一 Windows/Linux/macOS 差异，一套代码三平台运行
5. **API 简洁**：16 个核心 API，远少于 DDS 的上百个
6. **数据格式自由**：不强制 IDL 定义，支持原始二进制、文本、JSON 等任意格式

---

## 20. 路线图

### Phase 1 — MVP + PAL 基础 (v0.1, ✅ 已完成)

> **完成时间：** 2026-04 | **编译验证：** Windows (MSVC 2022 x64) ✅

- [x] 核心框架搭建
- [x] **平台抽象层(PAL)基础实现**
  - [x] EventLoop 接口 + Linux epoll 实现 (`src/platform/event_loop_epoll.cpp`)
  - [x] EventLoop macOS kqueue 实现 (`src/platform/event_loop_kqueue.cpp`)
  - [x] EventLoop Windows IOCP 实现 (`src/platform/event_loop_iocp.cpp`)
  - [x] 跨平台 Socket API 封装 BSD/Winsock (`src/platform/socket_api_{posix,win}.cpp`)
  - [x] 跨平台时钟、进程工具 (`src/platform/process.h`)
- [x] UDP Transport 实现（跨平台）(`src/transport/udp_transport.cpp`)
- [ ] LocalIPC Transport（Linux UDS + Windows Named Pipe + macOS UDS）— Phase 3 扩展
- [x] 基本发布-订阅 (同进程本地路由 + 远端 UDP 路由)
- [x] 基本请求-响应 (同进程本地直接调用 + 远端网络路由)
- [x] QoS Level 0 支持 (BestEffort)
- [x] UDP 多播自发现（跨平台，239.255.0.1:19900）
- [x] 消息编解码 (`src/core/message_codec.h`，Wire Format 40字节固定头)
- [x] Windows (MSVC 2022) 编译验证通过
- [x] 基本示例 (`examples/pub_sub/`, `examples/req_rep/`)

**实现亮点：**
- `WireHeader`（40B）+ CRC32 校验的完整编解码
- 无锁 SPSC 环形缓冲区（`src/util/ring_buffer.h`）
- 时间轮定时器（`src/util/timer_wheel.h`，精度 10ms）
- UDP 多播 Announce/Heartbeat/Farewell 发现协议

### Phase 2 — 可靠性 (v0.2, ✅ 已完成)

> **完成时间：** 2026-06 | **单元测试：** 见下方合计 ✅

- [x] QoS Level 1 (ACK + 重传，`src/core/qos_engine.h`)
- [x] QoS Level 2 (ExactlyOnce 去重，基于 sourceId+seqId)
- [x] 通配符主题匹配（`*` 单级 / `#` 多级，`src/core/topic_router.h`）
- [x] 保留消息 (Retained，`src/core/retained_store.h`，TransientLocal 持久性)
- [x] 遗嘱消息 (Last Will)：遗嘱随 ANNOUNCE 广告给对端；对端**超时**判定异常掉线时
      代为投递遗嘱（含保留），收到 **FAREWELL** 优雅退出则丢弃遗嘱
      （`src/discovery/peer_registry.h`、`MessageBus::deliverWill`）
- [x] 心跳与超时检测 (`src/discovery/peer_registry.h`，可配置超时时间)
- [x] TCP Transport（跨平台，`src/transport/tcp_transport.cpp`，长度前缀帧化）

**测试统计（Linux GCC x64 / Windows MSVC 2022）：**

> 运行：`xmake run emq_tests`（全部） / `xmake run emq_tests <module>`（按模块） / `xmake run emq_tests --list`（列出模块）

| 模块 (module) | 断言数 | 状态 |
|--------------|--------|------|
| topic_router | 20 | ✅ PASS |
| message_codec | 23 | ✅ PASS |
| qos_engine | 14 | ✅ PASS |
| pal | 24 | ✅ PASS |
| pub_sub | 10 | ✅ PASS |
| req_rep | 10 | ✅ PASS |
| last_will | 15 | ✅ PASS |
| phase3 | 49 | ✅ PASS |
| **合计** | **165** | **✅ 全部通过** |

### Phase 3 — 性能优化 (v0.3, ✅ 已完成)

- [x] 共享内存 Transport（POSIX shm_open+mmap / Win32 FileMapping，`src/transport/shm_transport.cpp`）
      —— 有界 MPSC 槽位环，多生产者 CAS 预留 + 单消费者轮询
- [x] 零拷贝发送 (scatter/gather: sendmsg/iovec、WSASendTo/WSABUF；`MessageCodec::encodeHeader` + `ITransport::sendv`)
- [x] 内存池（`src/util/memory_pool.h`，固定块 + 头部标记区分超大回退）
- [x] 无锁队列优化（`src/util/mpsc_queue.h`，Vyukov MPSC；保留 SPSC 环形缓冲）
- [x] CPU 亲和性 (Linux pthread_setaffinity_np / Windows SetThreadAffinityMask；`platform::setThreadAffinity`，由 `ParticipantConfig::threading` 驱动)
- [x] 性能基准测试（`bench/bench_main.cpp`，`emq_bench`：内存池/MPSC/SPSC/CRC32/Pub-Sub）
- [~] io_uring 支持 (Linux 可选，实验性)：构建选项 `enable_io_uring`，
      `src/platform/event_loop_io_uring.cpp`（IORING_OP_POLL_ADD），默认关闭，需 liburing

### Phase 4 — 嵌入式与外设拓展 (v0.4, 6 周)

- [ ] Serial Transport（termios + Win32 CommAPI）
- [ ] BLE Transport（BlueZ + CoreBluetooth + WinRT BLE）
- [ ] 串口自发现协议
- [ ] 大消息分片/重组
- [ ] 流量控制
- [ ] 交叉编译验证 (ARM Linux / Windows ARM64)

### Phase 5 — 跨语言与工具 (v0.5) ✅ 已完成

- [x] C ABI 封装（`include/embedmq/embedmq_c.h` + `src/capi/embedmq_c.cpp`，
  不透明句柄 + 异常隔离 + 返回码错误模型；共享库目标 `embedmq_c`）
- [x] Python 绑定 (ctypes，三平台动态库自动定位；`bindings/python/embedmq.py`)
- [x] 命令行监控工具（`emqtop`，`tools/emqtop/main.cpp`；monitor/sub/pub/req/echo/peers）
- [ ] 配置文件支持 (YAML)
- [ ] 网络桥接工具

> 实现说明：实际落地的文件/目标名与早期设计草案（`src/c_api/c_api.cpp`、`emq_monitor`）
> 略有差异，以本节与仓库实际为准：C ABI 实现位于 `src/capi/`，CLI 工具名为 `emqtop`。

### Phase 6 — 生产就绪 (v1.0, 4 周)

- [ ] 安全：TLS / 消息加密
- [ ] 压缩：LZ4 载荷压缩
- [ ] 完整文档
- [ ] API 稳定性声明
- [ ] CI/CD Pipeline（GitHub Actions: Windows + Linux + macOS 矩阵）
- [ ] 发布 v1.0（三平台预编译包）

---

## 附录 A：术语表

| 术语 | 定义 |
|------|------|
| Participant | 通信域中的一个参与节点 |
| Domain | 逻辑隔离的通信空间 |
| Topic | 发布-订阅的主题标识符 |
| Service | 请求-响应的服务标识符 |
| Payload | 消息中的用户数据载荷 |
| Transport | 底层传输通道的抽象 |
| Endpoint | 传输层中的通信地址 |
| Peer | 已发现的远程参与节点 |
| QoS | Quality of Service，服务质量 |
| Wire Format | 数据在传输层上的序列化格式 |
| Retained Message | 保留消息，新订阅者可收到最新值 |
| Last Will | 遗嘱消息，节点异常离线时发布 |
| Correlation ID | 关联ID，用于请求-响应匹配 |

## 附录 B：系统资源需求

**通用资源需求：**

| 资源 | 最小值 | 推荐值 |
|------|--------|--------|
| CPU | 单核 1 GHz | 双核 1.5 GHz |
| RAM | 16 MB 可用 | 64 MB 可用 |
| 存储 | 1 MB (静态库) | 4 MB |

**平台特定需求：**

| 平台 | 最低版本 | 编译器 | 特殊依赖 |
|------|---------|--------|----------|
| **Linux** | 内核 4.x | GCC 7+ / Clang 8+ | pthread, rt |
| **macOS** | macOS 11 (Big Sur) | AppleClang 12+ | — |
| **Windows** | Windows 10 (1809+) | MSVC 2019+ (v142) | ws2_32, mswsock |
| **嵌入式 Linux** | 内核 4.x, ARM Cortex-A7+ | 交叉编译 GCC 7+ | pthread, rt |

---

*文档版本：v2.2（Phase 1 + Phase 2 + Phase 3 实现完成）*
*最后更新：2026-06*
*构建状态：Linux (GCC x64) ✅ | Windows (MSVC 2022 x64) ✅ | macOS (代码已就绪，待验证)*
*测试状态：165 assertions / 57 tests — 全部通过 ✅*
*作者：EmbedMQ Design Team*