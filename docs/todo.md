# EmbedMQ 审查待办

本文记录对当前项目的设计审查、潜在 bug 和嵌入式 Linux 精简建议。
状态标记：`[x]` 已修复并自测；`[ ]` 待办。

> 进展（v0.4.x）：在上一轮完成 P0 全部 + 低风险 P1/P2/P3/P4 的基础上，
> 本轮进一步落地了**全部「合理但需较大改造」的条目**——协议 v2 紧凑头与显式小端、
> 可选 CRC、TLV/变长 ANNOUNCE + endpoint 列表 + 合并心跳、QoS2 完整状态机与滑动去重窗口、
> 嵌入式构建 profile + 默认瘦身（TCP 默认关闭）、UDP 接收接入 epoll 反应堆、
> Payload 小缓冲优化、SHM 几何/版本校验 + 失效段回收 + futex 唤醒、TCP/SHM 原生 sendv。
> 新增 `tests/test_refactor_v2.cpp`。全量测试：**2220 assertions / 72 tests passed, 0 failed**。

## P0: 正确性与稳定性（全部完成）

- [x] 修复对端 topic 更新不同步问题。
  - `PeerRegistry::addOrUpdate()` 在订阅/端点变化时触发 `onUpdated_`，经
    `DiscoveryAgent::setOnPeerUpdated` 透传到 `MessageBus::onPeerUpdated()` 刷新 `peers_`。
  - 测试：`peer_registry_update_fires_callback`。

- [x] 修复 Req/Rep future 可能永久挂起的问题。
  - `pendingRequests_` 改为 `{promise, deadline}` 统一超时；远端无 provider 立即 `set_exception`；
    分离发布侧 `seqId` 与请求侧 `correlationId`；同步 `request()` 捕获异常返回 `nullopt`。
  - 测试：`request_no_provider_returns_quickly`。

- [x] 修复 TCP 流式拆包读取问题。
  - 新增 `recvAll()` 循环读满长度头与负载；连接结束 `removeConnection()` 清理连接表。

- [x] 修复 SHM 槽位自旋失败后仍写入的问题。
  - 自旋耗尽（消费者卡死）时写入零长度 READY 占位让消费者推进 tail，返回 `false` 并 `dropped_` 计数。

- [x] 加固 `MessageCodec::decode()` 的长度校验。
  - `expectedSize` 用 `uint64_t` 累加，避免 32 位 `size_t` 回绕。测试：`decode_rejects_length_overflow`。

- [x] 修复 `Subscriber` / `Replier` 回调裸指针生命周期风险。
  - 计数/暂停状态移入 `shared_ptr<State>`，回调捕获 `weak_ptr`，杜绝 UAF。

- [x] 修复 `TimerWheel` 长定时器提前触发 / 停止阶段重装问题。
  - 改用绝对 `fireTick`；触发与重装前检查 `running_`；`cancelSet_` 持久保存直至对应定时器被处理。
  - 测试：`timer_long_delay_no_early_fire`、`timer_periodic_cancel`、`timer_stop_no_fire`。

## P1: 协议与发现机制（全部完成）

- [x] 完整实现 QoS2 语义与滑动去重窗口。
  - 位置：`src/core/qos_engine.h`、`src/core/message_bus.cpp`、`include/embedmq/types.h`
  - 实现：完整 PUBLISH→PUBREC→PUBREL→PUBCOMP 两阶段握手——发送方 PUBLISH 阶段挂起在
    `pending_`（重传至 PUBREC），转入 PUBREL 阶段挂起在 `pendingRel_`（重传至 PUBCOMP）；
    接收方收 PUBLISH 回 PUBREC、收 PUBREL 回 PUBCOMP。新增 `MessageType::PUBCOMP`。
    去重由无界集合改为**每 source 有界滑动窗口**（WINDOW=1024），内存不再无界增长。
  - 测试：`qos2_dedup_sliding_window_bounded`、`qos2_handshake_state_transitions`。

- [x] 协议字段改为显式字节序（协议 v2）。
  - 位置：`src/core/message_codec.h`
  - 实现：所有头字段用显式小端 put/get 读写，不再 `memcpy` 打包结构体，跨架构一致。
  - 测试：`codec_v2_explicit_little_endian`。

- [x] 校验 encode 阶段的 topic/payload 上限。
  - `MAX_TOPIC_LEN`/`MAX_PAYLOAD_LEN`，超限返回空。测试：`encode_rejects_oversized_topic`。

- [x] 精简/闭环协议字段。
  - `serializerId`、`hdrFlags`（HAS_TS/HAS_CRC）已纳入 v2 头并实际生效；timestamp 仅数据包携带。
    `COMPRESSED/ENCRYPTED/FRAGMENT` 等仍保留为占位 flag（语义后续按需补齐），不再有“声明却完全无效”的固定字段。

- [x] 让 `enableMulticast` / `enableLocalDiscovery` 真正生效。
  - UDP 按 `multicast_enabled` 决定多播组；`enableLocalDiscovery=false` 时 `DiscoveryAgent::start()` 直接返回。

- [x] 重新设计 ANNOUNCE 载荷（TLV/变长）+ 携带 endpoint 列表。
  - 位置：`src/discovery/discovery_agent.cpp`
  - 实现：ANNOUNCE 改为变长 TLV（u8/u16 长度前缀 + 显式小端），替代旧定长 64B 名 + 128B/topic；
    携带本地 endpoint 列表（type/addr/port）。接收侧解析：UDP 以收包来源 `from` 为准，
    TCP advertised `0.0.0.0` 用 `from` IP 补全，SHM 段名直采，为 TCP/SHM 跨节点寻址奠定基础。

- [x] 加固接收分派前的 magic/version 校验。
  - 分派前校验 `size>=HEADER_FIXED_SIZE` 且 magic/version 匹配。

- [x] 降低发现流量（合并心跳与 announce）。
  - 移除独立 HEARTBEAT 定时发送，ANNOUNCE 周期广播兼任保活（`addOrUpdate` 刷新 `lastSeen`）；
    订阅集合变化时立即 `sendAnnounce()`，缩短对端建立路由时延。

- [x] 降低 nodeId 碰撞风险。
  - `generateNodeId()` 引入 `random_device` 增强熵，规避保留值 0 与 0xFFFF。

## P2: 嵌入式 Linux 精简（全部完成）

- [x] 嵌入式构建 profile + 默认构建瘦身。
  - 位置：`xmake.lua`
  - 实现：新增 `--profile=full|embedded`。embedded 画像强制关闭 TCP、示例、基准、io_uring，
    并定义 `EMBEDMQ_EMBEDDED_PROFILE`；TCP 默认开关改为 `false`（按需 `--enable_tcp=y`）。

- [x] 设计 compact v2 包头。
  - 位置：`src/core/message_codec.h`
  - 实现：基础头 26 字节 + 可选 `timestamp(8)`/`checksum(4)`（由 `hdrFlags` 决定），
    控制/发现包不带 timestamp。控制包从旧 40B 压到 30B（含 CRC）/26B（无 CRC）。
  - 测试：`codec_v2_compact_header_sizes`。

- [x] CRC 可配置。
  - 位置：`include/embedmq/config.h`（`enableChecksum`）、`message_codec.h`、`message_bus.*`、`participant.cpp`
  - 实现：encode/encodeHeader 增 `withCrc` 参数；`MessageBus::setChecksumEnabled()` 由
    `config.enableChecksum` 驱动；decode 依据 `hdrFlags` 自描述，无需双方约定。
  - 测试：`codec_v2_crc_optional`。

- [x] 收敛线程模型：UDP 接收接入 epoll 反应堆。
  - 位置：`src/transport/udp_transport.*`
  - 实现：Linux 下用 `platform::EventLoop`（epoll）替代 `select()`，消除 1024 fd 限制与定时轮询；
    非 Linux 保留 select 回退。（SHM/TCP/定时器并入同一 reactor 仍可作为进一步优化方向。）

- [x] 缩小 UDP recv buffer 并可配置。
  - `config.transport.udpRecvBufferSize`（默认 2KB，嵌入式友好）。

- [x] 优化 `Payload` 热路径分配（小缓冲优化 SBO）。
  - 位置：`include/embedmq/types.h`
  - 实现：≤32 字节内联存储，零堆分配/零引用计数；大块退化为 `shared_ptr<vector>` 共享。
  - 测试：`payload_inline_small_roundtrip`、`payload_large_roundtrip`、`payload_boundary_exact_inline_cap`。

- [x] 内存池/无锁队列定位修正（降低宣传）。
  - README 标注 `FixedBlockPool`/`MpscQueue`/`SPSCRingBuffer` 为可选工具组件。

- [x] `SocketApi::sendToV()` 避免每次堆分配。
  - 栈上 `iovec[8]`，分片数 >8 才回退堆分配。

## P3: 传输层完善（全部完成）

- [x] 明确 `TransportManager::broadcast()` 语义。
  - 仅向 `capability().supportsBroadcast` 的传输（即 UDP）广播。

- [x] TCP 默认不进入嵌入式构建。
  - `enable_tcp` 默认 `false`；embedded profile 强制关闭。

- [x] SHM 资源回收 + header layout/几何校验。
  - 位置：`src/transport/shm_transport.*`
  - 实现：新增 `layoutVersion` 字段与校验；打开对端段按其**实际大小**映射（`fstat`/`VirtualQuery`），
    并校验映射可容纳声明几何，杜绝按本地几何 under-map 越界；`resolvePeer()` 检测失效段并回收 `peerRegions_`。

- [x] SHM 事件通知替代轮询 sleep。
  - Linux 下经共享内存 `notify` 字 + futex（`FUTEX_WAIT`/`WAKE`）唤醒消费者，带 2ms 超时兜底；
    其他平台保留 sleep 回退。

- [x] `ITransport::sendv()` 默认策略明确 + 原生实现。
  - 基类默认实现明确为“正确性兜底（拼接后 send）”；UDP（sendmsg）、TCP（分片写）、
    SHM（gather-copy 入槽）均原生重写 `sendv`。

## P4: 文档与构建一致性（全部完成）

- [x] 统一版本信息。`EMQ_VERSION_STRING` 单一来源。
- [x] 修正交叉编译平台判断（`is_os()`→`is_plat()`）。
- [x] 修正 `docs/architecture.md` 未来态内容、清理未接线配置项。
  - `enableLocalIpc`/`enableSerial`/`enableBle` 在 `config.h` 标注为 reserved 且默认关闭；
    architecture.md 增补“实现现状 vs 路线图”说明。
- [x] 更新测试/benchmark 文档口径（README/todo 同步至 v0.4，断言数 2220）。

## 进一步可选优化（非阻塞）

- 单 reactor 全收敛：将 SHM/TCP 接收与 TimerWheel 一并并入同一 epoll 事件循环（当前 UDP 已接入）。
- 协议 v2 扩展：nodeId 32 位扩展、消息分片（FRAGMENT/LAST_FRAG）、压缩/加密 flag 的实际编解码。
- 跨节点 TCP/SHM 数据面的能力化选择（基于 ANNOUNCE endpoint 列表自动择优）。
