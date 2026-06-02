# EmbedMQ 审查待办

本文记录对当前项目的设计审查、潜在 bug 和嵌入式 Linux 精简建议。
状态标记：`[x]` 已修复并自测；`[~]` 合理但需较大改造，已分阶段推迟；`[ ]` 待办。

> 本轮（v0.3.x）已完成 P0 全部 + P1/P2/P3/P4 中范围清晰、低风险的项，
> 并新增 `tests/test_review_fixes.cpp` 回归测试。全量测试：**180 passed, 0 failed**。
> 较大的架构性改造（compact v2 包头、TLV 发现、epoll reactor 线程收敛、
> QoS2 完整状态机、嵌入式构建 profile、显式字节序）评估为合理但风险/工作量较大，
> 拆分到后续里程碑，详见各条目说明。

## P0: 正确性与稳定性（本轮全部完成）

- [x] 修复对端 topic 更新不同步问题。
  - 位置：`src/discovery/peer_registry.h`、`src/core/message_bus.cpp`、`src/core/participant.cpp`、`src/discovery/discovery_agent.*`
  - 修复：`PeerRegistry::addOrUpdate()` 在订阅/端点变化时触发新增的 `onUpdated_`；
    经 `DiscoveryAgent::setOnPeerUpdated` 透传到 `MessageBus::onPeerUpdated()` 刷新 `peers_`。
  - 测试：`peer_registry_update_fires_callback`。

- [x] 修复 Req/Rep future 可能永久挂起的问题。
  - 位置：`src/core/message_bus.cpp`、`src/core/message_bus.h`
  - 修复：`pendingRequests_` 改为 `{promise, deadline}`，请求按截止时间统一超时；
    远端无 provider 时立即 `set_exception`；REPLY/超时设异常均在锁外执行。
    明确分离发布侧 `seqId`（QoS 重传/放弃）与请求侧 `correlationId`，不再混用索引。
    同步 `request()` 捕获异常返回 `nullopt`，保持原 API 语义。
  - 测试：`request_no_provider_returns_quickly`。

- [x] 修复 TCP 流式拆包读取问题。
  - 位置：`src/transport/tcp_transport.*`
  - 修复：新增 `recvAll()` 循环读满长度头与负载；连接结束时 `removeConnection()` 清理连接表。
  - 备注：TCP 仍为实验性（每连接一线程），后续随线程模型收敛重构（见 P3）。

- [x] 修复 SHM 槽位自旋失败后仍写入的问题。
  - 位置：`src/transport/shm_transport.*`
  - 修复：经容量不变量分析，正常路径写入安全；异常（消费者卡死）下自旋耗尽后
    写入零长度 READY 占位让消费者跳过并推进 tail，返回 `false` 且 `dropped_` 计数，
    既不破坏数据也不会让队列永久卡住。新增 `droppedCount()` 暴露背压。

- [x] 加固 `MessageCodec::decode()` 的长度校验。
  - 位置：`src/core/message_codec.h`
  - 修复：`expectedSize` 用 `uint64_t` 累加，避免 32 位 `size_t` 回绕绕过长度检查。
  - 测试：`decode_rejects_length_overflow`。

- [x] 修复 `Subscriber` / `Replier` 回调裸指针生命周期风险。
  - 位置：`src/core/message_bus.cpp`
  - 修复：计数/暂停状态移入独立 `shared_ptr<State>`，路由/服务回调捕获 `weak_ptr`，
    Impl 析构后在途回调 `lock()` 失败即安全跳过，杜绝 UAF。

- [x] 修复 `TimerWheel` 长定时器提前触发问题。
  - 位置：`src/util/timer_wheel.h`
  - 修复：改用绝对 `fireTick` 计数，同槽仅触发 `fireTick<=curTick_` 的定时器，
    支持任意长延时（不再受单层轮长度限制）。
  - 测试：`timer_long_delay_no_early_fire`。

- [x] 修复 `TimerWheel` 停止阶段仍可能重装周期任务的问题。
  - 位置：`src/util/timer_wheel.h`
  - 修复：触发回调与周期重装前检查 `running_`；`cancelSet_` 改为持久保存直到对应定时器
    被处理（修正“取消未来定时器被提前清空失效”），周期任务复用同一 id 使 cancel 生效。
  - 测试：`timer_periodic_cancel`、`timer_stop_no_fire`。

## P1: 协议与发现机制

- [~] 明确并修正 QoS2 语义。
  - 本轮：已周期调用 `QoSEngine::cleanupDedupWindow()`，修复去重集合无界增长；
    确认现有 ACK→去重顺序对“恰好一次投递给订阅者”是正确的（重复包需重新 ACK，但不重复路由）。
  - 推迟：完整 PUBREC/PUBREL/PUBCOMP 状态机与滑动去重窗口；嵌入式 profile 倾向降级 QoS2。

- [~] 协议字段改为显式字节序。
  - 推迟：当前目标均为小端 Linux，改造涉及 encode/decode 全面重写并影响线缆兼容，
    与 compact v2 包头一并在协议 v2 里统一处理。

- [x] 校验 encode 阶段的 topic/payload 上限。
  - 位置：`src/core/message_codec.h`
  - 修复：新增 `MAX_TOPIC_LEN`/`MAX_PAYLOAD_LEN`，`encode`/`encodeHeader` 超限直接返回空。
  - 测试：`encode_rejects_oversized_topic`。

- [~] 精简或实现未闭环的协议字段。
  - 推迟：`COMPRESSED/ENCRYPTED/FRAGMENT/LAST_FRAG/serializerId/SUBSCRIBE/...` 等保留枚举
    暂不删除（避免破坏 ABI/线缆），将随协议 v2 与嵌入式 profile 一并裁剪或补齐。

- [x] 让 `enableMulticast` 和 `enableLocalDiscovery` 真正生效。
  - 位置：`include/embedmq/config.h`、`src/transport/transport_manager.cpp`、`src/transport/udp_transport.*`、`src/discovery/discovery_agent.cpp`
  - 修复：UDP 传输按 `multicast_enabled` 决定是否创建/加入多播组并在 `broadcast` 中尊重该开关；
    `enableLocalDiscovery=false` 时 `DiscoveryAgent::start()` 不再启动 announce/heartbeat/timeout。

- [~] 重新设计 ANNOUNCE 载荷（TLV/变长、增量宣布）。
  - 推迟：与 compact v2 包头、endpoint 列表一并在发现协议 v2 中处理。

- [~] 在 ANNOUNCE 中携带 endpoint 列表（TCP/SHM 自动寻址）。
  - 推迟：依赖 ANNOUNCE 重新设计，归入发现协议 v2。

- [x] 加固接收分派前的 magic/version 校验。
  - 位置：`src/core/participant.cpp`
  - 修复：分派前校验 `size>=HEADER_FIXED_SIZE` 且 magic/version 匹配，过滤随机/损坏包。

- [~] 降低发现流量（合并心跳与 announce、稳定期拉长周期）。
  - 推迟：归入发现协议 v2；可后续将 heartbeat 合并进 announce。

- [x] 降低 nodeId 碰撞风险。
  - 位置：`src/core/participant.cpp`
  - 修复：`generateNodeId()` 引入 `random_device` 增强熵，规避保留值 0 与广播 0xFFFF。
  - 备注：固定 ID 配置项与 32 位扩展（需线缆变更）留待协议 v2。

## P2: 嵌入式 Linux 精简

- [~] 增加嵌入式构建 profile（默认仅 UDP + Pub/Sub + QoS0/1）。
  - 推迟：需引入 `--profile` 选项与一组联动开关，单独里程碑落地。

- [~] 默认构建不要全开。
  - 推迟：与构建 profile 一并调整默认值，避免影响现有 CI/测试流程。

- [~] 设计 compact v2 包头（12-16 字节基础头 + 可选扩展）。
  - 推迟：协议 v2 核心工作项。

- [~] 将 CRC 做成可配置能力。
  - 推迟：随协议 v2 增加编译/运行期开关。

- [~] 收敛线程模型到单 epoll reactor + timerfd/eventfd。
  - 推迟：较大重构，逐步将 UDP/SHM/TCP/定时器并入 reactor。

- [~] 缩小 UDP recv buffer 并按 MTU 管控。
  - 推迟：需配套分片/拒绝策略，避免截断大数据报。

- [~] 优化 `Payload` 热路径分配（PayloadView / 小 buffer 优化）。
  - 推迟：涉及公共类型与 API，单独评估。

- [x] 让内存池和无锁队列进入真实热路径，或降低宣传。
  - 处理：本轮选择“降低宣传”，在 README 标注 `FixedBlockPool`/`MpscQueue`/`SPSCRingBuffer`
    为可选工具组件，核心路径尚未默认接入（避免文档与实现不符）。

- [x] `SocketApi::sendToV()` 避免每次堆分配。
  - 位置：`src/platform/socket_api_posix.cpp`
  - 修复：`sendToV`/`sendV` 改用栈上 `iovec[8]`，仅在分片数超过 8 时回退堆分配。

## P3: 传输层完善

- [x] 明确 `TransportManager::broadcast()` 的语义。
  - 位置：`src/transport/transport_manager.cpp`
  - 修复：仅向 `capability().supportsBroadcast` 为真的传输（当前即 UDP）广播，
    避免对 TCP/SHM 调用无意义/未来误行为。

- [~] TCP 默认不进入嵌入式构建。
  - 推迟：随构建 profile（P2）一并调整默认开关。本轮已修复 TCP 拆包与连接清理（见 P0）。

- [~] SHM 增加可观测背压与资源回收。
  - 本轮：已加 `droppedCount()` 背压计数（见 P0）。
  - 推迟：`peerRegions_` 在对端 lost 时回收、header layout version/几何校验。

- [~] SHM 使用事件通知替代轮询 sleep（eventfd/futex）。
  - 推迟：与线程模型收敛一并处理；可先暴露轮询间隔配置。

- [~] `ITransport::sendv()` 默认实现策略需要明确。
  - 推迟：TCP/SHM 原生 sendv 留待传输重构；当前默认拼接实现仅在非 UDP 路径生效。

## P4: 文档与构建一致性

- [x] 统一版本信息。
  - 位置：`include/embedmq/platform.h`（新增 `EMQ_VERSION_STRING`）、`src/main.cpp`。
  - 修复：版本字符串单一来源，`main` 打印 `v0.3.0`，与 xmake/README 一致。

- [x] 修正交叉编译平台判断。
  - 位置：`xmake.lua`
  - 修复：平台源文件选择由 `is_os()` 改为 `is_plat()`，交叉编译按目标平台选择 PAL。

- [~] 修正 `docs/architecture.md` 中未来态内容（CMake/serial/BLE/local_ipc 等）。
  - 推迟：需拆分“当前 v0.3 实现”与“路线图”，单独做文档梳理。

- [~] 清理未接线配置项（`enableLocalIpc`/`enableSerial`/`enableBle`）。
  - 推迟：随 architecture 文档梳理统一标注为 reserved。

- [~] 更新测试和 benchmark 文档口径。
  - 本轮：README 测试断言数已更新为 180；其余构建选项口径随文档梳理统一。

## 后续里程碑（推迟项汇总）

1. 协议 v2：compact 包头 + 显式字节序 + TLV/增量 ANNOUNCE + endpoint 列表 + nodeId 扩展。
2. 嵌入式构建 profile + 默认构建瘦身 + TCP/SHM/QoS2/io_uring 默认开关。
3. 线程模型收敛：UDP/SHM/TCP/定时器并入单 epoll reactor + timerfd/eventfd。
4. QoS2 完整状态机与滑动去重窗口（或在嵌入式 profile 降级）。
5. 文档梳理：architecture 拆分“现状/路线图”，清理未实现内容与未接线配置。
