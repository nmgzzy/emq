# EmbedMQ 待办

本文仅记录**尚未实现 / 可选**的事项。已完成并自测的历史条目（P0–P4、Phase 5）
不再在此罗列，详见 `README.md` 路线图与 git 历史。

状态标记：`[x]` 已修复并自测；`[ ]` 待办。

## 已知遗留（非阻塞）

- [ ] `Publisher::subscriberCount()` 内部计数 `subCount` 恒为 0。
  - 现状：本地订阅不计入、远端匹配数也未在路由建立时回填，不影响收发。
  - C ABI / `emqtop` 仅忠实转发该值；后续可在订阅/路由变化时维护该计数。

## 进一步可选优化（非阻塞）

- [ ] 单 reactor 全收敛：将 SHM/TCP 接收与 TimerWheel 一并并入同一 epoll 事件循环（当前仅 UDP 已接入）。
- [ ] 协议 v2 扩展：nodeId 32 位扩展、消息分片（FRAGMENT/LAST_FRAG）、压缩/加密 flag 的实际编解码。
- [ ] 跨节点 TCP/SHM 数据面的能力化选择（基于 ANNOUNCE endpoint 列表自动择优）。

## 嵌入式优化评估（2026-06）

针对「嵌入式 Linux」用途做的一轮专项评估。已落地与待办分列。

已落地（已自测）：

- [x] **发布热路径零稳态分配**：编解码新增写入调用方缓冲的 `encodeInto()` /
  `encodeHeaderInto()`；`MessageBus::publish()` 的 BestEffort 路径改用 `thread_local`
  复用缓冲，稳态零堆分配、确定性分配延迟。原 `encode/encodeHeader` 改薄封装委托，
  逐字节语义不变。新增 codec 等价性/复用回归测试。
- [x] **embedded 画像编译取向（偏 CPU/吞吐）**：xmake 与 CMake 同步设为 `-O2`（小消息
  中间件甜点，不取 `-O3` 以免体积膨胀/i-cache 压力）+ LTO（跨模块内联，利于 CPU）+
  段级编译 + 链接期段 GC (`--gc-sections`) + strip（仅 GCC/Clang）。`.text` 较 full
  release（`-O3`）的 ~632 KB 降至 ~401 KB。如需极限瘦身可将 `-O2` 换回 `-Os`。

值得做、尚未实现（按嵌入式收益/风险排序）：

- [ ] `TopicRouter::matchWildcard` 每次调用都用 `split()` 分配两个 `vector<string>` 并拷贝
  各分段——通配订阅命中即触发。可改为零分配的原地分段扫描（不拷贝 token）。
- [ ] `config.threading.ioThreads` / `workerThreads` 为**死字段**（定义但代码无引用）：
  要么接上线程池，要么移除以免误导 API 使用者。
- [ ] QoS2 去重窗口用 `std::map<uint32_t,char>` 维护每 source 1024 条 seq——内存偏大且
  易碎片。改为固定大小环形 bitmap 更省内存、无分配、O(1)。
- [ ] 可靠路径 `encode()` 仍每条独立分配缓冲（重传需独占所有权，无法复用 thread_local）。
  可引入按 QoS 的发送缓冲池（如 `util::FixedBlockPool`）降低 Reliable/ExactlyOnce 分配率。
- [ ] 协议字段上限（`MAX_PAYLOAD_LEN` 64 MiB、`MAX_TOPIC_LEN`）、定时器轮精度
  （`TICK_MS=10` / `SLOT_COUNT=512`）、Payload SBO 容量（32B）等均为硬编码常量。
  对内存/Flash 受限设备，宜改为编译期可配（模板参数或 `-D` 宏），以收紧上界。
- [ ] TCP **每连接一个 detach 线程**（连接数→线程数线性增长）。注：embedded 画像本就
  不编译 TCP，对嵌入式用途低优先；其与下文「单 reactor 全收敛」同源，宜一并解决。

## 路线图剩余阶段

- [ ] Phase 4：串口 Transport、BLE Transport、大消息分片/重组、流量控制、ARM 交叉编译验证。
- [ ] Phase 5 收尾（可选）：配置文件支持（YAML）、网络桥接工具。
- [ ] Phase 6：TLS / 消息加密、LZ4 压缩、CI/CD（GitHub Actions 三平台矩阵）、API 稳定性声明、正式发布。

## 测试与质量

- [x] 压力测试与稳定性测试脚本（C++ `emq_stress` + Python `bindings/python/stress.py`）。
  - 场景：吞吐、扇出、并发多生产者、请求-响应负载、生命周期 churn、混合 soak。
  - 自测：`xmake run emq_stress all` 与 `python3 bindings/python/stress.py all` 全部通过。
