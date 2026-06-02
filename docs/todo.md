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

## 路线图剩余阶段

- [ ] Phase 4：串口 Transport、BLE Transport、大消息分片/重组、流量控制、ARM 交叉编译验证。
- [ ] Phase 5 收尾（可选）：配置文件支持（YAML）、网络桥接工具。
- [ ] Phase 6：TLS / 消息加密、LZ4 压缩、CI/CD（GitHub Actions 三平台矩阵）、API 稳定性声明、正式发布。

## 测试与质量

- [x] 压力测试与稳定性测试脚本（C++ `emq_stress` + Python `bindings/python/stress.py`）。
  - 场景：吞吐、扇出、并发多生产者、请求-响应负载、生命周期 churn、混合 soak。
  - 自测：`xmake run emq_stress all` 与 `python3 bindings/python/stress.py all` 全部通过。
