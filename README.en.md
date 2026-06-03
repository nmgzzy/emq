# EmbedMQ

**English** | **[简体中文](README.md)**

**Cross-platform lightweight communication middleware** | Embedded Message Queue / Middleware

[![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue)]()
[![Language](https://img.shields.io/badge/language-C%2B%2B17-orange)]()
[![Build](https://img.shields.io/badge/build-xmake-green)]()
[![Phase](https://img.shields.io/badge/phase-5%20done-brightgreen)]()
[![Tests](https://img.shields.io/badge/tests-2277%20passed-brightgreen)]()

---

## Overview

EmbedMQ is a **decentralized, pluggable-transport, pub-sub and request-reply** lightweight C++ communication middleware.

Inspired by DDS / ZMQ / MQTT, designed for **embedded Linux and desktop platforms (Windows / Linux / macOS)**.

| Feature | Description |
|---------|-------------|
| Decentralized P2P | No broker/server; peers communicate directly |
| Dual modes | Publish-subscribe (1:N) + request-reply (1:1) |
| Pluggable transport | UDP / TCP (experimental) / shared memory (same-host IPC, implemented); serial / BLE planned as plugins |
| Auto-discovery | UDP multicast (239.255.0.1:19900); no manual peer addressing |
| Reliable delivery | QoS 0/1/2: BestEffort / ACK+retransmit / exactly-once |
| Last Will | Peers publish Last Will when a node drops unexpectedly |
| Performance | Zero-copy scatter/gather, memory pools, lock-free MPSC/SPSC queues, CPU affinity |
| Cross-platform PAL | Platform abstraction for Windows / Linux / macOS |
| Zero third-party deps | C++17 stdlib + native APIs only (io_uring optional/experimental) |
| Simple API | 16 core calls; productive in ~5 minutes |
| Multi-language | Stable C ABI + Python binding (ctypes, zero deps) |
| CLI tools | `emqtop` monitor/publish/subscribe/diagnostics (like `mosquitto_pub/sub`) |

---

## Quick Start

### Requirements

| Component | Minimum |
|-----------|---------|
| C++ compiler | MSVC 2019+ / GCC 7+ / Clang 8+ / AppleClang 12+ |
| Build tool | [xmake](https://xmake.io) 2.7+ |
| OS | Windows 10+ / Linux 4.x+ / macOS 11+ |

### Build

```bash
# Clone
git clone https://github.com/<your-org>/embedmq.git
cd embedmq

# Debug
xmake f -m debug
xmake

# Release
xmake f -m release
xmake
```

### Run examples

```bash
# Pub/Sub
xmake run example_pub_sub

# Req/Rep
xmake run example_req_rep
```

### Run unit tests

```bash
# All tests (11 modules, 2277 assertions)
xmake run emq_tests

# Specific modules
xmake run emq_tests pal
xmake run emq_tests pub_sub req_rep
xmake run emq_tests capi

# List modules
xmake run emq_tests --list

# Help
xmake run emq_tests --help
```

---

## Core API

Include a single header:

```cpp
#include "embedmq/embedmq.h"
```

### Publish-subscribe

```cpp
// Create participant (node)
auto p = embedmq::Participant::create("my_node");

// Subscribe (wildcards * and #)
auto sub = p->createSubscriber("sensor/#",
    [](const embedmq::ReceivedMessage& msg) {
        std::cout << msg.topic << ": "
                  << msg.payload.asText() << "\n";
    });

// Publish
auto pub = p->createPublisher("sensor/temperature");
pub->publish("25.6°C");
pub->publish(rawBytes, size);  // binary
```

### Request-reply

```cpp
// Server: register handler
auto rep = p->createReplier("echo",
    [](const embedmq::ReceivedMessage& req) -> embedmq::Payload {
        return embedmq::Payload("echo:" + std::string(req.payload.asText()));
    });

// Client: sync request with timeout
auto req = p->createRequester("echo");
auto result = req->request(embedmq::Payload("hello"),
                            std::chrono::milliseconds(5000));
if (result) {
    std::cout << result->asText() << "\n";  // "echo:hello"
}
```

### QoS configuration

```cpp
// QoS Level 0: BestEffort (default, high-frequency data)
auto pub = p->createPublisher("sensor/temp", embedmq::QoSProfile::bestEffort());

// QoS Level 1: Reliable (ACK + retransmit)
auto pub = p->createPublisher("ctrl/cmd", embedmq::QoSProfile::reliable());

// QoS Level 2: ExactlyOnce
auto pub = p->createPublisher("config", embedmq::QoSProfile::exactlyOnce());

// Retained message (new subscribers get latest value immediately)
embedmq::QoSProfile retainQos;
retainQos.retain     = true;
retainQos.durability = embedmq::DurabilityKind::TransientLocal;
auto pub = p->createPublisher("status/online", retainQos);
pub->publish("true");
```

### Full node configuration

```cpp
embedmq::ParticipantConfig cfg;
cfg.nodeName                  = "sensor_node";
cfg.domainId                  = 0;                // isolate communication domain
cfg.discovery.multicastGroup  = "239.255.0.1";
cfg.discovery.multicastPort   = 19900;
cfg.discovery.heartbeatIntervalMs = 2000;
cfg.discovery.peerTimeoutMs   = 10000;
cfg.transport.enableUdp       = true;
cfg.transport.enableTcp       = false;

// Last Will: published automatically on abnormal disconnect
cfg.lastWill.topic   = "status/sensor_node";
cfg.lastWill.payload = embedmq::Payload("offline");
cfg.lastWill.retain  = true;
cfg.lastWill.enabled = true;

auto p = embedmq::Participant::create(cfg);

// Peer connect/disconnect callback
p->onPeerEvent([](uint16_t id, const std::string& name, bool connected) {
    std::cout << name << (connected ? " connected" : " disconnected") << "\n";
});
```

---

## Topic naming

```
Format:   segment/segment/segment
Examples: sensor/temperature/room1
          vehicle/can/engine/rpm
          $SYS/node/status        # reserved system prefix

Wildcards:
  *   single level    sensor/*/room1   → matches sensor/temperature/room1
  #   multi level     sensor/#         → matches sensor/temp/room1/detail
```

---

## Architecture

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

## Project layout

```
embedmq/
├── include/embedmq/              # Public API (include this tree only)
│   ├── embedmq.h                 # C++ main header (single entry)
│   ├── embedmq_c.h               # C ABI (Phase 5: stable FFI)
│   ├── platform.h                # Platform detection macros
│   ├── types.h                   # Payload / ReceivedMessage
│   ├── qos.h                     # QoSProfile
│   ├── config.h                  # ParticipantConfig
│   └── transport/itransport.h    # Transport plugin interface
│
├── src/
│   ├── capi/                     # C ABI wrapper (Phase 5)
│   │   └── embedmq_c.cpp         # Opaque handles + exception isolation
│   │
│   ├── platform/                 # Platform Abstraction Layer (PAL)
│   │   ├── event_loop.h          # Interface
│   │   ├── event_loop_epoll.cpp  # Linux
│   │   ├── event_loop_kqueue.cpp # macOS
│   │   ├── event_loop_iocp.cpp   # Windows
│   │   ├── socket_api.h + *.cpp  # Cross-platform sockets
│   │   └── process.h             # PID / hostname / temp dir
│   │
│   ├── util/                     # Utilities (pure C++17, no platform deps)
│   │   ├── crc32.h               # CRC32
│   │   ├── ring_buffer.h         # Lock-free SPSC ring buffer
│   │   ├── mpsc_queue.h          # Lock-free MPSC queue (Vyukov)
│   │   ├── memory_pool.h         # Fixed-block memory pool
│   │   ├── logger.h              # Lightweight logging
│   │   └── timer_wheel.h         # Timer wheel (CPU affinity support)
│   │
│   ├── core/                     # Core layer
│   │   ├── message_codec.h       # Wire format codec
│   │   ├── topic_router.h        # Topic routing + wildcards
│   │   ├── qos_engine.h          # QoS policy engine
│   │   ├── retained_store.h      # Retained messages
│   │   ├── message_bus.h/cpp     # Message bus
│   │   └── participant.cpp       # Participant implementation
│   │
│   ├── discovery/                # Auto-discovery
│   │   ├── peer_registry.h       # Peer registry
│   │   └── discovery_agent.h/cpp # Announce / Heartbeat / Farewell
│   │
│   └── transport/                # Transport plugins
│       ├── transport_manager.h/cpp
│       ├── udp_transport.h/cpp   # UDP unicast + multicast (zero-copy sendmsg)
│       ├── tcp_transport.h/cpp   # TCP (length-prefixed framing)
│       └── shm_transport.h/cpp   # Shared memory (POSIX shm / Win FileMapping)
│
├── tests/                        # Unit tests (single binary emq_tests)
│   ├── test_framework.h          # Lightweight framework (modules + CLI filter)
│   ├── test_main.cpp             # Entry
│   ├── test_topic_router.cpp     # module: topic_router
│   ├── test_message_codec.cpp    # module: message_codec
│   ├── test_qos_engine.cpp       # module: qos_engine
│   ├── test_pal.cpp              # module: pal
│   ├── test_pub_sub.cpp          # module: pub_sub
│   ├── test_req_rep.cpp          # module: req_rep
│   ├── test_last_will.cpp        # module: last_will
│   ├── test_phase3.cpp           # module: phase3 (pool/MPSC/SHM/affinity/zero-copy)
│   ├── test_review_fixes.cpp     # module: review_fixes
│   └── test_capi.cpp             # module: capi (C ABI handles/I/O/error codes)
│
├── examples/
│   ├── pub_sub/main.cpp          # Sensor pub/sub demo
│   └── req_rep/main.cpp          # Compute service req/rep demo
│
├── bindings/                     # Language bindings (Phase 5)
│   └── python/
│       ├── embedmq.py            # Python ctypes binding (zero deps)
│       ├── example.py            # Python self-test / demo
│       └── stress.py             # Python stress/stability tests
│
├── tools/                        # CLI tools (Phase 5)
│   ├── emqtop/main.cpp           # Monitor/publish/diagnostics (emqtop)
│   ├── emq_stress/main.cpp       # Stress/stability (emq_stress)
│   └── measure_resources.sh      # Runtime resource sampling script
│
├── bench/
│   └── bench_main.cpp            # Benchmarks (emq_bench)
│
├── docs/
│   └── architecture.md           # Full design document
│
└── xmake.lua                     # Build script
```

---

## Wire format

EmbedMQ uses a fixed header plus variable payload:

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
  Fixed header: 40 bytes
```

**Magic number:** `0xEBDC` (EmbedMQ Data Channel)

---

## QoS levels

| Level | Name | Semantics | Use case |
|-------|------|-----------|----------|
| 0 | BestEffort | At-most-once, no ACK | High-rate sensors, log streams |
| 1 | Reliable | At-least-once, ACK + retransmit | Control commands, status |
| 2 | ExactlyOnce | Exactly-once with dedup | Critical config, transactions |

---

## Auto-discovery

Nodes discover each other via **UDP multicast** without manual peer configuration:

```
Node A starts
  └── sends ANNOUNCE (239.255.0.1:19900)
        { id, name, topics, endpoints }

Node B starts
  ├── receives A's ANNOUNCE → discovers A
  └── sends its ANNOUNCE → A discovers B

Both send HEARTBEAT periodically (default 2s)
  └── no heartbeat within timeout (default 10s) → mark offline
```

For processes on the same machine, enable **shared memory transport** (below) for lowest latency.

---

## Performance (Phase 3)

### Shared memory transport

Same-host IPC via POSIX `shm_open`+`mmap` / Windows `CreateFileMapping`, bypassing the network stack:

```cpp
embedmq::ParticipantConfig cfg;
cfg.transport.enableShm = true;   // Enable SHM inbox
auto p = embedmq::Participant::create(cfg);
```

Each node has a bounded inbox slot ring (default 256 slots × 4 KB). Multiple processes write as producers (CAS slot reservation); the node's poll thread is the sole consumer.

### Zero-copy scatter/gather

BestEffort (QoS 0) publish uses `encodeHeader` for a compact wire header (protocol v2: 26-byte base + optional timestamp/CRC) and `sendmsg`/`WSASendTo` iovec `{header, topic, payload}` without copying payload into one buffer.

### Memory pool and lock-free queues

Optional **utility components** (validated in `tests`/`bench`). The default hot path still uses `std::vector`/standard containers; plug in custom transports/queues as needed:

- `util::FixedBlockPool`: O(1) alloc/free, predictable latency, less fragmentation.
- `util::MpscQueue`: Lock-free MPSC (Vyukov).
- `util::SPSCRingBuffer`: Lock-free SPSC ring.

### CPU affinity

```cpp
cfg.threading.pinCpu      = true;
cfg.threading.cpuAffinity = 2;   // Pin worker thread to core 2
```

### Benchmarks (Linux x64, Release, reference)

| Item | Throughput / latency |
|------|----------------------|
| Local Pub/Sub | ~11 M msg/s (avg publish ~0.09 µs) |
| SPSC ring | ~270 M msg/s |
| MPSC queue (4→1) | ~16 M msg/s |
| Memory pool vs malloc | ~1.06× (single-thread) |

> Run `xmake f -m release && xmake run emq_bench`. Numbers vary by hardware.

### Runtime resources (Linux x64, Release, 16-core measured)

`tools/measure_resources.sh` samples `/proc/<pid>` and cross-checks with `/usr/bin/time -v`:

| Scenario | Peak RSS | Threads | FDs | CPU |
|----------|----------|---------|-----|-----|
| Idle, in-process (`emqtop sub --no-udp`) | **3.91 MB** | 3 | 24 | ~0% |
| Idle, network (`emqtop monitor`, UDP+multicast) | **3.99 MB** | 4 | 28 | ~0.5% |
| Soak (pub+sub+churn) | **3.96 MB** | 4 | 24 | ~27% (~0.27 core) |
| High concurrency (8 producers × 8 subscribers) | **4.06 MB** | 10 | 24 | ~803% (~8 cores) |

Binary/library size: `emqtop` ~275 KB, `libembedmq_c.so` ~292 KB, `libembedmq.a` ~629 KB.

Highlights:

- **Low, stable memory**: RSS stays around **~4 MB** idle or loaded; no leak or unbounded growth in soak/churn.
- **Clear threading**: in-process node uses 3 threads (main + MessageBus worker + TimerWheel); UDP adds one epoll receive thread. Extra threads under load come from **caller** producers, not internal bloat.
- **CPU scales with load**: ~0% idle; ~0.27 core single stream; ~8 cores at 8-way concurrency; `shared_mutex` routing shows no obvious lock bottleneck.
- **Small FD usage**: 24 in-process, 28 network—well below typical 1024 soft limits.

```bash
# Reproduce (any binary):
tools/measure_resources.sh -i 0.5 -d 30 -- ./build/linux/x86_64/release/emq_stress soak -d 30
```

---

## Multi-language and tools (Phase 5)

### C ABI (stable FFI)

`include/embedmq/embedmq_c.h` exposes a flat C API with opaque handles over the C++ core: errors via return codes / NULL; C++ exceptions never cross the ABI—suitable for FFI from other languages.

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

Build artifact: `libembedmq_c.so` / `.dylib` / `embedmq_c.dll` (`xmake build embedmq_c`).

### Python binding

ctypes wrapper over the C ABI, **zero third-party dependencies**, Pythonic API:

```python
import embedmq

with embedmq.Participant("py_node") as p:
    sub = p.create_subscriber("sensor/#", lambda m: print(m.topic, m.text))
    pub = p.create_publisher("sensor/temp")
    pub.publish("25.6")

    # Request-reply
    rep = p.create_replier("multiply",
        lambda m: str(eval(m.text.replace(" ", "*"))))
    req = p.create_requester("multiply")
    print(req.request("6 7"))   # b'42'
```

Self-test: `xmake build embedmq_c`, then `python3 bindings/python/example.py`.
Library lookup: `EMBEDMQ_LIB` env → scan repo `build/` → system paths.

### emqtop — CLI monitor/diagnostics

Network Swiss Army knife (joins as a normal node), similar to `mosquitto_pub/sub`:

```bash
emqtop monitor [topic]              # Subscribe (default #), print messages + topology stats
emqtop sub <topic>                  # Subscribe and print only
emqtop pub <topic> <msg> [-n N] [-i ms]   # Publish (repeat N times, interval i ms)
emqtop req <service> <msg> [-t ms]  # Request and print response
emqtop echo <service>               # Echo service (for testing req)
emqtop peers                        # List discovered peers

# Common: --name <n>  --domain <d>  --no-udp  --shm
```

### Stress and stability tests

Equivalent C++ (`emq_stress`) and Python (`bindings/python/stress.py`) scripts cover throughput / fan-out / multi-producer / req-rep load / lifecycle churn / mixed soak. All use **in-process synchronous delivery** with exact loss-free counts plus throughput, latency, and RSS reporting.

```bash
# C++: run all scenarios (thresholds, exit 0/non-zero)
xmake run emq_stress all
xmake run emq_stress throughput -n 1000000 -p 64   # Single scenario, custom count/payload
xmake run emq_stress concurrent -t 8 -s 8 -d 5     # 8 producers × 8 subscribers, 5s
xmake run emq_stress soak -d 30                    # 30s mixed soak

# Python (build embedmq_c first)
python3 bindings/python/stress.py all
python3 bindings/python/stress.py reqrep -t 4
```

Reference (Linux x64, Release): C++ local throughput ~11.6 M msg/s, Req/Rep avg ~2 µs; zero RSS growth in churn/soak; exact send/receive counts under concurrency.

---

## Custom transport plugins

Implement `ITransport` to plug in any channel:

```cpp
class MyCanTransport : public embedmq::ITransport {
public:
    std::string typeName() const override { return "can"; }

    TransportCapability capability() const override {
        TransportCapability cap;
        cap.maxPayloadSize = 8;          // CAN frame 8 bytes
        cap.estimatedLatencyUs = 500;
        return cap;
    }

    bool init(const std::string& config) override {
        // Init CAN socket...
        return true;
    }

    bool send(const Endpoint& to,
              const uint8_t* data, size_t size) override {
        // Send CAN frame...
        return true;
    }

    // ... remaining pure virtuals
};

// Register
participant->registerTransport("can",
    std::make_shared<MyCanTransport>());
```

---

## Comparison

| Feature | EmbedMQ | MQTT | ZeroMQ | DDS |
|---------|---------|------|--------|-----|
| Server required | ❌ | ✅ Broker | ❌ | ❌ |
| Auto-discovery | ✅ | ❌ | ❌ | ✅ |
| Pub/Sub | ✅ | ✅ | ✅ | ✅ |
| Req/Rep | ✅ | ❌ | ✅ | ❌ |
| Pluggable transport | ✅ | ❌ | ❌ | Limited |
| Serial/BLE | 📋 Planned | ❌ | ❌ | ❌ |
| Retained messages | ✅ | ✅ | ❌ | ✅ |
| Last Will | ✅ | ✅ | ❌ | ✅ |
| Shared memory IPC | ✅ | ❌ | Limited | Limited |
| Wildcards | ✅ `* #` | ✅ | Prefix | Limited |
| Library size | ~300 KB | ~200 KB | ~500 KB | ~5 MB |
| Third-party deps | **None** | Yes | Yes | Yes |
| Embedded fit | ★★★★★ | ★★★★ | ★★★ | ★★ |

---

## Roadmap

| Phase | Version | Status | Content |
|-------|---------|--------|---------|
| Phase 1 | v0.1 | ✅ **Done** | PAL, UDP, Pub/Sub, Req/Rep, QoS 0, discovery |
| Phase 2 | v0.2 | ✅ **Done** | QoS 1/2, wildcards, retained, Last Will, heartbeat, TCP (experimental) |
| Phase 3 | v0.3 | ✅ **Done** | SHM transport, zero-copy scatter/gather, memory pool, lock-free MPSC, CPU affinity, io_uring (experimental), benchmarks |
| Phase 4 | v0.4 | 📋 Planned | Serial transport, BLE transport, large-message fragmentation |
| Phase 5 | v0.5 | ✅ **Done** | C ABI, Python binding (ctypes), `emqtop` CLI |
| Phase 6 | v1.0 | 📋 Planned | TLS, LZ4 compression, CI/CD, stable release |

---

## Build options

```bash
# Disable TCP (UDP only)
xmake f --enable_tcp=n

# Disable shared memory transport
xmake f --enable_shm=n

# io_uring event loop (Linux experimental, kernel 5.1+ and liburing)
xmake f --enable_io_uring=y

# Disable tests / examples / bench
xmake f --build_tests=n
xmake f --build_examples=n
xmake f --build_bench=n

# Disable C ABI shared lib / CLI (Phase 5; off by default in embedded profile)
xmake f --build_capi=n
xmake f --build_tools=n

# Release
xmake f -m release

# Run benchmarks
xmake run emq_bench
```

---

## Test coverage

> **Platforms:** Linux x64 (GCC) / Windows 10 x64 (MSVC 2022) | **Total: 2277 assertions / 81 tests, all passing**

| Suite | Coverage | Assertions |
|-------|----------|------------|
| `test_topic_router` | Exact match, `*`/`#` wildcards, unsubscribe, multiple subscribers | 20 |
| `test_message_codec` | Encode/decode, CRC32 integrity, edge cases | 23 |
| `test_qos_engine` | ACK, timeout retransmit, give-up, QoS 2 dedup | 14 |
| `test_pal` | Process utils, CRC32, lock-free ring, timer wheel | 24 |
| `test_pub_sub` | Local pub/sub, wildcard routing, retained, pause/resume | 10 |
| `test_req_rep` | Sync/async request, multiple requests, request count | 10 |
| `test_last_will` | Timeout Last Will, graceful discard, local delivery, retained will | 15 |
| `test_phase3` | Memory pool, lock-free MPSC, CPU affinity, SHM I/O, zero-copy encode | 49 |
| `test_review_fixes` | Codec hardening, timer wheel long/cancel, peer update, no-service request | 15 |
| `test_refactor_v2` | Payload SBO, protocol v2 LE/compact header/optional CRC, QoS2 sliding window | ~2040 |
| `test_capi` | C ABI lifecycle, pub/sub and req/rep round-trip, binary payload, timeout, NULL safety | 55 |

---

## Design documentation

Architecture, sequence diagrams, protocol, QoS state machines:

**[docs/architecture.md](docs/architecture.md)**

---

## License

MIT License — see [LICENSE](LICENSE).

---

*EmbedMQ v0.4.0 — Phase 1 + Phase 2 + Phase 3 + Phase 5 shipped; protocol v2 (compact header / explicit LE / optional CRC), full QoS2 state machine, TLV discovery, embedded build profile, plus C ABI / Python binding / `emqtop` CLI*
