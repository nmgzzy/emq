#include "shm_transport.h"
#include "../platform/process.h"
#include "../util/logger.h"
#include <atomic>
#include <chrono>
#include <cstring>

#if defined(EMQ_PLATFORM_WINDOWS)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#if defined(EMQ_PLATFORM_LINUX)
#include <linux/futex.h>
#include <sys/syscall.h>
#include <ctime>
#endif

namespace embedmq {

#if defined(EMQ_PLATFORM_LINUX)
// 跨进程 futex：作用于共享内存中的 32 位字（按物理页定位，可跨进程唤醒）
static inline int shmFutex(std::atomic<uint32_t>* addr, int op,
                           uint32_t val, const struct timespec* ts) {
    return static_cast<int>(::syscall(SYS_futex,
        reinterpret_cast<uint32_t*>(addr), op, val, ts, nullptr, 0));
}
#endif

// ===================== 共享内存布局 =====================
//
// [ShmHeader] [slot 0] [slot 1] ... [slot N-1]
// 每个 slot：[atomic<uint32_t> state][uint32_t len][payload bytes...]
//   state: 0=空闲, 1=就绪(可读)
//
// 生产者通过对 head 做 CAS 预留槽位；消费者按 tail 顺序读取。

struct ShmTransport::ShmHeader {
    uint32_t              magic;         // 校验段有效性
    uint32_t              layoutVersion; // 布局版本：不匹配则拒绝映射
    uint32_t              slotSize;
    uint32_t              slotCount;
    std::atomic<uint32_t> head;          // 生产者预留索引
    std::atomic<uint32_t> tail;          // 消费者读取索引
    std::atomic<uint32_t> notify;        // 事件唤醒序号（Linux futex）
    uint32_t              _pad;
};

static constexpr uint32_t SHM_MAGIC          = 0x4D485345; // "ESHM"
static constexpr uint32_t SHM_LAYOUT_VERSION = 1;
static constexpr uint32_t SLOT_EMPTY         = 0;
static constexpr uint32_t SLOT_READY         = 1;

namespace {
struct SlotHeader {
    std::atomic<uint32_t> state;
    uint32_t              len;
};
} // namespace

struct ShmTransport::Region {
    std::string name;
    void*       base{nullptr};
    size_t      totalSize{0};
    bool        owner{false};
#if defined(EMQ_PLATFORM_WINDOWS)
    HANDLE      mapping{nullptr};
#else
    int         fd{-1};
    std::string shmPath; // 形如 "/emq.<name>"
#endif

    ShmHeader* header() const { return reinterpret_cast<ShmHeader*>(base); }
    uint8_t*   slot(uint32_t idx) const {
        ShmHeader* h = header();
        uint8_t* slots = reinterpret_cast<uint8_t*>(base) + sizeof(ShmHeader);
        return slots + static_cast<size_t>(idx % h->slotCount) * h->slotSize;
    }
};

TransportCapability ShmTransport::capability() const {
    TransportCapability cap;
    cap.supportsMulticast       = false;
    cap.supportsBroadcast       = false;
    cap.supportsReliable        = true;   // 同主机内存拷贝，无丢包
    cap.supportsStreaming       = false;
    cap.maxPayloadSize         = slotSize_ - static_cast<uint32_t>(sizeof(SlotHeader));
    cap.estimatedLatencyUs     = 5;       // 远低于网络
    cap.estimatedBandwidthKbps = 10000000;
    return cap;
}

static std::string parseStr(const std::string& cfg, const std::string& key) {
    auto pos = cfg.find("\"" + key + "\"");
    if (pos == std::string::npos) return "";
    pos = cfg.find(':', pos);
    if (pos == std::string::npos) return "";
    pos++;
    while (pos < cfg.size() && (cfg[pos] == ' ' || cfg[pos] == '"')) pos++;
    size_t end = pos;
    while (end < cfg.size() && cfg[end] != '"' && cfg[end] != ',' && cfg[end] != '}')
        end++;
    return cfg.substr(pos, end - pos);
}

bool ShmTransport::init(const std::string& config) {
    std::string n = parseStr(config, "shm_name");
    if (n.empty()) {
        n = "emq_shm_" + std::to_string(platform::getProcessId());
    }
    name_ = n;

    std::string ss = parseStr(config, "shm_slot_size");
    if (!ss.empty()) { try { slotSize_ = static_cast<uint32_t>(std::stoul(ss)); } catch (...) {} }
    std::string sc = parseStr(config, "shm_slot_count");
    if (!sc.empty()) { try { slotCount_ = static_cast<uint32_t>(std::stoul(sc)); } catch (...) {} }
    if (slotSize_  < 64)  slotSize_  = 64;
    if (slotCount_ < 2)   slotCount_ = 2;

    inbox_ = openInbox(name_, /*create=*/true);
    if (!inbox_) {
        EMQ_LOG_E("SHM", "Failed to create inbox segment: %s", name_.c_str());
        return false;
    }

    active_ = true;
    recvThread_ = std::thread([this]() { recvLoop(); });
    EMQ_LOG_I("SHM", "Initialized inbox=%s (slot=%u x %u)",
              name_.c_str(), slotSize_, slotCount_);
    return true;
}

void ShmTransport::shutdown() {
    if (active_.exchange(false)) {
        if (recvThread_.joinable()) recvThread_.join();

        {
            std::lock_guard<std::mutex> lock(peerMutex_);
            for (auto& [k, r] : peerRegions_) closeRegion(r);
            peerRegions_.clear();
        }
        if (inbox_) { closeRegion(inbox_); inbox_ = nullptr; }
    }
}

ShmTransport::Region* ShmTransport::openInbox(const std::string& name, bool create) {
    auto* r = new Region();
    r->name      = name;
    r->owner     = create;
    r->totalSize = sizeof(ShmHeader) +
                   static_cast<size_t>(slotCount_) * slotSize_;

#if defined(EMQ_PLATFORM_WINDOWS)
    std::string objName = "Local\\" + name;
    if (create) {
        r->mapping = ::CreateFileMappingA(
            INVALID_HANDLE_VALUE, nullptr, PAGE_READWRITE,
            0, static_cast<DWORD>(r->totalSize), objName.c_str());
    } else {
        r->mapping = ::OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, objName.c_str());
    }
    if (!r->mapping) { delete r; return nullptr; }
    bool existed = (::GetLastError() == ERROR_ALREADY_EXISTS);
    // 非创建时映射整个 section（传 0），避免按本地几何 under-map
    SIZE_T mapBytes = create ? r->totalSize : 0;
    r->base = ::MapViewOfFile(r->mapping, FILE_MAP_ALL_ACCESS, 0, 0, mapBytes);
    if (!r->base) { ::CloseHandle(r->mapping); delete r; return nullptr; }
    if (!create) {
        MEMORY_BASIC_INFORMATION mbi{};
        if (::VirtualQuery(r->base, &mbi, sizeof(mbi)) != 0)
            r->totalSize = static_cast<size_t>(mbi.RegionSize);
    }
    if (create && !existed) {
        std::memset(r->base, 0, r->totalSize);
        ShmHeader* h = r->header();
        h->magic = SHM_MAGIC; h->layoutVersion = SHM_LAYOUT_VERSION;
        h->slotSize = slotSize_; h->slotCount = slotCount_;
        h->head.store(0); h->tail.store(0); h->notify.store(0);
    }
#else
    r->shmPath = "/" + name; // POSIX 名字以 '/' 开头
    int flags  = create ? (O_CREAT | O_RDWR) : O_RDWR;
    r->fd = ::shm_open(r->shmPath.c_str(), flags, 0666);
    if (r->fd < 0) { delete r; return nullptr; }
    if (create) {
        if (::ftruncate(r->fd, static_cast<off_t>(r->totalSize)) != 0) {
            ::close(r->fd); delete r; return nullptr;
        }
    } else {
        // 打开对端段：以对端实际段大小映射，避免按本地几何 under-map 造成越界访问
        struct stat st{};
        if (::fstat(r->fd, &st) == 0 && st.st_size > 0)
            r->totalSize = static_cast<size_t>(st.st_size);
    }
    r->base = ::mmap(nullptr, r->totalSize, PROT_READ | PROT_WRITE,
                     MAP_SHARED, r->fd, 0);
    if (r->base == MAP_FAILED) {
        r->base = nullptr; ::close(r->fd); delete r; return nullptr;
    }
    if (create) {
        ShmHeader* h = r->header();
        // 仅首次创建时初始化（magic 未匹配即视为全新段）
        if (h->magic != SHM_MAGIC) {
            std::memset(r->base, 0, r->totalSize);
            h->magic = SHM_MAGIC; h->layoutVersion = SHM_LAYOUT_VERSION;
            h->slotSize = slotSize_; h->slotCount = slotCount_;
            h->head.store(0); h->tail.store(0); h->notify.store(0);
        }
    }
#endif

    // 打开（非创建）时校验段是否就绪、布局版本与几何一致
    if (!create) {
        ShmHeader* h = r->header();
        if (h->magic != SHM_MAGIC)                  { closeRegion(r); return nullptr; }
        if (h->layoutVersion != SHM_LAYOUT_VERSION) { closeRegion(r); return nullptr; }
        if (h->slotCount == 0 || h->slotSize == 0)  { closeRegion(r); return nullptr; }
        // 槽必须至少能容纳槽头，否则 (slotSize - sizeof(SlotHeader)) 在无符号下溢，
        // 使长度上界检查形同虚设。
        if (h->slotSize < sizeof(SlotHeader))       { closeRegion(r); return nullptr; }
        // 防 slotCount*slotSize 溢出 size_t（32 位嵌入式目标尤甚）：溢出会让 need
        // 回绕成小值绕过下面的容量检查，进而 slot 偏移越界。
        if (static_cast<size_t>(h->slotCount) >
            (SIZE_MAX - sizeof(ShmHeader)) / h->slotSize) { closeRegion(r); return nullptr; }
        // 校验映射大小能容纳声明的几何，防止越界
        size_t need = sizeof(ShmHeader) +
                      static_cast<size_t>(h->slotCount) * h->slotSize;
        if (r->totalSize < need)                    { closeRegion(r); return nullptr; }
    }
    return r;
}

void ShmTransport::closeRegion(Region* r) {
    if (!r) return;
#if defined(EMQ_PLATFORM_WINDOWS)
    if (r->base)    ::UnmapViewOfFile(r->base);
    if (r->mapping) ::CloseHandle(r->mapping);
#else
    if (r->base)  ::munmap(r->base, r->totalSize);
    if (r->fd >= 0) ::close(r->fd);
    if (r->owner) ::shm_unlink(r->shmPath.c_str());
#endif
    delete r;
}

bool ShmTransport::writeToRegion(Region* r, const uint8_t* data, size_t size) {
    if (!r || !r->base) return false;
    ShmHeader* h = r->header();
    const uint32_t cap   = h->slotCount;
    const uint32_t ssize = h->slotSize;
    if (size + sizeof(SlotHeader) > ssize) return false; // 单槽放不下

    // CAS 预留一个 head 槽位（容量检查避免覆盖未消费数据）
    uint32_t idx;
    for (;;) {
        uint32_t head = h->head.load(std::memory_order_acquire);
        uint32_t tail = h->tail.load(std::memory_order_acquire);
        if (head - tail >= cap) {            // 收件箱已满 → 丢弃（可观测背压）
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        if (h->head.compare_exchange_weak(head, head + 1,
                std::memory_order_acq_rel)) {
            idx = head;
            break;
        }
    }

    uint8_t* slotPtr = reinterpret_cast<uint8_t*>(r->base) +
                       sizeof(ShmHeader) +
                       static_cast<size_t>(idx % cap) * ssize;
    auto* sh = reinterpret_cast<SlotHeader*>(slotPtr);

    // 容量不变量保证此槽（上一占用者为 idx-cap）已被消费者置空，自旋仅用于等待
    // 消费者 EMPTY 写入的可见性，正常情况下不会循环。
    bool empty = false;
    for (int spin = 0; spin < 100000; ++spin) {
        if (sh->state.load(std::memory_order_acquire) == SLOT_EMPTY) { empty = true; break; }
        std::this_thread::yield();
    }

    if (!empty) {
        // 异常：消费者长时间未推进（可能卡死）。已 CAS 预留的槽不能回滚，否则
        // 消费者会永久停在该 tail。这里写入零长度 READY 占位让消费者跳过并推进，
        // 避免数据竞争/损坏，同时把本条消息记为丢弃。
        sh->len = 0;
        sh->state.store(SLOT_READY, std::memory_order_release);
        dropped_.fetch_add(1, std::memory_order_relaxed);
        wakeConsumer(h);
        return false;
    }

    sh->len = static_cast<uint32_t>(size);
    std::memcpy(slotPtr + sizeof(SlotHeader), data, size);
    sh->state.store(SLOT_READY, std::memory_order_release);
    wakeConsumer(h);
    return true;
}

void ShmTransport::wakeConsumer(ShmHeader* h) {
    // 递增唤醒序号并唤醒可能在 futex 上等待的消费者（Linux）
    h->notify.fetch_add(1, std::memory_order_release);
#if defined(EMQ_PLATFORM_LINUX)
    shmFutex(&h->notify, FUTEX_WAKE, 1, nullptr);
#endif
}

ShmTransport::Region* ShmTransport::resolvePeer(const std::string& target) {
    std::lock_guard<std::mutex> lock(peerMutex_);
    auto it = peerRegions_.find(target);
    if (it != peerRegions_.end()) {
        // 校验缓存的对端段仍然有效（对端退出后段可能被 unlink/重建）
        ShmHeader* h = it->second->header();
        if (h && h->magic == SHM_MAGIC && h->layoutVersion == SHM_LAYOUT_VERSION)
            return it->second;
        // 失效：回收映射并从缓存移除
        closeRegion(it->second);
        peerRegions_.erase(it);
    }
    Region* r = openInbox(target, /*create=*/false);
    if (r) peerRegions_[target] = r;
    return r;
}

bool ShmTransport::send(const Endpoint& to, const uint8_t* data, size_t size) {
    if (!active_) return false;
    const std::string& target = to.address;
    if (target.empty()) return false;
    Region* r = resolvePeer(target);
    if (!r) return false;
    return writeToRegion(r, data, size);
}

// 原生 scatter/gather：将多个分片直接 gather-copy 进单个槽位，
// 避免调用方先拼接成连续缓冲（保持零拷贝语义到传输边界）。
bool ShmTransport::writevToRegion(Region* r, const IoSlice* slices, size_t count) {
    if (!r || !r->base) return false;
    ShmHeader* h = r->header();
    const uint32_t cap   = h->slotCount;
    const uint32_t ssize = h->slotSize;

    size_t total = 0;
    for (size_t i = 0; i < count; ++i) total += slices[i].len;
    if (total + sizeof(SlotHeader) > ssize) return false;

    uint32_t idx;
    for (;;) {
        uint32_t head = h->head.load(std::memory_order_acquire);
        uint32_t tail = h->tail.load(std::memory_order_acquire);
        if (head - tail >= cap) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        if (h->head.compare_exchange_weak(head, head + 1, std::memory_order_acq_rel)) {
            idx = head;
            break;
        }
    }

    uint8_t* slotPtr = reinterpret_cast<uint8_t*>(r->base) + sizeof(ShmHeader) +
                       static_cast<size_t>(idx % cap) * ssize;
    auto* sh = reinterpret_cast<SlotHeader*>(slotPtr);

    bool empty = false;
    for (int spin = 0; spin < 100000; ++spin) {
        if (sh->state.load(std::memory_order_acquire) == SLOT_EMPTY) { empty = true; break; }
        std::this_thread::yield();
    }
    if (!empty) {
        sh->len = 0;
        sh->state.store(SLOT_READY, std::memory_order_release);
        dropped_.fetch_add(1, std::memory_order_relaxed);
        wakeConsumer(h);
        return false;
    }

    uint8_t* dst = slotPtr + sizeof(SlotHeader);
    size_t off = 0;
    for (size_t i = 0; i < count; ++i) {
        if (slices[i].len) {
            std::memcpy(dst + off, slices[i].data, slices[i].len);
            off += slices[i].len;
        }
    }
    sh->len = static_cast<uint32_t>(total);
    sh->state.store(SLOT_READY, std::memory_order_release);
    wakeConsumer(h);
    return true;
}

bool ShmTransport::sendv(const Endpoint& to, const IoSlice* slices, size_t count) {
    if (!active_) return false;
    const std::string& target = to.address;
    if (target.empty()) return false;
    Region* r = resolvePeer(target);
    if (!r) return false;
    return writevToRegion(r, slices, count);
}

bool ShmTransport::broadcast(const uint8_t* /*data*/, size_t /*size*/) {
    // 共享内存无广播语义（需配合发现层逐一发送）
    return false;
}

void ShmTransport::recvLoop() {
    std::vector<uint8_t> buf(slotSize_);
    Region* r = inbox_;
    ShmHeader* h = r->header();

    while (active_) {
        bool gotAny = false;
        // 单消费者：顺序读取 tail 指向的槽位
        for (int batch = 0; batch < 64 && active_; ++batch) {
            uint32_t tail = h->tail.load(std::memory_order_acquire);
            uint32_t head = h->head.load(std::memory_order_acquire);
            if (tail == head) break; // 无新数据

            // 用本地可信几何（slotCount_/slotSize_，inbox 由本进程创建并 clamp 过）
            // 而非共享头里的 h->slotCount/h->slotSize：后者位于对端可写的共享内存，
            // 被恶意/损坏对端改成 <sizeof(SlotHeader) 会让下方长度上界计算下溢，
            // 从而把越界 len 拷进 slotSize_ 大小的 buf。
            uint8_t* slotPtr = reinterpret_cast<uint8_t*>(r->base) +
                               sizeof(ShmHeader) +
                               static_cast<size_t>(tail % slotCount_) * slotSize_;
            auto* sh = reinterpret_cast<SlotHeader*>(slotPtr);
            if (sh->state.load(std::memory_order_acquire) != SLOT_READY) {
                // 生产者已预留但尚未写完，稍后再试
                break;
            }
            uint32_t len = sh->len;
            if (len > slotSize_ - sizeof(SlotHeader)) len = 0;
            if (len > 0) {
                std::memcpy(buf.data(), slotPtr + sizeof(SlotHeader), len);
            }
            sh->state.store(SLOT_EMPTY, std::memory_order_release);
            h->tail.store(tail + 1, std::memory_order_release);

            if (len > 0) {
                TransportRecvCallback cb;
                { std::lock_guard<std::mutex> lock(mutex_); cb = recvCb_; }
                if (cb) {
                    Endpoint from;
                    from.transportType = "shm";
                    from.address       = ""; // 共享内存不携带来源地址
                    cb(from, buf.data(), len);
                }
            }
            gotAny = true;
        }
        if (!gotAny) {
#if defined(EMQ_PLATFORM_LINUX)
            // 事件驱动：无数据时在 notify 上等待，带 2ms 超时兜底（避免错过唤醒/关停延迟）
            uint32_t v = h->notify.load(std::memory_order_acquire);
            if (h->tail.load(std::memory_order_acquire) ==
                h->head.load(std::memory_order_acquire)) {
                struct timespec ts{0, 2 * 1000 * 1000};
                shmFutex(&h->notify, FUTEX_WAIT, v, &ts);
            }
#else
            std::this_thread::sleep_for(std::chrono::microseconds(200));
#endif
        }
    }
}

void ShmTransport::setRecvCallback(TransportRecvCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    recvCb_ = std::move(cb);
}

void ShmTransport::setEventCallback(TransportEventCallback cb) {
    std::lock_guard<std::mutex> lock(mutex_);
    eventCb_ = std::move(cb);
}

std::vector<Endpoint> ShmTransport::localEndpoints() const {
    Endpoint ep;
    ep.transportType = "shm";
    ep.address       = name_;
    ep.port          = 0;
    return {ep};
}

} // namespace embedmq
