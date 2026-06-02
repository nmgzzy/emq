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

namespace embedmq {

// ===================== 共享内存布局 =====================
//
// [ShmHeader] [slot 0] [slot 1] ... [slot N-1]
// 每个 slot：[atomic<uint32_t> state][uint32_t len][payload bytes...]
//   state: 0=空闲, 1=就绪(可读)
//
// 生产者通过对 head 做 CAS 预留槽位；消费者按 tail 顺序读取。

struct ShmTransport::ShmHeader {
    uint32_t              magic;       // 校验段有效性
    uint32_t              slotSize;
    uint32_t              slotCount;
    uint32_t              _pad;
    std::atomic<uint32_t> head;        // 生产者预留索引
    std::atomic<uint32_t> tail;        // 消费者读取索引
};

static constexpr uint32_t SHM_MAGIC      = 0x4D485345; // "ESHM"
static constexpr uint32_t SLOT_EMPTY     = 0;
static constexpr uint32_t SLOT_READY     = 1;

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
    r->base = ::MapViewOfFile(r->mapping, FILE_MAP_ALL_ACCESS, 0, 0, r->totalSize);
    if (!r->base) { ::CloseHandle(r->mapping); delete r; return nullptr; }
    if (create && !existed) {
        std::memset(r->base, 0, r->totalSize);
        ShmHeader* h = r->header();
        h->magic = SHM_MAGIC; h->slotSize = slotSize_; h->slotCount = slotCount_;
        h->head.store(0); h->tail.store(0);
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
            h->magic = SHM_MAGIC; h->slotSize = slotSize_; h->slotCount = slotCount_;
            h->head.store(0); h->tail.store(0);
        }
    }
#endif

    // 打开（非创建）时校验段是否就绪
    if (!create) {
        ShmHeader* h = r->header();
        if (h->magic != SHM_MAGIC) { closeRegion(r); return nullptr; }
        // 以对端的实际几何为准
        if (h->slotCount == 0 || h->slotSize == 0) { closeRegion(r); return nullptr; }
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
        if (head - tail >= cap) return false; // 收件箱已满 → 丢弃
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

    // 等待该槽位被消费者清空（容量检查后通常已空，做有界防御性自旋）
    for (int spin = 0; spin < 100000; ++spin) {
        if (sh->state.load(std::memory_order_acquire) == SLOT_EMPTY) break;
        std::this_thread::yield();
    }
    sh->len = static_cast<uint32_t>(size);
    std::memcpy(slotPtr + sizeof(SlotHeader), data, size);
    sh->state.store(SLOT_READY, std::memory_order_release);
    return true;
}

bool ShmTransport::send(const Endpoint& to, const uint8_t* data, size_t size) {
    if (!active_) return false;
    const std::string& target = to.address;
    if (target.empty()) return false;

    Region* r = nullptr;
    {
        std::lock_guard<std::mutex> lock(peerMutex_);
        auto it = peerRegions_.find(target);
        if (it != peerRegions_.end()) {
            r = it->second;
        } else {
            r = openInbox(target, /*create=*/false);
            if (r) peerRegions_[target] = r;
        }
    }
    if (!r) return false;
    return writeToRegion(r, data, size);
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

            uint8_t* slotPtr = reinterpret_cast<uint8_t*>(r->base) +
                               sizeof(ShmHeader) +
                               static_cast<size_t>(tail % h->slotCount) * h->slotSize;
            auto* sh = reinterpret_cast<SlotHeader*>(slotPtr);
            if (sh->state.load(std::memory_order_acquire) != SLOT_READY) {
                // 生产者已预留但尚未写完，稍后再试
                break;
            }
            uint32_t len = sh->len;
            if (len > h->slotSize - sizeof(SlotHeader)) len = 0;
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
            std::this_thread::sleep_for(std::chrono::microseconds(200));
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
