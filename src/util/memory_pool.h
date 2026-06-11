#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <vector>
#include <memory>

namespace embedmq {
namespace util {

/// 固定块大小的内存池（线程安全，空闲链表实现）
///
/// 用途：高频消息收发路径上避免反复 malloc/free 带来的开销与碎片，并提供
/// 确定性的分配延迟（嵌入式场景尤为重要）。
///
/// 实现：每个块前置一个 16 字节的隐藏头（保存来源标记），返回给用户的指针为
/// 头之后的地址，保持 16 字节对齐。归还时通过头部标记 O(1) 区分池内存与
/// 超大回退分配，无需哈希集合查找。当请求大小超过 blockSize 时回退 operator new；
/// 当池耗尽且允许增长时再分配一个 chunk，否则返回 nullptr。
class FixedBlockPool {
public:
    explicit FixedBlockPool(size_t blockSize,
                            size_t initialBlocks = 64,
                            bool   allowGrow     = true)
        : userBlockSize_(blockSize < sizeof(void*) ? sizeof(void*) : blockSize)
        , slotSize_(HEADER + (blockSize < sizeof(void*) ? sizeof(void*) : blockSize))
        , blocksPerChunk_(initialBlocks == 0 ? 1 : initialBlocks)
        , allowGrow_(allowGrow)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        allocateChunkLocked(blocksPerChunk_);
    }

    ~FixedBlockPool() = default;

    FixedBlockPool(const FixedBlockPool&)            = delete;
    FixedBlockPool& operator=(const FixedBlockPool&) = delete;

    /// 申请一个块。size 超过 blockSize 时回退到 operator new。
    void* allocate(size_t size = 0) {
        if (size > userBlockSize_) {
            uint8_t* raw = static_cast<uint8_t*>(::operator new(HEADER + size));
            writeTag(raw, TAG_OVERSIZED);
            return raw + HEADER;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (!freeList_) {
            if (!allowGrow_) return nullptr;
            allocateChunkLocked(blocksPerChunk_);
        }
        Node* n   = freeList_;
        freeList_ = freeList_->next;
        ++inUse_;
        uint8_t* raw = reinterpret_cast<uint8_t*>(n);
        writeTag(raw, TAG_POOL);
        return raw + HEADER;
    }

    /// 归还一个块。
    void deallocate(void* p) {
        if (!p) return;
        uint8_t* raw = static_cast<uint8_t*>(p) - HEADER;
        if (readTag(raw) == TAG_OVERSIZED) {
            ::operator delete(raw);
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        Node* n   = reinterpret_cast<Node*>(raw);
        n->next   = freeList_;
        freeList_ = n;
        --inUse_;
    }

    size_t blockSize() const { return userBlockSize_; }
    size_t inUse()     const { std::lock_guard<std::mutex> l(mutex_); return inUse_; }
    size_t capacity()  const { std::lock_guard<std::mutex> l(mutex_); return totalBlocks_; }

private:
    struct Node { Node* next; };

    static constexpr size_t   HEADER       = 16; // 隐藏头，保持 16 字节对齐
    static constexpr uint64_t TAG_POOL     = 0;
    static constexpr uint64_t TAG_OVERSIZED= 0xEB0CEB0CEB0CEB0Cull;

    // 用 memcpy 而非 reinterpret_cast<uint64_t*> 读写：后者违反严格别名
    // （-O2 -fstrict-aliasing 下 UB，嵌入式工具链尤甚），且不依赖对齐。
    static void writeTag(uint8_t* raw, uint64_t tag) {
        std::memcpy(raw, &tag, sizeof(tag));
    }
    static uint64_t readTag(const uint8_t* raw) {
        uint64_t tag;
        std::memcpy(&tag, raw, sizeof(tag));
        return tag;
    }

    void allocateChunkLocked(size_t blocks) {
        auto chunk = std::make_unique<uint8_t[]>(slotSize_ * blocks);
        uint8_t* base = chunk.get();
        for (size_t i = 0; i < blocks; ++i) {
            Node* n   = reinterpret_cast<Node*>(base + i * slotSize_);
            n->next   = freeList_;
            freeList_ = n;
        }
        totalBlocks_ += blocks;
        chunks_.push_back(std::move(chunk));
    }

    size_t              userBlockSize_;
    size_t              slotSize_;
    size_t              blocksPerChunk_;
    bool                allowGrow_;
    Node*               freeList_{nullptr};
    size_t              totalBlocks_{0};
    size_t              inUse_{0};
    mutable std::mutex  mutex_;
    std::vector<std::unique_ptr<uint8_t[]>> chunks_;
};

} // namespace util
} // namespace embedmq
