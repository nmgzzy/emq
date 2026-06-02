#pragma once
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace embedmq {

// ===================== Payload =====================

/// 消息载荷 —— 小缓冲优化（SBO）+ 引用计数：
/// - 载荷 <= kInlineCap 字节时内联存储，零堆分配、零引用计数（嵌入式高频小包友好）；
/// - 载荷较大时退化为 `shared_ptr<vector>`，拷贝共享底层缓冲，避免大块复制。
/// Payload 语义上不可变（无修改型接口），因此“内联值语义”与“大块共享语义”对外一致。
class Payload {
public:
    static constexpr size_t kInlineCap = 32;

    Payload() = default;

    /// 从二进制数据构造（拷贝）
    Payload(const void* data, size_t size) {
        assign(static_cast<const uint8_t*>(data), size);
    }

    /// 从字符串构造
    explicit Payload(std::string_view text)
        : Payload(text.data(), text.size()) {}

    /// 从移动语义构造（大块零拷贝；小块内联）
    explicit Payload(std::vector<uint8_t>&& data) {
        if (data.size() <= kInlineCap) {
            assign(data.data(), data.size());
        } else {
            big_ = std::make_shared<std::vector<uint8_t>>(std::move(data));
        }
    }

    const uint8_t* data() const {
        if (big_) return big_->data();
        return inlineSize_ ? inline_ : nullptr;
    }

    size_t size() const {
        return big_ ? big_->size() : inlineSize_;
    }

    std::string_view asText() const {
        const uint8_t* p = data();
        return p ? std::string_view(reinterpret_cast<const char*>(p), size())
                 : std::string_view{};
    }

    /// 以 `std::vector<uint8_t>` 形式返回载荷副本（内联/大块两种形态均可用）。
    /// 替代已移除的 `asBinaryVec()`：后者在 SBO 下对小载荷返回 nullptr，会静默丢数据。
    /// 零拷贝读取请直接用 `data()`/`size()`/`asText()`。
    std::vector<uint8_t> toVector() const {
        const uint8_t* p = data();
        size_t n = size();
        return (p && n) ? std::vector<uint8_t>(p, p + n) : std::vector<uint8_t>{};
    }

    bool empty() const { return size() == 0; }

private:
    void assign(const uint8_t* d, size_t n) {
        big_.reset();
        inlineSize_ = 0;
        if (!d || n == 0) return;
        if (n <= kInlineCap) {
            std::memcpy(inline_, d, n);
            inlineSize_ = static_cast<uint8_t>(n);
        } else {
            big_ = std::make_shared<std::vector<uint8_t>>(d, d + n);
        }
    }

    // 默认零初始化：使隐式拷贝/移动不会读取未初始化字节（避免 UB 与编译告警）
    uint8_t                               inline_[kInlineCap] = {};
    uint8_t                               inlineSize_{0}; // big_==nullptr 时有效
    std::shared_ptr<std::vector<uint8_t>> big_;
};


// ===================== 接收到的消息 =====================

struct ReceivedMessage {
    std::string topic;
    Payload     payload;
    uint64_t    timestamp{0};
    uint16_t    sourceId{0};
    uint32_t    sequenceId{0};
    uint32_t    correlationId{0};
};


// ===================== 消息类型枚举 =====================

enum class MessageType : uint8_t {
    PUBLISH       = 0x01,
    SUBSCRIBE     = 0x02,
    UNSUBSCRIBE   = 0x03,
    REQUEST       = 0x04,
    REPLY         = 0x05,

    ACK           = 0x10,
    NACK          = 0x11,
    PUBREC        = 0x12,
    PUBREL        = 0x13,
    PUBCOMP       = 0x14,

    ANNOUNCE      = 0x20,
    DISCOVER_REQ  = 0x21,
    DISCOVER_RSP  = 0x22,
    HEARTBEAT     = 0x23,
    FAREWELL      = 0x24,

    PING          = 0x30,
    PONG          = 0x31,
};


// ===================== Flags 位定义 =====================

struct MsgFlags {
    static constexpr uint8_t RETAIN     = 0x01;
    static constexpr uint8_t WILL       = 0x02;
    static constexpr uint8_t COMPRESSED = 0x04;
    static constexpr uint8_t ENCRYPTED  = 0x08;
    static constexpr uint8_t FRAGMENT   = 0x10;
    static constexpr uint8_t LAST_FRAG  = 0x20;
};

} // namespace embedmq
