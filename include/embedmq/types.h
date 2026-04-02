#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace embedmq {

// ===================== Payload =====================

/// 消息载荷 —— 引用计数设计，避免不必要的拷贝
class Payload {
public:
    Payload() = default;

    /// 从二进制数据构造（拷贝）
    Payload(const void* data, size_t size) {
        if (data && size > 0) {
            auto v = std::make_shared<std::vector<uint8_t>>(
                static_cast<const uint8_t*>(data),
                static_cast<const uint8_t*>(data) + size);
            data_ = std::move(v);
        }
    }

    /// 从字符串构造
    explicit Payload(std::string_view text)
        : Payload(text.data(), text.size()) {}

    /// 从移动语义构造（零拷贝）
    explicit Payload(std::vector<uint8_t>&& data)
        : data_(std::make_shared<std::vector<uint8_t>>(std::move(data))) {}

    const uint8_t* data() const {
        return data_ ? data_->data() : nullptr;
    }

    size_t size() const {
        return data_ ? data_->size() : 0;
    }

    std::string_view asText() const {
        if (!data_) return {};
        return {reinterpret_cast<const char*>(data_->data()), data_->size()};
    }

    // 返回原始数据指针和大小（C++17 兼容）
    const std::vector<uint8_t>* asBinaryVec() const {
        return data_.get();
    }

    bool empty() const { return !data_ || data_->empty(); }

private:
    std::shared_ptr<std::vector<uint8_t>> data_;
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
