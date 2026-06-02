#pragma once
#include "embedmq/types.h"
#include "embedmq/qos.h"
#include "../util/crc32.h"
#include <cstring>
#include <vector>
#include <chrono>

namespace embedmq {

constexpr uint16_t EMBEDMQ_MAGIC     = 0xEBDC;
constexpr uint8_t  EMBEDMQ_VERSION   = 1;

// 单条消息的字段上限（编码前校验，避免静默截断 / 解码越界）
constexpr size_t   MAX_TOPIC_LEN     = 0xFFFF;        // topicLen 为 uint16_t
constexpr size_t   MAX_PAYLOAD_LEN   = 64u * 1024 * 1024; // 64 MiB 上限

// WireHeader 实际占用 40 字节（#pragma pack 保证无填充）
// magic(2)+version(1)+msgType(1)+qosLevel(1)+flags(1)+sourceId(2)
// +destId(2)+topicLen(2)+sequenceId(4)+correlationId(4)
// +timestamp(8)+serializerId(1)+reserved(3)+payloadLen(4)+checksum(4) = 40
constexpr size_t   HEADER_FIXED_SIZE = 40;

#pragma pack(push, 1)
struct WireHeader {
    uint16_t magic;
    uint8_t  version;
    uint8_t  msgType;
    uint8_t  qosLevel;
    uint8_t  flags;
    uint16_t sourceId;
    uint16_t destId;
    uint16_t topicLen;
    uint32_t sequenceId;
    uint32_t correlationId;
    uint64_t timestamp;
    uint8_t  serializerId;
    uint8_t  reserved[3];
    uint32_t payloadLen;
    uint32_t checksum;
};
#pragma pack(pop)

static_assert(sizeof(WireHeader) == HEADER_FIXED_SIZE, "WireHeader size mismatch");

class MessageCodec {
public:
    static std::vector<uint8_t> encode(
        MessageType type,
        uint16_t sourceId,
        uint16_t destId,
        const std::string& topic,
        const Payload& payload,
        const QoSProfile& qos,
        uint32_t sequenceId,
        uint32_t correlationId = 0,
        uint8_t  flags         = 0,
        uint8_t  serializerId  = 0)
    {
        // 超限输入直接拒绝，避免 static_cast 静默回绕导致对端解出错误数据
        if (topic.size() > MAX_TOPIC_LEN || payload.size() > MAX_PAYLOAD_LEN)
            return {};

        WireHeader header{};
        header.magic         = EMBEDMQ_MAGIC;
        header.version       = EMBEDMQ_VERSION;
        header.msgType       = static_cast<uint8_t>(type);
        header.qosLevel      = static_cast<uint8_t>(qos.level);
        header.flags         = flags;
        header.sourceId      = sourceId;
        header.destId        = destId;
        header.topicLen      = static_cast<uint16_t>(topic.size());
        header.sequenceId    = sequenceId;
        header.correlationId = correlationId;
        header.timestamp     = currentTimestampNs();
        header.serializerId  = serializerId;
        header.payloadLen    = static_cast<uint32_t>(payload.size());
        header.checksum      = 0;

        size_t totalSize = HEADER_FIXED_SIZE + topic.size() + payload.size();
        std::vector<uint8_t> buffer(totalSize);

        std::memcpy(buffer.data(), &header, HEADER_FIXED_SIZE);
        if (!topic.empty())
            std::memcpy(buffer.data() + HEADER_FIXED_SIZE, topic.data(), topic.size());
        if (payload.size() > 0)
            std::memcpy(buffer.data() + HEADER_FIXED_SIZE + topic.size(),
                        payload.data(), payload.size());

        uint32_t crc = util::crc32(buffer.data() + 4, totalSize - 4);
        reinterpret_cast<WireHeader*>(buffer.data())->checksum = crc;

        return buffer;
    }

    /// 零拷贝编码：仅生成 40 字节固定头，topic 与 payload 不再拷贝进同一缓冲区，
    /// 由调用方以 scatter/gather（sendToV）方式分片发送：{header, topic, payload}。
    /// checksum 覆盖 header(checksum 字段置零) + topic + payload，与 decode() 完全兼容。
    static std::vector<uint8_t> encodeHeader(
        MessageType type,
        uint16_t sourceId,
        uint16_t destId,
        const std::string& topic,
        const Payload& payload,
        const QoSProfile& qos,
        uint32_t sequenceId,
        uint32_t correlationId = 0,
        uint8_t  flags         = 0,
        uint8_t  serializerId  = 0)
    {
        if (topic.size() > MAX_TOPIC_LEN || payload.size() > MAX_PAYLOAD_LEN)
            return {};

        WireHeader header{};
        header.magic         = EMBEDMQ_MAGIC;
        header.version       = EMBEDMQ_VERSION;
        header.msgType       = static_cast<uint8_t>(type);
        header.qosLevel      = static_cast<uint8_t>(qos.level);
        header.flags         = flags;
        header.sourceId      = sourceId;
        header.destId        = destId;
        header.topicLen      = static_cast<uint16_t>(topic.size());
        header.sequenceId    = sequenceId;
        header.correlationId = correlationId;
        header.timestamp     = currentTimestampNs();
        header.serializerId  = serializerId;
        header.payloadLen    = static_cast<uint32_t>(payload.size());
        header.checksum      = 0;

        std::vector<uint8_t> buffer(HEADER_FIXED_SIZE);
        std::memcpy(buffer.data(), &header, HEADER_FIXED_SIZE);

        // CRC 覆盖 header[4..40)（checksum 字段已为 0）+ topic + payload
        uint32_t state = 0xFFFFFFFFu;
        state = util::crc32_update(state, buffer.data() + 4, HEADER_FIXED_SIZE - 4);
        if (!topic.empty())
            state = util::crc32_update(state,
                        reinterpret_cast<const uint8_t*>(topic.data()), topic.size());
        if (payload.size() > 0)
            state = util::crc32_update(state, payload.data(), payload.size());
        uint32_t crc = ~state;

        reinterpret_cast<WireHeader*>(buffer.data())->checksum = crc;
        return buffer;
    }

    struct DecodeResult {
        bool       valid{false};
        WireHeader header{};
        std::string topic;
        Payload    payload;
    };

    static DecodeResult decode(const uint8_t* data, size_t size) {
        DecodeResult result;
        if (size < HEADER_FIXED_SIZE) return result;

        std::memcpy(&result.header, data, HEADER_FIXED_SIZE);

        if (result.header.magic   != EMBEDMQ_MAGIC)   return result;
        if (result.header.version != EMBEDMQ_VERSION) return result;

        // 用 64 位累加避免 32 位 size_t 下 HEADER+topicLen+payloadLen 回绕，
        // 否则构造的恶意/损坏包头可绕过 size 检查导致越界读。
        uint64_t expectedSize = static_cast<uint64_t>(HEADER_FIXED_SIZE) +
                                result.header.topicLen +
                                result.header.payloadLen;
        if (static_cast<uint64_t>(size) < expectedSize) return result;

        // CRC32 验证（不修改输入缓冲区）
        uint32_t savedCrc = result.header.checksum;
        constexpr size_t CHECKSUM_OFFSET = 36; // offsetof(WireHeader, checksum)
        static const uint8_t ZERO4[4] = {0, 0, 0, 0};
        uint32_t crcState = 0xFFFFFFFFu;
        crcState = util::crc32_update(crcState, data + 4, CHECKSUM_OFFSET - 4);
        crcState = util::crc32_update(crcState, ZERO4, 4);
        crcState = util::crc32_update(crcState, data + CHECKSUM_OFFSET + 4,
                                       static_cast<size_t>(expectedSize) - CHECKSUM_OFFSET - 4);
        uint32_t calcCrc = ~crcState;

        if (calcCrc != savedCrc) return result;

        result.topic.assign(
            reinterpret_cast<const char*>(data + HEADER_FIXED_SIZE),
            result.header.topicLen);

        if (result.header.payloadLen > 0) {
            result.payload = Payload(
                data + HEADER_FIXED_SIZE + result.header.topicLen,
                result.header.payloadLen);
        }

        result.valid = true;
        return result;
    }

private:
    static uint64_t currentTimestampNs() {
        auto now = std::chrono::high_resolution_clock::now();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                now.time_since_epoch()).count());
    }
};

} // namespace embedmq
