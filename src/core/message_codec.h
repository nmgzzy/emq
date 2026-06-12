#pragma once
#include "embedmq/types.h"
#include "embedmq/qos.h"
#include "../util/crc32.h"
#include <cstring>
#include <vector>
#include <chrono>

namespace embedmq {

constexpr uint16_t EMBEDMQ_MAGIC   = 0xEBDC;
constexpr uint8_t  EMBEDMQ_VERSION = 2;   // 协议 v2：紧凑头 + 显式小端 + 可选字段

// 单条消息字段上限（编码前校验，避免静默截断 / 解码越界）
constexpr size_t   MAX_TOPIC_LEN   = 0xFFFF;            // topicLen 为 uint16_t
constexpr size_t   MAX_PAYLOAD_LEN = 64u * 1024 * 1024; // 64 MiB

// ---------------- 紧凑线缆头（显式小端，跨架构一致） ----------------
// 基础 26 字节 + 可选 timestamp(8) + 可选 checksum(4)，按 hdrFlags 决定是否携带。
//   0  magic        u16
//   2  version      u8
//   3  msgType      u8
//   4  hdrFlags     u8   (bit0=HAS_TS, bit1=HAS_CRC)
//   5  qosLevel     u8
//   6  msgFlags     u8
//   7  serializerId u8
//   8  sourceId     u16
//  10  destId       u16
//  12  topicLen     u16
//  14  sequenceId   u32
//  18  correlationId u32
//  22  payloadLen   u32
//  26  [timestamp u64]  (仅当 HAS_TS)
//  ..  [checksum  u32]  (仅当 HAS_CRC)
constexpr size_t HEADER_BASE_SIZE  = 26;
constexpr size_t HEADER_MAX_SIZE   = HEADER_BASE_SIZE + 8 + 4; // 38
// 兼容旧名：表示“最小头长度”，用于接收侧快速长度过滤
constexpr size_t HEADER_FIXED_SIZE = HEADER_BASE_SIZE;

namespace hdrflag {
    constexpr uint8_t HAS_TS  = 0x01;
    constexpr uint8_t HAS_CRC = 0x02;
}

// 逻辑头（内存表示，与线缆字节序无关）
struct WireHeader {
    uint16_t magic{0};
    uint8_t  version{0};
    uint8_t  msgType{0};
    uint8_t  qosLevel{0};
    uint8_t  flags{0};
    uint8_t  serializerId{0};
    uint16_t sourceId{0};
    uint16_t destId{0};
    uint16_t topicLen{0};
    uint32_t sequenceId{0};
    uint32_t correlationId{0};
    uint64_t timestamp{0};
    uint32_t payloadLen{0};
    uint32_t checksum{0};
};

class MessageCodec {
public:
    /// 编码整包到调用方提供的缓冲区。复用 buf 已有容量——在高频收发热路径上
    /// 反复传入同一个（如 thread_local）缓冲，稳态可达零堆分配，并提供确定性
    /// 分配延迟（嵌入式关键）。字段超限时清空 buf 并返回 false。
    static bool encodeInto(
        std::vector<uint8_t>& buf,
        MessageType type,
        uint16_t sourceId,
        uint16_t destId,
        const std::string& topic,
        const Payload& payload,
        const QoSProfile& qos,
        uint32_t sequenceId,
        uint32_t correlationId = 0,
        uint8_t  flags         = 0,
        uint8_t  serializerId  = 0,
        bool     withCrc       = true)
    {
        buf.clear(); // clear() 保留容量，复用缓冲的关键
        if (topic.size() > MAX_TOPIC_LEN || payload.size() > MAX_PAYLOAD_LEN)
            return false;

        const bool withTs = isDataMsg(type);
        const size_t headerSize = HEADER_BASE_SIZE + (withTs ? 8 : 0) + (withCrc ? 4 : 0);
        const size_t total = headerSize + topic.size() + payload.size();

        // 单次分配后写头 + 追加 body。用 insert 追加（而非 buf.data()+headerSize 上的
        // memcpy）让写入位置与缓冲长度由 vector 自身维护，避免“控制/空载荷包”下
        // 一过末尾指针触发 GCC -Warray-bounds 误报。
        buf.reserve(total);
        buf.resize(headerSize);
        size_t checksumPos = 0;
        writeHeader(buf.data(), type, sourceId, destId,
                    static_cast<uint16_t>(topic.size()),
                    static_cast<uint32_t>(payload.size()),
                    qos, sequenceId, correlationId, flags, serializerId,
                    withTs, withCrc, checksumPos);

        buf.insert(buf.end(), topic.begin(), topic.end());
        if (!payload.empty())
            buf.insert(buf.end(), payload.data(), payload.data() + payload.size());

        if (withCrc) {
            uint32_t crc = computeCrcContiguous(buf.data(), buf.size(), checksumPos);
            putU32(buf.data() + checksumPos, crc);
        }
        return true;
    }

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
        uint8_t  serializerId  = 0,
        bool     withCrc       = true)
    {
        std::vector<uint8_t> buf;
        encodeInto(buf, type, sourceId, destId, topic, payload, qos,
                   sequenceId, correlationId, flags, serializerId, withCrc);
        return buf; // 失败时 encodeInto 已清空 buf，返回空 vector（与旧行为一致）
    }

    /// 零拷贝编码：仅生成线缆头到调用方缓冲，topic/payload 由调用方以 scatter/gather
    /// 分片发送。checksum（若启用）覆盖 header(去除 checksum 字段) + topic + payload，
    /// 与 decode 一致。复用 buf 容量，热路径稳态零分配。失败返回 false 并清空 buf。
    static bool encodeHeaderInto(
        std::vector<uint8_t>& buf,
        MessageType type,
        uint16_t sourceId,
        uint16_t destId,
        const std::string& topic,
        const Payload& payload,
        const QoSProfile& qos,
        uint32_t sequenceId,
        uint32_t correlationId = 0,
        uint8_t  flags         = 0,
        uint8_t  serializerId  = 0,
        bool     withCrc       = true)
    {
        buf.clear();
        if (topic.size() > MAX_TOPIC_LEN || payload.size() > MAX_PAYLOAD_LEN)
            return false;

        const bool withTs = isDataMsg(type);
        const size_t headerSize = HEADER_BASE_SIZE + (withTs ? 8 : 0) + (withCrc ? 4 : 0);

        buf.resize(headerSize); // writeHeader 覆写全部 headerSize 字节，无需依赖零初始化
        size_t checksumPos = 0;
        writeHeader(buf.data(), type, sourceId, destId,
                    static_cast<uint16_t>(topic.size()),
                    static_cast<uint32_t>(payload.size()),
                    qos, sequenceId, correlationId, flags, serializerId,
                    withTs, withCrc, checksumPos);

        if (withCrc) {
            uint32_t state = 0xFFFFFFFFu;
            state = util::crc32_update(state, buf.data() + 4, checksumPos - 4);
            if (!topic.empty())
                state = util::crc32_update(state,
                            reinterpret_cast<const uint8_t*>(topic.data()), topic.size());
            if (payload.size() > 0)
                state = util::crc32_update(state, payload.data(), payload.size());
            putU32(buf.data() + checksumPos, ~state);
        }
        return true;
    }

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
        uint8_t  serializerId  = 0,
        bool     withCrc       = true)
    {
        std::vector<uint8_t> buf;
        encodeHeaderInto(buf, type, sourceId, destId, topic, payload, qos,
                         sequenceId, correlationId, flags, serializerId, withCrc);
        return buf;
    }

    struct DecodeResult {
        bool        valid{false};
        WireHeader  header{};
        std::string topic;
        Payload     payload;
    };

    static DecodeResult decode(const uint8_t* data, size_t size) {
        DecodeResult result;
        if (size < HEADER_BASE_SIZE) return result;
        if (getU16(data) != EMBEDMQ_MAGIC) return result;
        if (data[2] != EMBEDMQ_VERSION)    return result;

        WireHeader& h   = result.header;
        h.magic         = EMBEDMQ_MAGIC;
        h.version       = data[2];
        h.msgType       = data[3];
        const uint8_t hdrFlags = data[4];
        h.qosLevel      = data[5];
        h.flags         = data[6];
        h.serializerId  = data[7];
        h.sourceId      = getU16(data + 8);
        h.destId        = getU16(data + 10);
        h.topicLen      = getU16(data + 12);
        h.sequenceId    = getU32(data + 14);
        h.correlationId = getU32(data + 18);
        h.payloadLen    = getU32(data + 22);

        const bool hasTs  = (hdrFlags & hdrflag::HAS_TS)  != 0;
        const bool hasCrc = (hdrFlags & hdrflag::HAS_CRC) != 0;
        const uint64_t headerSize = HEADER_BASE_SIZE + (hasTs ? 8 : 0) + (hasCrc ? 4 : 0);
        if (static_cast<uint64_t>(size) < headerSize) return result;

        size_t off = HEADER_BASE_SIZE;
        if (hasTs) { h.timestamp = getU64(data + off); off += 8; }
        const size_t checksumPos = off;
        if (hasCrc) { h.checksum = getU32(data + off); off += 4; }

        // 64 位累加避免 32 位 size_t 回绕绕过长度检查
        const uint64_t expected = headerSize +
                                  static_cast<uint64_t>(h.topicLen) + h.payloadLen;
        if (static_cast<uint64_t>(size) < expected) return result;

        if (hasCrc) {
            uint32_t state = 0xFFFFFFFFu;
            state = util::crc32_update(state, data + 4, checksumPos - 4);
            const size_t after = checksumPos + 4;             // 跳过 checksum 字段
            const size_t end   = static_cast<size_t>(expected);
            if (end > after) state = util::crc32_update(state, data + after, end - after);
            if (~state != h.checksum) return result;
        }

        const size_t hsz = static_cast<size_t>(headerSize);
        result.topic.assign(reinterpret_cast<const char*>(data + hsz), h.topicLen);
        if (h.payloadLen > 0)
            result.payload = Payload(data + hsz + h.topicLen, h.payloadLen);

        result.valid = true;
        return result;
    }

private:
    // ---- 显式小端读写 ----
    static void putU16(uint8_t* p, uint16_t v) { p[0]=uint8_t(v); p[1]=uint8_t(v>>8); }
    static void putU32(uint8_t* p, uint32_t v) {
        p[0]=uint8_t(v); p[1]=uint8_t(v>>8); p[2]=uint8_t(v>>16); p[3]=uint8_t(v>>24);
    }
    static void putU64(uint8_t* p, uint64_t v) {
        for (int i = 0; i < 8; ++i) p[i] = uint8_t(v >> (8 * i));
    }
    static uint16_t getU16(const uint8_t* p) {
        return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
    }
    static uint32_t getU32(const uint8_t* p) {
        return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
               (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
    }
    static uint64_t getU64(const uint8_t* p) {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
        return v;
    }

    static bool isDataMsg(MessageType t) {
        return t == MessageType::PUBLISH || t == MessageType::REQUEST ||
               t == MessageType::REPLY;
    }

    // 写头并返回 checksum 字段位置（无 CRC 时等于 headerSize）
    static size_t writeHeader(uint8_t* buf, MessageType type,
                              uint16_t sourceId, uint16_t destId,
                              uint16_t topicLen, uint32_t payloadLen,
                              const QoSProfile& qos, uint32_t sequenceId,
                              uint32_t correlationId, uint8_t flags,
                              uint8_t serializerId, bool withTs, bool withCrc,
                              size_t& checksumPos)
    {
        uint8_t hdrFlags = static_cast<uint8_t>(
            (withTs ? hdrflag::HAS_TS : 0) | (withCrc ? hdrflag::HAS_CRC : 0));
        putU16(buf + 0, EMBEDMQ_MAGIC);
        buf[2] = EMBEDMQ_VERSION;
        buf[3] = static_cast<uint8_t>(type);
        buf[4] = hdrFlags;
        buf[5] = static_cast<uint8_t>(qos.level);
        buf[6] = flags;
        buf[7] = serializerId;
        putU16(buf + 8,  sourceId);
        putU16(buf + 10, destId);
        putU16(buf + 12, topicLen);
        putU32(buf + 14, sequenceId);
        putU32(buf + 18, correlationId);
        putU32(buf + 22, payloadLen);

        size_t off = HEADER_BASE_SIZE;
        if (withTs) { putU64(buf + off, currentTimestampNs()); off += 8; }
        checksumPos = off;
        if (withCrc) { putU32(buf + off, 0); off += 4; }
        return off; // headerSize
    }

    // 连续缓冲 CRC：覆盖 [4, checksumPos) + [checksumPos+4, total)
    static uint32_t computeCrcContiguous(const uint8_t* buf, size_t total,
                                         size_t checksumPos) {
        uint32_t state = 0xFFFFFFFFu;
        state = util::crc32_update(state, buf + 4, checksumPos - 4);
        const size_t after = checksumPos + 4;
        if (total > after) state = util::crc32_update(state, buf + after, total - after);
        return ~state;
    }

    static uint64_t currentTimestampNs() {
        auto now = std::chrono::high_resolution_clock::now();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                now.time_since_epoch()).count());
    }
};

} // namespace embedmq
