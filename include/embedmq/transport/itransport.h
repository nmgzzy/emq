#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace embedmq {

struct Endpoint {
    std::string address;
    uint16_t    port = 0;
    std::string transportType;

    std::string toString() const {
        return transportType + "://" + address +
               (port ? ":" + std::to_string(port) : "");
    }

    bool operator==(const Endpoint& o) const {
        return address == o.address && port == o.port &&
               transportType == o.transportType;
    }
};

enum class TransportEvent {
    Connected,
    Disconnected,
    Error,
};

using TransportRecvCallback = std::function<void(
    const Endpoint& from, const uint8_t* data, size_t size)>;

using TransportEventCallback = std::function<void(
    const Endpoint& peer, TransportEvent event, const std::string& detail)>;

/// 零拷贝 scatter/gather 数据片（不拥有内存所有权）
struct IoSlice {
    const void* data{nullptr};
    size_t      len{0};
};

struct TransportCapability {
    bool     supportsMulticast         = false;
    bool     supportsBroadcast         = false;
    bool     supportsReliable          = false;
    bool     supportsStreaming         = false;
    uint32_t maxPayloadSize            = 65535;
    uint32_t estimatedLatencyUs        = 100;
    uint32_t estimatedBandwidthKbps    = 100000;
};

class ITransport {
public:
    virtual ~ITransport() = default;

    virtual std::string          typeName()    const = 0;
    virtual TransportCapability  capability()  const = 0;

    virtual bool init(const std::string& config) = 0;
    virtual void shutdown() = 0;

    virtual bool send(const Endpoint& to, const uint8_t* data, size_t size) = 0;
    virtual bool broadcast(const uint8_t* data, size_t size) = 0;

    /// scatter/gather 发送。
    /// 约定：内置传输（UDP=sendmsg/WSASendTo、TCP=分片写、SHM=gather-copy 入槽）
    /// 均原生重写本方法以保持零拷贝/少拷贝语义。
    /// 此处默认实现仅为“正确性兜底”——拼接为连续缓冲后调用 send()，会发生一次额外拷贝，
    /// 仅供未实现原生 sendv 的自定义传输使用。
    virtual bool sendv(const Endpoint& to, const IoSlice* slices, size_t count) {
        size_t total = 0;
        for (size_t i = 0; i < count; ++i) total += slices[i].len;
        std::vector<uint8_t> buf;
        buf.reserve(total);
        for (size_t i = 0; i < count; ++i) {
            const uint8_t* p = static_cast<const uint8_t*>(slices[i].data);
            buf.insert(buf.end(), p, p + slices[i].len);
        }
        return send(to, buf.data(), buf.size());
    }

    virtual void setRecvCallback(TransportRecvCallback cb)   = 0;
    virtual void setEventCallback(TransportEventCallback cb) = 0;

    virtual std::vector<Endpoint> localEndpoints() const = 0;
    virtual bool isActive() const = 0;
};

using TransportFactory = std::function<std::shared_ptr<ITransport>()>;

} // namespace embedmq
