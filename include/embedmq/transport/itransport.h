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

    virtual void setRecvCallback(TransportRecvCallback cb)   = 0;
    virtual void setEventCallback(TransportEventCallback cb) = 0;

    virtual std::vector<Endpoint> localEndpoints() const = 0;
    virtual bool isActive() const = 0;
};

using TransportFactory = std::function<std::shared_ptr<ITransport>()>;

} // namespace embedmq
