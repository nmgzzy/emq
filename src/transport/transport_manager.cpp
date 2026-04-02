#include "transport_manager.h"
#include "udp_transport.h"
#ifdef EMBEDMQ_ENABLE_TCP
#include "tcp_transport.h"
#endif
#include "../util/logger.h"

namespace embedmq {

void TransportManager::registerTransport(const std::string& name,
                                          std::shared_ptr<ITransport> transport)
{
    std::lock_guard<std::mutex> lock(mutex_);
    transports_[name] = std::move(transport);
}

void TransportManager::registerDefaultTransports(const ParticipantConfig& config) {
    if (config.transport.enableUdp) {
        registerTransport("udp", std::make_shared<UdpTransport>());
    }
#ifdef EMBEDMQ_ENABLE_TCP
    if (config.transport.enableTcp) {
        registerTransport("tcp", std::make_shared<TcpTransport>());
    }
#endif
}

void TransportManager::initAll(const ParticipantConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [name, transport] : transports_) {
        std::string cfg = "{\"port\":" + std::to_string(config.transport.udpPort) + ","
                        + "\"multicast_group\":\"" + config.discovery.multicastGroup + "\","
                        + "\"multicast_port\":" + std::to_string(config.discovery.multicastPort) + "}";

        if (!transport->init(cfg)) {
            EMQ_LOG_E("TransportMgr", "Failed to init transport: %s", name.c_str());
        } else {
            EMQ_LOG_I("TransportMgr", "Transport initialized: %s", name.c_str());
            if (recvCb_) {
                transport->setRecvCallback([this](const Endpoint& from,
                                                   const uint8_t* d, size_t s) {
                    if (recvCb_) recvCb_(from, d, s);
                });
            }
        }
    }
}

void TransportManager::shutdownAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [name, transport] : transports_) {
        transport->shutdown();
    }
    transports_.clear();
}

bool TransportManager::send(const Endpoint& to, const uint8_t* data, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = transports_.find(to.transportType);
    if (it != transports_.end() && it->second->isActive()) {
        return it->second->send(to, data, size);
    }
    // fallback: UDP
    auto udp = transports_.find("udp");
    if (udp != transports_.end() && udp->second->isActive()) {
        return udp->second->send(to, data, size);
    }
    return false;
}

bool TransportManager::broadcast(const uint8_t* data, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    bool ok = false;
    for (auto& [name, transport] : transports_) {
        if (transport->isActive()) {
            ok |= transport->broadcast(data, size);
        }
    }
    return ok;
}

void TransportManager::setRecvCallback(GlobalRecvCallback cb) {
    recvCb_ = std::move(cb);
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [name, transport] : transports_) {
        if (transport->isActive()) {
            transport->setRecvCallback([this](const Endpoint& from,
                                              const uint8_t* d, size_t s) {
                if (recvCb_) recvCb_(from, d, s);
            });
        }
    }
}

ITransport* TransportManager::get(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = transports_.find(name);
    return (it != transports_.end()) ? it->second.get() : nullptr;
}

std::vector<Endpoint> TransportManager::allLocalEndpoints() const {
    std::vector<Endpoint> result;
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [name, transport] : transports_) {
        auto eps = transport->localEndpoints();
        result.insert(result.end(), eps.begin(), eps.end());
    }
    return result;
}

} // namespace embedmq
