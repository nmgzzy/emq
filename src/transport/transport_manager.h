#pragma once
#include "embedmq/transport/itransport.h"
#include "embedmq/config.h"
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace embedmq {

using GlobalRecvCallback = std::function<void(const Endpoint& from,
                                               const uint8_t* data, size_t size)>;

class TransportManager {
public:
    TransportManager() = default;
    ~TransportManager() { shutdownAll(); }

    void registerTransport(const std::string& name,
                           std::shared_ptr<ITransport> transport);

    /// 注册默认传输（UDP, LocalIPC 等，由平台配置决定）
    void registerDefaultTransports(const ParticipantConfig& config);

    void initAll(const ParticipantConfig& config);
    void shutdownAll();

    bool send(const Endpoint& to, const uint8_t* data, size_t size);
    bool sendv(const Endpoint& to, const IoSlice* slices, size_t count);
    bool broadcast(const uint8_t* data, size_t size);

    void setRecvCallback(GlobalRecvCallback cb);

    ITransport* get(const std::string& name) const;
    std::vector<Endpoint> allLocalEndpoints() const;

    /// 指定类型的传输是否已注册且处于活动状态（供数据面端点优选）
    bool isActive(const std::string& name) const;

private:
    std::unordered_map<std::string, std::shared_ptr<ITransport>> transports_;
    GlobalRecvCallback recvCb_;
    mutable std::mutex mutex_;
};

} // namespace embedmq
