#pragma once

#include "platform.h"
#include "types.h"
#include "qos.h"
#include "config.h"
#include "transport/itransport.h"

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace embedmq {

// ===================== 前置声明 =====================
class Participant;
class Publisher;
class Subscriber;
class Requester;
class Replier;
class MessageBus; // 内部类，前置声明供 friend 使用

// ===================== 回调类型 =====================

using SubscribeCallback  = std::function<void(const ReceivedMessage& msg)>;
using RequestHandler     = std::function<Payload(const ReceivedMessage& req)>;
using PeerEventCallback  = std::function<void(uint16_t peerId,
                                               const std::string& peerName,
                                               bool connected)>;


// ===================== Publisher =====================

class EMQ_API Publisher {
public:
    ~Publisher();

    bool publish(const void* data, size_t size);
    bool publish(const Payload& payload);
    bool publish(std::string_view text);

    size_t             subscriberCount() const;
    const std::string& topic()           const;

private:
    friend class Participant;
    friend class MessageBus;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit Publisher(std::unique_ptr<Impl> impl);
};


// ===================== Subscriber =====================

class EMQ_API Subscriber {
public:
    ~Subscriber();

    const std::string& topic()        const;
    void               pause();
    void               resume();
    uint64_t           messageCount() const;

private:
    friend class Participant;
    friend class MessageBus;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit Subscriber(std::unique_ptr<Impl> impl);
};


// ===================== Requester =====================

class EMQ_API Requester {
public:
    ~Requester();

    std::optional<Payload> request(
        const Payload& payload,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

    std::future<Payload>   requestAsync(const Payload& payload);
    const std::string&     service() const;

private:
    friend class Participant;
    friend class MessageBus;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit Requester(std::unique_ptr<Impl> impl);
};


// ===================== Replier =====================

class EMQ_API Replier {
public:
    ~Replier();

    const std::string& service()      const;
    uint64_t           requestCount() const;

private:
    friend class Participant;
    friend class MessageBus;
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit Replier(std::unique_ptr<Impl> impl);
};


// ===================== ISerializer =====================

class ISerializer {
public:
    virtual ~ISerializer()                                    = default;
    virtual uint8_t     id()   const                          = 0;
    virtual std::string name() const                          = 0;
    virtual Payload     serialize(const void* obj)     const  = 0;
    virtual bool        deserialize(const Payload& data,
                                    void* obj)         const  = 0;
};


// ===================== Participant =====================

class EMQ_API Participant {
public:
    static std::unique_ptr<Participant> create(
        const std::string& name = "");

    static std::unique_ptr<Participant> create(
        const ParticipantConfig& config);

    ~Participant();

    // ---- 发布订阅 ----
    std::unique_ptr<Publisher> createPublisher(
        const std::string& topic,
        const QoSProfile& qos = QoSProfile::bestEffort());

    std::unique_ptr<Subscriber> createSubscriber(
        const std::string& topic,
        SubscribeCallback callback,
        const QoSProfile& qos = QoSProfile::bestEffort());

    // ---- 请求响应 ----
    std::unique_ptr<Requester> createRequester(
        const std::string& service,
        const QoSProfile& qos = QoSProfile::reliable());

    std::unique_ptr<Replier> createReplier(
        const std::string& service,
        RequestHandler handler,
        const QoSProfile& qos = QoSProfile::reliable());

    // ---- 序列化器注册 ----
    void registerSerializer(std::shared_ptr<ISerializer> serializer);

    // ---- 传输插件注册 ----
    void registerTransport(const std::string& name,
                           std::shared_ptr<ITransport> transport);

    // ---- 事件回调 ----
    void onPeerEvent(PeerEventCallback callback);

    // ---- 状态查询 ----
    uint16_t                  id()              const;
    const std::string&        name()            const;
    std::vector<std::string>  discoveredPeers() const;
    bool                      isRunning()       const;

    // ---- 生命周期 ----
    void shutdown();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit Participant(std::unique_ptr<Impl> impl);
};

} // namespace embedmq
