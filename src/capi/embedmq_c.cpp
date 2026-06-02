/**
 * EmbedMQ C ABI 实现（Phase 5）
 *
 * 把 C++ 核心库包装为纯 C 接口。设计要点：
 *   - 句柄结构体仅在本文件可见，对外是不透明指针；
 *   - 所有进入库的 C++ 调用都用 try/catch 兜底，绝不让异常跨越 ABI 边界；
 *   - 回调把 C++ 类型适配为 C 的 emq_message + user_data 透传模型。
 */
#include "embedmq/embedmq_c.h"
#include "embedmq/embedmq.h"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace embedmq;

/* ===================== 句柄定义 ===================== */

struct emq_participant {
    std::unique_ptr<Participant> p;
    // 透传给对端事件回调的上下文
    emq_peer_event_cb peerCb{nullptr};
    void*             peerUd{nullptr};
};

struct emq_publisher {
    std::unique_ptr<Publisher> pub;
};

struct emq_subscriber {
    std::unique_ptr<Subscriber> sub;
};

struct emq_requester {
    std::unique_ptr<Requester> req;
};

struct emq_replier {
    std::unique_ptr<Replier> rep;
};

namespace {

QoSProfile toQos(emq_qos_level level) {
    switch (level) {
        case EMQ_QOS_RELIABLE:     return QoSProfile::reliable();
        case EMQ_QOS_EXACTLY_ONCE: return QoSProfile::exactlyOnce();
        case EMQ_QOS_BEST_EFFORT:
        default:                   return QoSProfile::bestEffort();
    }
}

// 把 C++ ReceivedMessage 适配为 C 的只读视图（topic 需保证 '\0' 结尾）
void fillMessage(const ReceivedMessage& in, emq_message& out, std::string& topicBuf) {
    topicBuf            = in.topic;        // 拷贝以保证 NUL 结尾且生命周期覆盖回调
    out.topic           = topicBuf.c_str();
    out.payload         = in.payload.data();
    out.payload_len     = in.payload.size();
    out.timestamp       = in.timestamp;
    out.source_id       = in.sourceId;
    out.sequence_id     = in.sequenceId;
    out.correlation_id  = in.correlationId;
}

} // namespace

/* ===================== 全局 / 工具 ===================== */

extern "C" const char* emq_version(void) {
    return EMQ_VERSION_STRING;
}

extern "C" const char* emq_status_str(int status) {
    switch (status) {
        case EMQ_OK:              return "ok";
        case EMQ_ERR_INVALID_ARG: return "invalid argument";
        case EMQ_ERR_CREATE_FAIL: return "create failed";
        case EMQ_ERR_PUBLISH:     return "publish failed";
        case EMQ_ERR_TIMEOUT:     return "timeout";
        case EMQ_ERR_EXCEPTION:   return "internal exception";
        default:                  return "unknown";
    }
}

extern "C" uint8_t* emq_alloc(size_t size) {
    return static_cast<uint8_t*>(std::malloc(size ? size : 1));
}

extern "C" void emq_free(uint8_t* ptr) {
    std::free(ptr);
}

/* ===================== Participant ===================== */

extern "C" emq_participant* emq_participant_create(const char* name) {
    try {
        auto h = new emq_participant();
        h->p = Participant::create(name ? std::string(name) : std::string());
        if (!h->p) { delete h; return nullptr; }
        return h;
    } catch (...) {
        return nullptr;
    }
}

extern "C" emq_participant* emq_participant_create_ex(const char* name,
                                                      uint8_t domain_id,
                                                      int enable_udp,
                                                      int enable_shm,
                                                      int enable_multicast) {
    try {
        ParticipantConfig cfg;
        cfg.nodeName                  = name ? std::string(name) : std::string();
        cfg.domainId                  = domain_id;
        cfg.transport.enableUdp       = enable_udp != 0;
        cfg.transport.enableShm       = enable_shm != 0;
        cfg.discovery.enableMulticast = enable_multicast != 0;
        auto h = new emq_participant();
        h->p = Participant::create(cfg);
        if (!h->p) { delete h; return nullptr; }
        return h;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void emq_participant_destroy(emq_participant* p) {
    if (!p) return;
    try { p->p.reset(); } catch (...) {}
    delete p;
}

extern "C" uint16_t emq_participant_id(const emq_participant* p) {
    if (!p || !p->p) return 0;
    return p->p->id();
}

extern "C" const char* emq_participant_name(const emq_participant* p) {
    if (!p || !p->p) return "";
    return p->p->name().c_str();
}

extern "C" int emq_participant_is_running(const emq_participant* p) {
    if (!p || !p->p) return 0;
    return p->p->isRunning() ? 1 : 0;
}

extern "C" int emq_participant_peer_count(const emq_participant* p) {
    if (!p || !p->p) return EMQ_ERR_INVALID_ARG;
    try {
        return static_cast<int>(p->p->discoveredPeers().size());
    } catch (...) {
        return EMQ_ERR_EXCEPTION;
    }
}

extern "C" int emq_participant_peer_name(const emq_participant* p, int index,
                                         char* buf, size_t buf_size) {
    if (!p || !p->p || !buf || buf_size == 0 || index < 0)
        return EMQ_ERR_INVALID_ARG;
    try {
        auto peers = p->p->discoveredPeers();
        if (static_cast<size_t>(index) >= peers.size())
            return EMQ_ERR_INVALID_ARG;
        const std::string& n = peers[static_cast<size_t>(index)];
        size_t copyLen = n.size() < (buf_size - 1) ? n.size() : (buf_size - 1);
        std::memcpy(buf, n.data(), copyLen);
        buf[copyLen] = '\0';
        return EMQ_OK;
    } catch (...) {
        return EMQ_ERR_EXCEPTION;
    }
}

extern "C" int emq_participant_on_peer_event(emq_participant* p,
                                             emq_peer_event_cb cb,
                                             void* user_data) {
    if (!p || !p->p) return EMQ_ERR_INVALID_ARG;
    try {
        p->peerCb = cb;
        p->peerUd = user_data;
        if (!cb) {
            p->p->onPeerEvent(nullptr);
            return EMQ_OK;
        }
        emq_peer_event_cb localCb = cb;
        void* localUd = user_data;
        p->p->onPeerEvent([localCb, localUd](uint16_t id, const std::string& name,
                                             bool connected) {
            localCb(id, name.c_str(), connected ? 1 : 0, localUd);
        });
        return EMQ_OK;
    } catch (...) {
        return EMQ_ERR_EXCEPTION;
    }
}

extern "C" void emq_participant_shutdown(emq_participant* p) {
    if (!p || !p->p) return;
    try { p->p->shutdown(); } catch (...) {}
}

/* ===================== Publisher ===================== */

extern "C" emq_publisher* emq_publisher_create(emq_participant* p,
                                               const char* topic,
                                               emq_qos_level qos) {
    if (!p || !p->p || !topic) return nullptr;
    try {
        auto h = new emq_publisher();
        h->pub = p->p->createPublisher(topic, toQos(qos));
        if (!h->pub) { delete h; return nullptr; }
        return h;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void emq_publisher_destroy(emq_publisher* pub) {
    if (!pub) return;
    try { pub->pub.reset(); } catch (...) {}
    delete pub;
}

extern "C" int emq_publisher_publish(emq_publisher* pub,
                                     const void* data, size_t size) {
    if (!pub || !pub->pub) return EMQ_ERR_INVALID_ARG;
    if (size && !data)      return EMQ_ERR_INVALID_ARG;
    try {
        return pub->pub->publish(data, size) ? EMQ_OK : EMQ_ERR_PUBLISH;
    } catch (...) {
        return EMQ_ERR_EXCEPTION;
    }
}

extern "C" int emq_publisher_publish_str(emq_publisher* pub, const char* text) {
    if (!pub || !pub->pub || !text) return EMQ_ERR_INVALID_ARG;
    return emq_publisher_publish(pub, text, std::strlen(text));
}

extern "C" int emq_publisher_subscriber_count(const emq_publisher* pub) {
    if (!pub || !pub->pub) return EMQ_ERR_INVALID_ARG;
    try {
        return static_cast<int>(pub->pub->subscriberCount());
    } catch (...) {
        return EMQ_ERR_EXCEPTION;
    }
}

/* ===================== Subscriber ===================== */

extern "C" emq_subscriber* emq_subscriber_create(emq_participant* p,
                                                 const char* topic,
                                                 emq_qos_level qos,
                                                 emq_subscribe_cb cb,
                                                 void* user_data) {
    if (!p || !p->p || !topic || !cb) return nullptr;
    try {
        emq_subscribe_cb localCb = cb;
        void* localUd = user_data;
        auto h = new emq_subscriber();
        h->sub = p->p->createSubscriber(topic,
            [localCb, localUd](const ReceivedMessage& msg) {
                emq_message cm{};
                std::string topicBuf;
                fillMessage(msg, cm, topicBuf);
                localCb(&cm, localUd);
            },
            toQos(qos));
        if (!h->sub) { delete h; return nullptr; }
        return h;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void emq_subscriber_destroy(emq_subscriber* sub) {
    if (!sub) return;
    try { sub->sub.reset(); } catch (...) {}
    delete sub;
}

extern "C" int emq_subscriber_pause(emq_subscriber* sub) {
    if (!sub || !sub->sub) return EMQ_ERR_INVALID_ARG;
    try { sub->sub->pause(); return EMQ_OK; } catch (...) { return EMQ_ERR_EXCEPTION; }
}

extern "C" int emq_subscriber_resume(emq_subscriber* sub) {
    if (!sub || !sub->sub) return EMQ_ERR_INVALID_ARG;
    try { sub->sub->resume(); return EMQ_OK; } catch (...) { return EMQ_ERR_EXCEPTION; }
}

extern "C" uint64_t emq_subscriber_message_count(const emq_subscriber* sub) {
    if (!sub || !sub->sub) return 0;
    try { return sub->sub->messageCount(); } catch (...) { return 0; }
}

/* ===================== Requester ===================== */

extern "C" emq_requester* emq_requester_create(emq_participant* p,
                                               const char* service,
                                               emq_qos_level qos) {
    if (!p || !p->p || !service) return nullptr;
    try {
        auto h = new emq_requester();
        h->req = p->p->createRequester(service, toQos(qos));
        if (!h->req) { delete h; return nullptr; }
        return h;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void emq_requester_destroy(emq_requester* req) {
    if (!req) return;
    try { req->req.reset(); } catch (...) {}
    delete req;
}

extern "C" int emq_requester_request(emq_requester* req,
                                     const void* data, size_t size,
                                     uint32_t timeout_ms,
                                     uint8_t** out_payload, size_t* out_len) {
    if (!req || !req->req)   return EMQ_ERR_INVALID_ARG;
    if (size && !data)       return EMQ_ERR_INVALID_ARG;
    if (out_payload) *out_payload = nullptr;
    if (out_len)     *out_len = 0;
    try {
        auto result = req->req->request(Payload(data, size),
                                        std::chrono::milliseconds(timeout_ms));
        if (!result.has_value()) return EMQ_ERR_TIMEOUT;
        const Payload& rp = *result;
        size_t n = rp.size();
        if (out_len) *out_len = n;
        if (out_payload) {
            uint8_t* buf = emq_alloc(n);
            if (!buf) return EMQ_ERR_EXCEPTION;
            if (n) std::memcpy(buf, rp.data(), n);
            *out_payload = buf;
        }
        return EMQ_OK;
    } catch (...) {
        return EMQ_ERR_EXCEPTION;
    }
}

/* ===================== Replier ===================== */

extern "C" emq_replier* emq_replier_create(emq_participant* p,
                                           const char* service,
                                           emq_qos_level qos,
                                           emq_request_cb cb,
                                           void* user_data) {
    if (!p || !p->p || !service || !cb) return nullptr;
    try {
        emq_request_cb localCb = cb;
        void* localUd = user_data;
        auto h = new emq_replier();
        h->rep = p->p->createReplier(service,
            [localCb, localUd](const ReceivedMessage& reqMsg) -> Payload {
                emq_message cm{};
                std::string topicBuf;
                fillMessage(reqMsg, cm, topicBuf);
                uint8_t* outBuf = nullptr;
                size_t   outLen = 0;
                localCb(&cm, &outBuf, &outLen, localUd);
                Payload resp;
                if (outBuf && outLen) {
                    resp = Payload(outBuf, outLen);
                }
                // 回调通过 emq_alloc 分配，库这里负责释放
                if (outBuf) emq_free(outBuf);
                return resp;
            },
            toQos(qos));
        if (!h->rep) { delete h; return nullptr; }
        return h;
    } catch (...) {
        return nullptr;
    }
}

extern "C" void emq_replier_destroy(emq_replier* rep) {
    if (!rep) return;
    try { rep->rep.reset(); } catch (...) {}
    delete rep;
}

extern "C" uint64_t emq_replier_request_count(const emq_replier* rep) {
    if (!rep || !rep->rep) return 0;
    try { return rep->rep->requestCount(); } catch (...) { return 0; }
}
