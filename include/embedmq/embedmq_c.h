/**
 * EmbedMQ C ABI —— 稳定的 C 接口（Phase 5）
 *
 * 该头文件提供一套纯 C 的扁平 API，对 C++ 核心库做不透明句柄封装，
 * 目的：
 *   1. 提供跨语言 / 跨编译器稳定的 ABI（供 Python ctypes、其他 FFI 调用）；
 *   2. 屏蔽 C++ 异常 / RAII / 模板，所有错误以返回码或 NULL 暴露；
 *   3. 句柄全部不透明（opaque），调用方仅持有指针，不感知内部布局。
 *
 * 约定：
 *   - 所有 `emq_*_create*` 成功返回非 NULL 句柄，失败返回 NULL；
 *   - 所有句柄必须用对应的 `emq_*_destroy` 释放，且不可重复释放；
 *   - 返回 int 的函数：0 表示成功（EMQ_OK），负值为错误码；
 *   - 字符串参数均为以 '\0' 结尾的 UTF-8；二进制载荷用 (ptr,len) 表达；
 *   - 回调在库内部线程触发，回调内不要再调用会阻塞/销毁自身的 API。
 */
#ifndef EMBEDMQ_C_H
#define EMBEDMQ_C_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- 导出宏 ---- */
#if defined(_WIN32) || defined(_WIN64)
#  ifdef EMBEDMQ_C_EXPORTS
#    define EMQ_C_API __declspec(dllexport)
#  else
#    define EMQ_C_API __declspec(dllimport)
#  endif
#else
#  define EMQ_C_API __attribute__((visibility("default")))
#endif

/* ---- 错误码 ---- */
typedef enum emq_status {
    EMQ_OK              =  0,  /* 成功 */
    EMQ_ERR_INVALID_ARG = -1,  /* 参数非法（如 NULL 句柄） */
    EMQ_ERR_CREATE_FAIL = -2,  /* 对象创建失败 */
    EMQ_ERR_PUBLISH     = -3,  /* 发布失败 */
    EMQ_ERR_TIMEOUT     = -4,  /* 请求超时 / 无响应 */
    EMQ_ERR_EXCEPTION   = -5   /* 内部异常（已被吞掉，仅报告） */
} emq_status;

/* ---- QoS 级别（与 C++ enum 数值一致） ---- */
typedef enum emq_qos_level {
    EMQ_QOS_BEST_EFFORT  = 0,
    EMQ_QOS_RELIABLE     = 1,
    EMQ_QOS_EXACTLY_ONCE = 2
} emq_qos_level;

/* ---- 不透明句柄 ---- */
typedef struct emq_participant emq_participant;
typedef struct emq_publisher   emq_publisher;
typedef struct emq_subscriber  emq_subscriber;
typedef struct emq_requester   emq_requester;
typedef struct emq_replier     emq_replier;

/* ---- 接收消息（传给回调的只读视图，回调返回后失效） ---- */
typedef struct emq_message {
    const char*    topic;        /* '\0' 结尾主题字符串 */
    const uint8_t* payload;      /* 载荷指针（可能为 NULL，len=0 时） */
    size_t         payload_len;  /* 载荷字节数 */
    uint64_t       timestamp;    /* 发送时间戳 */
    uint16_t       source_id;    /* 源节点 id */
    uint32_t       sequence_id;  /* 序列号 */
    uint32_t       correlation_id;/* 关联号（请求-响应） */
} emq_message;

/* ---- 回调类型 ---- */

/* 订阅回调：收到匹配消息时触发。user_data 为注册时传入的透传指针。 */
typedef void (*emq_subscribe_cb)(const emq_message* msg, void* user_data);

/* 请求处理回调：被请求服务时触发，需把响应写入 *out_payload / *out_len。
 * 返回的缓冲区必须由库可安全释放——调用方应使用 emq_alloc() 分配，
 * 库收到后负责 emq_free()。若不写出（设 *out_payload=NULL），回复空载荷。 */
typedef void (*emq_request_cb)(const emq_message* req,
                               uint8_t** out_payload, size_t* out_len,
                               void* user_data);

/* 对端事件回调：connected 非 0 表示上线，0 表示下线。 */
typedef void (*emq_peer_event_cb)(uint16_t peer_id, const char* peer_name,
                                  int connected, void* user_data);

/* ===================== 全局 / 工具 ===================== */

/* 返回库版本字符串（静态存储，勿释放）。 */
EMQ_C_API const char* emq_version(void);

/* 把错误码转为可读字符串（静态存储，勿释放）。 */
EMQ_C_API const char* emq_status_str(int status);

/* 分配 / 释放与库内存分配器一致的缓冲（供 emq_request_cb 写出响应使用）。 */
EMQ_C_API uint8_t* emq_alloc(size_t size);
EMQ_C_API void     emq_free(uint8_t* ptr);

/* ===================== Participant ===================== */

/* 用节点名创建参与者（name 可为 NULL，表示自动命名）。失败返回 NULL。 */
EMQ_C_API emq_participant* emq_participant_create(const char* name);

/* 较完整的创建：可指定域、是否启用 UDP/SHM/多播。其他参数取默认值。
 * domain_id：通信域隔离；*_enable 传 0/1。 */
EMQ_C_API emq_participant* emq_participant_create_ex(const char* name,
                                                     uint8_t domain_id,
                                                     int enable_udp,
                                                     int enable_shm,
                                                     int enable_multicast);

/* 销毁参与者（会先 shutdown）。传 NULL 安全无操作。 */
EMQ_C_API void emq_participant_destroy(emq_participant* p);

/* 节点 id / 名称。名称返回内部存储指针，参与者存活期间有效。 */
EMQ_C_API uint16_t    emq_participant_id(const emq_participant* p);
EMQ_C_API const char* emq_participant_name(const emq_participant* p);

/* 是否在运行。 */
EMQ_C_API int emq_participant_is_running(const emq_participant* p);

/* 已发现对端数量。 */
EMQ_C_API int emq_participant_peer_count(const emq_participant* p);

/* 取第 index 个对端名称写入 buf（最多 buf_size-1 字节 + '\0'）。
 * 成功返回 EMQ_OK，index 越界返回 EMQ_ERR_INVALID_ARG。 */
EMQ_C_API int emq_participant_peer_name(const emq_participant* p, int index,
                                        char* buf, size_t buf_size);

/* 注册对端上下线事件回调。 */
EMQ_C_API int emq_participant_on_peer_event(emq_participant* p,
                                            emq_peer_event_cb cb,
                                            void* user_data);

/* 主动关闭（destroy 也会调用）。 */
EMQ_C_API void emq_participant_shutdown(emq_participant* p);

/* ===================== Publisher ===================== */

/* 创建发布者。qos 见 emq_qos_level。失败返回 NULL。 */
EMQ_C_API emq_publisher* emq_publisher_create(emq_participant* p,
                                              const char* topic,
                                              emq_qos_level qos);
EMQ_C_API void emq_publisher_destroy(emq_publisher* pub);

/* 发布二进制载荷。返回 EMQ_OK / 错误码。 */
EMQ_C_API int emq_publisher_publish(emq_publisher* pub,
                                    const void* data, size_t size);

/* 发布以 '\0' 结尾的文本（便捷封装）。 */
EMQ_C_API int emq_publisher_publish_str(emq_publisher* pub, const char* text);

/* 当前匹配订阅者数量（<0 表示出错）。 */
EMQ_C_API int emq_publisher_subscriber_count(const emq_publisher* pub);

/* ===================== Subscriber ===================== */

/* 创建订阅者，topic 支持通配符 * 和 #。回调在库线程触发。失败返回 NULL。 */
EMQ_C_API emq_subscriber* emq_subscriber_create(emq_participant* p,
                                                const char* topic,
                                                emq_qos_level qos,
                                                emq_subscribe_cb cb,
                                                void* user_data);
EMQ_C_API void emq_subscriber_destroy(emq_subscriber* sub);

EMQ_C_API int      emq_subscriber_pause(emq_subscriber* sub);
EMQ_C_API int      emq_subscriber_resume(emq_subscriber* sub);
/* 累计收到消息数（出错返回 0）。 */
EMQ_C_API uint64_t emq_subscriber_message_count(const emq_subscriber* sub);

/* ===================== Requester ===================== */

EMQ_C_API emq_requester* emq_requester_create(emq_participant* p,
                                              const char* service,
                                              emq_qos_level qos);
EMQ_C_API void emq_requester_destroy(emq_requester* req);

/* 同步请求。成功 (EMQ_OK) 时把响应写入新分配的 *out_payload（调用方用
 * emq_free 释放）与 *out_len。超时返回 EMQ_ERR_TIMEOUT 且不分配。 */
EMQ_C_API int emq_requester_request(emq_requester* req,
                                    const void* data, size_t size,
                                    uint32_t timeout_ms,
                                    uint8_t** out_payload, size_t* out_len);

/* ===================== Replier ===================== */

EMQ_C_API emq_replier* emq_replier_create(emq_participant* p,
                                          const char* service,
                                          emq_qos_level qos,
                                          emq_request_cb cb,
                                          void* user_data);
EMQ_C_API void emq_replier_destroy(emq_replier* rep);

/* 累计处理请求数（出错返回 0）。 */
EMQ_C_API uint64_t emq_replier_request_count(const emq_replier* rep);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* EMBEDMQ_C_H */
