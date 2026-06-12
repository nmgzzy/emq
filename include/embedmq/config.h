#pragma once
#include <cstdint>
#include <string>
#include "qos.h"
#include "types.h"

namespace embedmq {

struct ParticipantConfig {
    std::string nodeName = "";
    uint8_t     domainId = 0;

    struct {
        std::string multicastGroup       = "239.255.0.1";
        uint16_t    multicastPort        = 19900;
        uint32_t    announceIntervalMs   = 1000;
        uint32_t    heartbeatIntervalMs  = 2000;
        uint32_t    peerTimeoutMs        = 10000;
        bool        enableMulticast      = true;
        bool        enableLocalDiscovery = true;
    } discovery;

    struct {
        bool     enableUdp      = true;
        bool     enableTcp      = false;
        bool     enableShm      = false;
        // UDP 接收缓冲（单数据报上限）。默认 2KB 适配嵌入式；需要大数据报时调大。
        uint32_t udpRecvBufferSize = 2048;
        uint16_t udpPort        = 0;
        uint16_t tcpPort        = 0;

        // ---- 以下为预留（reserved）：当前版本未实现对应 transport，置位无效 ----
        bool     enableLocalIpc = false; // 预留：UDS / Named Pipe
        bool     enableSerial   = false; // 预留：串口
        bool     enableBle      = false; // 预留：BLE
        std::string serialDevice = "";   // 预留
        uint32_t    serialBaud   = 115200; // 预留
    } transport;

    // 线缆校验和（CRC32）。高频小包/可信链路（如本机 SHM）可关闭以省 CPU。
    bool enableChecksum = true;

    // 保留消息（retained）存储约束 —— 防止长期运行内存无界增长。
    // 保留消息在 MQTT 语义下会一直驻留直至被同主题覆盖或删除；长期运行 +
    // 大量主题时会持续累积。下列开关给出可配置的上界：默认启用 10 分钟 TTL
    // （非原始 MQTT 永驻语义），条目数上限默认不启用。
    struct {
        // 保留消息默认生存期（毫秒）。超过该时长未被刷新的保留消息将被丢弃，
        // 使保留消息内存占用有上界。默认 10 分钟：状态类消息通常有时效性，
        // 过期的"当前值"既无意义又会误导迟到的订阅者。设为 0 表示永不过期
        // （恢复原始 MQTT 语义）。单条发布的 QoSProfile.lifespanMs 若 >0 则
        // 覆盖此默认值。
        uint32_t ttlMs    = 10 * 60 * 1000; // 默认 10 min
        // 保留消息条目数上限（按主题计）。超过时驱逐最早存入的条目。
        // 0 = 不限制（默认）。
        uint32_t maxCount = 0;
    } retained;

    struct {
        uint32_t ioThreads     = 1;
        uint32_t workerThreads = 1;
        bool     pinCpu        = false;
        int      cpuAffinity   = -1;
    } threading;

    // 遗嘱消息
    struct {
        std::string topic;
        Payload     payload;
        QoSLevel    qos    = QoSLevel::Reliable;
        bool        retain = true;
        bool        enabled = false;
    } lastWill;
};

} // namespace embedmq
