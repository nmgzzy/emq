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
