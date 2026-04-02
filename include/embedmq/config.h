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
        bool     enableLocalIpc = true;
        bool     enableShm      = false;
        bool     enableSerial   = false;
        bool     enableBle      = false;
        uint16_t udpPort        = 0;
        uint16_t tcpPort        = 0;
        std::string serialDevice = "";
        uint32_t    serialBaud   = 115200;
    } transport;

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
