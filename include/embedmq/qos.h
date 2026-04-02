#pragma once
#include <cstdint>

namespace embedmq {

enum class QoSLevel : uint8_t {
    BestEffort  = 0,
    Reliable    = 1,
    ExactlyOnce = 2,
};

enum class HistoryKind : uint8_t {
    KeepLast = 0,
    KeepAll  = 1,
};

enum class DurabilityKind : uint8_t {
    Volatile       = 0,
    TransientLocal = 1,
};

struct QoSProfile {
    QoSLevel       level           = QoSLevel::BestEffort;
    uint32_t       maxRetries      = 3;
    uint32_t       retryIntervalMs = 100;
    uint32_t       ackTimeoutMs    = 500;
    HistoryKind    history         = HistoryKind::KeepLast;
    uint32_t       historyDepth    = 1;
    DurabilityKind durability      = DurabilityKind::Volatile;
    uint32_t       lifespanMs      = 0;
    uint32_t       maxRateHz       = 0;
    uint32_t       sendQueueSize   = 1024;
    uint32_t       recvQueueSize   = 1024;
    uint8_t        priority        = 128;
    bool           retain          = false;

    static QoSProfile bestEffort() {
        return QoSProfile{};
    }

    static QoSProfile reliable() {
        QoSProfile q;
        q.level = QoSLevel::Reliable;
        return q;
    }

    static QoSProfile exactlyOnce() {
        QoSProfile q;
        q.level = QoSLevel::ExactlyOnce;
        return q;
    }

    static QoSProfile sensorData() {
        QoSProfile q;
        q.level         = QoSLevel::BestEffort;
        q.maxRateHz     = 100;
        q.historyDepth  = 1;
        return q;
    }

    static QoSProfile controlCommand() {
        QoSProfile q;
        q.level           = QoSLevel::Reliable;
        q.ackTimeoutMs    = 200;
        q.retryIntervalMs = 50;
        q.maxRetries      = 5;
        return q;
    }
};

} // namespace embedmq
