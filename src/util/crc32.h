#pragma once
#include <cstdint>
#include <cstddef>

namespace embedmq {
namespace util {

inline uint32_t crc32_update(uint32_t crc, const uint8_t* data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        crc ^= static_cast<uint32_t>(data[i]);
        for (int j = 0; j < 8; j++) {
            uint32_t mask = static_cast<uint32_t>(-static_cast<int32_t>(crc & 1u));
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return crc;
}

inline uint32_t crc32(const uint8_t* data, size_t len) {
    return ~crc32_update(0xFFFFFFFFu, data, len);
}

} // namespace util
} // namespace embedmq
