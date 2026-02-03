#ifndef KVLITE_INTERNAL_CRC32_H
#define KVLITE_INTERNAL_CRC32_H

#include <cstddef>
#include <cstdint>

namespace kvlite {
namespace internal {

namespace detail {

inline const uint32_t* crc32Table() {
    static uint32_t table[256] = {};
    static bool initialized = false;
    if (!initialized) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) {
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
        initialized = true;
    }
    return table;
}

} // namespace detail

inline uint32_t crc32(const void* data, size_t len) {
    const uint32_t* table = detail::crc32Table();
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_CRC32_H
