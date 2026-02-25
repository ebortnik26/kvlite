#ifndef KVLITE_INTERNAL_BIT_STREAM_H
#define KVLITE_INTERNAL_BIT_STREAM_H

#include <cassert>
#include <cstdint>
#include <cstring>

namespace kvlite {
namespace internal {

// Load 8 bytes from memory in big-endian order (MSB first).
// The caller must ensure at least 8 bytes are readable from p.
inline uint64_t loadBE64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap64(v);
#else
    return ((v & 0x00000000000000FFULL) << 56) |
           ((v & 0x000000000000FF00ULL) << 40) |
           ((v & 0x0000000000FF0000ULL) << 24) |
           ((v & 0x00000000FF000000ULL) <<  8) |
           ((v & 0x000000FF00000000ULL) >>  8) |
           ((v & 0x0000FF0000000000ULL) >> 24) |
           ((v & 0x00FF000000000000ULL) >> 40) |
           ((v & 0xFF00000000000000ULL) >> 56);
#endif
}

// Store 8 bytes to memory in big-endian order.
inline void storeBE64(uint8_t* p, uint64_t v) {
#if defined(__GNUC__) || defined(__clang__)
    v = __builtin_bswap64(v);
#else
    v = ((v & 0x00000000000000FFULL) << 56) |
        ((v & 0x000000000000FF00ULL) << 40) |
        ((v & 0x0000000000FF0000ULL) << 24) |
        ((v & 0x00000000FF000000ULL) <<  8) |
        ((v & 0x000000FF00000000ULL) >>  8) |
        ((v & 0x0000FF0000000000ULL) >> 24) |
        ((v & 0x00FF000000000000ULL) >> 40) |
        ((v & 0xFF00000000000000ULL) >> 56);
#endif
    std::memcpy(p, &v, 8);
}

// Read bits from a byte buffer at arbitrary bit offsets.
//
// Uses word-level (64-bit) loads for bulk operations.
// The buffer must have at least 8 bytes of readable padding beyond
// the last bit that will be accessed (bucket extension pointer area
// naturally provides this).
class BitReader {
public:
    BitReader(const uint8_t* data, size_t bit_offset)
        : data_(data), pos_(bit_offset) {}

    // Read num_bits (1..64) from the current position, MSB-first.
    uint64_t read(uint8_t num_bits) {
        assert(num_bits >= 1 && num_bits <= 64);

        size_t byte_idx = pos_ >> 3;
        uint8_t bit_off = pos_ & 7;
        pos_ += num_bits;

        // Load 8 bytes as big-endian 64-bit word.
        // Our target bits start at (63 - bit_off) in this word.
        uint64_t word = loadBE64(data_ + byte_idx);

        // Shift left to discard the leading bit_off bits, bringing
        // our MSB to position 63.
        word <<= bit_off;

        // Right-align the result to the low num_bits positions.
        word >>= (64 - num_bits);

        if (bit_off + num_bits > 64) {
            // Some bits fell off the bottom of the first word.
            // They're in the MSB of the byte at byte_idx+8.
            uint8_t overflow = bit_off + num_bits - 64;
            uint64_t extra = data_[byte_idx + 8] >> (8 - overflow);
            word |= extra;
        }

        return word;
    }

    // Read a unary-encoded value: count of 1-bits before the 0-terminator.
    // E.g., "110" encodes 2, "0" encodes 0, "10" encodes 1.
    uint64_t readUnary() {
        size_t byte_idx = pos_ >> 3;
        uint8_t bit_off = pos_ & 7;

        // Load word and shift to align our start bit to MSB.
        uint64_t word = loadBE64(data_ + byte_idx);
        word <<= bit_off;

        // Count leading 1-bits by inverting and counting leading zeros.
        uint64_t inv = ~word;
        if (inv != 0) {
            uint8_t ones = __builtin_clzll(inv);
            uint8_t avail = 64 - bit_off;
            if (ones < avail) {
                // Found the terminating 0-bit within this word.
                pos_ += ones + 1;
                return ones;
            }
        }

        // Rare: all bits in this word are 1. Fall back to byte-at-a-time.
        uint64_t count = 64 - bit_off;
        pos_ += count;
        // Continue scanning.
        while (true) {
            uint8_t b = data_[pos_ >> 3];
            // Check from MSB down within this byte.
            for (int bit = 7 - (pos_ & 7); bit >= 0; --bit) {
                pos_++;
                if (!((b >> bit) & 1)) {
                    return count;
                }
                ++count;
            }
        }
    }

    // Read an Elias-gamma-coded value (result ≥ 1, 32-bit).
    uint32_t readEliasGamma() {
        uint8_t k = static_cast<uint8_t>(readUnary());
        if (k == 0) return 1;
        return (1u << k) | static_cast<uint32_t>(read(k));
    }

    // Read an Elias-gamma-coded value (result ≥ 1, 64-bit).
    uint64_t readEliasGamma64() {
        uint8_t k = static_cast<uint8_t>(readUnary());
        if (k == 0) return 1;
        return (1ULL << k) | read(k);
    }

    size_t position() const { return pos_; }

    void seek(size_t bit_offset) { pos_ = bit_offset; }

private:
    const uint8_t* data_;
    size_t pos_;
};

// Write bits to a byte buffer at arbitrary bit offsets.
//
// Uses word-level (64-bit) read-modify-write for bulk operations.
// The buffer must have at least 8 bytes of writable padding beyond
// the last bit that will be written.
class BitWriter {
public:
    BitWriter(uint8_t* data, size_t bit_offset)
        : data_(data), pos_(bit_offset) {}

    // Write num_bits (1..64) from value, MSB-first.
    void write(uint64_t value, uint8_t num_bits) {
        assert(num_bits >= 1 && num_bits <= 64);

        size_t byte_idx = pos_ >> 3;
        uint8_t bit_off = pos_ & 7;
        pos_ += num_bits;

        // Mask value to num_bits width.
        uint64_t mask = (num_bits == 64) ? ~0ULL : ((1ULL << num_bits) - 1);
        value &= mask;

        uint8_t total = bit_off + num_bits;

        if (total <= 64) {
            // Fits within one 64-bit word.
            uint64_t word = loadBE64(data_ + byte_idx);
            uint8_t shift = 64 - total;
            uint64_t clear = ~(mask << shift);
            word = (word & clear) | (value << shift);
            storeBE64(data_ + byte_idx, word);
        } else {
            // Spans two words. Split at the 8-byte boundary.
            uint8_t first_bits = 64 - bit_off;
            uint8_t second_bits = num_bits - first_bits;

            // High part: top first_bits of value go into low bits of first word.
            uint64_t hi_val = value >> second_bits;
            uint64_t word1 = loadBE64(data_ + byte_idx);
            uint64_t clear1 = ~((1ULL << first_bits) - 1);
            word1 = (word1 & clear1) | hi_val;
            storeBE64(data_ + byte_idx, word1);

            // Low part: bottom second_bits of value go into MSB of next byte.
            uint8_t next_byte = data_[byte_idx + 8];
            uint8_t lo_val = static_cast<uint8_t>(value << (8 - second_bits));
            uint8_t clear2 = (1 << (8 - second_bits)) - 1;
            data_[byte_idx + 8] = (next_byte & clear2) | lo_val;
        }
    }

    // Write a unary-encoded value: N ones followed by a zero.
    void writeUnary(uint64_t value) {
        if (value < 64) {
            // Encode as a single write: N 1-bits then a 0-bit.
            // Binary: 111...10 in (value+1) bits.
            uint64_t encoded = ((1ULL << value) - 1) << 1;
            write(encoded, static_cast<uint8_t>(value + 1));
        } else {
            // Extremely rare fallback for very large values.
            for (uint64_t i = 0; i < value; ++i) {
                write(1, 1);
            }
            write(0, 1);
        }
    }

    // Write an Elias-gamma-coded value (n ≥ 1, 32-bit).
    void writeEliasGamma(uint32_t n) {
        assert(n >= 1);
        uint8_t k = static_cast<uint8_t>(31 - __builtin_clz(n));
        writeUnary(k);
        if (k > 0) {
            write(n & ((1u << k) - 1), k);
        }
    }

    // Write an Elias-gamma-coded value (n ≥ 1, 64-bit).
    void writeEliasGamma64(uint64_t n) {
        assert(n >= 1);
        uint8_t k = static_cast<uint8_t>(63 - __builtin_clzll(n));
        writeUnary(k);
        if (k > 0) {
            write(n & ((1ULL << k) - 1), k);
        }
    }

    size_t position() const { return pos_; }

    void seek(size_t bit_offset) { pos_ = bit_offset; }

private:
    uint8_t* data_;
    size_t pos_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_BIT_STREAM_H
