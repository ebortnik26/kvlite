#ifndef KVLITE_INTERNAL_LSLOT_CODEC_H
#define KVLITE_INTERNAL_LSLOT_CODEC_H

#include <cstdint>
#include <vector>

namespace kvlite {
namespace internal {

// LSlot codec: encodes and decodes logical slots within DHT buckets.
//
// Format per lslot:
//   unary(K)                       — K unique fingerprints
//   for each fingerprint:
//     [fingerprint_bits]           — fingerprint value
//     unary(M)                     — M values for this fingerprint
//     [32 bits]                    — first value (highest, stored raw)
//     (M-1) x gamma(delta)        — deltas between consecutive desc values
//
// The codec operates on raw uint8_t* pointers and owns no memory.
class LSlotCodec {
public:
    // A single fingerprint group: one fingerprint with its associated values.
    struct TrieEntry {
        uint64_t fingerprint;
        std::vector<uint32_t> values;  // sorted desc (highest/latest first)
    };

    // An lslot's decoded contents: list of fingerprint groups.
    struct LSlotContents {
        std::vector<TrieEntry> entries;  // sorted by fingerprint asc
    };

    explicit LSlotCodec(uint8_t fingerprint_bits);

    LSlotContents decode(const uint8_t* data, size_t bit_offset,
                         size_t* end_bit_offset = nullptr) const;

    size_t encode(uint8_t* data, size_t bit_offset,
                  const LSlotContents& contents) const;

    static size_t bitsNeeded(const LSlotContents& contents, uint8_t fp_bits);

    // Navigate to the Nth lslot in a bucket's data region.
    size_t bitOffset(const uint8_t* data, uint32_t target_lslot) const;

    // Total bits used by num_lslots consecutive lslots.
    size_t totalBits(const uint8_t* data, uint32_t num_lslots) const;

private:
    uint8_t fingerprint_bits_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_LSLOT_CODEC_H
