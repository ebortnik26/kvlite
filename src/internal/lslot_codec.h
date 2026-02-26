#ifndef KVLITE_INTERNAL_LSLOT_CODEC_H
#define KVLITE_INTERNAL_LSLOT_CODEC_H

#include <cstdint>
#include <vector>

namespace kvlite {
namespace internal {

// LSlot codec: encodes and decodes logical slots for delta hash tables.
//
// Format per lslot:
//   unary(K)                              — K unique fingerprints
//   for each fingerprint:
//     [fingerprint_bits]                  — fingerprint value
//     unary(M)                            — M entries
//     [64 bits]                           — first packed_version (highest, raw)
//     (M-1) x gamma(delta_pv+1)          — packed_version deltas (desc order, zero-safe)
//     [32 bits]                           — first id (highest, raw)
//     (M-1) x gamma(delta_id+1)          — id deltas (desc order, zero-safe)
//
// The codec operates on raw uint8_t* pointers and owns no memory.
class LSlotCodec {
public:
    // A single fingerprint group with parallel packed_version and id lists.
    struct TrieEntry {
        uint64_t fingerprint;
        uint8_t fp_extra_bits = 0;  // extra bits beyond base fingerprint_bits_
        std::vector<uint64_t> packed_versions;  // sorted desc, parallel with ids
        std::vector<uint32_t> ids;              // sorted desc, parallel with packed_versions
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

    size_t bitOffset(const uint8_t* data, uint32_t target_lslot) const;

    size_t totalBits(const uint8_t* data, uint32_t num_lslots) const;

    // Skip one lslot without allocating, returning the bit position after it.
    size_t skipLSlot(const uint8_t* data, size_t bit_offset) const;

private:
    uint8_t fingerprint_bits_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_LSLOT_CODEC_H
