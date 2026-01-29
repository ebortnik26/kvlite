#ifndef KVLITE_INTERNAL_L2_LSLOT_CODEC_H
#define KVLITE_INTERNAL_L2_LSLOT_CODEC_H

#include <cstdint>
#include <vector>

namespace kvlite {
namespace internal {

// L2 LSlot codec: encodes and decodes logical slots for L2 (per-file) indexes.
//
// Format per lslot:
//   unary(K)                         — K unique fingerprints
//   for each fingerprint:
//     [fingerprint_bits]             — fingerprint value
//     unary(M)                       — M entries
//     [32 bits]                      — first offset (highest, raw)
//     (M-1) x gamma(delta_offset)   — offset deltas (desc order)
//     [32 bits]                      — first version (highest, raw)
//     (M-1) x gamma(delta_version)  — version deltas (desc order)
//
// Same encoding pattern as L1 (LSlotCodec), applied to two parallel sequences
// (offsets and versions) per fingerprint group.
//
// The codec operates on raw uint8_t* pointers and owns no memory.
class L2LSlotCodec {
public:
    // A single fingerprint group with parallel offset and version lists.
    struct TrieEntry {
        uint64_t fingerprint;
        std::vector<uint32_t> offsets;   // sorted desc, parallel with versions
        std::vector<uint32_t> versions;  // sorted desc, parallel with offsets
    };

    // An lslot's decoded contents: list of fingerprint groups.
    struct LSlotContents {
        std::vector<TrieEntry> entries;  // sorted by fingerprint asc
    };

    explicit L2LSlotCodec(uint8_t fingerprint_bits);

    LSlotContents decode(const uint8_t* data, size_t bit_offset,
                         size_t* end_bit_offset = nullptr) const;

    size_t encode(uint8_t* data, size_t bit_offset,
                  const LSlotContents& contents) const;

    static size_t bitsNeeded(const LSlotContents& contents, uint8_t fp_bits);

    size_t bitOffset(const uint8_t* data, uint32_t target_lslot) const;

    size_t totalBits(const uint8_t* data, uint32_t num_lslots) const;

private:
    uint8_t fingerprint_bits_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_L2_LSLOT_CODEC_H
