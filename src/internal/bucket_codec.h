#ifndef KVLITE_INTERNAL_BUCKET_CODEC_H
#define KVLITE_INTERNAL_BUCKET_CODEC_H

#include <cstdint>
#include <cstring>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/bucket_arena.h"

namespace kvlite {
namespace internal {

// Stateless codec for the binary bucket format used by DeltaHashTable.
//
// Initialized with suffix_bits and bucket_bytes; owns encode/decode/bit-budget
// logic. All methods are const or static — no mutable state.
//
// Bucket format (sorted suffix array):
//   Header: N_k (uint16_t) — number of unique keys
//   Suffix array: N_k suffixes, sorted ascending
//     First suffix: raw suffix_bits bits
//     Remaining: delta-encoded (Elias gamma of delta+1)
//   Per-key entries (N_k of them):
//     N_v (Elias gamma of count)
//     packed_versions[N_v]: first raw 64-bit, rest delta-encoded (desc, gamma of delta+1)
//     ids[N_v]: first raw 32-bit, rest zigzag-varint delta-encoded (gamma of zigzag(delta)+1)
class BucketCodec {
public:
    // Per-key entry within a decoded bucket.
    struct KeyEntry {
        uint64_t suffix;
        std::vector<uint64_t> packed_versions;  // sorted desc
        std::vector<uint32_t> ids;              // parallel with packed_versions
    };

    // Decoded contents of an entire bucket.
    struct BucketContents {
        std::vector<KeyEntry> keys;  // sorted by suffix ascending
    };

    // Decoded suffix array only (for targeted scans).
    struct SuffixScanResult {
        std::vector<uint64_t> suffixes;
        size_t versions_start_bit;
    };

    BucketCodec(uint8_t suffix_bits, uint32_t bucket_bytes);

    // --- Instance methods (need suffix_bits_ / bucket_bytes_) ---

    BucketContents decodeBucket(const Bucket& bucket) const;
    size_t encodeBucket(Bucket& bucket, const BucketContents& contents) const;
    SuffixScanResult decodeSuffixes(const Bucket& bucket) const;
    size_t decodeBucketUsedBits(const Bucket& bucket) const;
    size_t bucketDataBits() const;

    // --- Static methods (pure computation) ---

    static void skipKeyData(BitReader& reader);
    static KeyEntry decodeKeyData(BitReader& reader, uint64_t suffix);

    static size_t bitsForAppendVersion(uint64_t prev_pv, uint64_t new_pv,
                                       uint32_t prev_id, uint32_t new_id);

    static size_t bitsForNewEntry(uint64_t suffix, uint64_t prev_suffix,
                                  uint64_t next_suffix, bool has_prev,
                                  bool has_next, uint8_t suffix_bits,
                                  uint64_t packed_version, uint32_t id);

    static size_t contentsBitsNeeded(const BucketContents& contents,
                                     uint8_t suffix_bits);

    uint8_t suffixBits() const { return suffix_bits_; }

private:
    uint8_t suffix_bits_;
    uint32_t bucket_bytes_;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_BUCKET_CODEC_H
