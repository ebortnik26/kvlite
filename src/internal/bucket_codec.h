#ifndef KVLITE_INTERNAL_BUCKET_CODEC_H
#define KVLITE_INTERNAL_BUCKET_CODEC_H

#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/bucket_arena.h"

namespace kvlite {
namespace internal {

// Layout tag types for BucketCodec template.
struct RowLayout {};
struct ColumnarLayout {};

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
    size_t data_start_bit;
};

// Codec for binary bucket formats used by DeltaHashTable.
//
// Parameterized on Layout:
//   ColumnarLayout — columnar format with cross-key delta encoding.
//     Reordered layout: Suffixes | Counts | First-PVs | First-IDs | Tail-PVs | Tail-IDs
//   RowLayout — row-oriented format (per-key: count, versions, ids).
//
// Initialized with suffix_bits and bucket_bytes; owns encode/decode/bit-budget
// logic. All methods are const — no mutable state.
template<typename Layout>
class BucketCodec {
public:
    BucketCodec(uint8_t suffix_bits, uint32_t bucket_bytes);

    // --- Full bucket operations ---

    BucketContents decodeBucket(const Bucket& bucket) const;
    size_t encodeBucket(Bucket& bucket, const BucketContents& contents) const;
    SuffixScanResult decodeSuffixes(const Bucket& bucket) const;
    size_t decodeBucketUsedBits(const Bucket& bucket) const;
    size_t bucketDataBits() const;

    // --- Random-access decode ---

    KeyEntry decodeKeyAt(const Bucket& bucket, uint16_t key_index,
                         uint64_t suffix, size_t data_start_bit) const;

    // Decode only the first (pv, id) pair for the key at key_index.
    // For ColumnarLayout, this avoids scanning tail data entirely.
    std::pair<uint64_t, uint32_t> decodeFirstEntry(
        const Bucket& bucket, uint16_t key_index,
        size_t data_start_bit) const;

    // --- Incremental bit-budget methods ---

    size_t bitsForAddVersion(const Bucket& bucket, uint16_t key_index,
                             uint64_t new_pv, uint32_t new_id,
                             size_t insert_pos) const;

    size_t bitsForNewEntry(const Bucket& bucket, uint16_t insert_pos,
                           uint64_t suffix, uint64_t packed_version,
                           uint32_t id) const;

    size_t contentsBitsNeeded(const BucketContents& contents) const;

    uint8_t suffixBits() const;

private:
    uint8_t suffix_bits_;
    uint32_t bucket_bytes_;
};

extern template class BucketCodec<RowLayout>;
extern template class BucketCodec<ColumnarLayout>;

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_BUCKET_CODEC_H
