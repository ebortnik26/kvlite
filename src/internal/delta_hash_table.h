#ifndef KVLITE_INTERNAL_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_DELTA_HASH_TABLE_H

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/bucket_arena.h"
#include "internal/bucket_codec.h"

namespace kvlite {
namespace internal {

// 64-bit FNV-1a with avalanche mixing.
inline uint64_t dhtHashBytes(const void* data, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        hash ^= bytes[i];
        hash *= 1099511628211ULL;
    }
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

// Base class for Delta Hash Tables using sorted suffix arrays per bucket.
//
// Each bucket stores unique key suffixes (the hash bits not used for bucket
// selection) in sorted order, with per-key version/id arrays. Binary search
// replaces fingerprint matching. Full suffix comparison replaces collision
// resolution — no false positives, no I/O for disambiguation.
//
// Payload per key suffix: parallel arrays of
//   packed_version (uint64_t) — opaque 64-bit value
//   id            (uint32_t) — opaque 32-bit value (file-offset or segment-id)
class DeltaHashTable {
public:
    struct Config {
        uint8_t bucket_bits = 20;
        uint32_t bucket_bytes = 512;
    };

    // Type aliases forwarded from BucketCodec (preserves existing API).
    using KeyEntry = BucketCodec::KeyEntry;
    using BucketContents = BucketCodec::BucketContents;
    using SuffixScanResult = BucketCodec::SuffixScanResult;

    // --- Public read API (no locking) ---

    bool findAll(uint64_t hash,
                 std::vector<uint64_t>& packed_versions,
                 std::vector<uint32_t>& ids) const;

    bool findFirst(uint64_t hash,
                   uint64_t& packed_version, uint32_t& id) const;

    bool contains(uint64_t hash) const;

    void forEach(const std::function<void(uint64_t hash,
                                          uint64_t packed_version,
                                          uint32_t id)>& fn) const;

    void forEachGroup(const std::function<void(uint64_t hash,
                                               const std::vector<uint64_t>& packed_versions,
                                               const std::vector<uint32_t>& ids)>& fn) const;

    size_t memoryUsage() const;

    // --- Public accessors for binary snapshot ---

    const uint8_t* arenaData() const;
    size_t arenaBytes() const;
    uint32_t numBuckets() const;
    uint32_t bucketStride() const;
    const Config& config() const;
    uint8_t suffixBits() const;

protected:
    static constexpr uint32_t kBucketPadding = 0;

    explicit DeltaHashTable(const Config& config);
    ~DeltaHashTable();

    DeltaHashTable(const DeltaHashTable&) = delete;
    DeltaHashTable& operator=(const DeltaHashTable&) = delete;
    DeltaHashTable(DeltaHashTable&&) noexcept;
    DeltaHashTable& operator=(DeltaHashTable&&) noexcept;

    // Fast empty check: reads only the N_k count (first 2 bytes).
    bool isEmptyBucket(const Bucket& bucket) const {
        uint16_t num_keys = 0;
        std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));
        return num_keys == 0;
    }

    // --- Hash decomposition ---

    uint32_t bucketIndex(uint64_t hash) const;
    uint64_t suffixFromHash(uint64_t hash) const;

    // --- Bucket data ---

    uint64_t getExtensionPtr(const Bucket& bucket) const;
    void setExtensionPtr(Bucket& bucket, uint64_t ptr) const;

    // --- Extension chain ---

    const Bucket* nextBucket(const Bucket& bucket) const;
    Bucket* nextBucketMut(Bucket& bucket);
    Bucket* createExtension(Bucket& bucket);

    // Check if a suffix exists in the chain at bucket bi (suffix-only scan).
    bool containsByHash(uint32_t bi, uint64_t suffix) const;

    // --- Protected read helpers (pre-hashed) ---

    bool findAllByHash(uint32_t bi, uint64_t suffix,
                       std::vector<uint64_t>& packed_versions,
                       std::vector<uint32_t>& ids) const;

    bool findFirstByHash(uint32_t bi, uint64_t suffix,
                         uint64_t& packed_version, uint32_t& id) const;

    // --- Protected write helpers ---

    bool addToChain(uint32_t bi, uint64_t suffix,
                    uint64_t packed_version, uint32_t id,
                    const std::function<Bucket*(Bucket&)>& createExtFn);

    bool removeFromChain(uint32_t bi, uint64_t suffix,
                         uint64_t packed_version, uint32_t id);

    bool updateIdInChain(uint32_t bi, uint64_t suffix,
                         uint64_t packed_version, uint32_t old_id, uint32_t new_id,
                         const std::function<Bucket*(Bucket&)>& createExtFn);

    // --- Bulk helpers ---

    void clearBuckets();
    void loadArenaData(const uint8_t* data, size_t len);

    // --- Members ---

    Config config_;
    uint8_t suffix_bits_;   // 64 - bucket_bits
    BucketCodec codec_;     // bucket encode/decode
    std::unique_ptr<uint8_t[]> arena_;
    std::vector<Bucket> buckets_;
    BucketArena* ext_arena_;    // non-owning; set by derived class

private:
    void initBucket(Bucket& bucket);
    void pruneEmptyExtension(Bucket* bucket);
    bool isSuffixEmpty(uint32_t bi, uint64_t suffix) const;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_DELTA_HASH_TABLE_H
