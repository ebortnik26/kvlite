#ifndef KVLITE_INTERNAL_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_DELTA_HASH_TABLE_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "internal/bucket_arena.h"
#include "internal/lslot_codec.h"

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

// Secondary hash for fingerprint extension (collision disambiguation).
// Uses a different FNV offset basis to be independent of the primary hash.
inline uint64_t dhtSecondaryHash(const void* data, size_t len) {
    uint64_t hash = 6364136223846793005ULL;
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        hash ^= bytes[i];
        hash *= 1099511628211ULL;
    }
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    return hash;
}

// Base class for Delta Hash Tables.
//
// Provides all bucket management, hash decomposition, extension chain
// handling, and generic chain-traversal helpers. Derived classes add
// lifecycle-specific public APIs and optional locking.
//
// Payload per fingerprint group: parallel arrays of
//   packed_version (uint64_t) — opaque 64-bit value
//   id            (uint32_t) — opaque 32-bit value (file-offset or segment-id)
class DeltaHashTable {
public:
    using TrieEntry = LSlotCodec::TrieEntry;
    using LSlotContents = LSlotCodec::LSlotContents;

    // Resolves the actual key for a given (segment_id, packed_version) pair.
    // Used during collision detection to compare keys.
    using KeyResolver = std::function<std::string(uint32_t segment_id, uint64_t packed_version)>;

    struct Config {
        uint8_t bucket_bits = 20;
        uint8_t lslot_bits = 5;
        uint32_t bucket_bytes = 512;
    };

    // --- Public read API (no locking) ---

    bool findAll(std::string_view key,
                 std::vector<uint64_t>& packed_versions,
                 std::vector<uint32_t>& ids) const;

    bool findFirst(std::string_view key,
                   uint64_t& packed_version, uint32_t& id) const;

    bool contains(std::string_view key) const;

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
    uint8_t fingerprintBits() const;

protected:
    static constexpr uint32_t kBucketPadding = 8;

    explicit DeltaHashTable(const Config& config);
    ~DeltaHashTable();

    DeltaHashTable(const DeltaHashTable&) = delete;
    DeltaHashTable& operator=(const DeltaHashTable&) = delete;
    DeltaHashTable(DeltaHashTable&&) noexcept;
    DeltaHashTable& operator=(DeltaHashTable&&) noexcept;

    // --- Hash decomposition ---

    uint64_t hashKey(std::string_view key) const;
    uint32_t bucketIndex(uint64_t hash) const;
    uint32_t lslotIndex(uint64_t hash) const;
    uint64_t fingerprint(uint64_t hash) const;

    // --- Bucket data ---

    uint64_t getExtensionPtr(const Bucket& bucket) const;
    void setExtensionPtr(Bucket& bucket, uint64_t ptr) const;
    size_t bucketDataBits() const;
    uint32_t numLSlots() const;

    // --- Decode / Encode ---

    LSlotContents decodeLSlot(const Bucket& bucket, uint32_t lslot_idx) const;
    std::vector<LSlotContents> decodeAllLSlots(const Bucket& bucket) const;
    void reencodeAllLSlots(Bucket& bucket,
                           const std::vector<LSlotContents>& all_slots);
    size_t totalBitsNeeded(const std::vector<LSlotContents>& all_slots) const;

    // --- Extension chain ---

    const Bucket* nextBucket(const Bucket& bucket) const;
    Bucket* nextBucketMut(Bucket& bucket);
    Bucket* createExtension(Bucket& bucket);

    // --- Protected read helpers (pre-hashed) ---

    bool findAllByHash(uint32_t bi, uint32_t li, uint64_t fp,
                       std::vector<uint64_t>& packed_versions,
                       std::vector<uint32_t>& ids) const;

    bool findFirstByHash(uint32_t bi, uint32_t li, uint64_t fp,
                         uint64_t& packed_version, uint32_t& id) const;

    // --- Hash helpers ---
    uint64_t secondaryHash(std::string_view key) const;

    // --- Protected write helpers ---
    // Adds an entry to the chain at (bi, li) for fingerprint fp.
    // createExtFn is called (with bucket lock held by caller if needed)
    // when the current bucket overflows.
    // Returns true if the fingerprint group was newly created (key is new).
    bool addToChain(uint32_t bi, uint32_t li, uint64_t fp,
                    uint64_t packed_version, uint32_t id,
                    const std::function<Bucket*(Bucket&)>& createExtFn);

    // Like addToChain, but detects fingerprint collisions by resolving existing keys.
    // When a collision is detected, extends fingerprints with secondary hash bits.
    // Returns true if the key is new (no prior entry for this key).
    bool addToChainChecked(uint32_t bi, uint32_t li, uint64_t fp,
                           uint64_t packed_version, uint32_t id,
                           const std::function<Bucket*(Bucket&)>& createExtFn,
                           const KeyResolver& resolver,
                           uint64_t new_key_secondary_hash);

    // Remove entry matching (fp, packed_version, id) from chain at (bi, li).
    // Returns true if the fingerprint group is now empty (all entries removed).
    bool removeFromChain(uint32_t bi, uint32_t li, uint64_t fp,
                         uint64_t packed_version, uint32_t id);

    // Update id for entry matching (fp, packed_version, old_id) to new_id.
    // createExtFn called if re-encoding overflows the current bucket.
    // Returns true if the entry was found and updated.
    bool updateIdInChain(uint32_t bi, uint32_t li, uint64_t fp,
                         uint64_t packed_version, uint32_t old_id, uint32_t new_id,
                         const std::function<Bucket*(Bucket&)>& createExtFn);

    // --- Bulk helpers ---

    void clearBuckets();
    void loadArenaData(const uint8_t* data, size_t len);

    // --- Members ---

    Config config_;
    uint8_t fingerprint_bits_;
    LSlotCodec lslot_codec_;
    std::unique_ptr<uint8_t[]> arena_;
    std::vector<Bucket> buckets_;
    BucketArena* ext_arena_;    // non-owning; set by derived class

private:
    void initBucket(Bucket& bucket);

    // Prune an empty tail extension from a bucket's chain.
    void pruneEmptyExtension(Bucket* bucket);

    // Check whether any entry with matching base fp exists in the chain.
    bool isGroupEmpty(uint32_t bi, uint32_t li, uint64_t fp) const;

    // Remove one TrieEntry (by base fp match) from bucket's lslot,
    // reencode bucket, write entry to extension.
    void spillEntryToExtension(
        Bucket* bucket, uint32_t li, uint64_t base_mask, uint64_t fp,
        const std::function<Bucket*(Bucket&)>& createExtFn);

    // Try committing a candidate lslot to a bucket. Returns true if it fits.
    bool tryCommitSlot(Bucket* bucket, uint32_t li,
                       std::vector<LSlotContents>& all_slots,
                       LSlotContents&& candidate);

    // Append (packed_version, id) to an existing TrieEntry, handling overflow.
    void appendToEntry(TrieEntry& entry,
                       std::vector<LSlotContents>& all_slots,
                       Bucket* bucket, uint32_t li,
                       uint64_t packed_version, uint32_t id,
                       const std::function<Bucket*(Bucket&)>& createExtFn);

    // Find which matched entry is the same key. Returns index or -1.
    int findSameKeyMatch(
        const std::vector<LSlotContents>& all_slots, uint32_t li,
        const std::vector<size_t>& match_indices,
        const KeyResolver& resolver,
        uint64_t new_key_secondary_hash) const;

    // Resolve a collision by extending fingerprints and committing.
    void resolveCollision(
        std::vector<LSlotContents>& all_slots,
        Bucket* bucket, uint32_t li, uint64_t fp,
        const std::vector<size_t>& match_indices,
        const KeyResolver& resolver,
        uint64_t new_key_secondary_hash,
        uint64_t packed_version, uint32_t id,
        const std::function<Bucket*(Bucket&)>& createExtFn);
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_DELTA_HASH_TABLE_H
