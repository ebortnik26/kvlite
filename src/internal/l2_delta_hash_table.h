#ifndef KVLITE_INTERNAL_L2_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_L2_DELTA_HASH_TABLE_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "internal/delta_hash_table_base.h"
#include "internal/l2_lslot_codec.h"

namespace kvlite {
namespace internal {

// L2 Delta Hash Table: compact hash table for per-file indexes.
//
// Stores (offset, version) pairs per fingerprint.
// Append-only, write-once: no remove, update, or concurrency control.
// Writes are assumed to arrive in sorted hash order (no bucket contention).
class L2DeltaHashTable : private DeltaHashTableBase<L2LSlotCodec> {
    using Base = DeltaHashTableBase<L2LSlotCodec>;

public:
    using Base::Config;
    using TrieEntry = L2LSlotCodec::TrieEntry;
    using LSlotContents = L2LSlotCodec::LSlotContents;

    L2DeltaHashTable();
    explicit L2DeltaHashTable(const Config& config);
    ~L2DeltaHashTable();

    L2DeltaHashTable(const L2DeltaHashTable&) = delete;
    L2DeltaHashTable& operator=(const L2DeltaHashTable&) = delete;
    L2DeltaHashTable(L2DeltaHashTable&&) = delete;
    L2DeltaHashTable& operator=(L2DeltaHashTable&&) = delete;

    // Add an (offset, version) pair for a key's fingerprint.
    void addEntry(const std::string& key, uint32_t offset, uint32_t version);

    // Add by pre-computed hash (for snapshot loading).
    void addEntryByHash(uint64_t hash, uint32_t offset, uint32_t version);

    // Find all (offset, version) pairs for a key. Returns true if key exists.
    // Pairs are ordered by offset desc (highest/latest first).
    bool findAll(const std::string& key,
                 std::vector<uint32_t>& offsets,
                 std::vector<uint32_t>& versions) const;

    // Find the first (highest offset) entry. Returns true if found.
    bool findFirst(const std::string& key,
                   uint32_t& offset, uint32_t& version) const;

    bool contains(const std::string& key) const;

    // Iterate over all entries.
    void forEach(const std::function<void(uint64_t hash,
                                          uint32_t offset,
                                          uint32_t version)>& fn) const;

    // Iterate over all groups.
    void forEachGroup(const std::function<void(uint64_t hash,
                                               const std::vector<uint32_t>& offsets,
                                               const std::vector<uint32_t>& versions)>& fn) const;

    size_t size() const;
    size_t memoryUsage() const;
    void clear();

private:
    size_t size_ = 0;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_L2_DELTA_HASH_TABLE_H
