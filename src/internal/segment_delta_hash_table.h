#ifndef KVLITE_INTERNAL_L2_DELTA_HASH_TABLE_H
#define KVLITE_INTERNAL_L2_DELTA_HASH_TABLE_H

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "internal/delta_hash_table_base.h"
#include "internal/segment_lslot_codec.h"

namespace kvlite {
namespace internal {

// Segment Delta Hash Table: compact hash table for paired indexes.
//
// Stores (offset, version) pairs per fingerprint.
// No concurrency control — external synchronization required.
//
// Bucket format (managed by SegmentLSlotCodec header protocol):
//   [32 bits: base_offset] [lslot 0] [lslot 1] ... [lslot N-1] [8 bytes: ext_ptr]
//
// All offsets within a bucket are encoded as gamma(offset - base_offset + 1).
class SegmentDeltaHashTable : private DeltaHashTableBase<SegmentLSlotCodec> {
    using Base = DeltaHashTableBase<SegmentLSlotCodec>;

public:
    using Base::Config;
    using TrieEntry = SegmentLSlotCodec::TrieEntry;
    using LSlotContents = SegmentLSlotCodec::LSlotContents;

    SegmentDeltaHashTable();
    explicit SegmentDeltaHashTable(const Config& config);
    ~SegmentDeltaHashTable();

    SegmentDeltaHashTable(const SegmentDeltaHashTable&) = delete;
    SegmentDeltaHashTable& operator=(const SegmentDeltaHashTable&) = delete;
    SegmentDeltaHashTable(SegmentDeltaHashTable&&) noexcept;
    SegmentDeltaHashTable& operator=(SegmentDeltaHashTable&&) noexcept;

    // Add an (offset, version) pair for a key's fingerprint.
    void addEntry(std::string_view key, uint32_t offset, uint32_t version);

    // Add by pre-computed hash (for snapshot loading).
    void addEntryByHash(uint64_t hash, uint32_t offset, uint32_t version);

    // Find all (offset, version) pairs for a key. Returns true if key exists.
    // Pairs are ordered by offset desc (highest/latest first).
    bool findAll(std::string_view key,
                 std::vector<uint32_t>& offsets,
                 std::vector<uint32_t>& versions) const;

    // Find the first (highest offset) entry. Returns true if found.
    bool findFirst(std::string_view key,
                   uint32_t& offset, uint32_t& version) const;

    bool contains(std::string_view key) const;

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
