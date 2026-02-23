#ifndef KVLITE_INTERNAL_L2_INDEX_H
#define KVLITE_INTERNAL_L2_INDEX_H

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "kvlite/status.h"
#include "segment_delta_hash_table.h"

namespace kvlite {
namespace internal {

class LogFile;

// SegmentIndex: In-memory per-file index mapping keys to (offset, version) lists.
//
// Structure: key → [(offset₁, version₁), (offset₂, version₂), ...]
//            sorted by offset desc (latest/highest first)
//
// All entries pertain to a single file. The index is append-only:
// no remove or update operations are supported.
// Write-once: no concurrency control or persistence needed.
class SegmentIndex {
public:
    SegmentIndex();
    ~SegmentIndex();

    SegmentIndex(const SegmentIndex&) = delete;
    SegmentIndex& operator=(const SegmentIndex&) = delete;
    SegmentIndex(SegmentIndex&&) noexcept;
    SegmentIndex& operator=(SegmentIndex&&) noexcept;

    // Append (offset, version) to key's list.
    void put(std::string_view key, uint32_t offset, uint32_t version);

    // Get all (offset, version) pairs for a key. Returns false if key doesn't exist.
    // Pairs are ordered latest-first (highest offset first).
    bool get(const std::string& key,
             std::vector<uint32_t>& offsets,
             std::vector<uint32_t>& versions) const;

    // Get the latest entry for a key with version <= upper_bound.
    // Returns false if no matching entry exists.
    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& offset, uint64_t& version) const;

    // Get the latest (highest version) entry for a key.
    // Returns false if key doesn't exist.
    bool getLatest(const std::string& key,
                   uint32_t& offset, uint32_t& version) const;

    // Check if a key exists.
    bool contains(const std::string& key) const;

    // Serialize to a LogFile.
    Status writeTo(LogFile& file);

    // Deserialize from a LogFile starting at the given byte offset.
    Status readFrom(const LogFile& file, uint64_t offset = 0);

    // Iterate over all entries, calling fn(offset, version) for each.
    void forEach(const std::function<void(uint32_t offset, uint32_t version)>& fn) const;

    // Iterate over all groups (hash-sorted). Callback receives:
    //   hash: the FNV-1a hash
    //   offsets: sorted desc (latest first)
    //   versions: parallel array
    void forEachGroup(
        const std::function<void(uint64_t hash,
                                 const std::vector<uint32_t>& offsets,
                                 const std::vector<uint32_t>& versions)>& fn) const;

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;
    void clear();

private:
    SegmentDeltaHashTable dht_;
    size_t key_count_ = 0;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_L2_INDEX_H
