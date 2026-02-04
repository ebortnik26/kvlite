#ifndef KVLITE_INTERNAL_L2_INDEX_H
#define KVLITE_INTERNAL_L2_INDEX_H

#include <cstdint>
#include <string>
#include <vector>

#include "kvlite/status.h"
#include "l2_delta_hash_table.h"

namespace kvlite {
namespace internal {

class LogFile;

// L2 Index: In-memory per-file index mapping keys to (offset, version) lists.
//
// Structure: key → [(offset₁, version₁), (offset₂, version₂), ...]
//            sorted by offset desc (latest/highest first)
//
// All entries pertain to a single file. The index is append-only:
// no remove or update operations are supported.
// Write-once: no concurrency control or persistence needed.
class L2Index {
public:
    L2Index();
    ~L2Index();

    L2Index(const L2Index&) = delete;
    L2Index& operator=(const L2Index&) = delete;
    L2Index(L2Index&&) noexcept;
    L2Index& operator=(L2Index&&) noexcept;

    // Append (offset, version) to key's list.
    void put(const std::string& key, uint32_t offset, uint32_t version);

    // Get all (offset, version) pairs for a key. Returns false if key doesn't exist.
    // Pairs are ordered latest-first (highest offset first).
    bool get(const std::string& key,
             std::vector<uint32_t>& offsets,
             std::vector<uint32_t>& versions) const;

    // Get the latest entry for a key with version <= upper_bound.
    // Returns false if no matching entry exists.
    bool get(const std::string& key, uint64_t upper_bound,
             uint64_t& offset, uint64_t& version) const;

    // Get the latest (highest offset) entry for a key.
    // Returns false if key doesn't exist.
    bool getLatest(const std::string& key,
                   uint32_t& offset, uint32_t& version) const;

    // Check if a key exists.
    bool contains(const std::string& key) const;

    // Serialize to a LogFile.
    Status writeTo(LogFile& file);

    // Deserialize from a LogFile (reads from current position to end).
    Status readFrom(LogFile& file);

    size_t keyCount() const;
    size_t entryCount() const;
    size_t memoryUsage() const;
    void clear();

private:
    L2DeltaHashTable dht_;
    size_t key_count_ = 0;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_L2_INDEX_H
