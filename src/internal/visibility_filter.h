#ifndef KVLITE_INTERNAL_VISIBILITY_FILTER_H
#define KVLITE_INTERNAL_VISIBILITY_FILTER_H

#include <cstdint>
#include <set>
#include <unordered_map>
#include <vector>

#include "internal/log_entry.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class GlobalIndex;
class LogFile;
class SegmentIndex;

// Streaming iterator over visible entries in a segment file.
//
// Scans the data region sequentially, yielding only entries whose
// (hash, version) pair is in the visible set. Output order matches the
// on-disk order: (hash asc, version desc) — same as WriteBuffer::flush.
class VisibleVersionIterator {
public:
    struct Entry {
        uint64_t hash;
        LogEntry log_entry;
    };

    bool valid() const { return valid_; }
    const Entry& entry() const { return current_; }
    Status next();

private:
    friend class VisibilityFilter;
    VisibleVersionIterator(
        std::unordered_map<uint64_t, std::set<uint32_t>> visible_set,
        const LogFile& log_file, uint64_t data_size);

    Status readNextEntry(LogEntry& out);
    Status advance();

    const LogFile& log_file_;
    uint64_t data_size_;
    uint64_t file_offset_ = 0;
    std::unordered_map<uint64_t, std::set<uint32_t>> visible_set_;
    Entry current_;
    bool valid_ = false;
};

// VisibilityFilter: Computes visible version counts for segments.
//
// A version V for key-hash H in segment S is "visible" if some snapshot
// pins it — i.e., V is the latest version <= that snapshot and the
// GlobalIndex maps it to segment S.
//
// Uses a parallel DHT merge-join: both GlobalIndex and SegmentIndex
// expose forEachGroup which iterates in hash-ascending order (the
// reconstructed 64-bit hash is lossless). This enables an O(G+L)
// merge-join with zero random lookups.
class VisibilityFilter {
public:
    // Create a streaming iterator over the visible entries in a segment.
    static VisibleVersionIterator getVisibleVersions(
        const GlobalIndex& global_index,
        const SegmentIndex& segment_index,
        uint32_t segment_id,
        const std::vector<uint64_t>& snapshot_versions,
        const LogFile& log_file,
        uint64_t data_size);
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_VISIBILITY_FILTER_H
