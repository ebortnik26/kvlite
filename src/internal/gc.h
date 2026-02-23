#ifndef KVLITE_INTERNAL_GC_H
#define KVLITE_INTERNAL_GC_H

#include <cstdint>
#include <functional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "internal/log_entry.h"
#include "internal/segment.h"
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
// on-disk order: (hash asc, version asc) — same as WriteBuffer::flush.
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
    friend class GC;
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

// GC: K-way merge compaction of N input segments into M output segments.
//
// Merges visible entries from N input segments using a min-heap over
// VisibleVersionIterators. Output entries arrive in (hash asc, version asc)
// order. Output segments are split by a max size limit and are
// hash-partitioned by construction.
//
// GC::merge is a pure data transformation — the caller handles GlobalIndex
// bookkeeping (registering new segments, removing old ones).
class GC {
public:
    struct InputSegment {
        uint32_t segment_id;
        const SegmentIndex& index;
        const LogFile& log_file;
        uint64_t data_size;
    };

    struct OutputSegment {
        uint32_t segment_id;
        Segment segment;  // sealed, Readable state
    };

    struct Result {
        std::vector<OutputSegment> outputs;
        size_t entries_written = 0;
    };

    // Merge visible entries from input segments into new output segments.
    //
    // Parameters:
    //   global_index, snapshot_versions: passed to VisibilityFilter per input.
    //   inputs: the N input segments (Readable state).
    //   max_segment_size: data region limit per output segment.
    //   path_fn(segment_id) → path: generates file path for a new segment.
    //   id_fn() → segment_id: allocates a fresh segment ID.
    //   result: populated with output segments and stats.
    static Status merge(
        const GlobalIndex& global_index,
        const std::vector<uint64_t>& snapshot_versions,
        const std::vector<InputSegment>& inputs,
        uint64_t max_segment_size,
        const std::function<std::string(uint32_t)>& path_fn,
        const std::function<uint32_t()>& id_fn,
        Result& result);

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

#endif  // KVLITE_INTERNAL_GC_H
