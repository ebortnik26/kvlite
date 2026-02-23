#ifndef KVLITE_INTERNAL_GC_H
#define KVLITE_INTERNAL_GC_H

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "internal/log_entry.h"
#include "internal/segment.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class GlobalIndex;
class LogFile;
class SegmentIndex;

// GC: K-way merge compaction of N input segments into M output segments.
//
// Merges visible entries from N input segments using composable EntryStream
// operators (scan + filter + merge). Output entries arrive in (hash asc, version asc)
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

    struct Relocation {
        std::string key;
        uint64_t version;
        uint32_t old_segment_id;
        uint32_t new_segment_id;
    };

    struct Elimination {
        std::string key;
        uint64_t version;
        uint32_t old_segment_id;
    };

    struct Result {
        std::vector<OutputSegment> outputs;
        std::vector<Relocation> relocations;
        std::vector<Elimination> eliminations;
        size_t entries_written = 0;
        size_t entries_eliminated = 0;
    };

    // Merge visible entries from input segments into new output segments.
    //
    // Parameters:
    //   global_index, snapshot_versions: passed to stream::scanVisible per input.
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
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_GC_H
