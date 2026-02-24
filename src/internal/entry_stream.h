#ifndef KVLITE_INTERNAL_ENTRY_STREAM_H
#define KVLITE_INTERNAL_ENTRY_STREAM_H

#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <string_view>
#include <vector>

#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class GlobalIndex;
class LogFile;
class WriteBuffer;

// Abstract base for composable entry streams.
//
// All operators (Scan, Filter, Merge) implement this interface.
// Streams yield entries in (hash asc, version asc) order.
class EntryStream {
public:
    struct Entry {
        uint64_t hash;
        std::string_view key;
        std::string_view value;
        uint64_t version;
        bool tombstone;

        // Extension fields: flat buffer for operator-defined attributes.
        // Each operator declares a schema (XxxExt with Field enum + kSize)
        // and writes at ext[base + Field]. Pass-through operators (merge,
        // filter) preserve ext transparently.
        static constexpr size_t kMaxExt = 4;
        uint64_t ext[kMaxExt] = {};
    };

    virtual ~EntryStream() = default;
    virtual bool valid() const = 0;
    virtual const Entry& entry() const = 0;
    virtual Status next() = 0;
};

// ---------------------------------------------------------------------------
// Operator extension schemas.
// Each operator declares its ext fields and size. The pipeline builder
// stacks them by assigning base offsets: operator N starts at
// base = sum of kSize for operators 0..N-1.
// ---------------------------------------------------------------------------

struct GCTagSourceExt {
    enum Field : size_t { kSegmentId = 0 };
    static constexpr size_t kSize = 1;
};

struct GCClassifyExt {
    enum Field : size_t { kAction = 0 };
    static constexpr size_t kSize = 1;
};

enum class EntryAction : uint64_t { kKeep = 0, kEliminate = 1 };

using Predicate = std::function<bool(const EntryStream::Entry&)>;

namespace stream {

// Scan ALL entries from a LogFile's data region. No filtering.
std::unique_ptr<EntryStream> scan(const LogFile& lf, uint64_t data_size);

// Wrap any EntryStream, yielding only entries matching the predicate.
std::unique_ptr<EntryStream> filter(std::unique_ptr<EntryStream> input, Predicate pred);

// K-way merge over N EntryStreams in (hash asc, version asc) order.
std::unique_ptr<EntryStream> merge(std::vector<std::unique_ptr<EntryStream>> inputs);

// Tag each entry with a source segment_id.
// Writes to ext[base + GCTagSourceExt::kSegmentId].
std::unique_ptr<EntryStream> gcTagSource(
    std::unique_ptr<EntryStream> input, uint32_t segment_id, size_t base);

// Classify each entry as kKeep or kEliminate based on snapshot visibility.
// Entries must arrive in (hash asc, version asc) order. For each hash group,
// the latest version <= each snapshot is kept; all others are eliminated.
// Writes EntryAction to ext[base + GCClassifyExt::kAction].
std::unique_ptr<EntryStream> gcClassify(
    std::unique_ptr<EntryStream> input,
    const std::vector<uint64_t>& snapshot_versions,
    size_t base);

// Convenience: scan + latest-version filter for DB iteration.
// Checks GlobalIndex per entry during iteration — concurrent mutations visible.
std::unique_ptr<EntryStream> scanLatest(
    const GlobalIndex& gi, uint32_t segment_id,
    uint64_t snapshot_version,
    const LogFile& lf, uint64_t data_size);

// Stream entries from an in-memory WriteBuffer, filtered to snapshot_version.
// Thread-safe: locks each bucket during collection. Returns entries sorted
// by (hash asc, version asc), deduplicated per key (latest version <= snapshot).
std::unique_ptr<EntryStream> scanWriteBuffer(
    const WriteBuffer& wb, uint64_t snapshot_version);

}  // namespace stream

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_ENTRY_STREAM_H
