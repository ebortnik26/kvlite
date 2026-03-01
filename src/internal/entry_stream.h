#ifndef KVLITE_INTERNAL_ENTRY_STREAM_H
#define KVLITE_INTERNAL_ENTRY_STREAM_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>
#include <vector>

#include "log_entry.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

class LogFile;
class Memtable;

// Abstract base for composable entry streams.
//
// Streams yield entries sequentially. Scan streams iterate in file order;
// GC streams (see gc_stream.h) merge into (hash asc, version asc) order.
class EntryStream {
public:
    struct Entry {
        uint64_t hash;
        std::string_view key;
        std::string_view value;
        PackedVersion pv;    // packed: (logical_version << 1) | tombstone

        uint64_t version() const { return pv.version(); }
        bool tombstone() const { return pv.tombstone(); }

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

using Predicate = std::function<bool(const EntryStream::Entry&)>;

namespace stream {

// Scan ALL entries from a LogFile's data region. No filtering.
std::unique_ptr<EntryStream> scan(const LogFile& lf, uint64_t data_size);

// Wrap any EntryStream, yielding only entries matching the predicate.
std::unique_ptr<EntryStream> filter(std::unique_ptr<EntryStream> input, Predicate pred);

// Stream entries from an in-memory Memtable, filtered to snapshot_version.
// Thread-safe: locks each bucket during collection. Returns entries sorted
// by (hash asc, version asc), deduplicated per key (latest version <= snapshot).
std::unique_ptr<EntryStream> scanMemtable(
    const Memtable& mt, uint64_t snapshot_version);

}  // namespace stream

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_ENTRY_STREAM_H
