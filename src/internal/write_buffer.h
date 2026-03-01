#ifndef KVLITE_INTERNAL_WRITE_BUFFER_H
#define KVLITE_INTERNAL_WRITE_BUFFER_H

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "internal/memtable.h"
#include "internal/entry_stream.h"
#include "kvlite/status.h"

namespace kvlite {
namespace internal {

// N-way buffered write pipeline with background flush.
//
// Manages a mutable Memtable for incoming writes and a queue of immutable
// Memtables being flushed by a background daemon. When the active Memtable
// hits capacity, it is sealed (made immutable) and enqueued; a fresh mutable
// Memtable is created immediately so writes can continue. If all slots are
// occupied (queue saturated), put() stalls until the daemon frees a slot.
class WriteBuffer {
public:
    // Callback invoked by the flush daemon for each immutable Memtable.
    // Must flush it to storage + GlobalIndex. Called without mu_ held.
    using FlushFn = std::function<Status(Memtable&)>;

    struct Options {
        size_t memtable_size;    // capacity threshold per Memtable
        uint32_t flush_depth;    // max simultaneous Memtables (default 3)
    };

    WriteBuffer(const Options& opts, FlushFn flush_fn);
    ~WriteBuffer();

    WriteBuffer(const WriteBuffer&) = delete;
    WriteBuffer& operator=(const WriteBuffer&) = delete;

    // --- Write operations (delegate to active Memtable) ---
    // Stalls if all flush_depth slots are occupied.
    void put(const std::string& key, uint64_t version,
             const std::string& value, bool tombstone);
    void putBatch(const std::vector<Memtable::BatchOp>& ops, uint64_t version);

    // --- Read operations (check active + flushing + immutable queue) ---
    bool getByVersion(const std::string& key, uint64_t upper_bound,
                      std::string& value, uint64_t& version, bool& tombstone) const;

    // --- Lifecycle ---
    // Seal active Memtable and block until all queued flushes complete.
    void drainFlush();

    // Memory usage of active Memtable.
    size_t memoryUsage() const;
    bool empty() const;

    // --- Iterator support ---
    // Snapshot of pinned Memtables for iteration.
    struct PinnedSnapshot {
        std::vector<Memtable*> memtables;  // active first, then immutables oldest-to-newest
    };

    // Pin the active Memtable + all immutable ones for iteration.
    PinnedSnapshot pinAll();
    void unpinAll(const PinnedSnapshot& snap);

private:
    void maybeSeal();           // seal active if over capacity (must hold mu_)
    void sealActive();          // move active to immutable queue (must hold mu_)
    void flushLoop();           // daemon thread function

    Options opts_;
    FlushFn flush_fn_;

    mutable std::mutex mu_;     // protects active_, immutables_, flushing_, retired_, stop_
    std::condition_variable flush_cv_;   // signals daemon: new immutable available
    std::condition_variable stall_cv_;   // signals writers: slot freed after flush

    std::unique_ptr<Memtable> active_;
    std::deque<std::unique_ptr<Memtable>> immutables_;  // FIFO flush queue

    // Memtable currently being flushed by the daemon (readable but not in immutables_).
    // Kept here so getByVersion/pinAll can still see it during the flush.
    Memtable* flushing_ = nullptr;

    std::thread flush_thread_;
    bool stop_ = false;

    // Retired Memtables pinned by iterators (unpinned → deleted).
    std::vector<std::unique_ptr<Memtable>> retired_;
};

} // namespace internal
} // namespace kvlite

#endif // KVLITE_INTERNAL_WRITE_BUFFER_H
