#ifndef KVLITE_OPTIONS_H
#define KVLITE_OPTIONS_H

#include <cstddef>
#include <cstdint>

namespace kvlite {

class Snapshot;

// Garbage collection policy for selecting files to compact
enum class GCPolicy {
    // Select files with the highest ratio of dead entries (default)
    HIGHEST_DEAD_RATIO,

    // Select the oldest files first
    OLDEST_FIRST,

    // Select files with the most dead entries (absolute count)
    MOST_DEAD_ENTRIES,

    // Manual selection only via compact() API
    MANUAL
};

// Options to control the behavior of a database
struct Options {
    // --- Storage Options ---

    // Maximum size of a single log file in bytes
    // When exceeded, a new log file is created
    // Default: 1GB
    size_t log_file_size = 1ULL * 1024 * 1024 * 1024;

    // Capacity of each Memtable in bytes
    // When exceeded, the Memtable is sealed and flushed to a new log file
    // Default: 256MB
    size_t memtable_size = 256 * 1024 * 1024;

    // Number of Memtables in the write buffer pipeline (default 3).
    // 1 mutable + up to (flush_depth - 1) immutable in the flush queue.
    // Higher values reduce stall probability at the cost of memory.
    uint32_t flush_depth = 3;

    // --- GlobalIndex Options ---

    // Seconds between savepoint daemon wake-ups (0 = disable daemon).
    // The daemon creates a savepoint whenever new segments have been flushed
    // since the last savepoint, accelerating future recovery.
    // Default: 10
    uint32_t savepoint_interval_sec = 10;

    // Seconds between version pruning daemon wake-ups (0 = disable).
    // The daemon eliminates stale GlobalIndex versions not visible to any
    // active snapshot, reducing memory usage without touching segment files.
    // Default: 30
    uint32_t version_prune_interval_sec = 30;

    // --- Garbage Collection Options ---

    // Policy for selecting log files to compact
    // Default: HIGHEST_DEAD_RATIO
    GCPolicy gc_policy = GCPolicy::HIGHEST_DEAD_RATIO;

    // Minimum dead entry ratio to trigger automatic GC
    // Range: 0.0 to 1.0 (e.g., 0.5 = 50% dead entries)
    // Default: 0.5
    double gc_threshold = 0.5;

    // Maximum number of segments to compact in a single GC run
    // Default: 10
    int gc_max_segments = 10;

    // Seconds between GC daemon wake-ups (0 = disable daemon)
    // Default: 10
    uint32_t gc_interval_sec = 10;

    // --- I/O Options ---

    // Buffer segment writes through a 1MB userspace buffer.
    // Reduces syscall count per flush from ~N to ~N/60.
    // Default: true
    bool buffered_writes = true;

    // Number of partition files per segment (must be a power of 2).
    // Each partition is written and synced independently. K > 1 enables
    // parallel flush I/O, reducing seal latency on multi-queue SSDs.
    // Default: 1 (single file per segment)
    uint16_t segment_partitions = 1;

    // --- General Options ---

    // Create the database directory if it does not exist
    bool create_if_missing = false;

    // Raise an error if the database already exists
    bool error_if_exists = false;

    // Enable checksums for data integrity verification
    bool verify_checksums = true;
};

// Options for read operations
struct ReadOptions {
    // If true, verify checksum of data read from disk
    bool verify_checksums = false;

    // If true, cache the SegmentIndex if not already cached
    bool fill_cache = true;

    // If non-null, read at this snapshot's point-in-time.
    // The caller must keep the Snapshot alive for the duration of the operation.
    const Snapshot* snapshot = nullptr;
};

// Options for write operations
struct WriteOptions {
    // If true, the write will be flushed to disk before returning
    bool sync = false;
};

} // namespace kvlite

#endif // KVLITE_OPTIONS_H
