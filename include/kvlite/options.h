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

    // Size of the write buffer in bytes
    // Writes accumulate here before flushing to a new log file
    // Default: 64MB
    size_t write_buffer_size = 64 * 1024 * 1024;

    // --- GlobalIndex Options ---

    // Number of updates before persisting GlobalIndex savepoint.
    // WAL is truncated after successful savepoint.
    // Default: 10 million
    uint64_t global_index_savepoint_interval = 1ULL << 24;

    // --- SegmentIndex Cache Options ---

    // Size of the LRU cache for SegmentIndex instances in bytes
    // Hot log file indices are kept in memory
    // Default: 256MB
    size_t segment_index_cache_size = 256 * 1024 * 1024;

    // --- Garbage Collection Options ---

    // Policy for selecting log files to compact
    // Default: HIGHEST_DEAD_RATIO
    GCPolicy gc_policy = GCPolicy::HIGHEST_DEAD_RATIO;

    // Minimum dead entry ratio to trigger automatic GC
    // Range: 0.0 to 1.0 (e.g., 0.5 = 50% dead entries)
    // Default: 0.5
    double gc_threshold = 0.5;

    // Maximum number of files to compact in a single GC run
    // Default: 10
    int gc_max_files = 10;

    // --- General Options ---

    // Create the database directory if it does not exist
    bool create_if_missing = false;

    // Raise an error if the database already exists
    bool error_if_exists = false;

    // Sync writes to disk immediately (slower but more durable)
    bool sync_writes = false;

    // Enable checksums for data integrity verification
    bool verify_checksums = true;

    // Delete segment files on disk that are not tracked by the manifest.
    // When true, recover() removes orphan segment_*.data files left by crashes.
    bool purge_untracked_files = false;
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
