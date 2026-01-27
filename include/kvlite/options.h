#ifndef KVLITE_OPTIONS_H
#define KVLITE_OPTIONS_H

#include <cstddef>

namespace kvlite {

// Options to control the behavior of a database
struct Options {
    // Create the database if it does not exist
    bool create_if_missing = false;

    // Raise an error if the database already exists
    bool error_if_exists = false;

    // Amount of data to build up in memory before writing to disk
    // Default: 4MB
    size_t write_buffer_size = 4 * 1024 * 1024;

    // Maximum number of open files that can be used by the DB
    // Default: 1000
    int max_open_files = 1000;

    // Size of the block cache in bytes
    // Default: 8MB
    size_t block_cache_size = 8 * 1024 * 1024;

    // Approximate size of user data packed per block
    // Default: 4KB
    size_t block_size = 4 * 1024;

    // Use bloom filters to reduce disk reads
    bool use_bloom_filter = true;

    // Bits per key for bloom filter (if enabled)
    int bloom_filter_bits_per_key = 10;

    // Sync writes to disk immediately
    // Setting this to true will be slower but more durable
    bool sync_writes = false;

    // Compress data blocks using compression
    bool compression = true;
};

// Options for read operations
struct ReadOptions {
    // If true, all data read will be verified against checksums
    bool verify_checksums = false;

    // If true, do not cache data read by this operation
    bool fill_cache = true;
};

// Options for write operations
struct WriteOptions {
    // If true, the write will be flushed to disk before returning
    bool sync = false;
};

} // namespace kvlite

#endif // KVLITE_OPTIONS_H
