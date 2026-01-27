# kvlite

A high-performance persistent key-value store optimized for SSD drives.

Inspired by the [Pliops XDP paper](https://www.vldb.org/pvldb/vol14/p2932-dayan.pdf), kvlite implements an **index-plus-log** architecture in software, providing efficient point lookups with SSD-friendly sequential writes.

## Features

- **Index-Plus-Log Architecture**: Two-level hash index with append-only log files
- **SSD-Optimized**: Sequential writes minimize SSD wear and maximize throughput
- **Simple API**: Point operations only (get/put/remove) - no range queries
- **Concurrent GC**: Background garbage collection with configurable policies
- **Crash Recovery**: WAL-protected L1 index with periodic snapshots

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         kvlite API                              │
├─────────────────────────────────────────────────────────────────┤
│              Level 1 Index: key → log_file_id                   │
│                    (always in memory)                           │
├────────────────────────┬────────────────────────────────────────┤
│     L1 Index WAL       │       L1 Index Snapshot                │
│  (append-only deltas)  │   (every 10M updates + shutdown)       │
├────────────────────────┴────────────────────────────────────────┤
│                    L2 Index Cache (LRU)                         │
│              (hot log file indices in memory)                   │
├─────────────────────────────────────────────────────────────────┤
│                      Write Buffer                               │
│             (flushes to new log file at capacity)               │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│  log_N.data  │ log_N+1.data │ log_N+2.data │  ...    (≤1GB each)│
├──────────────┼──────────────┼──────────────┼────────────────────┤
│  log_N.idx   │ log_N+1.idx  │ log_N+2.idx  │  ...    (L2 files) │
└──────────────┴──────────────┴──────────────┴────────────────────┘
```

## File Layout

```
<db_path>/
├── MANIFEST              # Active log files, metadata
├── l1_index.snapshot     # L1 index full snapshot
├── l1_index.wal          # L1 index WAL (deltas since snapshot)
├── log_000000.data       # Log file (append-only entries)
├── log_000000.idx        # L2 index for this log file
├── log_000001.data
├── log_000001.idx
└── ...
```

## Requirements

- C++17 or later
- CMake 3.14+
- POSIX-compliant system (Linux, macOS)

## Building

```bash
mkdir build && cd build
cmake ..
make
```

## Quick Start

```cpp
#include <kvlite/kvlite.h>
#include <iostream>

int main() {
    kvlite::DB db;
    kvlite::Options options;
    options.create_if_missing = true;

    auto status = db.open("/path/to/db", options);
    if (!status.ok()) {
        std::cerr << "Failed to open: " << status.toString() << std::endl;
        return 1;
    }

    // Write
    db.put("hello", "world");

    // Read
    std::string value;
    status = db.get("hello", &value);
    if (status.ok()) {
        std::cout << "Value: " << value << std::endl;
    }

    // Delete
    db.remove("hello");

    // Batch write
    kvlite::WriteBatch batch;
    batch.put("key1", "value1");
    batch.put("key2", "value2");
    batch.remove("key3");
    db.write(batch);

    return 0;
}
```

## API Reference

### Basic Operations

```cpp
// Set a key-value pair (overwrites if exists)
Status put(const std::string& key, const std::string& value);

// Get value by key (returns NotFound if missing)
Status get(const std::string& key, std::string* value);

// Delete a key (writes tombstone, reclaimed by GC)
Status remove(const std::string& key);

// Check if key exists
bool exists(const std::string& key);

// Atomic batch write
Status write(const WriteBatch& batch);
```

### Garbage Collection

```cpp
// Trigger GC with configured policy
Status gc();

// Compact specific log files
Status gc(const std::vector<uint32_t>& file_ids);

// Check if GC is running
bool isGCRunning() const;

// Wait for GC to complete
Status waitForGC();
```

### Maintenance

```cpp
// Flush write buffer to disk
Status flush();

// Force L1 index snapshot
Status snapshotL1Index();

// Get database statistics
DBStats getStats() const;

// Get log file information
std::vector<LogFileInfo> getLogFiles() const;
```

## Configuration

```cpp
kvlite::Options options;

// Storage
options.log_file_size = 1ULL * 1024 * 1024 * 1024;  // 1GB per log file
options.write_buffer_size = 64 * 1024 * 1024;        // 64MB write buffer

// L1 Index
options.l1_snapshot_interval = 10'000'000;           // Snapshot every 10M updates

// L2 Cache
options.l2_cache_size = 256 * 1024 * 1024;           // 256MB LRU cache

// Garbage Collection
options.gc_policy = kvlite::GCPolicy::HIGHEST_DEAD_RATIO;  // Default policy
options.gc_threshold = 0.5;                          // 50% dead entries triggers GC
options.gc_max_files = 10;                           // Max files per GC run
```

### GC Policies

| Policy | Description |
|--------|-------------|
| `HIGHEST_DEAD_RATIO` | Select files with highest dead/total ratio (default) |
| `OLDEST_FIRST` | Select oldest files first |
| `MOST_DEAD_ENTRIES` | Select files with most dead entries (absolute) |
| `MANUAL` | Only compact via explicit `gc(file_ids)` call |

## Design Details

### Write Path

1. Write enters write buffer
2. When buffer is full → flush to new log file
3. Create L2 index file for new log
4. Update L1 index (key → file_id)
5. Append to L1 WAL

### Read Path

1. Look up key in L1 index → get file_id
2. Load L2 index (from cache or disk)
3. Look up key in L2 index → get offset
4. Read value from log file at offset

### Garbage Collection

1. Select N log files based on policy
2. Merge entries, keeping only latest version per key
3. Drop tombstones and dead entries
4. Write to M new log files (each ≤1GB)
5. Update L1 index atomically
6. Delete old log files and L2 indices

### Recovery

1. Load L1 snapshot from `l1_index.snapshot`
2. Replay L1 WAL to reconstruct current state
3. L2 indices loaded on-demand from `.idx` files

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

Design inspired by:
- [Pliops XDP (VLDB 2021)](https://www.vldb.org/pvldb/vol14/p2932-dayan.pdf)
- LevelDB, RocksDB, and other LSM-tree storage engines
