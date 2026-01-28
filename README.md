# kvlite

A high-performance persistent key-value store optimized for SSD drives.

Inspired by the [Pliops XDP paper](https://www.vldb.org/pvldb/vol14/p2932-dayan.pdf), kvlite implements an **index-plus-log** architecture in software, providing efficient point lookups with SSD-friendly sequential writes.

## Features

- **Index-Plus-Log Architecture**: Two-level hash index with append-only log files
- **SSD-Optimized**: Sequential writes minimize SSD wear and maximize throughput
- **Multi-Version Storage**: Global monotonic versioning enables point-in-time reads
- **Snapshots**: Consistent reads across multiple keys at a specific version
- **Simple API**: Point operations only (get/put/remove) - no range queries
- **Concurrent GC**: Background garbage collection respects active snapshots

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         kvlite API                              │
├─────────────────────────────────────────────────────────────────┤
│                  Global Version Counter                         │
│              (monotonic, persisted in MANIFEST)                 │
├─────────────────────────────────────────────────────────────────┤
│      Level 1 Index: key → [(version₁, file_id₁), ...]          │
│                    (always in memory)                           │
├────────────────────────┬────────────────────────────────────────┤
│     L1 Index WAL       │       L1 Index Snapshot                │
│  (append-only deltas)  │   (every 10M updates + shutdown)       │
├────────────────────────┴────────────────────────────────────────┤
│                    L2 Index Cache (LRU)                         │
│      L2 Index: key → [(version₁, offset₁), ...]                │
├─────────────────────────────────────────────────────────────────┤
│                      Write Buffer                               │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│  log_N.data  │ log_N+1.data │ log_N+2.data │  ...    (≤1GB each)│
├──────────────┼──────────────┼──────────────┼────────────────────┤
│  log_N.idx   │ log_N+1.idx  │ log_N+2.idx  │  ...    (L2 files) │
└──────────────┴──────────────┴──────────────┴────────────────────┘
```

## Log Entry Format

```
┌─────────┬─────────┬───────────┬───────────┬─────┬───────┬──────────┐
│ version │ key_len │ value_len │ tombstone │ key │ value │ checksum │
│ 8 bytes │ 4 bytes │  4 bytes  │  1 byte   │ var │  var  │ 4 bytes  │
└─────────┴─────────┴───────────┴───────────┴─────┴───────┴──────────┘
```

## Quick Start

```cpp
#include <kvlite/kvlite.h>
#include <iostream>

int main() {
    kvlite::DB db;
    kvlite::Options options;
    options.create_if_missing = true;

    db.open("/path/to/db", options);

    // Write (creates version 1)
    db.put("key", "value1");

    // Update (creates version 2)
    db.put("key", "value2");

    // Read latest
    std::string value;
    uint64_t version;
    db.get("key", value, version);  // value="value2", version=2

    // Read at specific version
    db.getByVersion("key", 2, value);  // value="value1" (version < 2)

    return 0;
}
```

## API Reference

### Basic Operations

```cpp
// Write (creates new version)
Status put(const std::string& key, const std::string& value);

// Read latest version
Status get(const std::string& key, std::string& value);
Status get(const std::string& key, std::string& value, uint64_t& version);

// Read at specific version (largest version < upper_bound)
Status getByVersion(const std::string& key, uint64_t upper_bound,
                    std::string& value);

// Delete (creates tombstone with new version)
Status remove(const std::string& key);

// Check existence
Status exists(const std::string& key, bool& exists);
```

### Batch Operations

```cpp
// Atomic write batch - all ops get same version
kvlite::WriteBatch wbatch;
wbatch.put("key1", "value1");
wbatch.put("key2", "value2");
wbatch.remove("key3");
db.write(wbatch);  // All 3 ops get version=N

// Consistent read batch - all reads at same snapshot
kvlite::ReadBatch rbatch;
rbatch.get("key1");
rbatch.get("key2");
rbatch.get("key3");
db.read(rbatch);  // All reads at same version

for (const auto& result : rbatch.results()) {
    if (result.ok()) {
        std::cout << result.key << " = " << result.value << std::endl;
    }
}
```

### Snapshots

```cpp
// Create a snapshot at current version
std::unique_ptr<kvlite::DB::Snapshot> snapshot;
db.createSnapshot(snapshot);
std::cout << "Snapshot at version " << snapshot->version() << std::endl;

// Reads through snapshot always see same version
std::string v1, v2;
snapshot->get("key1", v1);

db.put("key1", "new_value");  // New write after snapshot

snapshot->get("key1", v2);   // Still sees old value
assert(v1 == v2);

// Release snapshot to allow GC of old versions
db.releaseSnapshot(std::move(snapshot));
```

### Iteration

Unordered iteration over all keys (useful for full database copy/replication):

```cpp
std::unique_ptr<kvlite::DB::Iterator> iter;
db.createIterator(iter);

std::string key, value;
uint64_t version;
while (iter->next(key, value, version).ok()) {
    std::cout << key << " = " << value << " (v" << version << ")" << std::endl;
}
```

Note: Keys are returned in arbitrary order (not sorted). The iterator scans
L2 index files sequentially, using L1 to filter out old versions.

### Administrative Operations

For garbage collection, statistics, and maintenance operations, use `DBAdmin`:

```cpp
#include <kvlite/db_admin.h>

kvlite::DBAdmin admin(db);

// Trigger GC with configured policy
admin.gc();

// Compact specific log files
admin.gc({file_id1, file_id2, file_id3});

// Check GC status
if (admin.isGCRunning()) {
    admin.waitForGC();
}

// Get statistics
kvlite::DBStats stats;
admin.getStats(stats);
std::cout << "Current version: " << stats.current_version << std::endl;
std::cout << "Active snapshots: " << stats.active_snapshots << std::endl;

// Force flush and L1 snapshot
admin.flush();
admin.snapshotL1Index();
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
options.gc_policy = kvlite::GCPolicy::HIGHEST_DEAD_RATIO;
options.gc_threshold = 0.5;                          // 50% dead triggers GC
options.gc_max_files = 10;                           // Max files per GC run
```

## Design Details

### Versioning

- Global monotonic version counter increments on every write
- WriteBatch: all operations get the same version (atomic snapshot)
- Single put/remove: gets its own version
- Tombstones have versions (enables "key not found at version X")

### Index Structure

Both L1 and L2 indices have the same structure:

```
key → [(version₁, location₁), (version₂, location₂), ...]
       sorted by version, ascending

L1: location = file_id
L2: location = offset within file
```

### Snapshot & GC Interaction

```
Snapshots:    S1(v=100)              S2(v=200)
                 │                       │
Versions:  ──────┼───────────────────────┼───────────▶
           50   100   120   150   180   200   250

GC can collect: versions < 100 (oldest snapshot)
GC cannot collect: versions ≥ 100 (protected by S1)

After S1 released:
GC can collect: versions < 200 (now S2 is oldest)
```

### Write Path

1. Increment global version
2. Write to buffer with version
3. If buffer full → flush to new log file + create L2 index
4. Update L1 index: append (version, file_id) to key's list
5. Append to L1 WAL

### Read Path (latest)

1. L1 lookup: key → get last entry → (version, file_id)
2. L2 lookup: key → get last entry → (version, offset)
3. Read value from log file at offset

### Read Path (by version)

1. L1 lookup: key → binary search for largest version < upper_bound
2. L2 lookup: same binary search
3. Read value from log file at offset

## Examples

### Snapshot for Consistent Backup

```cpp
#include <kvlite/kvlite.h>
#include <fstream>

void backupToFile(kvlite::DB& db, const std::string& backup_path) {
    // Create snapshot - all reads will see consistent state
    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    db.createSnapshot(snapshot);
    std::cout << "Backing up at version " << snapshot->version() << std::endl;

    std::ofstream out(backup_path);

    // Even if writes happen during backup, we see consistent data
    std::vector<std::string> keys_to_backup = {"config", "users", "sessions"};
    for (const auto& key : keys_to_backup) {
        std::string value;
        uint64_t version;
        if (snapshot->get(key, value, version).ok()) {
            out << key << ":" << version << ":" << value << "\n";
        }
    }

    // Release when done - allows GC of old versions
    db.releaseSnapshot(std::move(snapshot));
}
```

### Multiple Concurrent Snapshots

```cpp
#include <kvlite/kvlite.h>
#include <thread>

void demonstrateConcurrentSnapshots(kvlite::DB& db) {
    db.put("counter", "100");  // version 1

    // Snapshot A sees counter=100
    std::unique_ptr<kvlite::DB::Snapshot> snapshotA;
    db.createSnapshot(snapshotA);

    db.put("counter", "200");  // version 2

    // Snapshot B sees counter=200
    std::unique_ptr<kvlite::DB::Snapshot> snapshotB;
    db.createSnapshot(snapshotB);

    db.put("counter", "300");  // version 3

    std::string vA, vB, vLatest;
    snapshotA->get("counter", vA);   // "100"
    snapshotB->get("counter", vB);   // "200"
    db.get("counter", vLatest);      // "300"

    std::cout << "Snapshot A (v" << snapshotA->version() << "): " << vA << std::endl;
    std::cout << "Snapshot B (v" << snapshotB->version() << "): " << vB << std::endl;
    std::cout << "Latest: " << vLatest << std::endl;

    // GC can only collect versions < snapshotA->version()
    // After releasing A, GC can collect up to snapshotB->version()
    db.releaseSnapshot(std::move(snapshotA));
    db.releaseSnapshot(std::move(snapshotB));
}
```

### ReadBatch for Atomic Multi-Key Reads

```cpp
#include <kvlite/kvlite.h>
#include <iostream>

void loadUserProfile(kvlite::DB& db, const std::string& user_id) {
    kvlite::ReadBatch batch;

    // Add all keys we need to read
    batch.get("user:" + user_id + ":name");
    batch.get("user:" + user_id + ":email");
    batch.get("user:" + user_id + ":preferences");
    batch.get("user:" + user_id + ":last_login");

    // Execute batch - all reads at same consistent version
    db.read(batch);

    std::cout << "Profile loaded at version " << batch.snapshotVersion() << std::endl;

    // Process results
    for (const auto& result : batch.results()) {
        if (result.ok()) {
            std::cout << result.key << " = " << result.value
                      << " (v" << result.version << ")" << std::endl;
        } else if (result.notFound()) {
            std::cout << result.key << " not found" << std::endl;
        }
    }
}
```

### ReadBatch with Bulk Keys

```cpp
#include <kvlite/kvlite.h>

std::map<std::string, std::string> bulkGet(
    kvlite::DB& db,
    const std::vector<std::string>& keys
) {
    kvlite::ReadBatch batch;
    batch.get(keys);  // Add all keys at once

    db.read(batch);

    std::map<std::string, std::string> result;
    for (const auto& r : batch.results()) {
        if (r.ok()) {
            result[r.key] = r.value;
        }
    }
    return result;
}

// Usage
void example(kvlite::DB& db) {
    std::vector<std::string> product_keys = {
        "product:1001", "product:1002", "product:1003"
    };

    auto products = bulkGet(db, product_keys);
    for (const auto& [key, value] : products) {
        std::cout << key << ": " << value << std::endl;
    }
}
```

### Combining Snapshot with Version History

```cpp
#include <kvlite/kvlite.h>
#include <kvlite/db_admin.h>

void auditKeyHistory(kvlite::DB& db, const std::string& key) {
    std::string value;
    uint64_t version;

    // Get current version info
    if (!db.get(key, value, version).ok()) {
        std::cout << "Key not found" << std::endl;
        return;
    }

    std::cout << "Current: " << value << " (v" << version << ")" << std::endl;

    // Walk backwards through history (if versions still retained)
    kvlite::DBAdmin admin(db);
    kvlite::DBStats stats;
    admin.getStats(stats);

    for (uint64_t v = version; v > stats.oldest_version; ) {
        std::string old_value;
        uint64_t entry_version;
        if (db.getByVersion(key, v, old_value, entry_version).ok()) {
            std::cout << "v" << entry_version << ": " << old_value << std::endl;
            v = entry_version;  // Move to previous version
        } else {
            break;  // No more history
        }
    }
}
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

Design inspired by:
- [Pliops XDP (VLDB 2021)](https://www.vldb.org/pvldb/vol14/p2932-dayan.pdf)
- LevelDB, RocksDB, and other LSM-tree storage engines
