# kvlite API Guide

## Basic Operations

```cpp
// Write (creates new version)
Status put(const std::string& key, const std::string& value,
           const WriteOptions& options = WriteOptions());

// Read latest version
Status get(const std::string& key, std::string& value,
           const ReadOptions& options = ReadOptions());
Status get(const std::string& key, std::string& value, uint64_t& version,
           const ReadOptions& options = ReadOptions());

// Delete (creates tombstone with new version)
Status remove(const std::string& key,
              const WriteOptions& options = WriteOptions());

// Check existence
Status exists(const std::string& key, bool& exists,
              const ReadOptions& options = ReadOptions());
```

## Batch Operations

```cpp
// Atomic write batch - all ops get same version
kvlite::WriteBatch wbatch;
wbatch.put("key1", "value1");
wbatch.put("key2", "value2");
db.write(wbatch);  // Both ops get the same version

// Consistent read batch - all reads at same snapshot
kvlite::ReadBatch rbatch;
rbatch.get("key1");
rbatch.get("key2");
rbatch.get("key3");
db.read(rbatch);

std::cout << "Read at version " << rbatch.snapshotVersion() << std::endl;
for (const auto& result : rbatch.results()) {
    if (result.status.ok()) {
        std::cout << result.key << " = " << result.value
                  << " (v" << result.version << ")" << std::endl;
    } else if (result.status.isNotFound()) {
        std::cout << result.key << " not found" << std::endl;
    }
}
```

## Snapshots

```cpp
// Create a snapshot at current version
kvlite::Snapshot snap = db.createSnapshot();
std::cout << "Snapshot at version " << snap.version() << std::endl;

// Reads through snapshot always see the same point-in-time
kvlite::ReadOptions ro;
ro.snapshot = &snap;

std::string v1, v2;
db.get("key1", v1, ro);

db.put("key1", "new_value");  // New write after snapshot

db.get("key1", v2, ro);   // Still sees old value
assert(v1 == v2);

// Release snapshot to allow GC of old versions
db.releaseSnapshot(snap);
```

## Iteration

Unordered iteration over all keys (useful for full database copy/replication):

```cpp
std::unique_ptr<kvlite::Iterator> iter;
db.createIterator(iter);

std::string key, value;
uint64_t version;
while (iter->next(key, value, version).ok()) {
    std::cout << key << " = " << value << " (v" << version << ")" << std::endl;
}
```

Note: Keys are returned in arbitrary order (not sorted). The iterator scans
segment files sequentially, using the GlobalIndex to filter out old versions.

## Statistics

```cpp
kvlite::DBStats stats;
db.getStats(stats);
std::cout << "Current version: " << stats.current_version << std::endl;
std::cout << "Active snapshots: " << stats.active_snapshots << std::endl;
std::cout << "Live entries: " << stats.num_live_entries << std::endl;
```

## Configuration

```cpp
kvlite::Options options;

// Storage
options.log_file_size = 1ULL * 1024 * 1024 * 1024;  // 1GB per segment file
options.memtable_size = 64 * 1024 * 1024;             // 64MB memtable capacity
options.flush_depth = 3;                              // Pipeline depth (mutable + immutable)
options.segment_partitions = 1;                       // Partition files per segment (power of 2)

// GlobalIndex Persistence
options.savepoint_interval_sec = 10;                  // Savepoint daemon interval (0 = disable)

// Garbage Collection
options.gc_policy = kvlite::GCPolicy::HIGHEST_DEAD_RATIO;
options.gc_threshold = 0.5;                           // 50% dead triggers GC
options.gc_max_segments = 10;                         // Max segments per GC run
options.gc_interval_sec = 10;                         // GC daemon interval (0 = disable)

// General
options.create_if_missing = true;                     // Create DB dir if absent
options.verify_checksums = true;                      // CRC32 verification on reads
```

### Read/Write Options

```cpp
// Per-operation read options
kvlite::ReadOptions ro;
ro.verify_checksums = true;             // Verify CRC32 on this read
ro.fill_cache = true;                   // Cache SegmentIndex if not cached
ro.snapshot = &snap;                    // Read at snapshot's point-in-time

// Per-operation write options
kvlite::WriteOptions wo;
wo.sync = true;                         // fsync this write to disk
db.put("key", "value", wo);
```

## Examples

### Snapshot for Consistent Backup

```cpp
#include <kvlite/kvlite.h>
#include <fstream>

void backupToFile(kvlite::DB& db, const std::string& backup_path) {
    kvlite::Snapshot snap = db.createSnapshot();
    std::cout << "Backing up at version " << snap.version() << std::endl;

    kvlite::ReadOptions ro;
    ro.snapshot = &snap;

    std::ofstream out(backup_path);

    std::vector<std::string> keys_to_backup = {"config", "users", "sessions"};
    for (const auto& key : keys_to_backup) {
        std::string value;
        uint64_t version;
        if (db.get(key, value, version, ro).ok()) {
            out << key << ":" << version << ":" << value << "\n";
        }
    }

    db.releaseSnapshot(snap);
}
```

### Multiple Concurrent Snapshots

```cpp
#include <kvlite/kvlite.h>

void demonstrateConcurrentSnapshots(kvlite::DB& db) {
    db.put("counter", "100");  // version N

    kvlite::Snapshot snapA = db.createSnapshot();
    db.put("counter", "200");  // version N+1

    kvlite::Snapshot snapB = db.createSnapshot();
    db.put("counter", "300");  // version N+2

    kvlite::ReadOptions roA, roB;
    roA.snapshot = &snapA;
    roB.snapshot = &snapB;

    std::string vA, vB, vLatest;
    db.get("counter", vA, roA);      // "100"
    db.get("counter", vB, roB);      // "200"
    db.get("counter", vLatest);      // "300"

    db.releaseSnapshot(snapA);
    db.releaseSnapshot(snapB);
}
```

### ReadBatch for Atomic Multi-Key Reads

```cpp
#include <kvlite/kvlite.h>
#include <iostream>

void loadUserProfile(kvlite::DB& db, const std::string& user_id) {
    kvlite::ReadBatch batch;

    batch.get("user:" + user_id + ":name");
    batch.get("user:" + user_id + ":email");
    batch.get("user:" + user_id + ":preferences");
    batch.get("user:" + user_id + ":last_login");

    db.read(batch);

    std::cout << "Profile loaded at version " << batch.snapshotVersion() << std::endl;

    for (const auto& result : batch.results()) {
        if (result.status.ok()) {
            std::cout << result.key << " = " << result.value
                      << " (v" << result.version << ")" << std::endl;
        } else if (result.status.isNotFound()) {
            std::cout << result.key << " not found" << std::endl;
        }
    }
}
```

### Iterator for Full Scan

```cpp
#include <kvlite/kvlite.h>
#include <iostream>

void dumpAllKeys(kvlite::DB& db) {
    std::unique_ptr<kvlite::Iterator> iter;
    kvlite::Status s = db.createIterator(iter);
    if (!s.ok()) {
        std::cerr << "Failed to create iterator: " << s.message() << std::endl;
        return;
    }

    std::cout << "Iterator snapshot at version "
              << iter->snapshot().version() << std::endl;

    std::string key, value;
    uint64_t version;
    size_t count = 0;
    while (iter->next(key, value, version).ok()) {
        std::cout << key << " = " << value << " (v" << version << ")" << std::endl;
        ++count;
    }

    std::cout << "Total keys: " << count << std::endl;
}
```
