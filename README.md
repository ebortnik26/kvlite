# kvlite

A persistent key-value store for point operations on SSD.

## What it does

- **Simple API** — get, put, remove, exists.  No range queries, no SQL.
- **Snapshots** — lightweight point-in-time reads.  Multiple concurrent snapshots at different versions.
- **Atomic batches** — WriteBatch groups puts and deletes into a single version (all-or-nothing visibility).  ReadBatch reads multiple keys at the same snapshot.
- **Unordered iteration** — full-scan iterator over all keys, useful for backup/replication.
- **Background GC** — compacts dead versions automatically, respects active snapshots.

## How it works

- **Index-plus-log** — two-level hash index (in-memory GlobalIndex + per-file SegmentIndex) with append-only data files.
- **SSD-friendly** — sequential writes, no in-place updates.
- **Multi-version** — global monotonic versioning; every write gets a unique version.
- **Partitioned flush** — each segment is K files written and synced in parallel.
- **No WAL** — durability via segment-embedded lineage sections; recovery replays lineage from segments above the last savepoint.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DB  (public API)                           │
│           put · get · remove · write · read · snapshot          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  VersionManager    WriteBuffer        GlobalIndex               │
│  (monotonic IDs)   (Memtable ×N)      (in-memory DHT)          │
│                         │                  │                    │
│                         │ flush            │ lookup             │
│                         ▼                  ▼                    │
│               SegmentStorageManager   SegmentIndex Cache        │
│                         │                  │  (LRU)             │
│                         ▼                  ▼                    │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐                     │
│   │ Segment  │  │ Segment  │  │ Segment  │  ...                 │
│   │ K files  │  │ K files  │  │ K files  │                      │
│   │(parallel)│  │(parallel)│  │(parallel)│                      │
│   └──────────┘  └──────────┘  └──────────┘                      │
├─────────────────────────────────────────────────────────────────┤
│  Manifest  ·  Segment Lineage  ·  GlobalIndex Savepoints        │
├─────────────────────────────────────────────────────────────────┤
│  Background:  GC Daemon (compaction) · Savepoint Daemon         │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

```cpp
#include <kvlite/kvlite.h>
#include <iostream>

int main() {
    kvlite::DB db;
    kvlite::Options options;
    options.create_if_missing = true;

    db.open("/tmp/mydb", options);

    // Write
    db.put("key", "value1");

    // Take a snapshot before updating
    kvlite::Snapshot snap = db.createSnapshot();

    // Update
    db.put("key", "value2");

    // Read latest
    std::string value;
    uint64_t version;
    db.get("key", value, version);
    std::cout << value << " (v" << version << ")" << std::endl;  // "value2"

    // Read at snapshot (sees state before the update)
    kvlite::ReadOptions ro;
    ro.snapshot = &snap;
    db.get("key", value, ro);
    std::cout << value << std::endl;  // "value1"

    db.releaseSnapshot(snap);
    db.close();
    return 0;
}
```

## API Reference

### Basic Operations

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

### Batch Operations

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

### Snapshots

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

### Iteration

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

### Statistics

```cpp
kvlite::DBStats stats;
db.getStats(stats);
std::cout << "Current version: " << stats.current_version << std::endl;
std::cout << "Active snapshots: " << stats.active_snapshots << std::endl;
std::cout << "Live entries: " << stats.num_live_entries << std::endl;
```

Note: Garbage collection runs automatically in the background based on the
configured `gc_policy` and `gc_threshold` options.

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

## Design Details

### Versioning

- Global monotonic version counter increments on every write
- WriteBatch: all operations get the same version (atomic snapshot)
- Single put/remove: gets its own version
- Tombstones have versions (enables "key not found at version X")
- Version counter persisted to Manifest using block-aligned write-ahead: advances in fixed blocks (default 2^20), bounding crash-recovery waste to one block

### Index Structure

Both GlobalIndex and SegmentIndex use the same DeltaHashTable structure:

```
key → [(version₁, location₁), (version₂, location₂), ...]
       sorted by version, descending (latest first)

GlobalIndex:   location = segment_id
SegmentIndex:  location = offset within file
```

### Snapshots

- Lightweight: a snapshot is just a pinned version number (~8 bytes), no data is copied
- Reads through a snapshot (via `ReadOptions`) return the value visible at that version (highest version <= snapshot version)
- Keys written after the snapshot are invisible; keys deleted after are still visible
- Multiple snapshots can coexist at different versions
- Iterators use snapshots internally: `createIterator()` creates an owned snapshot
- `ReadBatch` acquires an implicit snapshot (records the version used, but does not pin it for GC)
- Releasing a snapshot allows GC to reclaim the versions it was protecting

### Snapshot & GC Interaction

GC retains exactly **one version per key per observation point** (each active snapshot + latest). All intermediate versions between observation points are dropped.

```
Snapshots:    S1(v=100)              S2(v=200)
                 │                       │
Versions:  ──────┼───────────────────────┼───────────▶
           50   100   120   150   180   200   250

For a key with versions 50, 120, 180, 250:
  - S1 sees v100 → keeps v50 (highest <= 100)
  - S2 sees v200 → keeps v180 (highest <= 200)
  - Latest       → keeps v250
  - v120 dropped (not visible at any observation point)

After S1 released:
  - v50 dropped (no longer needed)
  - v180, v250 retained

No active snapshots:
  - Only the latest version per key survives GC
```

### Write Path

1. VersionManager allocates a monotonic version
2. Entry buffered in WriteBuffer (in-memory Memtable)
3. When buffer full → flush: write sorted entries to new Segment (K partition files), seal with SegmentIndex + lineage per partition (parallel fdatasync when K > 1)
4. Update in-memory GlobalIndex with (key, version, segment_id) for each flushed entry

### Read Path (latest)

1. Check WriteBuffer for unflushed entries (in-memory hit)
2. GlobalIndex lookup: key → first entry → (version, segment_id)
3. SegmentIndex lookup (from cache or disk): key → (version, offset)
4. Read value from segment data file at offset, verify CRC32

### Read Path (by snapshot)

1. Check WriteBuffer for matching entries <= snapshot version
2. GlobalIndex lookup: key → scan for largest version <= snapshot version
3. SegmentIndex lookup: same scan within the target segment
4. Read value from segment data file at offset, verify CRC32

## Examples

### Snapshot for Consistent Backup

```cpp
#include <kvlite/kvlite.h>
#include <fstream>

void backupToFile(kvlite::DB& db, const std::string& backup_path) {
    // Create snapshot - all reads will see consistent state
    kvlite::Snapshot snap = db.createSnapshot();
    std::cout << "Backing up at version " << snap.version() << std::endl;

    kvlite::ReadOptions ro;
    ro.snapshot = &snap;

    std::ofstream out(backup_path);

    // Even if writes happen during backup, we see consistent data
    std::vector<std::string> keys_to_backup = {"config", "users", "sessions"};
    for (const auto& key : keys_to_backup) {
        std::string value;
        uint64_t version;
        if (db.get(key, value, version, ro).ok()) {
            out << key << ":" << version << ":" << value << "\n";
        }
    }

    // Release when done - allows GC of old versions
    db.releaseSnapshot(snap);
}
```

### Multiple Concurrent Snapshots

```cpp
#include <kvlite/kvlite.h>

void demonstrateConcurrentSnapshots(kvlite::DB& db) {
    db.put("counter", "100");  // version N

    // Snapshot A sees counter=100
    kvlite::Snapshot snapA = db.createSnapshot();

    db.put("counter", "200");  // version N+1

    // Snapshot B sees counter=200
    kvlite::Snapshot snapB = db.createSnapshot();

    db.put("counter", "300");  // version N+2

    kvlite::ReadOptions roA, roB;
    roA.snapshot = &snapA;
    roB.snapshot = &snapB;

    std::string vA, vB, vLatest;
    db.get("counter", vA, roA);      // "100"
    db.get("counter", vB, roB);      // "200"
    db.get("counter", vLatest);      // "300"

    std::cout << "Snapshot A (v" << snapA.version() << "): " << vA << std::endl;
    std::cout << "Snapshot B (v" << snapB.version() << "): " << vB << std::endl;
    std::cout << "Latest: " << vLatest << std::endl;

    // GC can only collect versions < snapA.version()
    // After releasing A, GC can collect up to snapB.version()
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

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

Design inspired by:
- [Bitcask](https://riak.com/assets/bitcask-intro.pdf) — the index-plus-log model and in-memory key directory
- LevelDB, RocksDB, and other LSM-tree storage engines
