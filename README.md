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
+---------------------------------------------------------------+
|                      DB  (public API)                          |
|           put . get . remove . write . read . snapshot         |
+---------------------------------------------------------------+
|                                                                |
|  VersionManager    WriteBuffer        GlobalIndex              |
|  (monotonic IDs)   (Memtable xN)      (in-memory hash index)  |
|                         |                  |                   |
|                         | flush            | lookup            |
|                         v                  v                   |
|               SegmentStorageManager   SegmentIndex Cache       |
|                         |                  |  (LRU)            |
|                         v                  v                   |
|   +----------+  +----------+  +----------+                    |
|   | Segment  |  | Segment  |  | Segment  |  ...                |
|   | K files  |  | K files  |  | K files  |                     |
|   |(parallel)|  |(parallel)|  |(parallel)|                     |
|   +----------+  +----------+  +----------+                     |
+---------------------------------------------------------------+
|  Manifest  .  Segment Lineage  .  GlobalIndex Savepoints       |
+---------------------------------------------------------------+
|  Background:  GC Daemon (compaction) . Savepoint Daemon        |
+---------------------------------------------------------------+
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

    db.put("key", "value1");

    kvlite::Snapshot snap = db.createSnapshot();
    db.put("key", "value2");

    std::string value;
    db.get("key", value);
    std::cout << value << std::endl;  // "value2"

    kvlite::ReadOptions ro;
    ro.snapshot = &snap;
    db.get("key", value, ro);
    std::cout << value << std::endl;  // "value1"

    db.releaseSnapshot(snap);
    db.close();
    return 0;
}
```

## Documentation

- [API Guide](blogs/api-guide.md) — full API reference, configuration, code examples
- [Design Principles](blogs/design-principles.md) — versioning, index structure, snapshots, read/write paths
- [Architecture](blogs/architecture.md) — segments, lineage, two-level indexing, flush and GC pipelines
- [Crash Recovery](blogs/crash-recovery.md) — storage-embedded lineage, recovery algorithm, crash scenarios
- [DHT Internals](blogs/dht-internals.md) — bucket encoding, columnar codec, concurrency model

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

Design inspired by:
- [Bitcask](https://riak.com/assets/bitcask-intro.pdf) — the index-plus-log model and in-memory key directory
- [LevelDB](https://github.com/google/leveldb), [RocksDB](https://github.com/facebook/rocksdb), and other LSM-tree storage engines
