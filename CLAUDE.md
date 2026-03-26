# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Configure (from repo root)
cmake -B build -DKVLITE_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release

# Build everything
cmake --build build

# Build a single test target
cmake --build build --target kvlite_seg_test

# Run all tests
cd build && ctest

# Run a specific test executable
./build/tests/kvlite_seg_test

# Run a specific test case
./build/tests/kvlite_seg_test --gtest_filter=SegmentTest.SealThenGetLatest
```

## Test Targets

| Target | File | Scope |
|--------|------|-------|
| `kvlite_int_basic` | `test_basic.cpp` | Integration: put/get/remove |
| `kvlite_int_batch` | `test_batch.cpp` | Integration: WriteBatch/ReadBatch |
| `kvlite_int_versioning` | `test_versioning.cpp` | Integration: version ordering |
| `kvlite_int_iterator` | `test_iterator.cpp` | Integration: full-scan iterator |
| `kvlite_int_concurrency` | `test_concurrency.cpp` | Integration: concurrent access |
| `kvlite_int_db` | `test_db.cpp` | Integration: open/close, recovery, partitions |
| `kvlite_int_gc_daemon` | `test_gc_daemon.cpp` | Integration: GC + savepoint daemons |
| `kvlite_seg_test` | `test_segment.cpp` | Segment + SegmentPartition |
| `kvlite_wb_test` | `test_write_buffer.cpp` | WriteBuffer + Memtable flush |
| `kvlite_segment_index_test` | `test_segment_index.cpp` | SegmentIndex + GC integration |
| `kvlite_dht_test` | `test_delta_hash_table.cpp` | DeltaHashTable, GlobalIndex, savepoint, recovery |
| `kvlite_sm_test` | `test_segment_storage_manager.cpp` | SegmentStorageManager lifecycle |
| `kvlite_vm_test` | `test_version_manager.cpp` | VersionManager persistence |
| `kvlite_manifest_test` | `test_manifest.cpp` | Manifest KV store |
| `kvlite_entry_stream_test` | `test_entry_stream.cpp` | EntryStream + GC pipeline |
| `kvlite_lf_test` | `test_log_file.cpp` | LogFile POSIX I/O |
| `kvlite_bitstream_test` | `test_bit_stream.cpp` | BitStream encoding |

Unit test targets link only the sources they need (not the full library). The integration tests (`kvlite_int_*`) each link the full `kvlite` library.

## Architecture

kvlite is an **index-plus-log** key-value store. Point operations only (get/put/remove), no range queries. C++17, POSIX I/O.

### Two-Level Indexing

- **GlobalIndex** (`global_index.h`): Always in memory. Maps key → list of (packed_version, segment_id) pairs (latest first). Persisted via segment lineage sections + periodic savepoints.
- **SegmentIndex** (`segment_index.h`): Per-file. Maps key → list of (offset, packed_version) pairs. Stored inside each Segment file. Cached in `SegmentIndexCache` (LRU).

Both indexes use the **DeltaHashTable** family — compact hash tables with sorted suffix-array buckets and overflow chain buckets:

```
DeltaHashTable                      (non-template base, .h + .cpp)
├── ReadOnlyDeltaHashTable          (write-once lifecycle, no locks)
└── ReadWriteDeltaHashTable         (always-mutable, per-bucket spinlocks)
```

Each bucket stores unique key suffixes (the hash bits not used for bucket selection) in sorted order, with per-key version/id arrays. Binary search replaces fingerprint matching. Full suffix comparison eliminates false positives — no I/O for disambiguation.

### Storage Layer

- **Segment** (`segment.h`): Routes entries by hash across K `SegmentPartition` files (K=1 by default, configurable via `Options::segment_partitions`). State machine: `Closed → Writing → Readable`. Write methods dispatch to `partitions_[hash >> (64 - partition_bits)]`. `seal()` finalizes all partitions in parallel via a `FlushPool`. File naming: `segment_<id>_<partition>.data`.
- **SegmentPartition** (`segment.h`): One physical file owning a LogFile + SegmentIndex + lineage section. `put()`/`appendRawEntry()` serialize entries and record lineage. `seal()` writes SegmentIndex + lineage + footer + fdatasync. Footer: `[segment_id:4][index_offset:8][lineage_offset:8][magic:4]` (24 bytes). Segment discovers K by probing partition files on open.
- **LogFile** (`log_file.h`): Thin POSIX wrapper. `append()` for writes (not thread-safe), `readAt()` via pread (thread-safe, const).
- **WriteBuffer** (`write_buffer.h`): In-memory staging area with contiguous data buffer and hash index. `flush()` sorts entries by (hash, version), writes to a new Segment, and seals it.

### Write Path

`DB::put` → `VersionManager::allocateVersion` → `SegmentStorageManager::writeEntry` (buffers in WriteBuffer) → on capacity: `WriteBuffer::flush` → Segment → `GlobalIndex::applyPut` (updates in-memory GlobalIndex).

### Version Persistence

`VersionManager` (`version_manager.h`): Block-aligned write-ahead persistence. In-memory counter increments by 1 per allocation. Persisted counter advances in fixed blocks of `block_size` (default 2^20) — when in-memory counter exceeds `persisted_counter_`, persists `persisted_counter_ + block_size` to Manifest. On crash recovery, resumes from persisted value (wastes at most `block_size - 1` versions). On clean close, persists exact counter.

### Read Path

`DB::get` → `GlobalIndexManager::getLatest` → file_id → `SegmentStorageManager::readValue` → `SegmentIndexCache` → SegmentIndex lookup → LogFile read + CRC verify.

### Concurrency Model

- Per-bucket spinlocks in ReadWriteDeltaHashTable (GlobalIndex); no locks in ReadOnlyDeltaHashTable (SegmentIndex)
- `shared_mutex` on SegmentIndexCache (reader-writer)
- `LogFile::readAt` is thread-safe (pread); `append` is single-threaded (called only from flush)
- Background GC thread with condition_variable signaling; respects oldest active snapshot version

### Key Types

- **Status**: Returned by all fallible operations. Codes: OK, NotFound, Corruption, IOError, InvalidArgument, etc. Check with `.ok()`, `.isNotFound()`, etc.
- **PackedVersion** (`log_entry.h`): In-memory 8-byte value encoding version (63 bits) + tombstone flag (MSB).
- **LogEntry** (`log_entry.h`): `{ PackedVersion pv; string key; string value; }`. On-disk format: `[packed_ver:8][key_len:2][val_len:4][key][value][crc32:4]`. Header is 14 bytes. packed_ver encodes (logical_version << 1) | tombstone_bit. key_len uses full 16 bits (max 65535).

### Naming Conventions

- `k` prefix for constants (`kOk`, `kFooterMagic`)
- `_` suffix for member variables (`fd_`, `state_`)
- Internal implementation lives in `src/internal/`, public API in `include/kvlite/`
