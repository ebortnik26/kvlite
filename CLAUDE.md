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
| `kvlite_test` | `test_basic.cpp`, `test_batch.cpp`, `test_versioning.cpp`, `test_iterator.cpp`, `test_concurrency.cpp` | Integration tests (full DB) |
| `kvlite_seg_test` | `test_segment.cpp` | Segment (LogFile + SegmentIndex pair) |
| `kvlite_wb_test` | `test_write_buffer.cpp` | WriteBuffer + flush to Segment |
| `kvlite_segment_index_test` | `test_segment_index.cpp` | SegmentIndex serialization/queries |
| `kvlite_dht_test` | `test_delta_hash_table.cpp` | DeltaHashTable |
| `kvlite_lf_test` | `test_log_file.cpp` | LogFile POSIX I/O |
| `kvlite_bitstream_test` | `test_bit_stream.cpp` | BitStream encoding |

Unit test targets link only the sources they need (not the full library). The integration test (`kvlite_test`) links the full `kvlite` library.

## Architecture

kvlite is an **index-plus-log** key-value store inspired by the Pliops XDP paper. Point operations only (get/put/remove), no range queries. C++17, POSIX I/O.

### Two-Level Indexing

- **GlobalIndex** (`global_index.h`): Always in memory. Maps key → list of file_ids (latest first). Persisted via WAL + periodic snapshots. Managed by `GlobalIndexManager`.
- **SegmentIndex** (`segment_index.h`): Per-file. Maps key → list of (offset, version) pairs. Stored inside each Segment file. Cached in `SegmentIndexCache` (LRU).

Both indexes use `DeltaHashTable` — a compact hash table with per-bucket spinlocks, bit-packed slot encoding (`LSlotCodec`/`SegmentLSlotCodec`), and overflow chain buckets.

### Storage Layer

- **Segment** (`segment.h`): Owns a LogFile + SegmentIndex pair. State machine: `Closed → Writing → Readable`. Writing state: `put()` serializes LogEntry (header + key + value + CRC32), appends to file, updates index. `seal()` writes SegmentIndex + footer, transitions to Readable. Readable state: `getLatest()`/`get()` read and CRC-validate entries.
- **LogFile** (`log_file.h`): Thin POSIX wrapper. `append()` for writes (not thread-safe), `readAt()` via pread (thread-safe, const).
- **WriteBuffer** (`write_buffer.h`): In-memory staging area with contiguous data buffer and hash index. `flush()` sorts entries by (hash, version), writes to a new Segment, and seals it.

### Write Path

`DB::put` → `VersionManager::allocateVersion` → `StorageManager::writeEntry` (buffers in WriteBuffer) → on capacity: `WriteBuffer::flush` → Segment → `GlobalIndexManager::put` (updates GlobalIndex + WAL).

### Read Path

`DB::get` → `GlobalIndexManager::getLatest` → file_id → `StorageManager::readValue` → `SegmentIndexCache` → SegmentIndex lookup → `DataCache` hit or LogFile read + CRC verify.

### Concurrency Model

- Per-bucket spinlocks in DeltaHashTable (no global lock on indexes)
- `shared_mutex` on SegmentIndexCache and DataCache (reader-writer)
- `LogFile::readAt` is thread-safe (pread); `append` is single-threaded (called only from flush)
- Background GC thread with condition_variable signaling; respects oldest active snapshot version

### Key Types

- **Status**: Returned by all fallible operations. Codes: OK, NotFound, Corruption, IOError, InvalidArgument, etc. Check with `.ok()`, `.isNotFound()`, etc.
- **PackedVersion** (`log_entry.h`): In-memory 8-byte value encoding version (63 bits) + tombstone flag (MSB).
- **LogEntry** (`log_entry.h`): `{ PackedVersion pv; string key; string value; }`. On-disk format: `[version:8][key_len|tombstone:2][val_len:4][key][value][crc32:4]`. Header is 14 bytes. The key_len field uses 15 bits for length (max 32767) and 1 bit (MSB) for tombstone.

### Naming Conventions

- `k` prefix for constants (`kOk`, `kFooterMagic`)
- `_` suffix for member variables (`fd_`, `state_`)
- Internal implementation lives in `src/internal/`, public API in `include/kvlite/`
