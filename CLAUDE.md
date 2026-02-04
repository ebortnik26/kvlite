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
| `kvlite_seg_test` | `test_segment.cpp` | Segment (LogFile + L2Index pair) |
| `kvlite_wb_test` | `test_write_buffer.cpp` | WriteBuffer + flush to Segment |
| `kvlite_l2_test` | `test_l2_index.cpp` | L2Index serialization/queries |
| `kvlite_dht_test` | `test_delta_hash_table.cpp` | L1 DeltaHashTable |
| `kvlite_lf_test` | `test_log_file.cpp` | LogFile POSIX I/O |
| `kvlite_bitstream_test` | `test_bit_stream.cpp` | BitStream encoding |

Unit test targets link only the sources they need (not the full library). The integration test (`kvlite_test`) links the full `kvlite` library.

## Architecture

kvlite is an **index-plus-log** key-value store inspired by the Pliops XDP paper. Point operations only (get/put/remove), no range queries. C++17, POSIX I/O.

### Two-Level Indexing

- **L1 Index** (`l1_index.h`): Always in memory. Maps key → list of file_ids (latest first). Persisted via WAL + periodic snapshots. Managed by `L1IndexManager`.
- **L2 Index** (`l2_index.h`): Per-file. Maps key → list of (offset, version) pairs. Stored inside each Segment file. Cached in `L2Cache` (LRU).

Both indexes use `DeltaHashTable` — a compact hash table with per-bucket spinlocks, bit-packed slot encoding (`LSlotCodec`/`L2LSlotCodec`), and overflow chain buckets.

### Storage Layer

- **Segment** (`segment.h`): Owns a LogFile + L2Index pair. State machine: `Closed → Writing → Readable`. Writing state: `put()` serializes LogEntry (header + key + value + CRC32), appends to file, updates index. `seal()` writes L2 index + footer, transitions to Readable. Readable state: `getLatest()`/`get()` read and CRC-validate entries.
- **LogFile** (`log_file.h`): Thin POSIX wrapper. `append()` for writes (not thread-safe), `readAt()` via pread (thread-safe, const).
- **WriteBuffer** (`write_buffer.h`): In-memory staging area with contiguous data buffer and hash index. `flush()` sorts entries by (hash, version), writes to a new Segment, and seals it.

### Write Path

`DB::put` → `VersionManager::allocateVersion` → `StorageManager::writeEntry` (buffers in WriteBuffer) → on capacity: `WriteBuffer::flush` → Segment → `L1IndexManager::put` (updates L1 + WAL).

### Read Path

`DB::get` → `L1IndexManager::getLatest` → file_id → `StorageManager::readValue` → `L2Cache` → L2Index lookup → `DataCache` hit or LogFile read + CRC verify.

### Concurrency Model

- Per-bucket spinlocks in DeltaHashTable (no global lock on indexes)
- `shared_mutex` on L2Cache and DataCache (reader-writer)
- `LogFile::readAt` is thread-safe (pread); `append` is single-threaded (called only from flush)
- Background GC thread with condition_variable signaling; respects oldest active snapshot version

### Key Types

- **Status**: Returned by all fallible operations. Codes: OK, NotFound, Corruption, IOError, InvalidArgument, etc. Check with `.ok()`, `.isNotFound()`, etc.
- **PackedVersion** (`log_entry.h`): 8-byte value encoding version (63 bits) + tombstone flag (MSB).
- **LogEntry** (`log_entry.h`): `{ PackedVersion pv; string key; string value; }`. On-disk format: `[pv:8][key_len:4][val_len:4][key][value][crc32:4]`.

### Naming Conventions

- `k` prefix for constants (`kOk`, `kFooterMagic`)
- `_` suffix for member variables (`fd_`, `state_`)
- Internal implementation lives in `src/internal/`, public API in `include/kvlite/`
