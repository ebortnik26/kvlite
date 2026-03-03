# kvlite Architecture: Index-Plus-Log with Two-Level Indexing

kvlite is a key-value store that supports point operations — get, put,
and remove — but not range queries.  This constraint shapes every design
decision: there is no sorted memtable, no compaction into sorted runs,
and no block index.  Instead, kvlite uses a log-structured storage layer
paired with hash-based indexes that route lookups directly to the right
byte offset.

This post walks through the architecture bottom-up: storage formats,
segments, the two-level index, versioning, and finally the read and write
paths including batches and snapshots.

## Log entries

The fundamental unit of persistent storage is the **log entry**.  Each
entry records a single key-value mutation:

```
┌────────────┬─────────┬───────────┬─────┬───────┬──────────┐
│ packed_ver │ key_len │ value_len │ key │ value │ checksum │
│  8 bytes   │ 2 bytes │  4 bytes  │ var │  var  │ 4 bytes  │
└────────────┴─────────┴───────────┴─────┴───────┴──────────┘
         14-byte header                           CRC32
```

The **packed version** encodes two things in 8 bytes: a 63-bit logical
version number (shifted left by 1) and a 1-bit tombstone flag in the
LSB.  A delete is a log entry with the tombstone bit set and an empty
value.  This encoding preserves numeric ordering — comparing two packed
versions compares their logical versions first, which matters for
version-bounded queries.

The CRC32 checksum covers the header, key, and value (everything except
the checksum field itself).  Corruption is detected at read time before
any data is returned to the caller.

## Segments

A **segment** is a single file containing a sequence of log entries
followed by a per-file index and a footer.  It owns two components: a
`LogFile` (thin POSIX wrapper for I/O) and a `SegmentIndex` (hash-based
lookup structure).

Each segment follows a strict state machine:

```
Closed ──create()──► Writing ──seal()──► Readable
  ▲                                         │
  └──────────────── close() ────────────────┘
```

In the **Writing** state, `put()` serializes each log entry, appends it
to the file via the LogFile, records the entry's offset in the
SegmentIndex, and advances the data size counter.

When writing is complete, `seal()` freezes the segment:

1. Record `data_size` — the byte offset where log entries end.
2. Serialize the SegmentIndex and append it to the file.
3. Append a 16-byte footer: `[segment_id:4][index_offset:8][magic:4]`.
4. Transition to **Readable**.

The resulting file layout:

```
┌──────────────────────────────────────────────┐
│           Log Entries (data region)          │
│  [Entry 0] [Entry 1] ... [Entry N-1]        │
├──────────────────────────────────────────────┤
│           SegmentIndex (serialized)          │
├──────────────────────────────────────────────┤
│           Footer (16 bytes)                  │
│  [segment_id] [index_offset] [magic]         │
└──────────────────────────────────────────────┘
```

Opening an existing segment reads the footer from the last 16 bytes,
validates the magic number, then deserializes the SegmentIndex from the
recorded offset.  The segment enters the Readable state directly.

`LogFile::readAt` uses `pread`, which is thread-safe — multiple threads
can read from the same segment concurrently without coordination.
`LogFile::append` is not thread-safe, but only one thread ever writes
to a segment (the flush path, described later).

## SegmentIndex

Each segment carries a **SegmentIndex** — a hash table mapping keys to
their byte offsets within the segment's data region.  It uses a
`ReadOnlyDeltaHashTable`: entries are added during the Writing phase
(single-threaded, no locks needed), then the table is sealed and becomes
immutable.  After sealing, any number of readers can query the index
concurrently without synchronization.

For a given key, the SegmentIndex stores a list of
`(offset, packed_version)` pairs, sorted latest-first.  A segment can
contain multiple versions of the same key — this happens when the
WriteBuffer (described later) flushes entries that must be preserved for
active snapshots.

Lookup is a hash probe followed by binary search on sorted key suffixes
within the bucket.  The 44-bit suffix (the low bits of the hash not used
for bucket selection) is long enough to eliminate false positives without
reading the actual key from the log file.

The SegmentIndex is serialized as a header, an array of
`(hash, offset, packed_version)` entries, and a CRC32 checksum, then
appended to the segment file during sealing.

## GlobalIndex

The **GlobalIndex** is the primary in-memory index.  It maps each key to
a list of `(packed_version, segment_id)` pairs, sorted latest-first.
Given a key, the GlobalIndex tells you which segment file contains its
latest (or a specific historical) version — without touching any segment.

The GlobalIndex uses a `ReadWriteDeltaHashTable` — the always-mutable
variant with per-bucket spinlocks.  It is always resident in memory and
is updated on every flush.

The GlobalIndex is persisted through two mechanisms:

**Write-ahead log (WAL).**  Every mutation (put, relocate, eliminate) is
logged to a WAL file before being applied.  WAL records are batched:
multiple entries accumulate in memory and are committed as a single
transaction with a CRC-protected commit record.

**Periodic savepoints.**  A background task periodically snapshots the
entire DeltaHashTable to a binary checkpoint.  The savepoint uses an
atomic rename-swap pattern (write to `tmp/`, swap with `valid/`, remove
`old/`) so a crash mid-savepoint leaves the previous checkpoint intact.
After a successful savepoint, WAL files older than the checkpoint version
are deleted.

On recovery, kvlite loads the most recent savepoint, then replays any WAL
records written after it.

## Two-level index coordination

The two indexes serve complementary roles:

| Index | Scope | Maps to | Mutability |
|-------|-------|---------|------------|
| GlobalIndex | All segments | key → segment_id | Always mutable |
| SegmentIndex | One segment | key → byte offset | Immutable after seal |

A point lookup follows two hops:

1. **GlobalIndex**: key → segment_id (which file?)
2. **SegmentIndex**: key → offset within that file (where in the file?)

Neither index stores full keys.  Both use 44-bit hash suffixes for
disambiguation, which is sufficient to avoid false positives in practice.
The actual key is only read from the log file when returning data to the
caller.

This separation keeps the GlobalIndex compact — it stores segment IDs
(4 bytes each) rather than file offsets, and doesn't need to be updated
when segments are rewritten during garbage collection (only the segment
ID mapping changes, not the per-entry offsets).

## Versions

Every mutation in kvlite receives a **version** — a monotonically
increasing 64-bit counter managed by the `VersionManager`.

`allocateVersion()` atomically increments the counter and returns the
new value.  To avoid writing the counter to disk on every allocation,
persistence uses block-aligned checkpointing: the on-disk value advances
in fixed blocks (default 2^20).  When the in-memory counter crosses a
block boundary, the next block's upper bound is persisted to the
Manifest.  On crash recovery, the counter resumes from the persisted
value — at most `block_size - 1` versions are wasted.  On clean
shutdown, the exact counter is persisted.

Versions also support a **commit protocol**.  After allocating a version
and buffering the write, the caller calls `commitVersion(v)`, which
spins until version `v-1` is committed, then publishes `v` as the new
committed version.  This sequential commitment ensures that snapshots
(described later) observe all-or-nothing semantics for concurrent writes.

## Write path

A `put(key, value)` call flows through three stages:

**1. Version allocation.**  `VersionManager::allocateVersion()` returns
the next version number.

**2. Buffering.**  The entry is inserted into the active **WriteBuffer**,
an in-memory staging area.  The WriteBuffer maintains a mutable
`Memtable` — a contiguous byte array for entry data plus a hash index
with per-bucket spinlocks for concurrent access.

**3. Version commit.**  `commitVersion(v)` publishes the version, making
it visible to snapshot-based reads.

If the active Memtable exceeds its size threshold, it is **sealed**
(marked immutable) and enqueued for background flushing.  A new empty
Memtable takes its place.  If the flush queue is full, the writer stalls
until a slot opens — this provides natural backpressure.

A `remove(key)` follows the same path, but writes a tombstone entry
(tombstone bit set, empty value).

### Flush

A background daemon thread processes sealed Memtables in FIFO order:

1. **Create a new segment** in the Writing state.
2. **Extract and deduplicate entries** from the Memtable.  Entries are
   grouped by key hash and sorted by version.  If no snapshots are
   active, only the latest version of each key is kept.  If snapshots
   exist, a two-pointer algorithm identifies which historical versions
   must be preserved.
3. **Write entries** to the segment via `Segment::put()` — each entry is
   serialized with its CRC32 and appended to the log file.
4. **Seal the segment** — the SegmentIndex and footer are written.
5. **Update the GlobalIndex** — each flushed entry's
   `(hash, packed_version, segment_id)` tuple is staged in the WAL and
   applied to the in-memory DeltaHashTable, then committed as a batch.
6. **Register the segment** in the Manifest (the durable commit point).

During flushing, the sealed Memtable remains readable — the WriteBuffer
keeps a `flushing_` pointer so that in-flight reads can still find data
that hasn't yet appeared in the GlobalIndex.  Once the flush completes
and the data is accessible via the GlobalIndex, the Memtable is retired.

## Read path

A `get(key)` call searches two locations in order:

**1. WriteBuffer.**  The active Memtable is checked first, then the
immutable queue (newest to oldest), then the currently-flushing Memtable.
A hit here returns immediately without any disk I/O.

**2. GlobalIndex → Segment.**  If the WriteBuffer doesn't contain the
key, the GlobalIndex is queried.  It returns the `segment_id` and
`packed_version` of the latest entry.  The packed version is checked for
the tombstone bit — if set, the key has been deleted, and `NotFound` is
returned without touching the segment file.

If the entry is live, the segment is accessed:

- The **SegmentIndex** maps the key to a byte offset within the segment's
  data region.
- `LogFile::readAt` issues a `pread` at that offset.  A speculative
  4 KB read is attempted first; if the entry is larger, a second read
  fetches the exact size.
- The CRC32 checksum is validated.  On mismatch, a `Corruption` status
  is returned.
- The key and value are extracted from the buffer and returned.

The tombstone-before-I/O optimization is significant: deleted keys never
cause a segment read.  The GlobalIndex already carries the tombstone flag
in the packed version, so the read path can short-circuit.

## Snapshots

A **snapshot** captures a point-in-time view of the database.  Creating
a snapshot records the current version counter and registers it in a
tracked set:

```cpp
uint64_t version = current_version_.load(acquire);
active_snapshots_.insert(version);
```

When a read uses a snapshot, the version acts as an upper bound: only
entries with `packed_version <= (snapshot_version << 1) | 1` are visible.
The `| 1` ensures tombstones at the snapshot version are included (so
deleted keys correctly return `NotFound`).

Before executing a snapshot read, `waitForCommitted(snapshot_version)` is
called.  This spins until all versions up to the snapshot version have
been committed.  Without this wait, a concurrent write that allocated a
version before the snapshot but hasn't committed yet could be missed —
violating point-in-time consistency.

Snapshots also affect **garbage collection and flushing**.  The
VersionManager exposes `oldestSnapshotVersion()`, which the flush path
uses during deduplication: historical versions that are visible to any
active snapshot are preserved rather than discarded.  Releasing a
snapshot removes it from the tracked set, allowing those versions to be
collected on the next flush.

## Batch operations

### WriteBatch

A `WriteBatch` groups multiple puts and deletes into a single atomic
unit.  All operations in the batch receive the **same version number**,
so they become visible together.

Atomicity within the Memtable is enforced through **two-phase bucket
locking**:

1. Pre-compute the hash and bucket index for every operation.
2. Sort operations by bucket index.
3. Acquire all bucket spinlocks in sorted order (preventing deadlocks).
4. Insert all entries while holding all locks.
5. Release locks in reverse order.

After insertion, `commitVersion(v)` publishes the batch version.  Because
all operations share a single version and commitment is sequential, any
snapshot taken before the commit sees none of the batch's entries, and
any snapshot taken after sees all of them.

### ReadBatch

A `ReadBatch` reads multiple keys with **snapshot consistency** — all
keys are read at the same point in time, even if concurrent writes are
interleaved.

If the caller provides a snapshot, it is used directly.  Otherwise, the
ReadBatch internally creates a temporary snapshot, reads each key at that
version, and releases the snapshot when done.  Each result carries its
own status (a key can be `NotFound` without failing the batch), and the
snapshot version is recorded for the caller to inspect.

The implementation is straightforward: a loop over the keys, each calling
the regular `get()` path with the shared snapshot.  Consistency comes
entirely from the snapshot mechanism — no additional locking is needed.

## Putting it together

The architecture can be summarized as a pipeline:

```
put/remove ──► VersionManager ──► WriteBuffer (Memtable)
                                       │
                                  [seal on capacity]
                                       │
                              Background Flush Daemon
                                       │
                    ┌──────────────────┬┘
                    ▼                  ▼
               Segment            GlobalIndex
           (LogFile + Index)    (WAL + Savepoint)
```

```
get ──► WriteBuffer ──miss──► GlobalIndex ──► SegmentIndex ──► LogFile
             │                                                    │
           [hit]                                              [pread + CRC]
             │                                                    │
             ▼                                                    ▼
           return                                              return
```

The index-plus-log design trades range query support for simplicity and
speed at point operations.  Hash-based two-level indexing provides O(1)
routing from key to byte offset.  The WriteBuffer absorbs writes in
memory and flushes them in sorted batches, while snapshots provide
consistent reads without global locks.  Versioned entries and sequential
commitment give batch operations all-or-nothing visibility without a
write-ahead log at the application level — the version counter alone is
sufficient.
