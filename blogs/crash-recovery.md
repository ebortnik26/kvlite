# Crash Recovery in kvlite: Storage-Embedded Lineage

kvlite is an index-plus-log key-value store.  Its primary in-memory
structure, the **GlobalIndex**, maps every key to the segment file
containing its latest value.  This index must survive crashes.  The
conventional approach is a write-ahead log (WAL) — a separate file
that records every index mutation before it happens.  kvlite takes a
different path: it embeds the recovery information directly in the
segment files themselves.

This post explains how that works and why a segment is the fundamental
unit of both storage and recovery.

## The problem

When the process crashes, the in-memory GlobalIndex is lost.  On
restart, kvlite must reconstruct it — figure out which keys live in
which segments, at which versions.  The data is all in the segment
files, but scanning every entry in every file would be prohibitively
slow for a large database.

Two mechanisms make recovery fast:

1. **Savepoints** — periodic full snapshots of the GlobalIndex written
   to disk.  Loading a savepoint restores the index to a known state
   in one bulk read.

2. **Lineage sections** — compact summaries embedded in each segment
   file that describe exactly which GlobalIndex mutations that segment
   contributes.

Recovery loads the most recent savepoint, then replays lineage from
the segments created after it.  No separate log file, no WAL.

## Segments as the unit of recovery

A segment in kvlite is not a single file — it is a logical group of K
partition files (`segment_7_0.data` through `segment_7_3.data`), where
K is configurable (default 1).  Each partition is a self-contained
`SegmentPartition` with its own data region, SegmentIndex, lineage
section, and footer.

The critical property: **a segment is atomic with respect to recovery.**
Either all of its partition files are sealed (footer present, data
synced to disk), or the segment is treated as if it never existed.
There is no partial segment — if any partition file is missing or
corrupt, the entire segment is discarded during recovery.

This atomicity comes from two ordering constraints:

1. The segment is sealed (all partitions `fdatasync`'d) **before** it
   is registered in the Manifest.
2. The GlobalIndex is updated **after** the segment is sealed.

If a crash happens during sealing, the Manifest doesn't know about the
segment, so recovery ignores its files.  If a crash happens after
sealing but before the GlobalIndex update, the segment's lineage
section carries the information needed to reconstruct the missing
index entries.

## What lineage records

Each partition file carries a lineage section between its SegmentIndex
and its footer:

```
[Data entries] [SegmentIndex] [Lineage] [Footer]
```

The lineage section has a 13-byte header followed by entry payloads
and a CRC32 checksum:

```
Header:
  magic:4             "LING" (0x4C494E47)
  type:1              kFlush (1) or kGC (2)
  entry_count:4       number of put/relocate entries
  elimination_count:4 number of GC eliminations

Put entries (16 bytes each):
  hkey:8              FNV-1a hash of the key
  packed_version:8    (logical_version << 1) | tombstone_bit

Elimination entries (20 bytes each, GC segments only):
  hkey:8
  packed_version:8
  old_segment_id:4    the segment this entry was removed from

CRC32:4               covers header + all entries
```

For a **flush segment**, lineage records every key written — the hash
and packed version.  The segment ID is implicit (it's the segment
containing the lineage).  Replaying a flush lineage means: for each
entry, tell the GlobalIndex "this key at this version lives in this
segment."

For a **GC segment**, lineage records two things: which keys were
relocated into this segment (same format as flush entries), and which
keys were eliminated (removed from the index entirely because they are
no longer visible to any snapshot).  Replaying GC lineage applies both
puts and eliminations.

Lineage is recorded automatically — every call to `put()`,
`appendEntry()`, or `appendRawEntry()` on a SegmentPartition appends
a 16-byte lineage record to an in-memory buffer.  GC eliminations are
added explicitly via `addLineageElimination()`.  The buffer is written
to disk during seal, before the footer.

## The savepoint watermark

Savepoints are periodic full dumps of the GlobalIndex to disk.  A
savepoint captures the complete state of the index at a point in time,
identified by the highest segment ID it reflects:
`savepoint_max_segment_id`.

This value is stored in the Manifest under the key
`kGiSavepointMaxSegmentId`.  It acts as a watermark: during recovery,
only segments with IDs above this value need their lineage replayed.
Segments at or below the watermark are already captured in the
savepoint.

The savepoint is written using an atomic rename-swap pattern:

1. Write the new savepoint to a `tmp/` directory.
2. Persist the new `max_segment_id` to the Manifest (durable via
   O\_DSYNC).
3. Rename `valid/` to `old/`, then `tmp/` to `valid/`, then delete
   `old/`.

If a crash interrupts the swap, recovery detects the partial state
(`.old` exists without `valid/`, or `.tmp` leftover) and restores
consistency.

## The recovery algorithm

On startup, `DB::open()` initializes subsystems in dependency order:

**1. Manifest** — opens the persistent key-value store that tracks
segment IDs, version counter blocks, and the savepoint watermark.

**2. VersionManager** — recovers the version counter from the Manifest.
Uses block-aligned persistence: the on-disk value is always rounded up
to the next block boundary (default 2^20), so at most one block's
worth of versions are wasted on crash recovery.

**3. SegmentStorageManager** — reads the segment ID range
`[min_id, max_id]` from the Manifest and opens all segment files in
that range.  Missing files (gaps from incomplete GC) are tolerated.
Files outside the tracked range are **orphans** — remnants of flushes
that crashed before updating the Manifest — and are deleted.

**4. GlobalIndex** — the core recovery step:

```
recover():
  1. Handle partial savepoint swap (restore .old if needed, discard .tmp)
  2. Load savepoint if it exists → populates the in-memory index
  3. Read savepoint_max_segment_id from Manifest
  4. For each segment with ID > savepoint_max_segment_id (ascending order):
       Read lineage from all partition files
       For each put entry:      applyPut(hkey, packed_version, segment_id)
       For each elimination:    applyEliminate(hkey, packed_version, old_segment_id)
  5. Write convergence savepoint (captures fully recovered state)
```

Step 4 is the lineage replay.  Ascending segment ID order matters:
flush segments are replayed before GC segments that reference them,
ensuring relocations correctly supersede the original puts.

Step 5 writes a fresh savepoint so the next startup doesn't need to
replay the same lineage again.  This is the **convergence savepoint**
— after it completes, the system is in a clean state equivalent to a
normal shutdown.

## Crash scenarios

### Crash during flush

A memtable is being flushed to a new segment.  The segment file exists
on disk but is incomplete (no footer, no lineage).

**Resolution:** The segment ID was never registered in the Manifest
(registration happens after seal).  SegmentStorageManager's orphan
purge deletes the file.  The memtable's data is lost — but it was
already at risk during the entire time it was accumulating in memory.
The flush is just the tail end of the same vulnerability window.

### Crash during GC

GC creates output segments and removes entries from the GlobalIndex.
Output segment IDs are registered in the Manifest immediately on
allocation (before the merge runs), so they survive in the tracked
range.

If the crash happens after some output segments are sealed but before
others:

- Sealed outputs: their lineage is replayed during recovery.  The
  GlobalIndex correctly points to the new segments for relocated keys.
- Unsealed outputs: treated as incomplete, opened but fail (no footer),
  skipped by recovery.  The keys that would have gone there still
  point to the old input segments, which haven't been deleted yet (GC
  removes inputs only after full success).
- Eliminated entries that were in unsealed outputs: not applied.  The
  old entries remain in the GlobalIndex pointing to still-valid input
  segments.

The result is as if GC partially completed — some keys compacted,
some not.  Correctness is preserved because input segments are never
deleted until all outputs are sealed.

### Crash during savepoint

The atomic swap pattern handles three sub-cases:

- Crash while writing `tmp/`: the incomplete directory is discarded
  on recovery.  The previous `valid/` savepoint is used.
- Crash after renaming `valid/` to `old/` but before renaming `tmp/`
  to `valid/`: recovery finds `old/` without `valid/` and restores it.
- Crash after swap completes but before deleting `old/`: recovery
  cleans up the leftover `old/` directory.

In all cases, recovery proceeds with the best available savepoint and
replays lineage for any segments beyond its watermark.

## Why not a WAL?

A write-ahead log is the conventional solution for this problem.  kvlite
originally used one — a separate file that recorded every GlobalIndex
mutation (puts, relocations, eliminations) as CRC-protected transactions.
It was removed because:

**The segment is already the durability boundary.**  A crash during the
write buffer accumulation phase loses all buffered data regardless of
whether a WAL exists.  The WAL only protected the window between
segment seal and GlobalIndex update — a window that lineage eliminates
entirely by making the segment self-describing.

**One fewer I/O path.**  With a WAL, every flush wrote data twice: once
to the segment, once to the WAL.  The WAL commit included an fdatasync.
Eliminating it removed a synchronous I/O from the flush critical path.

**Simpler recovery.**  WAL replay required parsing a transaction log with
CRC-framed records, handling producer multiplexing (flush vs. GC
records interleaved in the same file), and managing multi-file WAL
rollover.  Lineage replay reads a flat array of fixed-size records
from each segment — simpler code, fewer failure modes.

**Recovery is parallelizable.**  Each segment's lineage is independent.
A future optimization could replay lineages from multiple segments
concurrently.  WAL replay was inherently sequential (transactions had
to be applied in order).

The tradeoff is space amplification: lineage duplicates information
that could be derived by scanning the data region.  But the lineage
section is small (16 bytes per entry vs. ~130 bytes per data entry)
and avoids the full-scan cost during recovery.

## The durability chain

Every durable state change in kvlite flows through the same pattern:

1. **Write data** to a buffered file (LogFile with 1MB userspace
   buffer).
2. **Flush buffer** to the kernel page cache.
3. **fdatasync** to stable storage.
4. **Update Manifest** (which itself uses O\_DSYNC).

The Manifest is the single source of truth for what exists.  Segment
files are the single source of truth for what data they contain and
what GlobalIndex state they imply.  The savepoint is an acceleration
structure — if it disappeared entirely, recovery would still succeed
by replaying lineage from all segments (just slower).

This layering means there is no moment where a crash can leave the
system in an unrecoverable state.  The worst case is lost data from
the in-memory write buffer — which the application already accepted
by not using synchronous writes.
