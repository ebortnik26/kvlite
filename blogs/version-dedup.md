# Version Deduplication in kvlite

## The Problem: Memory is Precious

kvlite is an index-plus-log key-value store. Every key-value pair lives in a segment file on disk, and the GlobalIndex — an in-memory hash table — maps each key to the list of segments that hold its versions. When a key is overwritten, the old version stays in its original segment, and the new version goes into the latest segment. Both entries coexist in the GlobalIndex.

With hot keys — keys that are written to repeatedly — version lists grow. A key overwritten 100 times across 100 flushes accumulates 100 entries in the GlobalIndex, each pointing to a different segment. Only the latest version matters for current reads. The rest occupy memory, slow down lookups (the index must walk the version chain to find the best match), and inflate bucket encoding in the DeltaHashTable.

Under sustained load with rotating snapshots, we observed keys accumulating 77+ versions in the GlobalIndex, causing bucket overflow and requiring cascading spills to extension buckets. The index consumed far more memory than the data it pointed to warranted.

## The Constraint: Don't Burden the Write Path

The naive fix — checking and pruning the GlobalIndex on every `put()` — is unacceptable. The write path must stay lean: hash the key, append the record to the memtable's data buffer, insert a slot into the bucket chain. Adding a global index lookup and potential chain rewrite to every write would roughly double the cost of `put()`.

The design principle: **move dedup work to background processes that already touch the data.** kvlite has three such processes — flush, GC, and now a dedicated dedup daemon — each operating at a different timescale and scope.

## Three Layers of Dedup

### Layer 1: Flush Dedup (Memtable → Segment)

When a memtable is sealed and flushed to a new segment, the flush path already deduplicates entries within the memtable. For each key, it keeps only the versions visible at the current set of active snapshots (plus the latest version). This uses the `dedupVersionGroup` algorithm — a two-pointer sweep over versions and snapshot observation points.

**Scope:** Within a single memtable. Multiple memtables in the flush pipeline can each hold a version of the same key; flush dedup doesn't see across memtables.

**Cost:** Zero marginal cost — the flush already decodes, sorts, and encodes every entry. Dedup is a filter step in the existing pipeline.

### Layer 2: Inline Dedup (Flush → GlobalIndex)

After a memtable flush writes deduplicated entries to a segment, those entries are added to the GlobalIndex via `applyPutBatch`. This is where version chains grow: each flush adds new entries alongside the existing ones from prior segments.

The inline dedup fires at this exact point. When `addToChain` inserts a new version for a key that already exists in the GlobalIndex (the `is_new == false` path), it calls `pruneKeyVersionsDesc` on the key's version list — a single-pass sweep that removes versions not visible at any active snapshot. The pruning happens between inserting the new version and encoding the bucket, so there is **zero extra decode/encode cost**.

```
addToChain:
  decode bucket → find key → insert new version
  → if key had prior versions: prune in-place  ← NEW
  → encode bucket
```

**Scope:** Within the current bucket only. If a key's versions span multiple extension buckets, only the bucket receiving the new version is pruned. The background daemon handles chain-wide pruning.

**Cost:** For non-overwrite workloads (unique keys), the dedup code is never reached — `addToChain` returns `is_new = true` and the prune path is skipped entirely. For overwrites, the cost is one linear scan of the key's version list (typically 2-3 entries after steady-state dedup).

### Layer 3: GC Dedup (Segment Compaction)

Garbage collection merges multiple segments into new ones, using a K-way merge with `dedupVersionGroup` to eliminate versions invisible at all snapshots. GC operates on the segment data directly, not the GlobalIndex — it reads entries from disk, deduplicates the stream, writes survivors to output segments, then updates the GlobalIndex via `applyRelocate` and `applyEliminate`.

**Scope:** All entries across the input segments. GC sees the full version history for every key in those segments.

**Cost:** Proportional to the data being compacted. GC is the most thorough dedup mechanism but runs infrequently (every 10 seconds by default) and only when the dead-entry ratio exceeds a threshold.

### Layer 4: Background Dedup Daemon (Optional)

For workloads with long-lived snapshots or read-heavy keys that accumulate stale versions without being written to, an optional background daemon periodically sweeps the GlobalIndex. It uses a lock-free bitmap (`DuplicatesBucketBitmap`) to track which primary buckets have multi-version keys, then visits only those buckets.

The sweep decodes the full bucket chain, merges per-suffix version lists, prunes against the current snapshot set, and re-encodes. This handles the cross-extension-bucket case that inline dedup skips.

**Scope:** The entire GlobalIndex, but only buckets flagged as having duplicates.

**Cost:** The bitmap check (one byte load per 8 buckets) is essentially free. Pruning a flagged bucket costs one decode-prune-encode cycle under the per-bucket spinlock. The daemon runs on a configurable interval (default: disabled, opt-in via `version_prune_interval_sec`).

## The Dedup Algorithm

Both inline and daemon dedup use the same core algorithm: `pruneKeyVersionsDesc`. It operates on a descending-sorted list of packed versions (each encoding `logical_version << 1 | tombstone_bit`) and an ascending-sorted list of snapshot observation points.

The algorithm is a single-pass two-pointer sweep:

1. Start with the highest snapshot (`si = snapshots.size() - 1`)
2. Walk versions left-to-right (descending — newest first)
3. For each version, check if its logical value satisfies the current snapshot (`logical_ver <= snapshots[si]`)
4. If yes: keep the version, consume all snapshots it serves (advance `si` past all snapshots ≥ this version)
5. If no: drop the version — no remaining snapshot needs it

Tombstones are handled naturally: the tombstone bit is stripped for comparison (`>> 1`), so a delete at version V is kept if V is the latest version visible at some snapshot. This ensures that reads at that snapshot correctly see "key deleted."

## Results

With 256 hot keys, 1MB memtable, and rotating snapshots over 30 seconds:

| Mode | Versions/key | Historical entries | GC runs needed |
|------|---|---|---|
| Inline dedup | **1.0** | 0 | 0 |
| Daemon only | **3.8** | 706 | 0 |
| Neither (GC only) | 110 | 28,000 | 10 |

The inline dedup eliminates 100% of stale versions at flush time, reducing the GlobalIndex to a single version per key regardless of write rate. The daemon achieves near-optimal results (3-4 versions) even without inline dedup, proving it can serve as a standalone dedup mechanism for workloads that prefer to keep the flush path completely untouched.

## Configuration

```cpp
kvlite::Options opts;
opts.dedup_on_put = true;                // inline dedup at flush time (default: on)
opts.version_prune_interval_sec = 0;     // background daemon (default: off)
```

For most workloads, `dedup_on_put = true` is sufficient. Enable the daemon for workloads with many long-lived snapshots or read-heavy keys that are written infrequently.

## Code Metrics

| Category | Lines |
|----------|-------|
| Production code | 10,853 |
| Tests | 12,950 |
| Benchmarks/tools | 899 |
