# kvlite Design Principles

## Versioning

- Global monotonic version counter increments on every write
- WriteBatch: all operations get the same version (atomic snapshot)
- Single put/remove: gets its own version
- Tombstones have versions (enables "key not found at version X")
- Version counter persisted to Manifest using block-aligned write-ahead: advances in fixed blocks (default 2^20), bounding crash-recovery waste to one block

## Index Structure

Both GlobalIndex and SegmentIndex use the same DeltaHashTable structure:

```
key -> [(version1, location1), (version2, location2), ...]
       sorted by version, descending (latest first)

GlobalIndex:   location = segment_id
SegmentIndex:  location = offset within file
```

## Snapshots

- Lightweight: a snapshot is just a pinned version number (~8 bytes), no data is copied
- Reads through a snapshot (via `ReadOptions`) return the value visible at that version (highest version <= snapshot version)
- Keys written after the snapshot are invisible; keys deleted after are still visible
- Multiple snapshots can coexist at different versions
- Iterators use snapshots internally: `createIterator()` creates an owned snapshot
- `ReadBatch` acquires an implicit snapshot (records the version used, but does not pin it for GC)
- Releasing a snapshot allows GC to reclaim the versions it was protecting

## Snapshot & GC Interaction

GC retains exactly **one version per key per observation point** (each active snapshot + latest). All intermediate versions between observation points are dropped.

```
Snapshots:    S1(v=100)              S2(v=200)
                 |                       |
Versions:  ------+-------------------+---+---------->
           50   100   120   150   180   200   250

For a key with versions 50, 120, 180, 250:
  - S1 sees v100 -> keeps v50 (highest <= 100)
  - S2 sees v200 -> keeps v180 (highest <= 200)
  - Latest       -> keeps v250
  - v120 dropped (not visible at any observation point)

After S1 released:
  - v50 dropped (no longer needed)
  - v180, v250 retained

No active snapshots:
  - Only the latest version per key survives GC
```

## Write Path

1. VersionManager allocates a monotonic version
2. Entry buffered in WriteBuffer (in-memory Memtable)
3. When buffer full: flush writes sorted entries to new Segment (K partition files), seals with SegmentIndex + lineage per partition (parallel fdatasync when K > 1)
4. Update in-memory GlobalIndex with (key, version, segment_id) for each flushed entry

## Read Path (latest)

1. Check WriteBuffer for unflushed entries (in-memory hit)
2. GlobalIndex lookup: key -> first entry -> (version, segment_id)
3. SegmentIndex lookup: key -> (version, offset)
4. Read value from segment data file at offset, verify CRC32

## Read Path (by snapshot)

1. Check WriteBuffer for matching entries <= snapshot version
2. GlobalIndex lookup: key -> highest version <= snapshot version
3. SegmentIndex lookup: same bounded scan within the target segment
4. Read value from segment data file at offset, verify CRC32
