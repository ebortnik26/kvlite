# Inside kvlite's DeltaHashTable: Bucket Encoding and Concurrency

kvlite is an index-plus-log key-value store.  Every key lookup bottlenecks
on the same data structure: the **DeltaHashTable** (DHT), a compact hash
table whose buckets store sorted suffix arrays with per-key version lists.
This post covers the two areas that define its performance: how data is
packed into buckets, and how concurrent access is coordinated.

## Hash decomposition

A 64-bit hash is split into two parts:

```
64-bit hash
├── high bucket_bits (default 20) ──► bucket index  (selects bucket)
└── low  suffix_bits (remaining 44) ► suffix        (stored in bucket)
```

The top bits route to one of 2^20 (~1M) buckets.  The bottom 44 bits are
the **suffix** — stored inside the bucket and used for key disambiguation.
With 44 bits of suffix, false positives are essentially impossible: full
suffix comparison eliminates collisions without touching the log file.

## Bucket structure

Each bucket is a fixed-size byte array (default 512 bytes).  The last 8
bytes are reserved for an **extension pointer** — a link to an overflow
bucket when the main bucket fills up.  That leaves 504 bytes of usable
data (4,032 bits).

```
0                                                     504   512
├──────────────── encoded data ────────────────────────┤ ext ┤
│ num_keys │ suffixes │ counts │ first-pvs │ first-ids │ ptr │
│          │          │        │ tail-pvs  │ tail-ids  │     │
└──────────────────────────────────────────────────────┴─────┘
```

Within a bucket, keys are stored in **sorted suffix order**.  Each key
has an associated list of `(packed_version, segment_id)` pairs, stored
latest-first.  Lookup is a binary search on suffixes followed by a scan
of the version list.

When a bucket runs out of space, an extension bucket is allocated from a
`BucketArena` (a chunk-based bump allocator with 64 slots per chunk) and
linked via the extension pointer.  Chains are rare with well-sized
buckets.

## Columnar bucket encoding

The encoding is the heart of the DHT's space efficiency.  kvlite uses a
**columnar layout** — instead of storing each key's data contiguously
(row-oriented), data is organized by column across all keys in the
bucket.  This enables cross-key delta encoding and targeted decoding.

### Column order

```
[num_keys] [suffixes] [counts] [first-PVs] [first-IDs] [tail-PVs] [tail-IDs]
  16 bits   variable   variable  variable     variable    variable   variable
```

All fields after `num_keys` are bit-packed with no byte alignment.

### Suffixes column

The first suffix is stored raw (44 bits).  Subsequent suffixes are
delta-encoded against the previous one (since they're sorted, deltas are
always positive) and written as **Elias-Gamma codes**:

```
suffix[0]:  raw 44 bits
suffix[1]:  ElGamma(suffix[1] - suffix[0])
suffix[2]:  ElGamma(suffix[2] - suffix[1])
...
```

Elias-Gamma is a universal code for positive integers that uses
`2*floor(log2(n)) + 1` bits.  Small deltas (common when the hash space
is well-distributed) compress to just a few bits.

### Counts column

Each key's version count is Elias-Gamma encoded:

```
count[0]:  ElGamma(count[0])
count[1]:  ElGamma(count[1])
...
```

A count of zero is valid — it means the key exists in the suffix array
but has no version entries yet.

### First-PVs and First-IDs columns

These store the **first** (latest) packed version and segment ID for each
key, delta-encoded across keys.  The first key's values are stored raw
(64 bits for PV, 32 bits for ID).  Subsequent keys use signed deltas via
**ZigZag encoding** before Elias-Gamma:

```
first_pv[0]:  raw 64 bits
first_pv[1]:  ElGamma64(ZigZag(first_pv[1] - first_pv[0]) + 1)
first_pv[2]:  ElGamma64(ZigZag(first_pv[2] - first_pv[1]) + 1)
```

ZigZag maps signed values to unsigned by interleaving positives and
negatives: `0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, ...`.  This keeps small
magnitudes small regardless of sign, which Elias-Gamma compresses well.

### Tail-PVs and Tail-IDs columns

After the first entry, remaining versions for each key are stored in
these columns.  Packed versions are in descending order (latest first),
so tail-PV deltas are always positive — no ZigZag needed:

```
For each key i:
  For v = 1 to count[i]-1:
    tail_pv:  ElGamma64(pv[v-1] - pv[v])     // always positive
    tail_id:  ElGamma(ZigZag(id[v] - id[v-1]) + 1)  // signed
```

### Why columnar?

The columnar layout provides two advantages over row-oriented encoding:

**Better compression.**  Cross-key delta encoding of first-PVs and
first-IDs exploits the fact that nearby keys in a bucket often have
similar version numbers and segment IDs.  This reduces deltas compared to
storing each key's data independently.

**Targeted decoding.**  The most common query is "get the latest version
of this key" — which only needs the suffix, count, first-PV, and
first-ID columns.  The decoder can skip the entire tail-PV and tail-ID
columns.  A specialized `decodeFirstEntry` method does exactly this,
avoiding the cost of decoding all historical versions.

### Encoding primitives

Two bit-level primitives underpin the encoding:

**Elias-Gamma coding.**  Encodes a positive integer `n` as a unary-coded
length prefix followed by the binary representation:

```
n=1:    "1"              (1 bit)
n=2:    "010"            (3 bits)
n=5:    "00101"          (5 bits)
n=13:   "0001101"        (7 bits)
```

Cost: `2*floor(log2(n)) + 1` bits.  Values 1-3 fit in 1-3 bits.

**ZigZag encoding.**  Maps signed integers to unsigned:

```cpp
uint64_t zigzag(int64_t v) { return (v << 1) ^ (v >> 63); }
```

Preserves magnitude, making signed deltas compressible by Elias-Gamma.

All bit I/O uses big-endian (MSB-first) ordering via `BitReader` /
`BitWriter` classes that operate at arbitrary bit offsets with 64-bit
word-level loads for performance.

### Incremental bit budgeting

Before modifying a bucket, the codec can calculate the exact number of
additional bits needed for an operation (adding a version, inserting a
new key) without performing a full re-encode.  Methods like
`bitsForAddVersion` and `bitsForNewEntry` account for changes to each
column — delta chain splits, new anchor values, count adjustments — and
compare against the remaining bit budget.  If the operation won't fit,
the entry spills to an extension bucket.

## Concurrency control

kvlite instantiates the DHT in two flavors, each with different
concurrency characteristics:

| Variant | Used by | Mutability | Locking |
|---------|---------|------------|---------|
| `ReadWriteDeltaHashTable` | GlobalIndex | Always mutable | Per-bucket spinlocks |
| `ReadOnlyDeltaHashTable` | SegmentIndex | Write-once, then sealed | None |

### ReadOnlyDeltaHashTable

The read-only variant follows a strict lifecycle: **build** (single-
threaded) then **seal** (immutable forever).  During the build phase,
entries are added without any synchronization.  After `seal()` is called,
the table becomes read-only and can be queried from any number of threads
without locks.  This is used for SegmentIndex — each sealed segment file
has an immutable index that never changes.

### ReadWriteDeltaHashTable

The read-write variant backs the GlobalIndex, which is the primary
in-memory index updated on every put and queried on every get.  It uses
**per-bucket spinlocks** for fine-grained concurrency.

#### Spinlock design

Each of the 2^20 buckets gets its own spinlock — a single `atomic<uint8_t>`
using a test-and-test-and-set (TTTS) strategy:

```cpp
void lock() const {
    while (locked.exchange(1, memory_order_acquire) != 0) {
        while (locked.load(memory_order_relaxed) != 0) {
            __builtin_ia32_pause();  // x86 spin hint
        }
    }
}

void unlock() const {
    locked.store(0, memory_order_release);
}
```

The outer loop attempts the atomic exchange (acquire semantics).  On
contention, the inner loop spins on a relaxed load — cheaper than
repeated exchanges — with a CPU pause hint to reduce power and improve
latency on hyperthreaded cores.  Unlock uses release semantics to ensure
all bucket modifications are visible before the lock is dropped.

RAII guards ensure locks are always released:

```cpp
struct SpinlockGuard {
    const Spinlock& lock_;
    SpinlockGuard(const Spinlock& l) : lock_(l) { lock_.lock(); }
    ~SpinlockGuard() { lock_.unlock(); }
};
```

#### Locking protocol

Every operation — read or write — acquires the bucket's spinlock:

```cpp
bool findFirst(uint64_t hash, uint64_t& pv, uint32_t& id) const {
    uint32_t bi = bucketIndex(hash);
    SpinlockGuard guard(bucket_locks_[bi]);
    return findFirstByHash(bi, suffixFromHash(hash), pv, id);
}

bool addImpl(uint32_t bi, uint64_t suffix, uint64_t pv, uint32_t id) {
    SpinlockGuard guard(bucket_locks_[bi]);
    bool is_new = addToChain(bi, suffix, pv, id, ...);
    size_.fetch_add(1, memory_order_relaxed);
    return is_new;
}
```

Reads are locked because they traverse extension chains that concurrent
writes may be modifying.  The lock ensures a reader never observes a
partially-linked extension bucket.

The `size_` counter is a separate `atomic<size_t>` updated with relaxed
ordering outside the critical section — it's informational and doesn't
need to be synchronized with bucket contents.

#### Why spinlocks work here

With 2^20 buckets and well-distributed hashes, the probability of two
concurrent operations hitting the same bucket is low.  When contention
does occur, critical sections are short (a binary search plus a few
Elias-Gamma decodes/encodes), so spinning wastes minimal CPU.  The
1-byte footprint per lock means the entire lock array fits in ~1 MB,
well within L2 cache.

### GlobalIndex: multi-level locking

The GlobalIndex layers a `shared_mutex` over the DHT's bucket spinlocks:

```
GlobalIndex
  savepoint_mu_ (shared_mutex)
    shared:    put / get / remove    (concurrent, non-blocking)
    exclusive: storeSavepoint / GC   (blocks new mutations)
  ReadWriteDeltaHashTable
    bucket_locks_[0..2^20]           (per-bucket spinlocks)
    BucketArena mutex_               (extension allocation)
```

Normal operations (put, get, remove) take a **shared lock** on
`savepoint_mu_` — they don't block each other.  Savepoint creation and
GC take an **exclusive lock**, pausing new mutations while the DHT
state is snapshotted or entries are relocated.

This two-level scheme separates fast-path locking (bucket spinlocks,
sub-microsecond hold times) from slow-path coordination (savepoint
mutex, held for milliseconds during I/O).  In practice, the shared lock
on `savepoint_mu_` is uncontended during normal operation, adding
negligible overhead.

#### Lock-free counters

Two atomic counters avoid lock acquisition on the hot path:

```cpp
std::atomic<size_t> key_count_{0};     // Approximate live key count
std::atomic<uint64_t> max_version_{0}; // Highest version seen
```

`max_version_` uses a CAS loop that only updates when the new version
exceeds the current maximum:

```cpp
uint64_t cur = max_version_.load(memory_order_relaxed);
while (packed_version > cur &&
       !max_version_.compare_exchange_weak(cur, packed_version,
           memory_order_relaxed)) {}
```

Both use relaxed ordering — they're monotonic and informational, with no
other operations depending on their exact values.

## Putting it together

The DHT's design reflects kvlite's workload: point lookups and small
batch writes, no range scans.

The columnar bucket encoding packs data tightly through delta encoding,
ZigZag mapping, and Elias-Gamma coding.  Cross-key deltas in the
columnar layout improve compression, and targeted decoding (skipping tail
columns) accelerates the common "get latest version" query.

The concurrency model matches each use case: lock-free immutable indexes
for sealed segments, fine-grained spinlocks for the mutable global index,
and a shared mutex for rare structural operations like savepoints.  With
a million independent bucket locks, the global index scales to high
thread counts with minimal contention.
