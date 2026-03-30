#include "internal/memtable.h"

#include <algorithm>
#include <unordered_map>

#include "internal/delta_hash_table.h"
#include "internal/entry_stream.h"
#include "internal/segment.h"
#include "internal/version_dedup.h"

namespace kvlite {
namespace internal {

Memtable::Memtable(size_t data_capacity)
    : data_capacity_(std::max(data_capacity, kMinDataCapacity)),
      data_(new uint8_t[data_capacity_]),
      buckets_(new uint8_t[static_cast<size_t>(kNumBuckets) * kBucketBytes]()),
      locks_(std::make_unique<Spinlock[]>(kNumBuckets)),
      ext_arena_(sizeof(Bucket) + kBucketBytes, /*concurrent=*/true) {}

Memtable::~Memtable() = default;

// --- Data buffer I/O -----------------------------------------------------
//
// Record layout (all little-endian on x86, matches LogEntry on-disk):
//   offset + 0:               uint64_t packed_version  (kPackedVerSize)
//   offset + kKeyLenOffset:   uint16_t key_len         (kKeyLenSize)
//   offset + kValueLenOffset: uint32_t value_len       (kValueLenSize)
//   offset + kRecordHeaderSize:         key[key_len]
//   offset + kRecordHeaderSize + key_len: value[value_len]

uint32_t Memtable::appendRecord(const std::string& key, PackedVersion pv,
                                const std::string& value) {
    uint16_t key_len = static_cast<uint16_t>(key.size());
    uint32_t val_len = static_cast<uint32_t>(value.size());
    size_t record_size = kRecordHeaderSize + key_len + val_len;

    size_t off = data_end_.fetch_add(record_size, std::memory_order_relaxed);

    uint8_t* p = data_.get() + off;
    std::memcpy(p, &pv.data, kPackedVerSize); p += kPackedVerSize;
    std::memcpy(p, &key_len, kKeyLenSize);    p += kKeyLenSize;
    std::memcpy(p, &val_len, kValueLenSize);  p += kValueLenSize;
    std::memcpy(p, key.data(), key_len);      p += key_len;
    std::memcpy(p, value.data(), val_len);

    return static_cast<uint32_t>(off);
}

void Memtable::readValue(uint32_t off, std::string& value) const {
    const uint8_t* p = data_.get() + off;
    uint16_t kl;
    uint32_t vl;
    std::memcpy(&kl, p + kKeyLenOffset, kKeyLenSize);
    std::memcpy(&vl, p + kValueLenOffset, kValueLenSize);
    value.assign(reinterpret_cast<const char*>(p + kRecordHeaderSize + kl), vl);
}

std::string Memtable::readKey(uint32_t off) const {
    const uint8_t* p = data_.get() + off;
    uint16_t kl;
    std::memcpy(&kl, p + kKeyLenOffset, kKeyLenSize);
    return std::string(reinterpret_cast<const char*>(p + kRecordHeaderSize), kl);
}

// --- Public API ----------------------------------------------------------

bool Memtable::keyExistsInBucket(uint32_t bi, uint64_t hash) const {
    const uint8_t* scan = primaryBucket(bi);
    while (scan) {
        uint16_t cnt = bucketCount(scan);
        const Slot* s = bucketSlots(scan);
        uint16_t lo = 0, hi = cnt;
        while (lo < hi) {
            uint16_t mid = (lo + hi) / 2;
            if (s[mid].hash < hash) lo = mid + 1;
            else hi = mid;
        }
        if (lo < cnt && s[lo].hash == hash) return true;
        uint32_t ext = bucketExtPtr(scan);
        scan = ext ? extBucketData(ext) : nullptr;
    }
    return false;
}

void Memtable::put(const std::string& key, uint64_t version,
                   const std::string& value, bool tombstone) {
    uint64_t hash = dhtHashBytes(key.data(), key.size());
    uint32_t bi = bucketIndex(hash);
    PackedVersion pv(version, tombstone);

    // Append record to data buffer (lock-free, space reserved atomically).
    uint32_t offset = appendRecord(key, pv, value);

    locks_[bi].lock();

    bool key_exists = keyExistsInBucket(bi, hash);

    // Find a bucket in the chain with a free slot.
    uint8_t* b = primaryBucket(bi);
    while (bucketCount(b) >= kSlotsPerBucket) {
        uint32_t ext = bucketExtPtr(b);
        if (ext) {
            b = extBucketData(ext);
        } else {
            uint32_t new_ext = ext_arena_.allocate();
            setBucketExtPtr(b, new_ext);
            b = extBucketData(new_ext);
        }
    }

    // Sorted insert: maintain (hash asc, pv desc) within each page.
    Slot* s = bucketSlots(b);
    uint16_t cnt = bucketCount(b);
    Slot entry = {hash, pv.data, offset};
    uint16_t pos = 0;
    while (pos < cnt && (s[pos].hash < hash ||
           (s[pos].hash == hash && s[pos].pv > pv.data))) {
        ++pos;
    }
    if (pos < cnt) {
        std::memmove(&s[pos + 1], &s[pos], (cnt - pos) * sizeof(Slot));
    }
    s[pos] = entry;
    setBucketCount(b, cnt + 1);

    locks_[bi].unlock();

    size_.fetch_add(1, std::memory_order_relaxed);
    if (!key_exists) {
        key_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void Memtable::putBatch(const std::vector<BatchOp>& ops, uint64_t version) {
    if (ops.empty()) return;

    // Pre-compute hashes and bucket indices for all operations.
    struct Prepared {
        uint32_t op_idx;
        uint64_t hash;
        uint32_t bi;
        uint32_t offset;  // filled after appendRecord
    };

    std::vector<Prepared> items;
    items.reserve(ops.size());
    for (uint32_t i = 0; i < ops.size(); ++i) {
        uint64_t h = dhtHashBytes(ops[i].key->data(), ops[i].key->size());
        items.push_back({i, h, bucketIndex(h), 0});
    }

    // Sort by bucket index to acquire locks in order (deadlock avoidance).
    std::sort(items.begin(), items.end(),
              [](const Prepared& a, const Prepared& b) {
                  return a.bi < b.bi;
              });

    // Append all records to data buffer (lock-free).
    for (auto& item : items) {
        const auto& op = ops[item.op_idx];
        PackedVersion pv(version, op.tombstone);
        item.offset = appendRecord(*op.key, pv, *op.value);
    }

    // Phase 1: acquire all distinct bucket locks in order.
    std::vector<uint32_t> locked_buckets;
    locked_buckets.reserve(items.size());
    for (const auto& item : items) {
        if (locked_buckets.empty() || locked_buckets.back() != item.bi) {
            locks_[item.bi].lock();
            locked_buckets.push_back(item.bi);
        }
    }

    // Phase 2: insert all slots while holding all locks.
    size_t new_entries = 0;
    size_t new_keys = 0;

    for (const auto& item : items) {
        PackedVersion pv(version, ops[item.op_idx].tombstone);

        bool key_exists = keyExistsInBucket(item.bi, item.hash);

        // Find a bucket with a free slot.
        uint8_t* b = primaryBucket(item.bi);
        while (bucketCount(b) >= kSlotsPerBucket) {
            uint32_t ext = bucketExtPtr(b);
            if (ext) {
                b = extBucketData(ext);
            } else {
                uint32_t new_ext = ext_arena_.allocate();
                setBucketExtPtr(b, new_ext);
                b = extBucketData(new_ext);
            }
        }

        // Sorted insert: maintain (hash asc, pv desc) within each page.
        Slot* s = bucketSlots(b);
        uint16_t cnt = bucketCount(b);
        Slot entry = {item.hash, pv.data, item.offset};
        uint16_t pos = 0;
        while (pos < cnt && (s[pos].hash < item.hash ||
               (s[pos].hash == item.hash && s[pos].pv > pv.data))) {
            ++pos;
        }
        if (pos < cnt) {
            std::memmove(&s[pos + 1], &s[pos], (cnt - pos) * sizeof(Slot));
        }
        s[pos] = entry;
        setBucketCount(b, cnt + 1);

        new_entries++;
        if (!key_exists) new_keys++;
    }

    // Phase 3: release all locks.
    for (uint32_t bi : locked_buckets) {
        locks_[bi].unlock();
    }

    size_.fetch_add(new_entries, std::memory_order_relaxed);
    key_count_.fetch_add(new_keys, std::memory_order_relaxed);
}

bool Memtable::get(uint64_t hash,
                   std::string& value, uint64_t& version, bool& tombstone) const {
    return getByVersion(hash, UINT64_MAX, value, version, tombstone);
}

bool Memtable::getByVersion(uint64_t hash, uint64_t upper_bound,
                            std::string& value, uint64_t& version,
                            bool& tombstone) const {
    uint32_t bi = bucketIndex(hash);

    bool sealed = sealed_.load(std::memory_order_acquire);
    if (!sealed) locks_[bi].lock();

    bool found = false;
    uint64_t best_pv = 0;
    uint32_t best_offset = 0;

    const uint8_t* b = primaryBucket(bi);
    while (b) {
        uint16_t cnt = bucketCount(b);
        const Slot* s = bucketSlots(b);
        // Binary search for first slot with matching hash.
        uint16_t lo = 0, hi = cnt;
        while (lo < hi) {
            uint16_t mid = (lo + hi) / 2;
            if (s[mid].hash < hash) lo = mid + 1;
            else hi = mid;
        }
        // Scan contiguous same-hash entries starting at lo.
        for (uint16_t i = lo; i < cnt && s[i].hash == hash; ++i) {
            if (s[i].pv <= upper_bound && (!found || s[i].pv > best_pv)) {
                best_pv = s[i].pv;
                best_offset = s[i].offset;
                found = true;
            }
        }
        uint32_t ext = bucketExtPtr(b);
        b = ext ? extBucketData(ext) : nullptr;
    }

    if (found) {
        readValue(best_offset, value);
        PackedVersion pv(best_pv);
        version = pv.version();
        tombstone = pv.tombstone();
    }

    if (!sealed) locks_[bi].unlock();
    return found;
}

void Memtable::forEach(const std::function<void(uint64_t,
                                                const std::vector<Entry>&)>& fn) const {
    for (uint32_t bi = 0; bi < kNumBuckets; ++bi) {
        const uint8_t* b = primaryBucket(bi);
        if (bucketCount(b) == 0 && bucketExtPtr(b) == 0) continue;

        // Group entries by hash within this bucket chain.
        std::unordered_map<uint64_t, std::vector<Entry>> groups;

        while (b) {
            uint16_t cnt = bucketCount(b);
            const Slot* s = bucketSlots(b);
            for (uint16_t i = 0; i < cnt; ++i) {
                PackedVersion pv(s[i].pv);
                std::string val;
                readValue(s[i].offset, val);
                groups[s[i].hash].push_back({pv, std::move(val)});
            }
            uint32_t ext = bucketExtPtr(b);
            b = ext ? extBucketData(ext) : nullptr;
        }

        for (auto& kv : groups) {
            fn(kv.first, kv.second);
        }
    }
}

void Memtable::clear() {
    data_end_.store(0, std::memory_order_relaxed);
    std::memset(buckets_.get(), 0, static_cast<size_t>(kNumBuckets) * kBucketBytes);
    ext_arena_.clear();
    size_.store(0, std::memory_order_relaxed);
    key_count_.store(0, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// MemtableStream — EntryStream backed by compact offset records
// into Memtable::data_
// ---------------------------------------------------------------------------

namespace {

class MemtableStream : public EntryStream {
public:
    struct Record {
        uint64_t hash;
        uint32_t offset;   // into Memtable::data_
        uint16_t key_len;
        uint32_t val_len;
        PackedVersion pv;
    };

    MemtableStream(const uint8_t* data, std::vector<Record> records)
        : data_(data), records_(std::move(records)) {
        if (!records_.empty()) {
            materialize();
        }
    }

    bool valid() const override { return idx_ < records_.size(); }
    const Entry& entry() const override { return current_; }

    Status next() override {
        if (idx_ < records_.size()) {
            ++idx_;
            if (idx_ < records_.size()) {
                materialize();
            }
        }
        return Status::OK();
    }

private:
    void materialize() {
        const auto& r = records_[idx_];
        const char* key_ptr = reinterpret_cast<const char*>(
            data_ + r.offset + Memtable::kRecordHeaderSize);
        const char* val_ptr = key_ptr + r.key_len;

        current_.hash = r.hash;
        current_.key = std::string_view(key_ptr, r.key_len);
        current_.value = std::string_view(val_ptr, r.val_len);
        current_.pv = r.pv;
    }

    const uint8_t* data_;
    std::vector<Record> records_;
    size_t idx_ = 0;
    Entry current_;
};

}  // namespace

// ---------------------------------------------------------------------------
// Memtable::createStream
// ---------------------------------------------------------------------------

std::unique_ptr<EntryStream> Memtable::createStream(uint64_t snapshot_version) const {
    std::vector<MemtableStream::Record> deduped;
    bool sealed = sealed_.load(std::memory_order_acquire);
    const uint8_t* data_ptr = data_.get();

    // Per-bucket collection + dedup. Slots are sorted (hash asc, pv desc)
    // within each page; bucket iteration 0→65535 gives global hash order.
    std::vector<MemtableStream::Record> bucket_recs;

    for (uint32_t bi = 0; bi < kNumBuckets; ++bi) {
        if (!sealed) locks_[bi].lock();

        const uint8_t* b = primaryBucket(bi);
        if (bucketCount(b) == 0 && bucketExtPtr(b) == 0) {
            if (!sealed) locks_[bi].unlock();
            continue;
        }

        bucket_recs.clear();

        // Collect from chain: pages are concatenated, each internally sorted.
        while (b) {
            uint16_t cnt = bucketCount(b);
            const Slot* s = bucketSlots(b);
            for (uint16_t i = 0; i < cnt; ++i) {
                if (s[i].pv > snapshot_version) continue;

                const uint8_t* p = data_ptr + s[i].offset;
                uint16_t kl;
                uint32_t vl;
                std::memcpy(&kl, p + kKeyLenOffset, kKeyLenSize);
                std::memcpy(&vl, p + kValueLenOffset, kValueLenSize);

                bucket_recs.push_back({s[i].hash, s[i].offset, kl, vl,
                                       PackedVersion(s[i].pv)});
            }
            uint32_t ext = bucketExtPtr(b);
            b = ext ? extBucketData(ext) : nullptr;
        }

        if (!sealed) locks_[bi].unlock();

        // Dedup per bucket: entries are in (hash asc, pv desc) order.
        // For each hash group, keep only the latest version <= snapshot
        // (first entry in each group, since pv is descending).
        size_t i = 0;
        while (i < bucket_recs.size()) {
            // Find end of this hash group.
            size_t j = i + 1;
            while (j < bucket_recs.size() &&
                   bucket_recs[j].hash == bucket_recs[i].hash) {
                ++j;
            }
            // First entry in descending pv order = latest version.
            deduped.push_back(bucket_recs[i]);
            i = j;
        }
    }

    return std::make_unique<MemtableStream>(data_ptr, std::move(deduped));
}

Memtable::FlushResult Memtable::flush(Segment& out,
                                      const std::vector<uint64_t>& snapshot_versions,
                                      FlushPool* flush_pool) {
    const uint8_t* data_base = data_.get();
    const uint16_t num_parts = out.numPartitions();

    // Per-partition flush state. Each partition handles a contiguous bucket
    // range and produces its own flushed-entry list.  When K > 1 and a pool
    // is provided, partitions are flushed in parallel — no synchronization
    // needed because the bucket ranges are disjoint.
    struct PartFlush {
        std::vector<HashVersionPair> flushed;
        uint64_t max_ver = 0;
        Status status;
    };
    std::vector<PartFlush> parts(num_parts);

    // Flush one bucket range [bucket_lo, bucket_hi) into the segment.
    // When seal_partition is true, also seals the partition after writing
    // (merges emit + seal into a single parallel task).
    auto flushRange = [&](uint32_t bucket_lo, uint32_t bucket_hi,
                          uint16_t part_idx, bool seal_partition,
                          PartFlush& pf) {
        std::vector<Slot> bucket_entries;

        for (uint32_t bi = bucket_lo; bi < bucket_hi; ++bi) {
            const uint8_t* b = primaryBucket(bi);
            if (bucketCount(b) == 0 && bucketExtPtr(b) == 0) continue;

            bucket_entries.clear();

            while (b) {
                uint16_t cnt = bucketCount(b);
                const Slot* sl = bucketSlots(b);
                for (uint16_t i = 0; i < cnt; ++i) {
                    bucket_entries.push_back({sl[i].hash, sl[i].pv, sl[i].offset});
                }
                uint32_t ext = bucketExtPtr(b);
                b = ext ? extBucketData(ext) : nullptr;
            }

            size_t i = 0;
            while (i < bucket_entries.size()) {
                size_t j = i + 1;
                while (j < bucket_entries.size() &&
                       bucket_entries[j].hash == bucket_entries[i].hash) {
                    ++j;
                }
                size_t group_size = j - i;

                // Emit one entry: append to segment, record in flushed list.
                auto emitEntry = [&](const Slot& e) -> bool {
                    const uint8_t* p = data_base + e.offset;
                    uint16_t kl;
                    uint32_t vl;
                    std::memcpy(&kl, p + kKeyLenOffset, kKeyLenSize);
                    std::memcpy(&vl, p + kValueLenOffset, kValueLenSize);
                    size_t payload_len = kRecordHeaderSize + kl + vl;
                    uint64_t entry_offset;
                    Status st = out.appendRawEntry(p, payload_len, e.hash, entry_offset);
                    if (!st.ok()) { pf.status = st; return false; }
                    pf.flushed.push_back({e.hash, e.pv});
                    uint64_t v = PackedVersion(e.pv).version();
                    if (v > pf.max_ver) pf.max_ver = v;
                    return true;
                };

                if (snapshot_versions.empty() || group_size == 1) {
                    if (!emitEntry(bucket_entries[i])) return;
                } else {
                    std::vector<uint64_t> vers(group_size);
                    for (size_t k = 0; k < group_size; ++k) {
                        vers[k] = PackedVersion(
                            bucket_entries[i + group_size - 1 - k].pv).version();
                    }
                    std::unique_ptr<bool[]> keep(new bool[group_size]);
                    dedupVersionGroup(vers.data(), group_size,
                                         snapshot_versions, keep.get());

                    for (size_t k = 0; k < group_size; ++k) {
                        if (keep[group_size - 1 - k]) {
                            if (!emitEntry(bucket_entries[i + k])) return;
                        }
                    }
                }
                i = j;
            }
        }
        if (seal_partition) {
            Status st = out.partition(part_idx).seal(out.getId());
            if (!st.ok()) { pf.status = st; return; }
        }
        pf.status = Status::OK();
    };

    uint32_t buckets_per_part = kNumBuckets / num_parts;

    if (num_parts == 1 || !flush_pool) {
        // Sequential: emit + seal each partition in order.
        for (uint16_t p = 0; p < num_parts; ++p) {
            flushRange(p * buckets_per_part, (p + 1) * buckets_per_part,
                       p, true, parts[p]);
            if (!parts[p].status.ok()) return {parts[p].status, {}, 0};
        }
    } else {
        // Parallel: each task emits entries + seals its partition.
        for (uint16_t p = 0; p < num_parts; ++p) {
            flush_pool->submit([&, p] {
                flushRange(p * buckets_per_part, (p + 1) * buckets_per_part,
                           p, true, parts[p]);
            });
        }
        flush_pool->wait();
        for (const auto& pf : parts) {
            if (!pf.status.ok()) return {pf.status, {}, 0};
        }
    }
    out.markSealed();

    // Merge results from all partitions.
    size_t total_flushed = 0;
    uint64_t max_ver = 0;
    for (const auto& pf : parts) {
        total_flushed += pf.flushed.size();
        if (pf.max_ver > max_ver) max_ver = pf.max_ver;
    }

    std::vector<HashVersionPair> flushed;
    flushed.reserve(total_flushed);
    for (auto& pf : parts) {
        flushed.insert(flushed.end(), pf.flushed.begin(), pf.flushed.end());
    }

    return {Status::OK(), std::move(flushed), max_ver};
}

} // namespace internal
} // namespace kvlite
