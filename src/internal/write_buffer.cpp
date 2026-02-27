#include "internal/write_buffer.h"

#include <algorithm>
#include <unordered_map>

#include "internal/delta_hash_table.h"
#include "internal/entry_stream.h"
#include "internal/segment.h"

namespace kvlite {
namespace internal {

WriteBuffer::WriteBuffer()
    : data_(new uint8_t[kDefaultDataCapacity]),  // uninitialized — pages allocated lazily
      data_capacity_(kDefaultDataCapacity),
      buckets_(std::make_unique<Bucket[]>(kNumBuckets)),   // zero-initialized
      locks_(std::make_unique<Spinlock[]>(kNumBuckets)) {
    std::memset(overflow_blocks_, 0, sizeof(overflow_blocks_));
}

WriteBuffer::~WriteBuffer() {
    freeOverflow();
}

// --- Record I/O ----------------------------------------------------------
//
// Data record layout (all little-endian on x86):
//   offset + 0:  uint16_t key_len
//   offset + 2:  uint32_t value_len
//   offset + 6:  uint64_t packed_version
//   offset + 14: uint8_t  key[key_len]
//   offset + 14 + key_len: uint8_t value[value_len]

uint32_t WriteBuffer::appendRecord(const std::string& key, PackedVersion pv,
                                   const std::string& value) {
    uint16_t key_len = static_cast<uint16_t>(key.size());
    uint32_t val_len = static_cast<uint32_t>(value.size());
    size_t record_size = kRecordHeaderSize + key_len + val_len;

    size_t off = data_end_.fetch_add(record_size, std::memory_order_relaxed);

    uint8_t* p = data_.get() + off;
    std::memcpy(p, &key_len, 2);  p += 2;
    std::memcpy(p, &val_len, 4);  p += 4;
    std::memcpy(p, &pv.data, 8);  p += 8;
    std::memcpy(p, key.data(), key_len);  p += key_len;
    std::memcpy(p, value.data(), val_len);

    return static_cast<uint32_t>(off);
}

bool WriteBuffer::keyMatches(uint32_t off, const std::string& key) const {
    const uint8_t* p = data_.get() + off;
    uint16_t kl;
    std::memcpy(&kl, p, 2);
    if (kl != key.size()) return false;
    return std::memcmp(p + kRecordHeaderSize, key.data(), kl) == 0;
}

PackedVersion WriteBuffer::readPackedVersion(uint32_t off) const {
    uint64_t d;
    std::memcpy(&d, data_.get() + off + 6, 8);
    return PackedVersion(d);
}

void WriteBuffer::readValue(uint32_t off, std::string& value) const {
    const uint8_t* p = data_.get() + off;
    uint16_t kl;
    uint32_t vl;
    std::memcpy(&kl, p, 2);
    std::memcpy(&vl, p + 2, 4);
    value.assign(reinterpret_cast<const char*>(p + kRecordHeaderSize + kl), vl);
}

std::string WriteBuffer::readKey(uint32_t off) const {
    const uint8_t* p = data_.get() + off;
    uint16_t kl;
    std::memcpy(&kl, p, 2);
    return std::string(reinterpret_cast<const char*>(p + kRecordHeaderSize), kl);
}

// --- Overflow pool -------------------------------------------------------

WriteBuffer::Bucket& WriteBuffer::getOverflowBucket(uint32_t idx) {
    uint32_t i = idx - 1;  // 1-based → 0-based
    return overflow_blocks_[i >> kOverflowBlockShift][i & (kOverflowBlockSize - 1)];
}

const WriteBuffer::Bucket& WriteBuffer::getOverflowBucket(uint32_t idx) const {
    uint32_t i = idx - 1;
    return overflow_blocks_[i >> kOverflowBlockShift][i & (kOverflowBlockSize - 1)];
}

uint32_t WriteBuffer::allocOverflowBucket() {
    overflow_alloc_lock_.lock();
    uint32_t idx = overflow_count_.fetch_add(1, std::memory_order_relaxed);
    uint32_t block = idx >> kOverflowBlockShift;
    if (!overflow_blocks_[block]) {
        overflow_blocks_[block] = new Bucket[kOverflowBlockSize]();
    }
    overflow_alloc_lock_.unlock();
    return idx + 1;  // 1-based
}

void WriteBuffer::freeOverflow() {
    uint32_t count = overflow_count_.load(std::memory_order_relaxed);
    if (count == 0) return;
    uint32_t blocks = ((count - 1) >> kOverflowBlockShift) + 1;
    for (uint32_t i = 0; i < blocks; ++i) {
        delete[] overflow_blocks_[i];
        overflow_blocks_[i] = nullptr;
    }
    overflow_count_.store(0, std::memory_order_relaxed);
}

// --- Public API ----------------------------------------------------------

void WriteBuffer::put(const std::string& key, uint64_t version,
                      const std::string& value, bool tombstone) {
    uint64_t hash = dhtHashBytes(key.data(), key.size());
    uint32_t bi = bucketIndex(hash);
    uint32_t fp = fingerprint(hash);
    PackedVersion pv(version, tombstone);

    // Append record to data buffer (lock-free, space reserved atomically)
    uint32_t offset = appendRecord(key, pv, value);

    locks_[bi].lock();

    // Check if key already exists in this bucket chain
    bool key_exists = false;
    {
        const Bucket* scan = &buckets_[bi];
        while (scan) {
            for (uint32_t i = 0; i < scan->count; ++i) {
                if (scan->slots[i].fingerprint == fp &&
                    keyMatches(scan->slots[i].offset, key)) {
                    key_exists = true;
                    break;
                }
            }
            if (key_exists) break;
            scan = scan->overflow ? &getOverflowBucket(scan->overflow) : nullptr;
        }
    }

    // Find a bucket in the chain with a free slot
    Bucket* b = &buckets_[bi];
    while (b->count >= kSlotsPerBucket) {
        if (b->overflow) {
            b = &getOverflowBucket(b->overflow);
        } else {
            b->overflow = allocOverflowBucket();
            b = &getOverflowBucket(b->overflow);
        }
    }

    b->slots[b->count] = {fp, offset};
    b->count++;

    locks_[bi].unlock();

    size_.fetch_add(1, std::memory_order_relaxed);
    if (!key_exists) {
        key_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

void WriteBuffer::putBatch(const std::vector<BatchOp>& ops, uint64_t version) {
    if (ops.empty()) return;

    // Pre-compute hashes and bucket indices for all operations.
    struct Prepared {
        uint32_t op_idx;     // index into ops
        uint64_t hash;
        uint32_t bi;         // bucket index
        uint32_t fp;         // fingerprint
        uint32_t offset;     // data buffer offset (filled after appendRecord)
    };

    std::vector<Prepared> items;
    items.reserve(ops.size());
    for (uint32_t i = 0; i < ops.size(); ++i) {
        uint64_t h = dhtHashBytes(ops[i].key->data(), ops[i].key->size());
        items.push_back({i, h, bucketIndex(h), fingerprint(h), 0});
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
        const auto& op = ops[item.op_idx];

        // Check if key already exists in this bucket chain.
        bool key_exists = false;
        {
            const Bucket* scan = &buckets_[item.bi];
            while (scan) {
                for (uint32_t i = 0; i < scan->count; ++i) {
                    if (scan->slots[i].fingerprint == item.fp &&
                        keyMatches(scan->slots[i].offset, *op.key)) {
                        key_exists = true;
                        break;
                    }
                }
                if (key_exists) break;
                scan = scan->overflow ? &getOverflowBucket(scan->overflow) : nullptr;
            }
        }

        // Find a bucket with a free slot.
        Bucket* b = &buckets_[item.bi];
        while (b->count >= kSlotsPerBucket) {
            if (b->overflow) {
                b = &getOverflowBucket(b->overflow);
            } else {
                b->overflow = allocOverflowBucket();
                b = &getOverflowBucket(b->overflow);
            }
        }

        b->slots[b->count] = {item.fp, item.offset};
        b->count++;

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

bool WriteBuffer::get(const std::string& key,
                      std::string& value, uint64_t& version, bool& tombstone) const {
    return getByVersion(key, UINT64_MAX, value, version, tombstone);
}

bool WriteBuffer::getByVersion(const std::string& key, uint64_t upper_bound,
                               std::string& value, uint64_t& version,
                               bool& tombstone) const {
    uint64_t hash = dhtHashBytes(key.data(), key.size());
    uint32_t bi = bucketIndex(hash);
    uint32_t fp = fingerprint(hash);

    locks_[bi].lock();

    bool found = false;
    uint64_t best_version = 0;
    PackedVersion best_pv;
    uint32_t best_offset = 0;

    const Bucket* b = &buckets_[bi];
    while (b) {
        for (uint32_t i = 0; i < b->count; ++i) {
            if (b->slots[i].fingerprint != fp) continue;
            uint32_t off = b->slots[i].offset;
            if (!keyMatches(off, key)) continue;

            PackedVersion pv = readPackedVersion(off);
            if (pv.data <= upper_bound &&
                (!found || pv.data > best_version)) {
                best_version = pv.data;
                best_pv = pv;
                best_offset = off;
                found = true;
            }
        }
        b = b->overflow ? &getOverflowBucket(b->overflow) : nullptr;
    }

    if (found) {
        readValue(best_offset, value);
        version = best_pv.version();
        tombstone = best_pv.tombstone();
    }

    locks_[bi].unlock();
    return found;
}

void WriteBuffer::forEach(const std::function<void(const std::string&,
                                                    const std::vector<Entry>&)>& fn) const {
    for (uint32_t bi = 0; bi < kNumBuckets; ++bi) {
        const Bucket* b = &buckets_[bi];
        if (b->count == 0 && b->overflow == 0) continue;

        // Group entries by key within this bucket chain.
        // All entries for a given key hash to the same chain.
        std::unordered_map<std::string, std::vector<Entry>> groups;

        while (b) {
            for (uint32_t i = 0; i < b->count; ++i) {
                uint32_t off = b->slots[i].offset;
                std::string key = readKey(off);
                PackedVersion pv = readPackedVersion(off);
                std::string val;
                readValue(off, val);
                groups[key].push_back({pv, std::move(val)});
            }
            b = b->overflow ? &getOverflowBucket(b->overflow) : nullptr;
        }

        for (auto& kv : groups) {
            fn(kv.first, kv.second);
        }
    }
}

void WriteBuffer::clear() {
    data_end_.store(0, std::memory_order_relaxed);
    for (uint32_t i = 0; i < kNumBuckets; ++i) {
        buckets_[i].count = 0;
        buckets_[i].overflow = 0;
    }
    freeOverflow();
    size_.store(0, std::memory_order_relaxed);
    key_count_.store(0, std::memory_order_relaxed);
}

// ---------------------------------------------------------------------------
// WriteBufferStream — EntryStream backed by compact offset records
// into WriteBuffer::data_
// ---------------------------------------------------------------------------

namespace {

class WriteBufferStream : public EntryStream {
public:
    struct Record {
        uint64_t hash;
        uint32_t offset;   // into WriteBuffer::data_
        uint16_t key_len;
        uint32_t val_len;
        PackedVersion pv;
    };

    WriteBufferStream(const uint8_t* data, std::vector<Record> records)
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
            data_ + r.offset + 14);  // kRecordHeaderSize = 14
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
// WriteBuffer::createStream
// ---------------------------------------------------------------------------

std::unique_ptr<EntryStream> WriteBuffer::createStream(uint64_t snapshot_version) const {
    std::vector<WriteBufferStream::Record> all;

    for (uint32_t bi = 0; bi < kNumBuckets; ++bi) {
        locks_[bi].lock();

        const Bucket* b = &buckets_[bi];
        while (b) {
            for (uint32_t i = 0; i < b->count; ++i) {
                uint32_t off = b->slots[i].offset;
                PackedVersion pv = readPackedVersion(off);
                if (pv.data > snapshot_version) continue;

                const uint8_t* p = data_.get() + off;
                uint16_t kl;
                uint32_t vl;
                std::memcpy(&kl, p, 2);
                std::memcpy(&vl, p + 2, 4);

                uint64_t h = dhtHashBytes(p + kRecordHeaderSize, kl);

                all.push_back({h, off, kl, vl, pv});
            }
            b = b->overflow ? &getOverflowBucket(b->overflow) : nullptr;
        }

        locks_[bi].unlock();
    }

    const uint8_t* data_ptr = data_.get();

    // Deduplicate: keep only the latest version per key (by hash+key bytes).
    // Sort by (hash, key bytes, version desc) to group duplicates together.
    std::sort(all.begin(), all.end(),
              [data_ptr](const WriteBufferStream::Record& a,
                         const WriteBufferStream::Record& b) {
                  if (a.hash != b.hash) return a.hash < b.hash;
                  // Compare key bytes directly in data_
                  const uint8_t* ka = data_ptr + a.offset + 14;
                  const uint8_t* kb = data_ptr + b.offset + 14;
                  uint16_t min_len = a.key_len < b.key_len ? a.key_len : b.key_len;
                  int cmp = std::memcmp(ka, kb, min_len);
                  if (cmp != 0) return cmp < 0;
                  if (a.key_len != b.key_len) return a.key_len < b.key_len;
                  return a.pv.version() > b.pv.version();  // desc for dedup
              });

    // Build output: one record per key (first in each group = latest).
    std::vector<WriteBufferStream::Record> deduped;
    for (size_t i = 0; i < all.size(); ) {
        deduped.push_back(all[i]);

        // Skip remaining entries with same hash+key.
        uint64_t h = all[i].hash;
        const uint8_t* key_i = data_ptr + all[i].offset + 14;
        uint16_t kl_i = all[i].key_len;
        ++i;
        while (i < all.size() && all[i].hash == h &&
               all[i].key_len == kl_i &&
               std::memcmp(data_ptr + all[i].offset + 14, key_i, kl_i) == 0) {
            ++i;
        }
    }

    // Re-sort into merge order: (hash asc, version asc).
    std::sort(deduped.begin(), deduped.end(),
              [](const WriteBufferStream::Record& a,
                 const WriteBufferStream::Record& b) {
                  if (a.hash != b.hash) return a.hash < b.hash;
                  return a.pv.version() < b.pv.version();
              });

    return std::make_unique<WriteBufferStream>(data_ptr, std::move(deduped));
}

WriteBuffer::FlushResult WriteBuffer::flush(Segment& out) {
    Status s;

    struct FlatEntry {
        uint64_t hash;
        uint64_t packed_ver;  // (logical_version << 1) | tombstone
        std::string key;
        PackedVersion pv;
        std::string value;
    };

    std::vector<FlushedEntry> flushed;
    flushed.reserve(size_.load(std::memory_order_relaxed));

    // Process one bucket at a time. Iterating buckets 0..N gives hash-prefix
    // order; we only need to sort within each bucket by (hash, version).
    std::vector<FlatEntry> bucket_entries;

    for (uint32_t bi = 0; bi < kNumBuckets; ++bi) {
        const Bucket* b = &buckets_[bi];
        if (b->count == 0 && b->overflow == 0) continue;

        bucket_entries.clear();

        while (b) {
            for (uint32_t i = 0; i < b->count; ++i) {
                uint32_t off = b->slots[i].offset;
                std::string key = readKey(off);
                uint64_t h = dhtHashBytes(key.data(), key.size());
                PackedVersion pv = readPackedVersion(off);
                std::string val;
                readValue(off, val);
                bucket_entries.push_back({h, pv.data, std::move(key),
                                          pv, std::move(val)});
            }
            b = b->overflow ? &getOverflowBucket(b->overflow) : nullptr;
        }

        std::sort(bucket_entries.begin(), bucket_entries.end(),
                  [](const FlatEntry& a, const FlatEntry& b) {
                      if (a.hash != b.hash) return a.hash < b.hash;
                      return a.packed_ver < b.packed_ver;
                  });

        for (const auto& e : bucket_entries) {
            s = out.put(e.key, e.pv.version(), e.value, e.pv.tombstone());
            if (!s.ok()) return {s, {}};
            flushed.push_back({e.key, e.packed_ver});
        }
    }

    s = out.seal();
    if (!s.ok()) return {s, {}};

    return {Status::OK(), std::move(flushed)};
}

} // namespace internal
} // namespace kvlite
