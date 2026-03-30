#include "internal/read_only_delta_hash_table.h"

#include <algorithm>
#include <cassert>

#include "internal/profiling.h"

namespace kvlite {
namespace internal {

ReadOnlyDeltaHashTable::ReadOnlyDeltaHashTable()
    : ReadOnlyDeltaHashTable(Config{}) {}

ReadOnlyDeltaHashTable::ReadOnlyDeltaHashTable(const Config& config)
    : DeltaHashTable(config),
      ext_arena_owned_(sizeof(Bucket) + bucketStride(), /*concurrent=*/false) {
    ext_arena_ = &ext_arena_owned_;
}

ReadOnlyDeltaHashTable::~ReadOnlyDeltaHashTable() = default;

ReadOnlyDeltaHashTable::ReadOnlyDeltaHashTable(ReadOnlyDeltaHashTable&& o) noexcept
    : DeltaHashTable(std::move(o)),
      ext_arena_owned_(std::move(o.ext_arena_owned_)),
      sealed_(o.sealed_),
      size_(o.size_) {
    ext_arena_ = &ext_arena_owned_;
    o.size_ = 0;
}

ReadOnlyDeltaHashTable& ReadOnlyDeltaHashTable::operator=(ReadOnlyDeltaHashTable&& o) noexcept {
    if (this != &o) {
        DeltaHashTable::operator=(std::move(o));
        ext_arena_owned_ = std::move(o.ext_arena_owned_);
        sealed_ = o.sealed_;
        size_ = o.size_;
        ext_arena_ = &ext_arena_owned_;
        o.size_ = 0;
    }
    return *this;
}

void ReadOnlyDeltaHashTable::addEntry(uint64_t hash,
                                       uint64_t packed_version, uint32_t id) {
    assert(!sealed_);

    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    addToChain(bi, suffix, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    ++size_;
}

bool ReadOnlyDeltaHashTable::addEntryIsNew(uint64_t hash,
                                            uint64_t packed_version, uint32_t id) {
    assert(!sealed_);

    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    bool is_new = addToChain(bi, suffix, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    ++size_;
    return is_new;
}

// --- Streaming batch build ---

void ReadOnlyDeltaHashTable::flushPendingBucket() {
    if (pending_bi_ == UINT32_MAX) return;

    if (codec_.contentsBitsNeeded(pending_contents_) <= codec_.bucketDataBits()) {
        auto t0 = now();
        codec_.encodeBucket(buckets_[pending_bi_], pending_contents_);
        trackTime(encode_count_, encode_total_ns_, t0);
        size_ += pending_entries_;
        batch_key_count_ += pending_keys_;
    } else {
        // Overflow — replay entries through per-entry addToChain.
        for (auto& ke : pending_contents_.keys) {
            // Versions are descending; iterate backward to feed ascending
            // to addEntryIsNew which inserts in desc order internally.
            bool is_new = true;
            for (size_t i = ke.packed_versions.size(); i > 0; --i) {
                uint64_t hash = (static_cast<uint64_t>(pending_bi_)
                                 << (64 - config_.bucket_bits)) | ke.suffix;
                if (addEntryIsNew(hash, ke.packed_versions[i - 1], ke.ids[i - 1])) {
                    if (is_new) {
                        ++batch_key_count_;
                        is_new = false;
                    }
                }
            }
        }
    }

    pending_contents_.keys.clear();
    pending_keys_ = 0;
    pending_entries_ = 0;
    pending_bi_ = UINT32_MAX;
}

void ReadOnlyDeltaHashTable::addBatchEntry(uint64_t hash,
                                            uint64_t packed_version, uint32_t id) {
    assert(!sealed_);

    uint32_t bi = bucketIndex(hash);
    if (bi != pending_bi_) {
        flushPendingBucket();
        pending_bi_ = bi;
    }

    uint64_t suffix = suffixFromHash(hash);

    // Append to existing KeyEntry or create new one.
    // Keys within a bucket arrive in suffix order (same hash order).
    if (!pending_contents_.keys.empty() &&
        pending_contents_.keys.back().suffix == suffix) {
        // Same key — versions arrive descending (matching DHT convention).
        pending_contents_.keys.back().packed_versions.push_back(packed_version);
        pending_contents_.keys.back().ids.push_back(id);
    } else {
        KeyEntry ke;
        ke.suffix = suffix;
        ke.packed_versions.push_back(packed_version);
        ke.ids.push_back(id);
        pending_contents_.keys.push_back(std::move(ke));
        ++pending_keys_;
    }
    ++pending_entries_;
}

size_t ReadOnlyDeltaHashTable::endBatch() {
    flushPendingBucket();
    size_t result = batch_key_count_;
    batch_key_count_ = 0;
    return result;
}

// --- Binary snapshot ---

void ReadOnlyDeltaHashTable::writeExtData(uint8_t* dst) const {
    uint32_t stride = bucketStride();
    for (uint32_t i = 1; i <= ext_arena_owned_.size(); ++i) {
        const Bucket* b = ext_arena_owned_.get(i);
        std::memcpy(dst, b->data, stride);
        dst += stride;
    }
}

void ReadOnlyDeltaHashTable::loadBinary(const uint8_t* arena_data, size_t arena_len,
                                         const uint8_t* ext_data, uint32_t ext_count,
                                         size_t entry_count) {
    assert(!sealed_);
    loadArenaData(arena_data, arena_len);

    // Reconstruct extension buckets in the same 1-based index order.
    uint32_t stride = bucketStride();
    for (uint32_t i = 0; i < ext_count; ++i) {
        uint32_t idx = ext_arena_owned_.allocate();
        Bucket* b = ext_arena_owned_.get(idx);
        std::memcpy(b->data, ext_data + static_cast<size_t>(i) * stride, stride);
    }

    size_ = entry_count;
    sealed_ = true;
}

void ReadOnlyDeltaHashTable::seal() {
    assert(!sealed_);
    sealed_ = true;
}

size_t ReadOnlyDeltaHashTable::size() const {
    return size_;
}

void ReadOnlyDeltaHashTable::clear() {
    clearBuckets();
    size_ = 0;
    sealed_ = false;
}

}  // namespace internal
}  // namespace kvlite
