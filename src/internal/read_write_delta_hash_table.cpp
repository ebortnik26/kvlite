#include "internal/read_write_delta_hash_table.h"

#include <algorithm>
#include <cstring>

namespace kvlite {
namespace internal {

ReadWriteDeltaHashTable::ReadWriteDeltaHashTable()
    : ReadWriteDeltaHashTable(Config{}) {}

ReadWriteDeltaHashTable::ReadWriteDeltaHashTable(const Config& config)
    : DeltaHashTable(config),
      ext_arena_owned_(sizeof(Bucket) + bucketStride(), /*concurrent=*/true) {
    ext_arena_ = &ext_arena_owned_;
    bucket_locks_ = std::make_unique<Spinlock[]>(1u << config.bucket_bits);
}

ReadWriteDeltaHashTable::~ReadWriteDeltaHashTable() = default;

// --- Find (locked) ---

bool ReadWriteDeltaHashTable::findAll(uint64_t hash,
                                       std::vector<uint64_t>& packed_versions,
                                       std::vector<uint32_t>& ids) const {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return findAllByHash(bi, suffix, packed_versions, ids);
}

bool ReadWriteDeltaHashTable::findFirst(uint64_t hash,
                                         uint64_t& packed_version,
                                         uint32_t& id) const {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return findFirstByHash(bi, suffix, packed_version, id);
}

bool ReadWriteDeltaHashTable::contains(uint64_t hash) const {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return containsByHash(bi, suffix);
}

// --- Add ---

void ReadWriteDeltaHashTable::addEntry(uint64_t hash,
                                        uint64_t packed_version, uint32_t id) {
    addImpl(bucketIndex(hash), suffixFromHash(hash),
            packed_version, id);
}

bool ReadWriteDeltaHashTable::addEntryIsNew(uint64_t hash,
                                              uint64_t packed_version, uint32_t id) {
    return addImpl(bucketIndex(hash), suffixFromHash(hash),
                   packed_version, id);
}

bool ReadWriteDeltaHashTable::removeEntry(uint64_t hash,
                                           uint64_t packed_version, uint32_t id) {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    bool group_empty = removeFromChain(bi, suffix, packed_version, id);
    size_.fetch_sub(1, std::memory_order_relaxed);
    return group_empty;
}

bool ReadWriteDeltaHashTable::updateEntryId(uint64_t hash,
                                              uint64_t packed_version,
                                              uint32_t old_id, uint32_t new_id) {
    uint32_t bi = bucketIndex(hash);
    uint64_t suffix = suffixFromHash(hash);

    SpinlockGuard guard(bucket_locks_[bi]);
    return updateIdInChain(bi, suffix, packed_version, old_id, new_id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
}

bool ReadWriteDeltaHashTable::addImpl(uint32_t bi, uint64_t suffix,
                                       uint64_t packed_version, uint32_t id) {
    SpinlockGuard guard(bucket_locks_[bi]);

    bool is_new = addToChain(bi, suffix, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    size_.fetch_add(1, std::memory_order_relaxed);
    return is_new;
}

// --- Stats ---

size_t ReadWriteDeltaHashTable::size() const {
    return size_.load(std::memory_order_relaxed);
}

size_t ReadWriteDeltaHashTable::memoryUsage() const {
    return DeltaHashTable::memoryUsage()
         + (1u << config_.bucket_bits) * sizeof(Spinlock);
}

void ReadWriteDeltaHashTable::clear() {
    clearBuckets();
    size_.store(0, std::memory_order_relaxed);
}

// --- Binary snapshot support ---

uint32_t ReadWriteDeltaHashTable::extCount() const {
    return ext_arena_owned_.size();
}

const uint8_t* ReadWriteDeltaHashTable::extSlotData(uint32_t one_based) const {
    const Bucket* b = ext_arena_owned_.get(one_based);
    return b->data;
}

void ReadWriteDeltaHashTable::loadExtensions(const uint8_t* data, uint32_t count,
                                              uint32_t data_stride) {
    ext_arena_owned_.clear();
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t idx = ext_arena_owned_.allocate();
        Bucket* b = ext_arena_owned_.get(idx);
        std::memcpy(b->data, data + static_cast<size_t>(i) * data_stride, data_stride);
    }
}

void ReadWriteDeltaHashTable::setSize(size_t n) {
    size_.store(n, std::memory_order_relaxed);
}

uint32_t ReadWriteDeltaHashTable::snapshotBucketChain(
    uint32_t bi, std::vector<uint8_t>& buf) const {
    SpinlockGuard guard(bucket_locks_[bi]);
    const Bucket* b = &buckets_[bi];
    if (isEmptyBucket(*b) && !nextBucket(*b)) return 0;

    uint32_t stride = bucketStride();
    buf.clear();
    uint32_t chain_len = 0;
    while (b) {
        buf.insert(buf.end(), b->data, b->data + stride);
        ++chain_len;
        b = nextBucket(*b);
    }
    return chain_len;
}

void ReadWriteDeltaHashTable::loadBucketChain(
    uint32_t bi, const uint8_t* data, uint8_t chain_len) {
    uint32_t stride = bucketStride();
    // Main bucket
    std::memcpy(buckets_[bi].data, data, stride);
    Bucket* prev = &buckets_[bi];
    for (uint8_t i = 1; i < chain_len; ++i) {
        uint32_t slot = ext_arena_owned_.allocate();
        Bucket* ext = ext_arena_owned_.get(slot);
        std::memcpy(ext->data, data + static_cast<size_t>(i) * stride, stride);
        setExtensionPtr(*prev, slot);
        prev = ext;
    }
    setExtensionPtr(*prev, 0);  // terminate chain
}

}  // namespace internal
}  // namespace kvlite
