#include "internal/read_write_delta_hash_table.h"

#include <algorithm>

namespace kvlite {
namespace internal {

ReadWriteDeltaHashTable::ReadWriteDeltaHashTable()
    : ReadWriteDeltaHashTable(Config{}) {}

ReadWriteDeltaHashTable::ReadWriteDeltaHashTable(const Config& config)
    : DeltaHashTable(config) {
    bucket_locks_ = std::make_unique<BucketLock[]>(1u << config.bucket_bits);
}

ReadWriteDeltaHashTable::~ReadWriteDeltaHashTable() = default;

// --- Find (locked) ---

bool ReadWriteDeltaHashTable::findAll(std::string_view key,
                                       std::vector<uint64_t>& packed_versions,
                                       std::vector<uint32_t>& ids) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(
        const_cast<ReadWriteDeltaHashTable*>(this)->bucket_locks_[bi]);
    return findAllByHash(bi, li, fp, packed_versions, ids);
}

bool ReadWriteDeltaHashTable::findFirst(std::string_view key,
                                         uint64_t& packed_version,
                                         uint32_t& id) const {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(
        const_cast<ReadWriteDeltaHashTable*>(this)->bucket_locks_[bi]);
    return findFirstByHash(bi, li, fp, packed_version, id);
}

bool ReadWriteDeltaHashTable::contains(std::string_view key) const {
    uint64_t pv;
    uint32_t id;
    return findFirst(key, pv, id);
}

// --- Add ---

void ReadWriteDeltaHashTable::addEntry(std::string_view key,
                                        uint64_t packed_version, uint32_t id) {
    uint64_t h = hashKey(key);
    addImpl(bucketIndex(h), lslotIndex(h), fingerprint(h),
            packed_version, id);
}

bool ReadWriteDeltaHashTable::addEntryIsNew(std::string_view key,
                                              uint64_t packed_version, uint32_t id) {
    uint64_t h = hashKey(key);
    return addImpl(bucketIndex(h), lslotIndex(h), fingerprint(h),
                   packed_version, id);
}

void ReadWriteDeltaHashTable::addEntryByHash(uint64_t hash,
                                              uint64_t packed_version,
                                              uint32_t id) {
    addImpl(bucketIndex(hash), lslotIndex(hash), fingerprint(hash),
            packed_version, id);
}

bool ReadWriteDeltaHashTable::removeEntry(std::string_view key,
                                           uint64_t packed_version, uint32_t id) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(bucket_locks_[bi]);
    bool group_empty = removeFromChain(bi, li, fp, packed_version, id);
    size_.fetch_sub(1, std::memory_order_relaxed);
    return group_empty;
}

bool ReadWriteDeltaHashTable::updateEntryId(std::string_view key,
                                              uint64_t packed_version,
                                              uint32_t old_id, uint32_t new_id) {
    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    BucketLockGuard guard(bucket_locks_[bi]);
    return updateIdInChain(bi, li, fp, packed_version, old_id, new_id,
        [this](Bucket& bucket) -> Bucket* {
            std::lock_guard<std::mutex> g(ext_mutex_);
            return createExtension(bucket);
        });
}

bool ReadWriteDeltaHashTable::addImpl(uint32_t bi, uint32_t li, uint64_t fp,
                                       uint64_t packed_version, uint32_t id) {
    BucketLockGuard guard(bucket_locks_[bi]);

    bool is_new = addToChain(bi, li, fp, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            std::lock_guard<std::mutex> g(ext_mutex_);
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
         + (1u << config_.bucket_bits) * sizeof(BucketLock);
}

void ReadWriteDeltaHashTable::clear() {
    clearBuckets();
    size_.store(0, std::memory_order_relaxed);
}

}  // namespace internal
}  // namespace kvlite
