#include "internal/read_only_delta_hash_table.h"

#include <cassert>

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

void ReadOnlyDeltaHashTable::addEntry(std::string_view key,
                                       uint64_t packed_version, uint32_t id) {
    assert(!sealed_);

    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    addToChain(bi, li, fp, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    ++size_;
}

bool ReadOnlyDeltaHashTable::addEntryIsNew(std::string_view key,
                                            uint64_t packed_version, uint32_t id) {
    assert(!sealed_);

    uint64_t h = hashKey(key);
    uint32_t bi = bucketIndex(h);
    uint32_t li = lslotIndex(h);
    uint64_t fp = fingerprint(h);

    bool is_new = addToChain(bi, li, fp, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    ++size_;
    return is_new;
}

void ReadOnlyDeltaHashTable::addEntryByHash(uint64_t hash,
                                             uint64_t packed_version, uint32_t id) {
    assert(!sealed_);

    uint32_t bi = bucketIndex(hash);
    uint32_t li = lslotIndex(hash);
    uint64_t fp = fingerprint(hash);

    addToChain(bi, li, fp, packed_version, id,
        [this](Bucket& bucket) -> Bucket* {
            return createExtension(bucket);
        });
    ++size_;
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
