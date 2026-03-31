#include "internal/delta_hash_table.h"

#include <algorithm>
#include <cstring>

#include "internal/bit_stream.h"
#include "internal/profiling.h"

namespace kvlite {
namespace internal {

// --- Construction / Move ---

DeltaHashTable::DeltaHashTable(const Config& config)
    : config_(config),
      suffix_bits_(64 - config.bucket_bits),
      codec_(suffix_bits_, config.bucket_bytes),
      ext_arena_(nullptr) {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t stride = bucketStride();
    arena_ = std::make_unique<uint8_t[]>(
        static_cast<size_t>(num_buckets) * stride);
    std::memset(arena_.get(), 0,
                static_cast<size_t>(num_buckets) * stride);
    buckets_.resize(num_buckets);
    for (uint32_t i = 0; i < num_buckets; ++i) {
        buckets_[i].data = arena_.get() + static_cast<size_t>(i) * stride;
    }
    for (auto& bucket : buckets_) {
        initBucket(bucket);
    }
}

DeltaHashTable::~DeltaHashTable() = default;

DeltaHashTable::DeltaHashTable(DeltaHashTable&& other) noexcept
    : config_(other.config_),
      suffix_bits_(other.suffix_bits_),
      codec_(std::move(other.codec_)),
      arena_(std::move(other.arena_)),
      buckets_(std::move(other.buckets_)),
      ext_arena_(other.ext_arena_),
      encode_count_(other.encode_count_.load(std::memory_order_relaxed)),
      encode_total_ns_(other.encode_total_ns_.load(std::memory_order_relaxed)),
      decode_count_(other.decode_count_.load(std::memory_order_relaxed)),
      decode_total_ns_(other.decode_total_ns_.load(std::memory_order_relaxed)) {
    other.ext_arena_ = nullptr;
}

DeltaHashTable& DeltaHashTable::operator=(DeltaHashTable&& other) noexcept {
    if (this != &other) {
        config_ = other.config_;
        suffix_bits_ = other.suffix_bits_;
        codec_ = std::move(other.codec_);
        arena_ = std::move(other.arena_);
        buckets_ = std::move(other.buckets_);
        ext_arena_ = other.ext_arena_;
        other.ext_arena_ = nullptr;
        encode_count_.store(other.encode_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        encode_total_ns_.store(other.encode_total_ns_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        decode_count_.store(other.decode_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        decode_total_ns_.store(other.decode_total_ns_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
    return *this;
}

// --- Hash decomposition ---

uint32_t DeltaHashTable::bucketIndex(uint64_t hash) const {
    return static_cast<uint32_t>(hash >> (64 - config_.bucket_bits));
}

uint64_t DeltaHashTable::suffixFromHash(uint64_t hash) const {
    return hash & ((suffix_bits_ == 64) ? ~0ULL : ((1ULL << suffix_bits_) - 1));
}

// --- Bucket data ---

uint32_t DeltaHashTable::bucketStride() const {
    return config_.bucket_bytes + kBucketPadding;
}

uint64_t DeltaHashTable::getExtensionPtr(const Bucket& bucket) const {
    uint64_t ptr = 0;
    std::memcpy(&ptr, bucket.data + config_.bucket_bytes - 8, 8);
    return ptr;
}

void DeltaHashTable::setExtensionPtr(Bucket& bucket, uint64_t ptr) const {
    std::memcpy(bucket.data + config_.bucket_bytes - 8, &ptr, 8);
}

// --- Extension chain ---

const Bucket* DeltaHashTable::nextBucket(const Bucket& bucket) const {
    uint64_t ext = getExtensionPtr(bucket);
    return ext ? ext_arena_->get(static_cast<uint32_t>(ext)) : nullptr;
}

Bucket* DeltaHashTable::nextBucketMut(Bucket& bucket) {
    uint64_t ext = getExtensionPtr(bucket);
    return ext ? ext_arena_->get(static_cast<uint32_t>(ext)) : nullptr;
}

Bucket* DeltaHashTable::createExtension(Bucket& bucket) {
    uint32_t ext_ptr = ext_arena_->allocate();
    setExtensionPtr(bucket, ext_ptr);
    Bucket* ext_bucket = ext_arena_->get(ext_ptr);
    initBucket(*ext_bucket);
    return ext_bucket;
}

// --- Targeted scan helpers ---

bool DeltaHashTable::containsByHash(uint32_t bi, uint64_t suffix) const {
    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        auto t0 = now();
        auto scan = codec_.decodeSuffixes(*bucket);
        trackTime(decode_count_, decode_total_ns_, t0);
        if (std::binary_search(scan.suffixes.begin(), scan.suffixes.end(), suffix)) {
            return true;
        }
        bucket = nextBucket(*bucket);
    }
    return false;
}

// --- addToChain ---

bool DeltaHashTable::addToChain(uint32_t bi, uint64_t suffix,
                                 uint64_t packed_version, uint32_t id,
                                 const std::function<Bucket*(Bucket&)>& createExtFn) {
    Bucket* bucket = &buckets_[bi];
    assert(bucket->data != nullptr && "primary bucket->data null in addToChain");
    bool is_new = true;

    while (true) {
        assert(bucket->data != nullptr && "bucket->data null at top of addToChain loop");
        auto t0 = now();
        auto scan = codec_.decodeSuffixes(*bucket);
        trackTime(decode_count_, decode_total_ns_, t0);

        auto sit = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        bool suffix_found = (sit != scan.suffixes.end() && *sit == suffix);

        if (suffix_found) {
            is_new = false;

            // Single full decode replaces decodeKeyAt + decodeBucketUsedBits
            // + bitsForAddVersion + decodeBucket.
            t0 = now();
            auto contents = codec_.decodeBucket(*bucket);
            trackTime(decode_count_, decode_total_ns_, t0);

            auto cit = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
                [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
            auto vpos = std::lower_bound(cit->packed_versions.begin(),
                                         cit->packed_versions.end(),
                                         packed_version,
                                         std::greater<uint64_t>());
            size_t vi = vpos - cit->packed_versions.begin();
            cit->packed_versions.insert(vpos, packed_version);
            cit->ids.insert(cit->ids.begin() + vi, id);

            if (codec_.contentsBitsNeeded(contents) <= codec_.bucketDataBits()) {
                t0 = now();
                codec_.encodeBucket(*bucket, contents);
                trackTime(encode_count_, encode_total_ns_, t0);
                return is_new;
            }

            // Doesn't fit — discard local contents, move to extension.
            Bucket* ext = nextBucketMut(*bucket);
            if (!ext) ext = createExtFn(*bucket);
            assert(ext->data != nullptr && "ext->data null after suffix_found spill in addToChain");
            bucket = ext;
        } else {
            if (is_new) {
                const Bucket* check = nextBucket(*bucket);
                while (check) {
                    t0 = now();
                    auto check_scan = codec_.decodeSuffixes(*check);
                    trackTime(decode_count_, decode_total_ns_, t0);
                    if (std::binary_search(check_scan.suffixes.begin(),
                                           check_scan.suffixes.end(), suffix)) {
                        is_new = false;
                        break;
                    }
                    check = nextBucket(*check);
                }
            }

            // Single full decode replaces decodeBucketUsedBits +
            // bitsForNewEntry + decodeBucket.
            t0 = now();
            auto contents = codec_.decodeBucket(*bucket);
            trackTime(decode_count_, decode_total_ns_, t0);

            auto cit = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
                [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
            KeyEntry new_entry;
            new_entry.suffix = suffix;
            new_entry.packed_versions.push_back(packed_version);
            new_entry.ids.push_back(id);
            contents.keys.insert(cit, std::move(new_entry));

            if (codec_.contentsBitsNeeded(contents) <= codec_.bucketDataBits()) {
                t0 = now();
                codec_.encodeBucket(*bucket, contents);
                trackTime(encode_count_, encode_total_ns_, t0);
                return is_new;
            }

            // Doesn't fit — discard local contents, move to extension.
            Bucket* ext = nextBucketMut(*bucket);
            if (!ext) ext = createExtFn(*bucket);
            assert(ext->data != nullptr && "ext->data null after new-key spill in addToChain");
            bucket = ext;
        }
    }
}

// --- removeFromChain ---

bool DeltaHashTable::removeFromChain(uint32_t bi, uint64_t suffix,
                                      uint64_t packed_version, uint32_t id) {
    Bucket* bucket = &buckets_[bi];

    while (bucket) {
        auto t0 = now();
        auto contents = codec_.decodeBucket(*bucket);
        trackTime(decode_count_, decode_total_ns_, t0);

        auto it = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
            [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });

        if (it != contents.keys.end() && it->suffix == suffix) {
            for (size_t j = 0; j < it->packed_versions.size(); ++j) {
                if (it->packed_versions[j] == packed_version && it->ids[j] == id) {
                    it->packed_versions.erase(it->packed_versions.begin() + j);
                    it->ids.erase(it->ids.begin() + j);
                    if (it->packed_versions.empty()) {
                        contents.keys.erase(it);
                    }
                    t0 = now();
                    codec_.encodeBucket(*bucket, contents);
                    trackTime(encode_count_, encode_total_ns_, t0);
                    pruneEmptyExtension(bucket);
                    return isSuffixEmpty(bi, suffix);
                }
            }
        }

        bucket = nextBucketMut(*bucket);
    }

    return isSuffixEmpty(bi, suffix);
}

// --- updateIdInChain ---

bool DeltaHashTable::updateIdInChain(uint32_t bi, uint64_t suffix,
                                      uint64_t packed_version, uint32_t old_id,
                                      uint32_t new_id,
                                      const std::function<Bucket*(Bucket&)>& createExtFn) {
    Bucket* bucket = &buckets_[bi];

    while (bucket) {
        assert(bucket->data != nullptr && "bucket->data is null in updateIdInChain");
        auto t0 = now();
        auto contents = codec_.decodeBucket(*bucket);
        trackTime(decode_count_, decode_total_ns_, t0);

        auto it = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
            [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });

        if (it != contents.keys.end() && it->suffix == suffix) {
            for (size_t j = 0; j < it->packed_versions.size(); ++j) {
                if (it->packed_versions[j] == packed_version && it->ids[j] == old_id) {
                    it->ids[j] = new_id;

                    t0 = now();
                    size_t bits = codec_.contentsBitsNeeded(contents);
                    trackTime(decode_count_, decode_total_ns_, t0);

                    if (bits <= codec_.bucketDataBits()) {
                        t0 = now();
                        codec_.encodeBucket(*bucket, contents);
                        trackTime(encode_count_, encode_total_ns_, t0);
                        return true;
                    }
                    // Spill the modified key to extension, cascading if needed.
                    KeyEntry spilled = std::move(*it);
                    contents.keys.erase(it);

                    // Cascade: keep spilling the LAST key while bucket overflows.
                    std::vector<KeyEntry> spill_queue;
                    spill_queue.push_back(std::move(spilled));
                    while (!contents.keys.empty() &&
                           codec_.contentsBitsNeeded(contents) > codec_.bucketDataBits()) {
                        spill_queue.push_back(std::move(contents.keys.back()));
                        contents.keys.pop_back();
                    }

                    t0 = now();
                    codec_.encodeBucket(*bucket, contents);
                    trackTime(encode_count_, encode_total_ns_, t0);

                    // Insert all spilled keys into extensions.
                    for (auto& spilled_key : spill_queue) {
                        Bucket* ext = nextBucketMut(*bucket);
                        if (!ext) ext = createExtFn(*bucket);
                        assert(ext->data != nullptr && "ext->data null after spill in updateIdInChain");

                        t0 = now();
                        auto ext_contents = codec_.decodeBucket(*ext);
                        trackTime(decode_count_, decode_total_ns_, t0);

                        auto ext_it = std::lower_bound(ext_contents.keys.begin(),
                            ext_contents.keys.end(), spilled_key.suffix,
                            [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
                        ext_contents.keys.insert(ext_it, std::move(spilled_key));

                        // If extension also overflows, continue cascading.
                        while (!ext_contents.keys.empty() &&
                               codec_.contentsBitsNeeded(ext_contents) > codec_.bucketDataBits()) {
                            spill_queue.push_back(std::move(ext_contents.keys.back()));
                            ext_contents.keys.pop_back();
                        }

                        t0 = now();
                        codec_.encodeBucket(*ext, ext_contents);
                        trackTime(encode_count_, encode_total_ns_, t0);
                        bucket = ext;
                    }
                    return true;
                }
            }
        }

        bucket = nextBucketMut(*bucket);
    }

    return false;
}

// --- Public read API ---

// Walk the bucket chain for (bi, suffix). If found, decode the key and
// call visitor(bucket, key_index, scan). Returns whatever the visitor returns.
bool DeltaHashTable::findAllByHash(uint32_t bi, uint64_t suffix,
                                    std::vector<uint64_t>& packed_versions,
                                    std::vector<uint32_t>& ids) const {
    packed_versions.clear();
    ids.clear();

    // Walk the full chain — a key can span extension
    // buckets.  Keep the existing manual walk for this case.
    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        auto t0 = now();
        auto scan = codec_.decodeSuffixes(*bucket);
        trackTime(decode_count_, decode_total_ns_, t0);

        auto it = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        if (it != scan.suffixes.end() && *it == suffix) {
            size_t idx = it - scan.suffixes.begin();
            t0 = now();
            auto key = codec_.decodeKeyAt(*bucket, static_cast<uint16_t>(idx),
                                          suffix, scan.data_start_bit);
            trackTime(decode_count_, decode_total_ns_, t0);

            packed_versions.insert(packed_versions.end(),
                                   key.packed_versions.begin(),
                                   key.packed_versions.end());
            ids.insert(ids.end(), key.ids.begin(), key.ids.end());
        }
        bucket = nextBucket(*bucket);
    }
    return !packed_versions.empty();
}

// Walk the full bucket chain, decode all versions for `suffix`, and return
// the highest version (optionally bounded by upper_bound).  Both findFirst
// and findFirstBounded use this — a key can span extension buckets so the
// highest version may be in any bucket.
bool DeltaHashTable::findBestInChain(uint32_t bi, uint64_t suffix,
                                      bool bounded, uint64_t upper_bound,
                                      uint64_t& packed_version, uint32_t& id) const {
    bool found = false;
    uint64_t best_pv = 0;
    uint32_t best_id = 0;

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        auto t0 = now();
        auto scan = codec_.decodeSuffixes(*bucket);
        trackTime(decode_count_, decode_total_ns_, t0);

        auto it = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        if (it != scan.suffixes.end() && *it == suffix) {
            uint16_t idx = static_cast<uint16_t>(it - scan.suffixes.begin());
            t0 = now();
            auto key = codec_.decodeKeyAt(*bucket, idx, suffix, scan.data_start_bit);
            trackTime(decode_count_, decode_total_ns_, t0);

            for (size_t i = 0; i < key.packed_versions.size(); ++i) {
                uint64_t pv = key.packed_versions[i];
                if (bounded && pv > upper_bound) continue;
                if (!found || pv > best_pv) {
                    best_pv = pv;
                    best_id = key.ids[i];
                    found = true;
                }
            }
        }
        bucket = nextBucket(*bucket);
    }

    if (found) {
        packed_version = best_pv;
        id = best_id;
    }
    return found;
}

bool DeltaHashTable::findFirstByHash(uint32_t bi, uint64_t suffix,
                                      uint64_t& packed_version, uint32_t& id) const {
    return findBestInChain(bi, suffix, false, 0, packed_version, id);
}

bool DeltaHashTable::findFirstBoundedByHash(uint32_t bi, uint64_t suffix,
                                             uint64_t upper_bound,
                                             uint64_t& packed_version,
                                             uint32_t& id) const {
    return findBestInChain(bi, suffix, true, upper_bound, packed_version, id);
}

bool DeltaHashTable::findFirstBounded(uint64_t hash, uint64_t upper_bound,
                                       uint64_t& packed_version, uint32_t& id) const {
    return findFirstBoundedByHash(bucketIndex(hash), suffixFromHash(hash),
                                  upper_bound, packed_version, id);
}

bool DeltaHashTable::findAll(uint64_t hash,
                              std::vector<uint64_t>& packed_versions,
                              std::vector<uint32_t>& ids) const {
    return findAllByHash(bucketIndex(hash), suffixFromHash(hash),
                         packed_versions, ids);
}

bool DeltaHashTable::findFirst(uint64_t hash,
                                uint64_t& packed_version, uint32_t& id) const {
    return findFirstByHash(bucketIndex(hash), suffixFromHash(hash),
                           packed_version, id);
}

bool DeltaHashTable::contains(uint64_t hash) const {
    return containsByHash(bucketIndex(hash), suffixFromHash(hash));
}

void DeltaHashTable::forEach(
    const std::function<void(uint64_t hash, uint64_t packed_version,
                             uint32_t id)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        const Bucket* b = &buckets_[bi];
        if (isEmptyBucket(*b) && !nextBucket(*b)) continue;
        while (b) {
            auto t0 = now();
            auto contents = codec_.decodeBucket(*b);
            trackTime(decode_count_, decode_total_ns_, t0);

            for (const auto& key : contents.keys) {
                uint64_t hash =
                    (static_cast<uint64_t>(bi) << (64 - config_.bucket_bits)) | key.suffix;
                for (size_t i = 0; i < key.packed_versions.size(); ++i) {
                    fn(hash, key.packed_versions[i], key.ids[i]);
                }
            }
            b = nextBucket(*b);
        }
    }
}

void DeltaHashTable::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint64_t>& packed_versions,
                             const std::vector<uint32_t>& ids)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        const Bucket* first = &buckets_[bi];
        if (isEmptyBucket(*first) && !nextBucket(*first)) continue;

        std::vector<KeyEntry> groups;

        const Bucket* b = first;
        while (b) {
            auto t0 = now();
            auto contents = codec_.decodeBucket(*b);
            trackTime(decode_count_, decode_total_ns_, t0);

            for (auto& key : contents.keys) {
                auto it = std::lower_bound(groups.begin(), groups.end(),
                    key.suffix, [](const KeyEntry& e, uint64_t s) { return e.suffix < s; });
                if (it != groups.end() && it->suffix == key.suffix) {
                    it->packed_versions.insert(it->packed_versions.end(),
                                               key.packed_versions.begin(),
                                               key.packed_versions.end());
                    it->ids.insert(it->ids.end(),
                                   key.ids.begin(),
                                   key.ids.end());
                } else {
                    groups.insert(it, std::move(key));
                }
            }
            b = nextBucket(*b);
        }

        for (const auto& g : groups) {
            uint64_t hash =
                (static_cast<uint64_t>(bi) << (64 - config_.bucket_bits)) | g.suffix;
            fn(hash, g.packed_versions, g.ids);
        }
    }
}

// --- Stats ---

size_t DeltaHashTable::memoryUsage() const {
    size_t stride = config_.bucket_bytes + kBucketPadding;
    return buckets_.size() * stride + ext_arena_->dataBytes();
}

void DeltaHashTable::clearBuckets() {
    for (auto& bucket : buckets_) {
        setExtensionPtr(bucket, 0);
        initBucket(bucket);
    }
    ext_arena_->clear();
}

// --- Public accessors for binary snapshot ---

const uint8_t* DeltaHashTable::arenaData() const {
    return arena_.get();
}

size_t DeltaHashTable::arenaBytes() const {
    return static_cast<size_t>(1u << config_.bucket_bits) * bucketStride();
}

uint32_t DeltaHashTable::numBuckets() const {
    return 1u << config_.bucket_bits;
}

const DeltaHashTable::Config& DeltaHashTable::config() const {
    return config_;
}

uint8_t DeltaHashTable::suffixBits() const {
    return suffix_bits_;
}

void DeltaHashTable::loadArenaData(const uint8_t* data, size_t len) {
    std::memcpy(arena_.get(), data, len);
}

void DeltaHashTable::initBucket(Bucket& bucket) {
    size_t data_bytes = config_.bucket_bytes - 8;
    uint64_t ext_ptr = getExtensionPtr(bucket);
    std::memset(bucket.data, 0, data_bytes);
    setExtensionPtr(bucket, ext_ptr);
}

void DeltaHashTable::pruneEmptyExtension(Bucket* bucket) {
    Bucket* ext = nextBucketMut(*bucket);
    if (!ext) return;
    auto t0 = now();
    auto contents = codec_.decodeBucket(*ext);
    trackTime(decode_count_, decode_total_ns_, t0);
    if (contents.keys.empty() && !nextBucket(*ext)) {
        setExtensionPtr(*bucket, 0);
    }
}

bool DeltaHashTable::isSuffixEmpty(uint32_t bi, uint64_t suffix) const {
    return !containsByHash(bi, suffix);
}

}  // namespace internal
}  // namespace kvlite
