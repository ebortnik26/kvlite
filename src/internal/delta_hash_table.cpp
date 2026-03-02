#include "internal/delta_hash_table.h"

#include <algorithm>
#include <cstring>

#include "internal/bit_stream.h"

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
DeltaHashTable::DeltaHashTable(DeltaHashTable&&) noexcept = default;
DeltaHashTable& DeltaHashTable::operator=(DeltaHashTable&&) noexcept = default;

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
        auto scan = codec_.decodeSuffixes(*bucket);
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
    bool is_new = true;

    while (true) {
        auto scan = codec_.decodeSuffixes(*bucket);
        auto sit = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        size_t suffix_idx = sit - scan.suffixes.begin();
        bool suffix_found = (sit != scan.suffixes.end() && *sit == suffix);

        if (suffix_found) {
            BitReader reader(bucket->data, scan.versions_start_bit);
            for (size_t k = 0; k < suffix_idx; ++k) {
                BucketCodec::skipKeyData(reader);
            }
            auto key = BucketCodec::decodeKeyData(reader, suffix);

            is_new = false;
            uint32_t old_nv = static_cast<uint32_t>(key.packed_versions.size());

            auto pos = std::lower_bound(key.packed_versions.begin(),
                                        key.packed_versions.end(),
                                        packed_version,
                                        std::greater<uint64_t>());
            size_t vidx = pos - key.packed_versions.begin();

            size_t current_bits = codec_.decodeBucketUsedBits(*bucket);
            size_t extra_bits = 0;

            {
                // Helper: eliasGammaBits for uint32_t.
                auto egb = [](uint32_t n) -> uint8_t {
                    uint8_t k = 31 - __builtin_clz(n);
                    return 2 * k + 1;
                };
                auto egb64 = [](uint64_t n) -> uint8_t {
                    uint8_t k = 63 - __builtin_clzll(n);
                    return 2 * k + 1;
                };
                auto zzEnc = [](int32_t v) -> uint32_t {
                    return static_cast<uint32_t>((v << 1) ^ (v >> 31));
                };

                extra_bits += egb(old_nv + 1);
                extra_bits -= egb(old_nv);

                if (old_nv == 0) {
                    extra_bits += 64 + 32;
                } else if (vidx == 0) {
                    uint64_t old_first_pv = key.packed_versions[0];
                    uint32_t old_first_id = key.ids[0];
                    uint64_t pv_delta = packed_version - old_first_pv;
                    int32_t id_delta = static_cast<int32_t>(old_first_id) - static_cast<int32_t>(id);
                    extra_bits += egb64(pv_delta + 1);
                    extra_bits += egb(zzEnc(id_delta) + 1);
                } else if (vidx == old_nv) {
                    uint64_t prev_pv = key.packed_versions[old_nv - 1];
                    uint32_t prev_id = key.ids[old_nv - 1];
                    extra_bits += BucketCodec::bitsForAppendVersion(prev_pv, packed_version, prev_id, id);
                } else {
                    uint64_t prev_pv = key.packed_versions[vidx - 1];
                    uint64_t next_pv = key.packed_versions[vidx];
                    uint32_t prev_id = key.ids[vidx - 1];
                    uint32_t next_id = key.ids[vidx];
                    uint64_t old_pv_delta = prev_pv - next_pv;
                    uint64_t delta_prev_new = prev_pv - packed_version;
                    uint64_t delta_new_next = packed_version - next_pv;
                    extra_bits += egb64(delta_prev_new + 1);
                    extra_bits += egb64(delta_new_next + 1);
                    extra_bits -= egb64(old_pv_delta + 1);

                    int32_t old_id_delta = static_cast<int32_t>(next_id) - static_cast<int32_t>(prev_id);
                    int32_t id_delta_prev_new = static_cast<int32_t>(id) - static_cast<int32_t>(prev_id);
                    int32_t id_delta_new_next = static_cast<int32_t>(next_id) - static_cast<int32_t>(id);
                    extra_bits += egb(zzEnc(id_delta_prev_new) + 1);
                    extra_bits += egb(zzEnc(id_delta_new_next) + 1);
                    extra_bits -= egb(zzEnc(old_id_delta) + 1);
                }
            }

            if (current_bits + extra_bits <= codec_.bucketDataBits()) {
                auto contents = codec_.decodeBucket(*bucket);
                auto cit = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
                    [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
                auto vpos = std::lower_bound(cit->packed_versions.begin(),
                                             cit->packed_versions.end(),
                                             packed_version,
                                             std::greater<uint64_t>());
                size_t vi = vpos - cit->packed_versions.begin();
                cit->packed_versions.insert(vpos, packed_version);
                cit->ids.insert(cit->ids.begin() + vi, id);
                codec_.encodeBucket(*bucket, contents);
                return is_new;
            }

            Bucket* ext = nextBucketMut(*bucket);
            if (!ext) ext = createExtFn(*bucket);
            bucket = ext;
        } else {
            if (is_new) {
                const Bucket* check = nextBucket(*bucket);
                while (check) {
                    auto check_scan = codec_.decodeSuffixes(*check);
                    if (std::binary_search(check_scan.suffixes.begin(),
                                           check_scan.suffixes.end(), suffix)) {
                        is_new = false;
                        break;
                    }
                    check = nextBucket(*check);
                }
            }

            size_t current_bits = codec_.decodeBucketUsedBits(*bucket);
            bool has_prev = (suffix_idx > 0);
            bool has_next = (suffix_idx < scan.suffixes.size());
            uint64_t prev_suffix = has_prev ? scan.suffixes[suffix_idx - 1] : 0;
            uint64_t next_suffix = has_next ? scan.suffixes[suffix_idx] : 0;

            size_t extra_bits = BucketCodec::bitsForNewEntry(suffix, prev_suffix, next_suffix,
                                                has_prev, has_next, suffix_bits_,
                                                packed_version, id);

            if (current_bits + extra_bits <= codec_.bucketDataBits()) {
                auto contents = codec_.decodeBucket(*bucket);
                auto cit = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
                    [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
                KeyEntry new_entry;
                new_entry.suffix = suffix;
                new_entry.packed_versions.push_back(packed_version);
                new_entry.ids.push_back(id);
                contents.keys.insert(cit, std::move(new_entry));
                codec_.encodeBucket(*bucket, contents);
                return is_new;
            }

            Bucket* ext = nextBucketMut(*bucket);
            if (!ext) ext = createExtFn(*bucket);
            bucket = ext;
        }
    }
}

// --- removeFromChain ---

bool DeltaHashTable::removeFromChain(uint32_t bi, uint64_t suffix,
                                      uint64_t packed_version, uint32_t id) {
    Bucket* bucket = &buckets_[bi];

    while (bucket) {
        auto contents = codec_.decodeBucket(*bucket);

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
                    codec_.encodeBucket(*bucket, contents);
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
        auto contents = codec_.decodeBucket(*bucket);

        auto it = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
            [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });

        if (it != contents.keys.end() && it->suffix == suffix) {
            for (size_t j = 0; j < it->packed_versions.size(); ++j) {
                if (it->packed_versions[j] == packed_version && it->ids[j] == old_id) {
                    it->ids[j] = new_id;
                    size_t bits = BucketCodec::contentsBitsNeeded(contents, suffix_bits_);
                    if (bits <= codec_.bucketDataBits()) {
                        codec_.encodeBucket(*bucket, contents);
                        return true;
                    }
                    KeyEntry spilled = std::move(*it);
                    contents.keys.erase(it);
                    codec_.encodeBucket(*bucket, contents);

                    Bucket* ext = nextBucketMut(*bucket);
                    if (!ext) ext = createExtFn(*bucket);
                    auto ext_contents = codec_.decodeBucket(*ext);
                    auto ext_it = std::lower_bound(ext_contents.keys.begin(),
                        ext_contents.keys.end(), spilled.suffix,
                        [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
                    ext_contents.keys.insert(ext_it, std::move(spilled));
                    codec_.encodeBucket(*ext, ext_contents);
                    return true;
                }
            }
        }

        bucket = nextBucketMut(*bucket);
    }

    return false;
}

// --- Public read API ---

bool DeltaHashTable::findAllByHash(uint32_t bi, uint64_t suffix,
                                    std::vector<uint64_t>& packed_versions,
                                    std::vector<uint32_t>& ids) const {
    packed_versions.clear();
    ids.clear();

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        auto scan = codec_.decodeSuffixes(*bucket);
        auto it = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        if (it != scan.suffixes.end() && *it == suffix) {
            size_t idx = it - scan.suffixes.begin();
            BitReader reader(bucket->data, scan.versions_start_bit);
            for (size_t k = 0; k < idx; ++k) {
                BucketCodec::skipKeyData(reader);
            }
            auto key = BucketCodec::decodeKeyData(reader, suffix);
            packed_versions.insert(packed_versions.end(),
                                   key.packed_versions.begin(),
                                   key.packed_versions.end());
            ids.insert(ids.end(), key.ids.begin(), key.ids.end());
        }
        bucket = nextBucket(*bucket);
    }
    return !packed_versions.empty();
}

bool DeltaHashTable::findFirstByHash(uint32_t bi, uint64_t suffix,
                                      uint64_t& packed_version, uint32_t& id) const {
    bool found = false;
    uint64_t best_pv = 0;
    uint32_t best_id = 0;

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        auto scan = codec_.decodeSuffixes(*bucket);
        auto it = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        if (it != scan.suffixes.end() && *it == suffix) {
            size_t idx = it - scan.suffixes.begin();
            BitReader reader(bucket->data, scan.versions_start_bit);
            for (size_t k = 0; k < idx; ++k) {
                BucketCodec::skipKeyData(reader);
            }
            auto key = BucketCodec::decodeKeyData(reader, suffix);
            if (!key.packed_versions.empty()) {
                uint64_t pv = key.packed_versions[0];
                if (!found || pv > best_pv) {
                    best_pv = pv;
                    best_id = key.ids[0];
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
            auto contents = codec_.decodeBucket(*b);
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
            auto contents = codec_.decodeBucket(*b);
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
    auto contents = codec_.decodeBucket(*ext);
    if (contents.keys.empty() && !nextBucket(*ext)) {
        setExtensionPtr(*bucket, 0);
    }
}

bool DeltaHashTable::isSuffixEmpty(uint32_t bi, uint64_t suffix) const {
    return !containsByHash(bi, suffix);
}

}  // namespace internal
}  // namespace kvlite
