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

size_t DeltaHashTable::bucketDataBits() const {
    return (config_.bucket_bytes - 8) * 8;
}

// --- Decode / Encode ---
//
// Bucket format (sorted suffix array):
//   Header: N_k (uint16_t) — number of unique keys
//   Suffix array: N_k suffixes, sorted ascending
//     First suffix: raw suffix_bits bits
//     Remaining: delta-encoded (Elias gamma of delta+1)
//   Per-key entries (N_k of them):
//     N_v (Elias gamma of count)
//     packed_versions[N_v]: first raw 64-bit, rest delta-encoded (desc, gamma of delta+1)
//     ids[N_v]: first raw 32-bit, rest zigzag-varint delta-encoded (gamma of zigzag(delta)+1)

static uint32_t zigzagEncode(int32_t v) {
    return static_cast<uint32_t>((v << 1) ^ (v >> 31));
}

static int32_t zigzagDecode(uint32_t v) {
    return static_cast<int32_t>((v >> 1) ^ -(v & 1));
}

DeltaHashTable::BucketContents DeltaHashTable::decodeBucket(const Bucket& bucket) const {
    BucketContents contents;

    // Read N_k from first 16 bits.
    uint16_t num_keys = 0;
    std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));
    if (num_keys == 0) return contents;

    BitReader reader(bucket.data, 16);

    contents.keys.resize(num_keys);

    // Decode suffixes (sorted ascending, delta-encoded).
    contents.keys[0].suffix = reader.read(suffix_bits_);
    for (uint16_t i = 1; i < num_keys; ++i) {
        uint64_t delta = reader.readEliasGamma64() - 1;
        contents.keys[i].suffix = contents.keys[i - 1].suffix + delta;
    }

    // Decode per-key entries.
    for (uint16_t i = 0; i < num_keys; ++i) {
        auto& key = contents.keys[i];
        uint32_t nv = reader.readEliasGamma();
        key.packed_versions.resize(nv);
        key.ids.resize(nv);

        if (nv > 0) {
            // Packed versions: raw first, gamma(delta+1) for rest (desc order).
            key.packed_versions[0] = reader.read(64);
            for (uint32_t v = 1; v < nv; ++v) {
                uint64_t delta = reader.readEliasGamma64() - 1;
                key.packed_versions[v] = key.packed_versions[v - 1] - delta;
            }
            // IDs: raw first, gamma(zigzag(delta)+1) for rest.
            key.ids[0] = static_cast<uint32_t>(reader.read(32));
            for (uint32_t v = 1; v < nv; ++v) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                key.ids[v] = static_cast<uint32_t>(static_cast<int32_t>(key.ids[v - 1]) + delta);
            }
        }
    }

    return contents;
}

// Returns the number of bits written (excluding the 16-bit header).
size_t DeltaHashTable::encodeBucket(Bucket& bucket, const BucketContents& contents) const {
    uint64_t ext_ptr = getExtensionPtr(bucket);
    size_t data_bytes = config_.bucket_bytes - 8;
    std::memset(bucket.data, 0, data_bytes);
    setExtensionPtr(bucket, ext_ptr);

    uint16_t num_keys = static_cast<uint16_t>(contents.keys.size());
    std::memcpy(bucket.data, &num_keys, sizeof(uint16_t));
    if (num_keys == 0) return 16;

    BitWriter writer(bucket.data, 16);

    // Encode suffixes (sorted ascending, delta-encoded).
    writer.write(contents.keys[0].suffix, suffix_bits_);
    for (uint16_t i = 1; i < num_keys; ++i) {
        uint64_t delta = contents.keys[i].suffix - contents.keys[i - 1].suffix;
        writer.writeEliasGamma64(delta + 1);
    }

    // Encode per-key entries.
    for (uint16_t i = 0; i < num_keys; ++i) {
        const auto& key = contents.keys[i];
        uint32_t nv = static_cast<uint32_t>(key.packed_versions.size());
        writer.writeEliasGamma(nv);

        if (nv > 0) {
            // Packed versions: raw first, gamma(delta+1) for rest (desc order).
            writer.write(key.packed_versions[0], 64);
            for (uint32_t v = 1; v < nv; ++v) {
                uint64_t delta = key.packed_versions[v - 1] - key.packed_versions[v];
                writer.writeEliasGamma64(delta + 1);
            }
            // IDs: raw first, gamma(zigzag(delta)+1) for rest.
            writer.write(key.ids[0], 32);
            for (uint32_t v = 1; v < nv; ++v) {
                int32_t delta = static_cast<int32_t>(key.ids[v]) -
                                static_cast<int32_t>(key.ids[v - 1]);
                writer.writeEliasGamma(zigzagEncode(delta) + 1);
            }
        }
    }

    return writer.position();
}

// Compute the bits needed to encode a BucketContents.
static uint8_t eliasGammaBits(uint32_t n) {
    uint8_t k = 31 - __builtin_clz(n);
    return 2 * k + 1;
}

static uint8_t eliasGammaBits64(uint64_t n) {
    uint8_t k = 63 - __builtin_clzll(n);
    return 2 * k + 1;
}

static size_t contentsBitsNeeded(const DeltaHashTable::BucketContents& contents,
                                  uint8_t suffix_bits) {
    if (contents.keys.empty()) return 16;  // just the header

    size_t bits = 16;  // uint16_t header

    // First suffix raw, rest delta-encoded.
    bits += suffix_bits;
    for (size_t i = 1; i < contents.keys.size(); ++i) {
        uint64_t delta = contents.keys[i].suffix - contents.keys[i - 1].suffix;
        bits += eliasGammaBits64(delta + 1);
    }

    // Per-key entries.
    for (const auto& key : contents.keys) {
        uint32_t nv = static_cast<uint32_t>(key.packed_versions.size());
        bits += eliasGammaBits(nv);
        if (nv > 0) {
            bits += 64;  // first packed_version
            for (uint32_t v = 1; v < nv; ++v) {
                uint64_t delta = key.packed_versions[v - 1] - key.packed_versions[v];
                bits += eliasGammaBits64(delta + 1);
            }
            bits += 32;  // first id
            for (uint32_t v = 1; v < nv; ++v) {
                int32_t delta = static_cast<int32_t>(key.ids[v]) -
                                static_cast<int32_t>(key.ids[v - 1]);
                bits += eliasGammaBits(zigzagEncode(delta) + 1);
            }
        }
    }

    return bits;
}

// --- Targeted scan helpers ---

DeltaHashTable::SuffixScanResult DeltaHashTable::decodeSuffixes(const Bucket& bucket) const {
    SuffixScanResult result;

    uint16_t num_keys = 0;
    std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));
    if (num_keys == 0) {
        result.versions_start_bit = 16;
        return result;
    }

    BitReader reader(bucket.data, 16);
    result.suffixes.resize(num_keys);

    result.suffixes[0] = reader.read(suffix_bits_);
    for (uint16_t i = 1; i < num_keys; ++i) {
        uint64_t delta = reader.readEliasGamma64() - 1;
        result.suffixes[i] = result.suffixes[i - 1] + delta;
    }

    result.versions_start_bit = reader.position();
    return result;
}

void DeltaHashTable::skipKeyData(BitReader& reader) {
    uint32_t nv = reader.readEliasGamma();
    if (nv > 0) {
        // Skip first packed_version (64 bits).
        reader.read(64);
        // Skip remaining packed_versions (gamma-coded deltas).
        for (uint32_t v = 1; v < nv; ++v) {
            reader.readEliasGamma64();
        }
        // Skip first id (32 bits).
        reader.read(32);
        // Skip remaining ids (gamma-coded zigzag deltas).
        for (uint32_t v = 1; v < nv; ++v) {
            reader.readEliasGamma();
        }
    }
}

DeltaHashTable::KeyEntry DeltaHashTable::decodeKeyData(BitReader& reader, uint64_t suffix) {
    KeyEntry key;
    key.suffix = suffix;
    uint32_t nv = reader.readEliasGamma();
    key.packed_versions.resize(nv);
    key.ids.resize(nv);

    if (nv > 0) {
        key.packed_versions[0] = reader.read(64);
        for (uint32_t v = 1; v < nv; ++v) {
            uint64_t delta = reader.readEliasGamma64() - 1;
            key.packed_versions[v] = key.packed_versions[v - 1] - delta;
        }
        key.ids[0] = static_cast<uint32_t>(reader.read(32));
        for (uint32_t v = 1; v < nv; ++v) {
            uint32_t zz = reader.readEliasGamma() - 1;
            int32_t delta = zigzagDecode(zz);
            key.ids[v] = static_cast<uint32_t>(static_cast<int32_t>(key.ids[v - 1]) + delta);
        }
    }

    return key;
}

bool DeltaHashTable::containsByHash(uint32_t bi, uint64_t suffix) const {
    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        auto scan = decodeSuffixes(*bucket);
        if (std::binary_search(scan.suffixes.begin(), scan.suffixes.end(), suffix)) {
            return true;
        }
        bucket = nextBucket(*bucket);
    }
    return false;
}

// --- Incremental bit budget helpers ---

size_t DeltaHashTable::decodeBucketUsedBits(const Bucket& bucket) const {
    uint16_t num_keys = 0;
    std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));
    if (num_keys == 0) return 16;

    BitReader reader(bucket.data, 16);

    // Skip suffixes.
    reader.read(suffix_bits_);
    for (uint16_t i = 1; i < num_keys; ++i) {
        reader.readEliasGamma64();
    }

    // Skip per-key entries.
    for (uint16_t i = 0; i < num_keys; ++i) {
        skipKeyData(reader);
    }

    return reader.position();
}

size_t DeltaHashTable::bitsForAppendVersion(uint64_t prev_pv, uint64_t new_pv,
                                             uint32_t prev_id, uint32_t new_id) {
    // The version count gamma code grows. We compute the difference in gamma bits
    // for count vs count+1, but since we don't know count, we must account for it.
    // Actually, the caller deals with the count gamma change separately.
    // Here we just compute the bits for the new delta-encoded version + id.

    // New packed_version delta (desc order: prev_pv > new_pv for append at end,
    // but insertion position varies). For simplicity, compute based on the
    // deltas that will actually be encoded.

    // Version delta: gamma(prev_pv - new_pv + 1) if appended after prev_pv.
    // But if inserted in middle, both neighbor deltas change. For typical
    // append (latest version), this is the common case.
    size_t bits = 0;
    uint64_t pv_delta = (prev_pv > new_pv) ? (prev_pv - new_pv) : (new_pv - prev_pv);
    bits += eliasGammaBits64(pv_delta + 1);

    int32_t id_delta = static_cast<int32_t>(new_id) - static_cast<int32_t>(prev_id);
    bits += eliasGammaBits(zigzagEncode(id_delta) + 1);

    return bits;
}

size_t DeltaHashTable::bitsForNewEntry(uint64_t suffix, uint64_t prev_suffix,
                                        uint64_t next_suffix, bool has_prev,
                                        bool has_next, uint8_t suffix_bits,
                                        uint64_t packed_version, uint32_t id) {
    size_t bits = 0;

    // Suffix delta chain change.
    if (!has_prev) {
        // First key: raw suffix_bits.
        bits += suffix_bits;
        if (has_next) {
            // Remove the raw next suffix encoding, add delta from new→next.
            // Net: remove suffix_bits (the old raw next), add gamma(delta+1).
            uint64_t delta_new_next = next_suffix - suffix;
            // We're replacing the old raw encoding of next_suffix.
            // Old cost for next_suffix was suffix_bits (it was the first key).
            // New cost: suffix (raw) + gamma(next-suffix delta).
            // So extra = gamma(delta+1) - suffix_bits + suffix_bits = gamma(delta+1).
            // Wait, let's think again. Before insertion:
            //   next_suffix was first, encoded as raw suffix_bits.
            // After insertion:
            //   suffix (new first) as raw suffix_bits, then next_suffix as gamma(next-suffix+1).
            // Net change: +suffix_bits + gamma(delta+1) - suffix_bits = +gamma(delta+1).
            bits += eliasGammaBits64(delta_new_next + 1);
        }
    } else if (!has_next) {
        // Last key: add delta from prev→new.
        uint64_t delta_prev_new = suffix - prev_suffix;
        bits += eliasGammaBits64(delta_prev_new + 1);
    } else {
        // Middle: remove delta(prev→next), add delta(prev→new) + delta(new→next).
        uint64_t old_delta = next_suffix - prev_suffix;
        uint64_t delta_prev_new = suffix - prev_suffix;
        uint64_t delta_new_next = next_suffix - suffix;
        bits += eliasGammaBits64(delta_prev_new + 1);
        bits += eliasGammaBits64(delta_new_next + 1);
        bits -= eliasGammaBits64(old_delta + 1);
    }

    // Entry data: gamma(1) + 64-bit packed_version + 32-bit id.
    bits += 1 + 64 + 32;  // eliasGammaBits(1) == 1

    return bits;
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

// --- addToChain ---

bool DeltaHashTable::addToChain(uint32_t bi, uint64_t suffix,
                                 uint64_t packed_version, uint32_t id,
                                 const std::function<Bucket*(Bucket&)>& createExtFn) {
    Bucket* bucket = &buckets_[bi];
    bool is_new = true;

    while (true) {
        // First, do a targeted suffix scan to check fit incrementally.
        auto scan = decodeSuffixes(*bucket);
        auto sit = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        size_t suffix_idx = sit - scan.suffixes.begin();
        bool suffix_found = (sit != scan.suffixes.end() && *sit == suffix);

        if (suffix_found) {
            // Suffix exists in this bucket — need to append version.
            // Decode only the target key to check the new version fits.
            BitReader reader(bucket->data, scan.versions_start_bit);
            for (size_t k = 0; k < suffix_idx; ++k) {
                skipKeyData(reader);
            }
            auto key = decodeKeyData(reader, suffix);

            is_new = false;
            uint32_t old_nv = static_cast<uint32_t>(key.packed_versions.size());

            // Find insertion position for the new version (desc order).
            auto pos = std::lower_bound(key.packed_versions.begin(),
                                        key.packed_versions.end(),
                                        packed_version,
                                        std::greater<uint64_t>());
            size_t vidx = pos - key.packed_versions.begin();

            // Compute incremental bit cost.
            size_t current_bits = decodeBucketUsedBits(*bucket);
            size_t extra_bits = 0;

            // The version count gamma code changes: gamma(old_nv) → gamma(old_nv+1).
            extra_bits += eliasGammaBits(old_nv + 1);
            extra_bits -= eliasGammaBits(old_nv);

            // New version/id encoding depends on insertion position.
            if (old_nv == 0) {
                // Was empty (shouldn't happen for found suffix, but handle it).
                extra_bits += 64 + 32;
            } else if (vidx == 0) {
                // Insert at front (new highest version).
                uint64_t old_first_pv = key.packed_versions[0];
                uint32_t old_first_id = key.ids[0];
                // New first is raw 64+32, old first becomes delta-encoded.
                // Remove old raw cost, add new raw + delta for old first.
                // Net: old was raw(64+32). New: raw(64+32) for new + delta for old.
                uint64_t pv_delta = packed_version - old_first_pv;
                int32_t id_delta = static_cast<int32_t>(old_first_id) - static_cast<int32_t>(id);
                extra_bits += eliasGammaBits64(pv_delta + 1);
                extra_bits += eliasGammaBits(zigzagEncode(id_delta) + 1);
            } else if (vidx == old_nv) {
                // Append at end.
                uint64_t prev_pv = key.packed_versions[old_nv - 1];
                uint32_t prev_id = key.ids[old_nv - 1];
                extra_bits += bitsForAppendVersion(prev_pv, packed_version, prev_id, id);
            } else {
                // Insert in middle between vidx-1 and vidx.
                uint64_t prev_pv = key.packed_versions[vidx - 1];
                uint64_t next_pv = key.packed_versions[vidx];
                uint32_t prev_id = key.ids[vidx - 1];
                uint32_t next_id = key.ids[vidx];
                // Remove old delta(prev→next), add delta(prev→new) + delta(new→next).
                uint64_t old_pv_delta = prev_pv - next_pv;
                uint64_t delta_prev_new = prev_pv - packed_version;
                uint64_t delta_new_next = packed_version - next_pv;
                extra_bits += eliasGammaBits64(delta_prev_new + 1);
                extra_bits += eliasGammaBits64(delta_new_next + 1);
                extra_bits -= eliasGammaBits64(old_pv_delta + 1);

                int32_t old_id_delta = static_cast<int32_t>(next_id) - static_cast<int32_t>(prev_id);
                int32_t id_delta_prev_new = static_cast<int32_t>(id) - static_cast<int32_t>(prev_id);
                int32_t id_delta_new_next = static_cast<int32_t>(next_id) - static_cast<int32_t>(id);
                extra_bits += eliasGammaBits(zigzagEncode(id_delta_prev_new) + 1);
                extra_bits += eliasGammaBits(zigzagEncode(id_delta_new_next) + 1);
                extra_bits -= eliasGammaBits(zigzagEncode(old_id_delta) + 1);
            }

            if (current_bits + extra_bits <= bucketDataBits()) {
                // Fits — do full decode/modify/encode for this bucket only.
                auto contents = decodeBucket(*bucket);
                auto cit = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
                    [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
                auto vpos = std::lower_bound(cit->packed_versions.begin(),
                                             cit->packed_versions.end(),
                                             packed_version,
                                             std::greater<uint64_t>());
                size_t vi = vpos - cit->packed_versions.begin();
                cit->packed_versions.insert(vpos, packed_version);
                cit->ids.insert(cit->ids.begin() + vi, id);
                encodeBucket(*bucket, contents);
                return is_new;
            }

            // Doesn't fit — move to extension (bucket data is pristine).
            Bucket* ext = nextBucketMut(*bucket);
            if (!ext) ext = createExtFn(*bucket);
            bucket = ext;
        } else {
            // Suffix not found in this bucket — check remaining extensions for is_new.
            if (is_new) {
                const Bucket* check = nextBucket(*bucket);
                while (check) {
                    auto check_scan = decodeSuffixes(*check);
                    if (std::binary_search(check_scan.suffixes.begin(),
                                           check_scan.suffixes.end(), suffix)) {
                        is_new = false;
                        break;
                    }
                    check = nextBucket(*check);
                }
            }

            // Compute incremental bit cost for new entry.
            size_t current_bits = decodeBucketUsedBits(*bucket);
            bool has_prev = (suffix_idx > 0);
            bool has_next = (suffix_idx < scan.suffixes.size());
            uint64_t prev_suffix = has_prev ? scan.suffixes[suffix_idx - 1] : 0;
            uint64_t next_suffix = has_next ? scan.suffixes[suffix_idx] : 0;

            size_t extra_bits = bitsForNewEntry(suffix, prev_suffix, next_suffix,
                                                has_prev, has_next, suffix_bits_,
                                                packed_version, id);

            if (current_bits + extra_bits <= bucketDataBits()) {
                // Fits — do full decode/modify/encode.
                auto contents = decodeBucket(*bucket);
                auto cit = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
                    [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
                KeyEntry new_entry;
                new_entry.suffix = suffix;
                new_entry.packed_versions.push_back(packed_version);
                new_entry.ids.push_back(id);
                contents.keys.insert(cit, std::move(new_entry));
                encodeBucket(*bucket, contents);
                return is_new;
            }

            // Doesn't fit — move to extension (bucket data is pristine).
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
        auto contents = decodeBucket(*bucket);

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
                    encodeBucket(*bucket, contents);
                    pruneEmptyExtension(bucket);
                    // Check if suffix is fully gone from the chain.
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
        auto contents = decodeBucket(*bucket);

        auto it = std::lower_bound(contents.keys.begin(), contents.keys.end(), suffix,
            [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });

        if (it != contents.keys.end() && it->suffix == suffix) {
            for (size_t j = 0; j < it->packed_versions.size(); ++j) {
                if (it->packed_versions[j] == packed_version && it->ids[j] == old_id) {
                    it->ids[j] = new_id;
                    size_t bits = contentsBitsNeeded(contents, suffix_bits_);
                    if (bits <= bucketDataBits()) {
                        encodeBucket(*bucket, contents);
                        return true;
                    }
                    // Doesn't fit after update — spill this key entry to extension.
                    KeyEntry spilled = std::move(*it);
                    contents.keys.erase(it);
                    encodeBucket(*bucket, contents);

                    Bucket* ext = nextBucketMut(*bucket);
                    if (!ext) ext = createExtFn(*bucket);
                    auto ext_contents = decodeBucket(*ext);
                    auto ext_it = std::lower_bound(ext_contents.keys.begin(),
                        ext_contents.keys.end(), spilled.suffix,
                        [](const KeyEntry& entry, uint64_t s) { return entry.suffix < s; });
                    ext_contents.keys.insert(ext_it, std::move(spilled));
                    encodeBucket(*ext, ext_contents);
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
        auto scan = decodeSuffixes(*bucket);
        auto it = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        if (it != scan.suffixes.end() && *it == suffix) {
            size_t idx = it - scan.suffixes.begin();
            // Position reader at versions_start_bit and skip preceding keys.
            BitReader reader(bucket->data, scan.versions_start_bit);
            for (size_t k = 0; k < idx; ++k) {
                skipKeyData(reader);
            }
            auto key = decodeKeyData(reader, suffix);
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
        auto scan = decodeSuffixes(*bucket);
        auto it = std::lower_bound(scan.suffixes.begin(), scan.suffixes.end(), suffix);
        if (it != scan.suffixes.end() && *it == suffix) {
            size_t idx = it - scan.suffixes.begin();
            BitReader reader(bucket->data, scan.versions_start_bit);
            for (size_t k = 0; k < idx; ++k) {
                skipKeyData(reader);
            }
            auto key = decodeKeyData(reader, suffix);
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
            auto contents = decodeBucket(*b);
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

        // Merge across extension chain by suffix using sorted vector + binary search.
        std::vector<KeyEntry> groups;

        const Bucket* b = first;
        while (b) {
            auto contents = decodeBucket(*b);
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
    // Header: N_k = 0 (uint16_t). Already zero from memset.
}

void DeltaHashTable::pruneEmptyExtension(Bucket* bucket) {
    Bucket* ext = nextBucketMut(*bucket);
    if (!ext) return;
    auto contents = decodeBucket(*ext);
    if (contents.keys.empty() && !nextBucket(*ext)) {
        setExtensionPtr(*bucket, 0);
    }
}

bool DeltaHashTable::isSuffixEmpty(uint32_t bi, uint64_t suffix) const {
    return !containsByHash(bi, suffix);
}

}  // namespace internal
}  // namespace kvlite
