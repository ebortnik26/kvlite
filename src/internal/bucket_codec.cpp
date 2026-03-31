#include "internal/bucket_codec.h"

#include <cstring>
#include <type_traits>

#include "internal/bit_stream.h"

namespace kvlite {
namespace internal {

// --- Helpers ---

static uint32_t zigzagEncode(int32_t v) {
    return static_cast<uint32_t>((v << 1) ^ (v >> 31));
}

static int32_t zigzagDecode(uint32_t v) {
    return static_cast<int32_t>((v >> 1) ^ -(v & 1));
}

static uint64_t zigzag64Encode(int64_t v) {
    return static_cast<uint64_t>((v << 1) ^ (v >> 63));
}

static int64_t zigzag64Decode(uint64_t v) {
    return static_cast<int64_t>((v >> 1) ^ -(v & 1));
}

static uint8_t eliasGammaBits(uint32_t n) {
    uint8_t k = 31 - __builtin_clz(n);
    return 2 * k + 1;
}

static uint8_t eliasGammaBits64(uint64_t n) {
    uint8_t k = 63 - __builtin_clzll(n);
    return 2 * k + 1;
}

// ============================================================
// Template method implementations using if constexpr
// ============================================================

template<typename Layout>
BucketCodec<Layout>::BucketCodec(uint8_t suffix_bits, uint32_t bucket_bytes)
    : suffix_bits_(suffix_bits), bucket_bytes_(bucket_bytes) {}

template<typename Layout>
size_t BucketCodec<Layout>::bucketDataBits() const {
    return (bucket_bytes_ - 8) * 8;
}

template<typename Layout>
uint8_t BucketCodec<Layout>::suffixBits() const {
    return suffix_bits_;
}

template<typename Layout>
SuffixScanResult BucketCodec<Layout>::decodeSuffixes(const Bucket& bucket) const {
    SuffixScanResult result;
    assert(bucket.data != nullptr && "decodeSuffixes: bucket.data is null");

    uint16_t num_keys = 0;
    std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));
    if (num_keys == 0) {
        result.data_start_bit = 16;
        return result;
    }

    BitReader reader(bucket.data, 16);
    result.suffixes.resize(num_keys);

    result.suffixes[0] = reader.read(suffix_bits_);
    for (uint16_t i = 1; i < num_keys; ++i) {
        uint64_t delta = reader.readEliasGamma64() - 1;
        result.suffixes[i] = result.suffixes[i - 1] + delta;
    }

    result.data_start_bit = reader.position();
    return result;
}

// ============================================================
// decodeBucket
// ============================================================

template<typename Layout>
BucketContents BucketCodec<Layout>::decodeBucket(const Bucket& bucket) const {
    BucketContents contents;
    assert(bucket.data != nullptr && "decodeBucket: bucket.data is null");

    uint16_t num_keys = 0;
    std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));
    if (num_keys == 0) return contents;

    BitReader reader(bucket.data, 16);
    contents.keys.resize(num_keys);

    // Suffixes (shared).
    contents.keys[0].suffix = reader.read(suffix_bits_);
    for (uint16_t i = 1; i < num_keys; ++i) {
        uint64_t delta = reader.readEliasGamma64() - 1;
        contents.keys[i].suffix = contents.keys[i - 1].suffix + delta;
    }

    if constexpr (std::is_same_v<Layout, ColumnarLayout>) {
        // Columnar: Counts | First-PVs | First-IDs | Tail-PVs | Tail-IDs
        uint32_t counts[256];
        for (uint16_t i = 0; i < num_keys; ++i) {
            counts[i] = reader.readEliasGamma();
            contents.keys[i].packed_versions.resize(counts[i]);
            contents.keys[i].ids.resize(counts[i]);
        }

        // First-PVs.
        if (num_keys > 0 && counts[0] > 0) {
            contents.keys[0].packed_versions[0] = reader.read(64);
        }
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (counts[i] > 0) {
                uint64_t zz = reader.readEliasGamma64() - 1;
                int64_t delta = zigzag64Decode(zz);
                contents.keys[i].packed_versions[0] =
                    static_cast<uint64_t>(static_cast<int64_t>(
                        contents.keys[i - 1].packed_versions[0]) + delta);
            }
        }

        // First-IDs (reordered: immediately after first-PVs).
        if (num_keys > 0 && counts[0] > 0) {
            contents.keys[0].ids[0] = static_cast<uint32_t>(reader.read(32));
        }
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (counts[i] > 0) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                contents.keys[i].ids[0] =
                    static_cast<uint32_t>(static_cast<int32_t>(
                        contents.keys[i - 1].ids[0]) + delta);
            }
        }

        // Tail versions.
        for (uint16_t i = 0; i < num_keys; ++i) {
            for (uint32_t v = 1; v < counts[i]; ++v) {
                uint64_t delta = reader.readEliasGamma64() - 1;
                contents.keys[i].packed_versions[v] =
                    contents.keys[i].packed_versions[v - 1] - delta;
            }
        }

        // Tail IDs.
        for (uint16_t i = 0; i < num_keys; ++i) {
            for (uint32_t v = 1; v < counts[i]; ++v) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                contents.keys[i].ids[v] =
                    static_cast<uint32_t>(static_cast<int32_t>(
                        contents.keys[i].ids[v - 1]) + delta);
            }
        }
    } else {
        // Row: per-key count, versions, ids.
        for (uint16_t i = 0; i < num_keys; ++i) {
            auto& key = contents.keys[i];
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
                    key.ids[v] = static_cast<uint32_t>(
                        static_cast<int32_t>(key.ids[v - 1]) + delta);
                }
            }
        }
    }

    return contents;
}

// ============================================================
// encodeBucket
// ============================================================

template<typename Layout>
size_t BucketCodec<Layout>::encodeBucket(Bucket& bucket, const BucketContents& contents) const {
    // Save the extension pointer (last 8 bytes) before clearing.
    // Restore it AFTER BitWriter is done — BitWriter's 64-bit stores
    // can overwrite trailing bytes as padding.
    uint64_t ext_ptr = 0;
    std::memcpy(&ext_ptr, bucket.data + bucket_bytes_ - 8, 8);
    std::memset(bucket.data, 0, bucket_bytes_);

    uint16_t num_keys = static_cast<uint16_t>(contents.keys.size());
    std::memcpy(bucket.data, &num_keys, sizeof(uint16_t));
    if (num_keys == 0) {
        std::memcpy(bucket.data + bucket_bytes_ - 8, &ext_ptr, 8);
        return 16;
    }

    BitWriter writer(bucket.data, 16);

    // Suffixes (shared).
    writer.write(contents.keys[0].suffix, suffix_bits_);
    for (uint16_t i = 1; i < num_keys; ++i) {
        uint64_t delta = contents.keys[i].suffix - contents.keys[i - 1].suffix;
        writer.writeEliasGamma64(delta + 1);
    }

    if constexpr (std::is_same_v<Layout, ColumnarLayout>) {
        // Counts.
        for (uint16_t i = 0; i < num_keys; ++i) {
            uint32_t nv = static_cast<uint32_t>(contents.keys[i].packed_versions.size());
            writer.writeEliasGamma(nv);
        }

        // First-PVs.
        if (num_keys > 0 && !contents.keys[0].packed_versions.empty()) {
            writer.write(contents.keys[0].packed_versions[0], 64);
        }
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (!contents.keys[i].packed_versions.empty()) {
                int64_t delta = static_cast<int64_t>(contents.keys[i].packed_versions[0]) -
                                static_cast<int64_t>(contents.keys[i - 1].packed_versions[0]);
                writer.writeEliasGamma64(zigzag64Encode(delta) + 1);
            }
        }

        // First-IDs (reordered).
        if (num_keys > 0 && !contents.keys[0].ids.empty()) {
            writer.write(contents.keys[0].ids[0], 32);
        }
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (!contents.keys[i].ids.empty()) {
                int32_t delta = static_cast<int32_t>(contents.keys[i].ids[0]) -
                                static_cast<int32_t>(contents.keys[i - 1].ids[0]);
                writer.writeEliasGamma(zigzagEncode(delta) + 1);
            }
        }

        // Tail versions.
        for (uint16_t i = 0; i < num_keys; ++i) {
            const auto& key = contents.keys[i];
            for (uint32_t v = 1; v < key.packed_versions.size(); ++v) {
                uint64_t delta = key.packed_versions[v - 1] - key.packed_versions[v];
                writer.writeEliasGamma64(delta + 1);
            }
        }

        // Tail IDs.
        for (uint16_t i = 0; i < num_keys; ++i) {
            const auto& key = contents.keys[i];
            for (uint32_t v = 1; v < key.ids.size(); ++v) {
                int32_t delta = static_cast<int32_t>(key.ids[v]) -
                                static_cast<int32_t>(key.ids[v - 1]);
                writer.writeEliasGamma(zigzagEncode(delta) + 1);
            }
        }
    } else {
        // Row: per-key count, versions, ids.
        for (uint16_t i = 0; i < num_keys; ++i) {
            const auto& key = contents.keys[i];
            uint32_t nv = static_cast<uint32_t>(key.packed_versions.size());
            writer.writeEliasGamma(nv);

            if (nv > 0) {
                writer.write(key.packed_versions[0], 64);
                for (uint32_t v = 1; v < nv; ++v) {
                    uint64_t delta = key.packed_versions[v - 1] - key.packed_versions[v];
                    writer.writeEliasGamma64(delta + 1);
                }
                writer.write(key.ids[0], 32);
                for (uint32_t v = 1; v < nv; ++v) {
                    int32_t delta = static_cast<int32_t>(key.ids[v]) -
                                    static_cast<int32_t>(key.ids[v - 1]);
                    writer.writeEliasGamma(zigzagEncode(delta) + 1);
                }
            }
        }
    }

    // Verify BitWriter didn't overflow into extension pointer area.
    assert(writer.position() <= (bucket_bytes_ - 8) * 8 &&
           "encodeBucket: BitWriter overflowed into extension pointer area");

    // Restore the extension pointer after BitWriter is done.
    std::memcpy(bucket.data + bucket_bytes_ - 8, &ext_ptr, 8);

    return writer.position();
}

// ============================================================
// decodeBucketUsedBits
// ============================================================

template<typename Layout>
size_t BucketCodec<Layout>::decodeBucketUsedBits(const Bucket& bucket) const {
    uint16_t num_keys = 0;
    std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));
    if (num_keys == 0) return 16;

    BitReader reader(bucket.data, 16);

    // Skip suffixes.
    reader.read(suffix_bits_);
    for (uint16_t i = 1; i < num_keys; ++i) {
        reader.readEliasGamma64();
    }

    if constexpr (std::is_same_v<Layout, ColumnarLayout>) {
        // Read counts.
        uint32_t counts[256];
        for (uint16_t i = 0; i < num_keys; ++i) {
            counts[i] = reader.readEliasGamma();
        }

        bool any_has_versions = false;
        for (uint16_t i = 0; i < num_keys; ++i) {
            if (counts[i] > 0) { any_has_versions = true; break; }
        }
        if (any_has_versions) {
            // First-PVs.
            if (counts[0] > 0) reader.read(64);
            for (uint16_t i = 1; i < num_keys; ++i) {
                if (counts[i] > 0) reader.readEliasGamma64();
            }
            // First-IDs (reordered).
            if (counts[0] > 0) reader.read(32);
            for (uint16_t i = 1; i < num_keys; ++i) {
                if (counts[i] > 0) reader.readEliasGamma();
            }
            // Tail versions.
            for (uint16_t i = 0; i < num_keys; ++i) {
                for (uint32_t v = 1; v < counts[i]; ++v) reader.readEliasGamma64();
            }
            // Tail IDs.
            for (uint16_t i = 0; i < num_keys; ++i) {
                for (uint32_t v = 1; v < counts[i]; ++v) reader.readEliasGamma();
            }
        }
    } else {
        // Row: skip per-key data.
        for (uint16_t i = 0; i < num_keys; ++i) {
            uint32_t nv = reader.readEliasGamma();
            if (nv > 0) {
                reader.read(64);
                for (uint32_t v = 1; v < nv; ++v) reader.readEliasGamma64();
                reader.read(32);
                for (uint32_t v = 1; v < nv; ++v) reader.readEliasGamma();
            }
        }
    }

    return reader.position();
}

// ============================================================
// contentsBitsNeeded
// ============================================================

template<typename Layout>
size_t BucketCodec<Layout>::contentsBitsNeeded(const BucketContents& contents) const {
    if (contents.keys.empty()) return 16;

    size_t bits = 16;
    uint16_t num_keys = static_cast<uint16_t>(contents.keys.size());

    // Suffixes (shared).
    bits += suffix_bits_;
    for (size_t i = 1; i < num_keys; ++i) {
        uint64_t delta = contents.keys[i].suffix - contents.keys[i - 1].suffix;
        bits += eliasGammaBits64(delta + 1);
    }

    if constexpr (std::is_same_v<Layout, ColumnarLayout>) {
        // Counts.
        for (const auto& key : contents.keys) {
            uint32_t nv = static_cast<uint32_t>(key.packed_versions.size());
            bits += eliasGammaBits(nv);
        }

        bool any_has_versions = false;
        for (const auto& key : contents.keys) {
            if (!key.packed_versions.empty()) { any_has_versions = true; break; }
        }
        if (any_has_versions) {
            // First-PVs.
            if (!contents.keys[0].packed_versions.empty()) bits += 64;
            for (size_t i = 1; i < num_keys; ++i) {
                if (!contents.keys[i].packed_versions.empty()) {
                    int64_t delta = static_cast<int64_t>(contents.keys[i].packed_versions[0]) -
                                    static_cast<int64_t>(contents.keys[i - 1].packed_versions[0]);
                    bits += eliasGammaBits64(zigzag64Encode(delta) + 1);
                }
            }
            // First-IDs (reordered).
            if (!contents.keys[0].ids.empty()) bits += 32;
            for (size_t i = 1; i < num_keys; ++i) {
                if (!contents.keys[i].ids.empty()) {
                    int32_t delta = static_cast<int32_t>(contents.keys[i].ids[0]) -
                                    static_cast<int32_t>(contents.keys[i - 1].ids[0]);
                    bits += eliasGammaBits(zigzagEncode(delta) + 1);
                }
            }
            // Tail versions.
            for (const auto& key : contents.keys) {
                for (size_t v = 1; v < key.packed_versions.size(); ++v) {
                    uint64_t delta = key.packed_versions[v - 1] - key.packed_versions[v];
                    bits += eliasGammaBits64(delta + 1);
                }
            }
            // Tail IDs.
            for (const auto& key : contents.keys) {
                for (size_t v = 1; v < key.ids.size(); ++v) {
                    int32_t delta = static_cast<int32_t>(key.ids[v]) -
                                    static_cast<int32_t>(key.ids[v - 1]);
                    bits += eliasGammaBits(zigzagEncode(delta) + 1);
                }
            }
        }
    } else {
        // Row: per-key data.
        for (const auto& key : contents.keys) {
            uint32_t nv = static_cast<uint32_t>(key.packed_versions.size());
            bits += eliasGammaBits(nv);
            if (nv > 0) {
                bits += 64;
                for (size_t v = 1; v < nv; ++v) {
                    uint64_t delta = key.packed_versions[v - 1] - key.packed_versions[v];
                    bits += eliasGammaBits64(delta + 1);
                }
                bits += 32;
                for (size_t v = 1; v < nv; ++v) {
                    int32_t delta = static_cast<int32_t>(key.ids[v]) -
                                    static_cast<int32_t>(key.ids[v - 1]);
                    bits += eliasGammaBits(zigzagEncode(delta) + 1);
                }
            }
        }
    }

    return bits;
}

// ============================================================
// decodeKeyAt
// ============================================================

template<typename Layout>
KeyEntry BucketCodec<Layout>::decodeKeyAt(const Bucket& bucket,
                                           uint16_t key_index,
                                           uint64_t suffix,
                                           size_t data_start_bit) const {
    KeyEntry entry;
    entry.suffix = suffix;

    uint16_t num_keys = 0;
    std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));

    BitReader reader(bucket.data, data_start_bit);

    if constexpr (std::is_same_v<Layout, ColumnarLayout>) {
        // Read counts.
        uint32_t counts[256];
        for (uint16_t i = 0; i < num_keys; ++i) {
            counts[i] = reader.readEliasGamma();
        }

        uint32_t nv = counts[key_index];
        entry.packed_versions.resize(nv);
        entry.ids.resize(nv);
        if (nv == 0) return entry;

        // First-PVs up to key_index.
        uint64_t prev_first_pv = 0;
        if (counts[0] > 0) {
            prev_first_pv = reader.read(64);
            if (key_index == 0) entry.packed_versions[0] = prev_first_pv;
        }
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (counts[i] > 0) {
                uint64_t zz = reader.readEliasGamma64() - 1;
                int64_t delta = zigzag64Decode(zz);
                uint64_t first_pv = static_cast<uint64_t>(
                    static_cast<int64_t>(prev_first_pv) + delta);
                if (i == key_index) entry.packed_versions[0] = first_pv;
                prev_first_pv = first_pv;
            }
        }

        // First-IDs up to key_index (reordered: right after first-PVs).
        uint32_t prev_first_id = 0;
        if (counts[0] > 0) {
            prev_first_id = static_cast<uint32_t>(reader.read(32));
            if (key_index == 0) entry.ids[0] = prev_first_id;
        }
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (counts[i] > 0) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                uint32_t first_id = static_cast<uint32_t>(
                    static_cast<int32_t>(prev_first_id) + delta);
                if (i == key_index) entry.ids[0] = first_id;
                prev_first_id = first_id;
            }
        }

        // Tail versions — must scan all keys to advance past the column.
        for (uint16_t i = 0; i < num_keys; ++i) {
            if (i == key_index) {
                for (uint32_t v = 1; v < counts[i]; ++v) {
                    uint64_t delta = reader.readEliasGamma64() - 1;
                    entry.packed_versions[v] = entry.packed_versions[v - 1] - delta;
                }
            } else {
                for (uint32_t v = 1; v < counts[i]; ++v) reader.readEliasGamma64();
            }
        }

        // Tail IDs — last column, can early-terminate after target key.
        for (uint16_t i = 0; i <= key_index; ++i) {
            if (i == key_index) {
                for (uint32_t v = 1; v < counts[i]; ++v) {
                    uint32_t zz = reader.readEliasGamma() - 1;
                    int32_t delta = zigzagDecode(zz);
                    entry.ids[v] = static_cast<uint32_t>(
                        static_cast<int32_t>(entry.ids[v - 1]) + delta);
                }
            } else {
                for (uint32_t v = 1; v < counts[i]; ++v) reader.readEliasGamma();
            }
        }
    } else {
        // Row: skip preceding keys, decode target key.
        for (uint16_t k = 0; k < key_index; ++k) {
            uint32_t nv = reader.readEliasGamma();
            if (nv > 0) {
                reader.read(64);
                for (uint32_t v = 1; v < nv; ++v) reader.readEliasGamma64();
                reader.read(32);
                for (uint32_t v = 1; v < nv; ++v) reader.readEliasGamma();
            }
        }

        uint32_t nv = reader.readEliasGamma();
        entry.packed_versions.resize(nv);
        entry.ids.resize(nv);
        if (nv > 0) {
            entry.packed_versions[0] = reader.read(64);
            for (uint32_t v = 1; v < nv; ++v) {
                uint64_t delta = reader.readEliasGamma64() - 1;
                entry.packed_versions[v] = entry.packed_versions[v - 1] - delta;
            }
            entry.ids[0] = static_cast<uint32_t>(reader.read(32));
            for (uint32_t v = 1; v < nv; ++v) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                entry.ids[v] = static_cast<uint32_t>(
                    static_cast<int32_t>(entry.ids[v - 1]) + delta);
            }
        }
    }

    return entry;
}

// ============================================================
// decodeFirstEntry
// ============================================================

template<typename Layout>
std::pair<uint64_t, uint32_t> BucketCodec<Layout>::decodeFirstEntry(
    const Bucket& bucket, uint16_t key_index,
    size_t data_start_bit) const {

    uint16_t num_keys = 0;
    std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));

    BitReader reader(bucket.data, data_start_bit);

    if constexpr (std::is_same_v<Layout, ColumnarLayout>) {
        // Read counts.
        uint32_t counts[256];
        for (uint16_t i = 0; i < num_keys; ++i) {
            counts[i] = reader.readEliasGamma();
        }

        if (counts[key_index] == 0) return {0, 0};

        // First-PV chain up to key_index.
        uint64_t first_pv = 0;
        if (counts[0] > 0) {
            first_pv = reader.read(64);
        }
        for (uint16_t i = 1; i <= key_index; ++i) {
            if (counts[i] > 0) {
                uint64_t zz = reader.readEliasGamma64() - 1;
                int64_t delta = zigzag64Decode(zz);
                first_pv = static_cast<uint64_t>(static_cast<int64_t>(first_pv) + delta);
            }
        }
        // Skip remaining first-PV entries past key_index.
        for (uint16_t i = key_index + 1; i < num_keys; ++i) {
            if (counts[i] > 0) reader.readEliasGamma64();
        }

        // First-ID chain up to key_index.
        uint32_t first_id = 0;
        if (counts[0] > 0) {
            first_id = static_cast<uint32_t>(reader.read(32));
        }
        for (uint16_t i = 1; i <= key_index; ++i) {
            if (counts[i] > 0) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                first_id = static_cast<uint32_t>(static_cast<int32_t>(first_id) + delta);
            }
        }

        return {first_pv, first_id};
    } else {
        // Row: skip preceding keys, read first pv/id.
        for (uint16_t k = 0; k < key_index; ++k) {
            uint32_t nv = reader.readEliasGamma();
            if (nv > 0) {
                reader.read(64);
                for (uint32_t v = 1; v < nv; ++v) reader.readEliasGamma64();
                reader.read(32);
                for (uint32_t v = 1; v < nv; ++v) reader.readEliasGamma();
            }
        }

        uint32_t nv = reader.readEliasGamma();
        if (nv == 0) return {0, 0};

        uint64_t first_pv = reader.read(64);
        for (uint32_t v = 1; v < nv; ++v) reader.readEliasGamma64();
        uint32_t first_id = static_cast<uint32_t>(reader.read(32));

        return {first_pv, first_id};
    }
}

// ============================================================
// bitsForAddVersion
// ============================================================

template<typename Layout>
size_t BucketCodec<Layout>::bitsForAddVersion(const Bucket& bucket, uint16_t key_index,
                                                uint64_t new_pv, uint32_t new_id,
                                                size_t insert_pos) const {
    if constexpr (std::is_same_v<Layout, RowLayout>) {
        // Row layout doesn't support incremental bit budget.
        (void)bucket; (void)key_index; (void)new_pv; (void)new_id; (void)insert_pos;
        return 0;
    } else {
        uint16_t num_keys = 0;
        std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));

        BitReader reader(bucket.data, 16);

        // Skip suffixes.
        reader.read(suffix_bits_);
        for (uint16_t i = 1; i < num_keys; ++i) {
            reader.readEliasGamma64();
        }

        // Read counts.
        uint32_t counts[256];
        for (uint16_t i = 0; i < num_keys; ++i) {
            counts[i] = reader.readEliasGamma();
        }

        uint32_t old_nv = counts[key_index];

        // Read first-pv values.
        uint64_t first_pvs[256];
        if (counts[0] > 0) first_pvs[0] = reader.read(64);
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (counts[i] > 0) {
                uint64_t zz = reader.readEliasGamma64() - 1;
                int64_t delta = zigzag64Decode(zz);
                first_pvs[i] = static_cast<uint64_t>(
                    static_cast<int64_t>(first_pvs[i - 1]) + delta);
            }
        }

        // Read first-id values (reordered: right after first-PVs).
        uint32_t first_ids[256];
        if (counts[0] > 0) first_ids[0] = static_cast<uint32_t>(reader.read(32));
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (counts[i] > 0) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                first_ids[i] = static_cast<uint32_t>(
                    static_cast<int32_t>(first_ids[i - 1]) + delta);
            }
        }

        // Read tail versions for target key.
        std::vector<uint64_t> pvs(old_nv);
        if (old_nv > 0) pvs[0] = first_pvs[key_index];
        for (uint16_t i = 0; i < num_keys; ++i) {
            for (uint32_t v = 1; v < counts[i]; ++v) {
                uint64_t delta = reader.readEliasGamma64() - 1;
                if (i == key_index) {
                    pvs[v] = pvs[v - 1] - delta;
                }
            }
        }

        // Read tail IDs for target key.
        std::vector<uint32_t> ids(old_nv);
        if (old_nv > 0) ids[0] = first_ids[key_index];
        for (uint16_t i = 0; i < num_keys; ++i) {
            for (uint32_t v = 1; v < counts[i]; ++v) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                if (i == key_index) {
                    ids[v] = static_cast<uint32_t>(
                        static_cast<int32_t>(ids[v - 1]) + delta);
                }
            }
        }

        // Compute the bit difference.
        size_t extra_bits = 0;

        extra_bits += eliasGammaBits(old_nv + 1);
        extra_bits -= eliasGammaBits(old_nv);

        if (old_nv == 0) {
            if (key_index == 0) {
                extra_bits += 64;
            } else {
                int64_t delta = static_cast<int64_t>(new_pv) -
                                static_cast<int64_t>(first_pvs[key_index - 1]);
                extra_bits += eliasGammaBits64(zigzag64Encode(delta) + 1);
            }
            if (key_index + 1 < num_keys && counts[key_index + 1] > 0) {
                int64_t new_next_delta = static_cast<int64_t>(first_pvs[key_index + 1]) -
                                         static_cast<int64_t>(new_pv);
                extra_bits += eliasGammaBits64(zigzag64Encode(new_next_delta) + 1);
                if (key_index == 0) {
                    extra_bits -= 64;
                } else {
                    int64_t old_next_delta = static_cast<int64_t>(first_pvs[key_index + 1]) -
                                             static_cast<int64_t>(first_pvs[key_index - 1]);
                    extra_bits -= eliasGammaBits64(zigzag64Encode(old_next_delta) + 1);
                }
            }

            if (key_index == 0) {
                extra_bits += 32;
            } else {
                int32_t id_delta = static_cast<int32_t>(new_id) -
                                   static_cast<int32_t>(first_ids[key_index - 1]);
                extra_bits += eliasGammaBits(zigzagEncode(id_delta) + 1);
            }
            if (key_index + 1 < num_keys && counts[key_index + 1] > 0) {
                int32_t new_next_delta = static_cast<int32_t>(first_ids[key_index + 1]) -
                                         static_cast<int32_t>(new_id);
                extra_bits += eliasGammaBits(zigzagEncode(new_next_delta) + 1);
                if (key_index == 0) {
                    extra_bits -= 32;
                } else {
                    int32_t old_next_delta = static_cast<int32_t>(first_ids[key_index + 1]) -
                                             static_cast<int32_t>(first_ids[key_index - 1]);
                    extra_bits -= eliasGammaBits(zigzagEncode(old_next_delta) + 1);
                }
            }
        } else if (insert_pos == 0) {
            uint64_t old_first_pv = pvs[0];
            uint32_t old_first_id = ids[0];

            if (key_index == 0) {
                // raw anchor, no size change
            } else {
                int64_t old_delta = static_cast<int64_t>(old_first_pv) -
                                    static_cast<int64_t>(first_pvs[key_index - 1]);
                size_t old_bits = eliasGammaBits64(zigzag64Encode(old_delta) + 1);
                int64_t new_delta = static_cast<int64_t>(new_pv) -
                                    static_cast<int64_t>(first_pvs[key_index - 1]);
                size_t new_bits = eliasGammaBits64(zigzag64Encode(new_delta) + 1);
                extra_bits += new_bits - old_bits;
            }
            if (key_index + 1 < num_keys && counts[key_index + 1] > 0) {
                int64_t old_next = static_cast<int64_t>(first_pvs[key_index + 1]) -
                                   static_cast<int64_t>(old_first_pv);
                int64_t new_next = static_cast<int64_t>(first_pvs[key_index + 1]) -
                                   static_cast<int64_t>(new_pv);
                extra_bits += eliasGammaBits64(zigzag64Encode(new_next) + 1);
                extra_bits -= eliasGammaBits64(zigzag64Encode(old_next) + 1);
            }
            uint64_t pv_delta = new_pv - old_first_pv;
            extra_bits += eliasGammaBits64(pv_delta + 1);

            if (key_index == 0) {
                // raw anchor, no size change
            } else {
                int32_t old_id_delta = static_cast<int32_t>(old_first_id) -
                                       static_cast<int32_t>(first_ids[key_index - 1]);
                int32_t new_id_delta = static_cast<int32_t>(new_id) -
                                       static_cast<int32_t>(first_ids[key_index - 1]);
                extra_bits += eliasGammaBits(zigzagEncode(new_id_delta) + 1);
                extra_bits -= eliasGammaBits(zigzagEncode(old_id_delta) + 1);
            }
            if (key_index + 1 < num_keys && counts[key_index + 1] > 0) {
                int32_t old_next = static_cast<int32_t>(first_ids[key_index + 1]) -
                                   static_cast<int32_t>(old_first_id);
                int32_t new_next = static_cast<int32_t>(first_ids[key_index + 1]) -
                                   static_cast<int32_t>(new_id);
                extra_bits += eliasGammaBits(zigzagEncode(new_next) + 1);
                extra_bits -= eliasGammaBits(zigzagEncode(old_next) + 1);
            }
            int32_t tail_id_delta = static_cast<int32_t>(old_first_id) -
                                    static_cast<int32_t>(new_id);
            extra_bits += eliasGammaBits(zigzagEncode(tail_id_delta) + 1);
        } else if (insert_pos == old_nv) {
            uint64_t prev_pv = pvs[old_nv - 1];
            uint32_t prev_id = ids[old_nv - 1];
            uint64_t pv_delta = prev_pv - new_pv;
            extra_bits += eliasGammaBits64(pv_delta + 1);
            int32_t id_delta = static_cast<int32_t>(new_id) - static_cast<int32_t>(prev_id);
            extra_bits += eliasGammaBits(zigzagEncode(id_delta) + 1);
        } else {
            uint64_t prev_pv = pvs[insert_pos - 1];
            uint64_t next_pv = pvs[insert_pos];
            uint64_t old_pv_delta = prev_pv - next_pv;
            uint64_t delta_prev_new = prev_pv - new_pv;
            uint64_t delta_new_next = new_pv - next_pv;
            extra_bits += eliasGammaBits64(delta_prev_new + 1);
            extra_bits += eliasGammaBits64(delta_new_next + 1);
            extra_bits -= eliasGammaBits64(old_pv_delta + 1);

            uint32_t prev_id_val = ids[insert_pos - 1];
            uint32_t next_id_val = ids[insert_pos];
            int32_t old_id_delta = static_cast<int32_t>(next_id_val) -
                                   static_cast<int32_t>(prev_id_val);
            int32_t id_delta_prev_new = static_cast<int32_t>(new_id) -
                                        static_cast<int32_t>(prev_id_val);
            int32_t id_delta_new_next = static_cast<int32_t>(next_id_val) -
                                        static_cast<int32_t>(new_id);
            extra_bits += eliasGammaBits(zigzagEncode(id_delta_prev_new) + 1);
            extra_bits += eliasGammaBits(zigzagEncode(id_delta_new_next) + 1);
            extra_bits -= eliasGammaBits(zigzagEncode(old_id_delta) + 1);
        }

        return extra_bits;
    }
}

// ============================================================
// bitsForNewEntry
// ============================================================

template<typename Layout>
size_t BucketCodec<Layout>::bitsForNewEntry(const Bucket& bucket, uint16_t insert_pos,
                                              uint64_t suffix, uint64_t packed_version,
                                              uint32_t id) const {
    if constexpr (std::is_same_v<Layout, RowLayout>) {
        (void)bucket; (void)insert_pos; (void)suffix; (void)packed_version; (void)id;
        return 0;
    } else {
        uint16_t num_keys = 0;
        std::memcpy(&num_keys, bucket.data, sizeof(uint16_t));

        size_t bits = 0;

        if (num_keys == 0) {
            bits += suffix_bits_;
            bits += 1 + 64 + 32;
            return bits;
        }

        BitReader reader(bucket.data, 16);
        std::vector<uint64_t> suffixes(num_keys);
        suffixes[0] = reader.read(suffix_bits_);
        for (uint16_t i = 1; i < num_keys; ++i) {
            uint64_t delta = reader.readEliasGamma64() - 1;
            suffixes[i] = suffixes[i - 1] + delta;
        }

        bool has_prev = (insert_pos > 0);
        bool has_next = (insert_pos < num_keys);
        uint64_t prev_suffix = has_prev ? suffixes[insert_pos - 1] : 0;
        uint64_t next_suffix = has_next ? suffixes[insert_pos] : 0;

        if (!has_prev) {
            bits += suffix_bits_;
            if (has_next) {
                uint64_t delta_new_next = next_suffix - suffix;
                bits += eliasGammaBits64(delta_new_next + 1);
            }
        } else if (!has_next) {
            uint64_t delta_prev_new = suffix - prev_suffix;
            bits += eliasGammaBits64(delta_prev_new + 1);
        } else {
            uint64_t old_delta = next_suffix - prev_suffix;
            uint64_t delta_prev_new = suffix - prev_suffix;
            uint64_t delta_new_next = next_suffix - suffix;
            bits += eliasGammaBits64(delta_prev_new + 1);
            bits += eliasGammaBits64(delta_new_next + 1);
            bits -= eliasGammaBits64(old_delta + 1);
        }

        // Read counts and first-pv/first-id for cross-key delta impact.
        uint32_t counts[256];
        for (uint16_t i = 0; i < num_keys; ++i) {
            counts[i] = reader.readEliasGamma();
        }

        uint64_t first_pvs[256];
        if (counts[0] > 0) first_pvs[0] = reader.read(64);
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (counts[i] > 0) {
                uint64_t zz = reader.readEliasGamma64() - 1;
                int64_t delta = zigzag64Decode(zz);
                first_pvs[i] = static_cast<uint64_t>(
                    static_cast<int64_t>(first_pvs[i - 1]) + delta);
            }
        }

        // Read first-id values (reordered: right after first-PVs).
        uint32_t first_ids[256];
        if (counts[0] > 0) first_ids[0] = static_cast<uint32_t>(reader.read(32));
        for (uint16_t i = 1; i < num_keys; ++i) {
            if (counts[i] > 0) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                first_ids[i] = static_cast<uint32_t>(
                    static_cast<int32_t>(first_ids[i - 1]) + delta);
            }
        }

        // Cross-key PV delta.
        if (insert_pos == 0) {
            bits += 64;
            if (counts[0] > 0) {
                int64_t delta = static_cast<int64_t>(first_pvs[0]) -
                                static_cast<int64_t>(packed_version);
                bits += eliasGammaBits64(zigzag64Encode(delta) + 1);
                bits -= 64;
            }
        } else if (insert_pos == num_keys) {
            uint16_t prev_key = insert_pos - 1;
            if (counts[prev_key] > 0) {
                int64_t delta = static_cast<int64_t>(packed_version) -
                                static_cast<int64_t>(first_pvs[prev_key]);
                bits += eliasGammaBits64(zigzag64Encode(delta) + 1);
            } else {
                bits += 64;
            }
        } else {
            uint16_t prev_key = insert_pos - 1;
            uint16_t next_key = insert_pos;
            if (counts[prev_key] > 0 && counts[next_key] > 0) {
                int64_t old_delta = static_cast<int64_t>(first_pvs[next_key]) -
                                    static_cast<int64_t>(first_pvs[prev_key]);
                int64_t delta_prev_new = static_cast<int64_t>(packed_version) -
                                         static_cast<int64_t>(first_pvs[prev_key]);
                int64_t delta_new_next = static_cast<int64_t>(first_pvs[next_key]) -
                                         static_cast<int64_t>(packed_version);
                bits += eliasGammaBits64(zigzag64Encode(delta_prev_new) + 1);
                bits += eliasGammaBits64(zigzag64Encode(delta_new_next) + 1);
                bits -= eliasGammaBits64(zigzag64Encode(old_delta) + 1);
            } else if (counts[prev_key] > 0) {
                int64_t delta = static_cast<int64_t>(packed_version) -
                                static_cast<int64_t>(first_pvs[prev_key]);
                bits += eliasGammaBits64(zigzag64Encode(delta) + 1);
            } else if (counts[next_key] > 0) {
                int64_t delta = static_cast<int64_t>(first_pvs[next_key]) -
                                static_cast<int64_t>(packed_version);
                bits += eliasGammaBits64(zigzag64Encode(delta) + 1);
            } else {
                bits += 64;
            }
        }

        // Cross-key ID delta.
        if (insert_pos == 0) {
            bits += 32;
            if (counts[0] > 0) {
                int32_t delta = static_cast<int32_t>(first_ids[0]) -
                                static_cast<int32_t>(id);
                bits += eliasGammaBits(zigzagEncode(delta) + 1);
                bits -= 32;
            }
        } else if (insert_pos == num_keys) {
            uint16_t prev_key = insert_pos - 1;
            if (counts[prev_key] > 0) {
                int32_t delta = static_cast<int32_t>(id) -
                                static_cast<int32_t>(first_ids[prev_key]);
                bits += eliasGammaBits(zigzagEncode(delta) + 1);
            } else {
                bits += 32;
            }
        } else {
            uint16_t prev_key = insert_pos - 1;
            uint16_t next_key = insert_pos;
            if (counts[prev_key] > 0 && counts[next_key] > 0) {
                int32_t old_delta = static_cast<int32_t>(first_ids[next_key]) -
                                    static_cast<int32_t>(first_ids[prev_key]);
                int32_t delta_prev_new = static_cast<int32_t>(id) -
                                         static_cast<int32_t>(first_ids[prev_key]);
                int32_t delta_new_next = static_cast<int32_t>(first_ids[next_key]) -
                                         static_cast<int32_t>(id);
                bits += eliasGammaBits(zigzagEncode(delta_prev_new) + 1);
                bits += eliasGammaBits(zigzagEncode(delta_new_next) + 1);
                bits -= eliasGammaBits(zigzagEncode(old_delta) + 1);
            } else if (counts[prev_key] > 0) {
                int32_t delta = static_cast<int32_t>(id) -
                                static_cast<int32_t>(first_ids[prev_key]);
                bits += eliasGammaBits(zigzagEncode(delta) + 1);
            } else if (counts[next_key] > 0) {
                int32_t delta = static_cast<int32_t>(first_ids[next_key]) -
                                static_cast<int32_t>(id);
                bits += eliasGammaBits(zigzagEncode(delta) + 1);
            } else {
                bits += 32;
            }
        }

        // Count for the new key: gamma(1) = 1 bit.
        bits += 1;

        return bits;
    }
}

// ============================================================
// Explicit template instantiations
// ============================================================

template class BucketCodec<RowLayout>;
template class BucketCodec<ColumnarLayout>;

}  // namespace internal
}  // namespace kvlite
