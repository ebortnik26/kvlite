#include "internal/bucket_codec.h"

#include <cstring>

#include "internal/bit_stream.h"

namespace kvlite {
namespace internal {

BucketCodec::BucketCodec(uint8_t suffix_bits, uint32_t bucket_bytes)
    : suffix_bits_(suffix_bits), bucket_bytes_(bucket_bytes) {}

// --- Helpers ---

static uint32_t zigzagEncode(int32_t v) {
    return static_cast<uint32_t>((v << 1) ^ (v >> 31));
}

static int32_t zigzagDecode(uint32_t v) {
    return static_cast<int32_t>((v >> 1) ^ -(v & 1));
}

static uint8_t eliasGammaBits(uint32_t n) {
    uint8_t k = 31 - __builtin_clz(n);
    return 2 * k + 1;
}

static uint8_t eliasGammaBits64(uint64_t n) {
    uint8_t k = 63 - __builtin_clzll(n);
    return 2 * k + 1;
}

// --- Instance methods ---

size_t BucketCodec::bucketDataBits() const {
    return (bucket_bytes_ - 8) * 8;
}

BucketCodec::BucketContents BucketCodec::decodeBucket(const Bucket& bucket) const {
    BucketContents contents;

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
    }

    return contents;
}

size_t BucketCodec::encodeBucket(Bucket& bucket, const BucketContents& contents) const {
    // Preserve the extension pointer in the last 8 bytes.
    uint64_t ext_ptr = 0;
    std::memcpy(&ext_ptr, bucket.data + bucket_bytes_ - 8, 8);
    size_t data_bytes = bucket_bytes_ - 8;
    std::memset(bucket.data, 0, data_bytes);
    std::memcpy(bucket.data + bucket_bytes_ - 8, &ext_ptr, 8);

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

    return writer.position();
}

BucketCodec::SuffixScanResult BucketCodec::decodeSuffixes(const Bucket& bucket) const {
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

size_t BucketCodec::decodeBucketUsedBits(const Bucket& bucket) const {
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

// --- Static methods ---

void BucketCodec::skipKeyData(BitReader& reader) {
    uint32_t nv = reader.readEliasGamma();
    if (nv > 0) {
        reader.read(64);
        for (uint32_t v = 1; v < nv; ++v) {
            reader.readEliasGamma64();
        }
        reader.read(32);
        for (uint32_t v = 1; v < nv; ++v) {
            reader.readEliasGamma();
        }
    }
}

BucketCodec::KeyEntry BucketCodec::decodeKeyData(BitReader& reader, uint64_t suffix) {
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

size_t BucketCodec::bitsForAppendVersion(uint64_t prev_pv, uint64_t new_pv,
                                          uint32_t prev_id, uint32_t new_id) {
    size_t bits = 0;
    uint64_t pv_delta = (prev_pv > new_pv) ? (prev_pv - new_pv) : (new_pv - prev_pv);
    bits += eliasGammaBits64(pv_delta + 1);

    int32_t id_delta = static_cast<int32_t>(new_id) - static_cast<int32_t>(prev_id);
    bits += eliasGammaBits(zigzagEncode(id_delta) + 1);

    return bits;
}

size_t BucketCodec::bitsForNewEntry(uint64_t suffix, uint64_t prev_suffix,
                                     uint64_t next_suffix, bool has_prev,
                                     bool has_next, uint8_t suffix_bits,
                                     uint64_t packed_version, uint32_t id) {
    (void)packed_version;
    (void)id;
    size_t bits = 0;

    if (!has_prev) {
        bits += suffix_bits;
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

    // Entry data: gamma(1) + 64-bit packed_version + 32-bit id.
    bits += 1 + 64 + 32;  // eliasGammaBits(1) == 1

    return bits;
}

size_t BucketCodec::contentsBitsNeeded(const BucketContents& contents,
                                        uint8_t suffix_bits) {
    if (contents.keys.empty()) return 16;

    size_t bits = 16;  // uint16_t header

    bits += suffix_bits;
    for (size_t i = 1; i < contents.keys.size(); ++i) {
        uint64_t delta = contents.keys[i].suffix - contents.keys[i - 1].suffix;
        bits += eliasGammaBits64(delta + 1);
    }

    for (const auto& key : contents.keys) {
        uint32_t nv = static_cast<uint32_t>(key.packed_versions.size());
        bits += eliasGammaBits(nv);
        if (nv > 0) {
            bits += 64;
            for (uint32_t v = 1; v < nv; ++v) {
                uint64_t delta = key.packed_versions[v - 1] - key.packed_versions[v];
                bits += eliasGammaBits64(delta + 1);
            }
            bits += 32;
            for (uint32_t v = 1; v < nv; ++v) {
                int32_t delta = static_cast<int32_t>(key.ids[v]) -
                                static_cast<int32_t>(key.ids[v - 1]);
                bits += eliasGammaBits(zigzagEncode(delta) + 1);
            }
        }
    }

    return bits;
}

}  // namespace internal
}  // namespace kvlite
