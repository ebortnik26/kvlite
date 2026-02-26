#include "internal/lslot_codec.h"
#include "internal/bit_stream.h"

namespace kvlite {
namespace internal {

LSlotCodec::LSlotCodec(uint8_t fingerprint_bits)
    : fingerprint_bits_(fingerprint_bits) {}

LSlotCodec::LSlotContents LSlotCodec::decode(
    const uint8_t* data, size_t bit_offset,
    size_t* end_bit_offset) const {

    BitReader reader(data, bit_offset);
    LSlotContents contents;

    uint64_t num_fps = reader.readUnary();
    contents.entries.resize(num_fps);

    for (uint64_t i = 0; i < num_fps; ++i) {
        auto& entry = contents.entries[i];
        uint64_t extra = reader.readEliasGamma64() - 1;
        entry.fp_extra_bits = static_cast<uint8_t>(extra);
        entry.fingerprint = reader.read(fingerprint_bits_ + extra);
        uint64_t num_entries = reader.readUnary();
        entry.packed_versions.resize(num_entries);
        entry.ids.resize(num_entries);
        if (num_entries > 0) {
            // Packed versions: 64-bit raw first, gamma(delta+1) for rest (desc, zero-delta safe)
            entry.packed_versions[0] = reader.read(64);
            for (uint64_t v = 1; v < num_entries; ++v) {
                uint64_t delta = reader.readEliasGamma64() - 1;
                entry.packed_versions[v] = entry.packed_versions[v - 1] - delta;
            }
            // IDs: 32-bit raw first, gamma(delta+1) for rest (desc, zero-delta safe)
            entry.ids[0] = static_cast<uint32_t>(reader.read(32));
            for (uint64_t v = 1; v < num_entries; ++v) {
                uint32_t delta = reader.readEliasGamma() - 1;
                entry.ids[v] = entry.ids[v - 1] - delta;
            }
        }
    }

    if (end_bit_offset) {
        *end_bit_offset = reader.position();
    }
    return contents;
}

size_t LSlotCodec::encode(
    uint8_t* data, size_t bit_offset,
    const LSlotContents& contents) const {

    BitWriter writer(data, bit_offset);

    uint64_t num_fps = contents.entries.size();
    writer.writeUnary(num_fps);

    for (uint64_t i = 0; i < num_fps; ++i) {
        const auto& entry = contents.entries[i];
        writer.writeEliasGamma64(entry.fp_extra_bits + 1);
        writer.write(entry.fingerprint, fingerprint_bits_ + entry.fp_extra_bits);
        uint64_t num_entries = entry.packed_versions.size();
        writer.writeUnary(num_entries);
        if (num_entries > 0) {
            // Packed versions: 64-bit raw first, gamma(delta+1) for rest (desc, zero-delta safe)
            writer.write(entry.packed_versions[0], 64);
            for (uint64_t v = 1; v < num_entries; ++v) {
                uint64_t delta = entry.packed_versions[v - 1] - entry.packed_versions[v];
                writer.writeEliasGamma64(delta + 1);
            }
            // IDs: 32-bit raw first, gamma(delta+1) for rest (desc, zero-delta safe)
            writer.write(entry.ids[0], 32);
            for (uint64_t v = 1; v < num_entries; ++v) {
                uint32_t delta = entry.ids[v - 1] - entry.ids[v];
                writer.writeEliasGamma(delta + 1);
            }
        }
    }

    return writer.position();
}

// Bits needed to Elias-gamma-encode n (n >= 1).
static uint8_t eliasGammaBits(uint32_t n) {
    uint8_t k = 31 - __builtin_clz(n);  // floor(log2(n))
    return 2 * k + 1;
}

// Bits needed to Elias-gamma-encode a 64-bit n (n >= 1).
static uint8_t eliasGammaBits64(uint64_t n) {
    uint8_t k = 63 - __builtin_clzll(n);  // floor(log2(n))
    return 2 * k + 1;
}

size_t LSlotCodec::bitsNeeded(const LSlotContents& contents,
                                 uint8_t fp_bits) {
    size_t bits = contents.entries.size() + 1;  // unary(K)
    for (const auto& entry : contents.entries) {
        bits += eliasGammaBits64(entry.fp_extra_bits + 1);  // gamma(extra+1)
        bits += fp_bits + entry.fp_extra_bits;              // fingerprint
        bits += entry.packed_versions.size() + 1;      // unary(M)
        if (!entry.packed_versions.empty()) {
            // Packed versions: 64-bit raw first + gamma(delta+1) deltas
            bits += 64;
            for (size_t v = 1; v < entry.packed_versions.size(); ++v) {
                uint64_t delta = entry.packed_versions[v - 1] - entry.packed_versions[v];
                bits += eliasGammaBits64(delta + 1);
            }
            // IDs: 32-bit raw first + gamma(delta+1) deltas
            bits += 32;
            for (size_t v = 1; v < entry.ids.size(); ++v) {
                uint32_t delta = entry.ids[v - 1] - entry.ids[v];
                bits += eliasGammaBits(delta + 1);
            }
        }
    }
    return bits;
}

size_t LSlotCodec::skipLSlot(const uint8_t* data, size_t bit_offset) const {
    BitReader reader(data, bit_offset);
    uint64_t num_fps = reader.readUnary();
    for (uint64_t i = 0; i < num_fps; ++i) {
        uint64_t extra = reader.readEliasGamma64() - 1;
        reader.read(fingerprint_bits_ + extra);
        uint64_t M = reader.readUnary();
        if (M > 0) {
            reader.read(64);                                // first packed_version
            for (uint64_t v = 1; v < M; ++v) reader.readEliasGamma64(); // pv deltas
            reader.read(32);                                // first id
            for (uint64_t v = 1; v < M; ++v) reader.readEliasGamma();   // id deltas
        }
    }
    return reader.position();
}

size_t LSlotCodec::bitOffset(const uint8_t* data,
                                uint32_t target_lslot) const {
    size_t offset = 0;
    for (uint32_t s = 0; s < target_lslot; ++s) {
        offset = skipLSlot(data, offset);
    }
    return offset;
}

size_t LSlotCodec::totalBits(const uint8_t* data,
                                uint32_t num_lslots) const {
    return bitOffset(data, num_lslots);
}

}  // namespace internal
}  // namespace kvlite
