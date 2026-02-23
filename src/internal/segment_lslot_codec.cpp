#include "internal/segment_lslot_codec.h"
#include "internal/bit_stream.h"

namespace kvlite {
namespace internal {

// Zigzag encode: map signed int32 to uint32 (small magnitudes → small values).
//   0 → 0, -1 → 1, 1 → 2, -2 → 3, 2 → 4, ...
static uint32_t zigzagEncode(int32_t n) {
    return static_cast<uint32_t>((n << 1) ^ (n >> 31));
}

static int32_t zigzagDecode(uint32_t n) {
    return static_cast<int32_t>((n >> 1) ^ -(n & 1));
}

SegmentLSlotCodec::SegmentLSlotCodec(uint8_t fingerprint_bits)
    : fingerprint_bits_(fingerprint_bits) {}

SegmentLSlotCodec::LSlotContents SegmentLSlotCodec::decode(
    const uint8_t* data, size_t bit_offset,
    size_t* end_bit_offset) const {

    BitReader reader(data, bit_offset);
    LSlotContents contents;

    uint64_t num_fps = reader.readUnary();
    contents.entries.resize(num_fps);

    for (uint64_t i = 0; i < num_fps; ++i) {
        auto& entry = contents.entries[i];
        entry.fingerprint = reader.read(fingerprint_bits_);
        uint64_t num_entries = reader.readUnary();
        entry.offsets.resize(num_entries);
        entry.versions.resize(num_entries);
        if (num_entries > 0) {
            // Offsets: 32-bit raw first, gamma(delta+1) for rest (desc, zero-delta safe)
            entry.offsets[0] = static_cast<uint32_t>(reader.read(32));
            for (uint64_t v = 1; v < num_entries; ++v) {
                uint32_t delta = reader.readEliasGamma() - 1;
                entry.offsets[v] = entry.offsets[v - 1] - delta;
            }
            // Versions: 32-bit raw first, gamma(zigzag(delta)+1) for rest.
            // Versions are parallel to offsets and NOT necessarily descending,
            // so we use zigzag encoding to handle negative deltas.
            entry.versions[0] = static_cast<uint32_t>(reader.read(32));
            for (uint64_t v = 1; v < num_entries; ++v) {
                uint32_t zz = reader.readEliasGamma() - 1;
                int32_t delta = zigzagDecode(zz);
                entry.versions[v] = static_cast<uint32_t>(
                    static_cast<int32_t>(entry.versions[v - 1]) - delta);
            }
        }
    }

    if (end_bit_offset) {
        *end_bit_offset = reader.position();
    }
    return contents;
}

size_t SegmentLSlotCodec::encode(
    uint8_t* data, size_t bit_offset,
    const LSlotContents& contents) const {

    BitWriter writer(data, bit_offset);

    uint64_t num_fps = contents.entries.size();
    writer.writeUnary(num_fps);

    for (uint64_t i = 0; i < num_fps; ++i) {
        const auto& entry = contents.entries[i];
        writer.write(entry.fingerprint, fingerprint_bits_);
        uint64_t num_entries = entry.offsets.size();
        writer.writeUnary(num_entries);
        if (num_entries > 0) {
            // Offsets: 32-bit raw first, gamma(delta+1) for rest (desc, zero-delta safe)
            writer.write(entry.offsets[0], 32);
            for (uint64_t v = 1; v < num_entries; ++v) {
                uint32_t delta = entry.offsets[v - 1] - entry.offsets[v];
                writer.writeEliasGamma(delta + 1);
            }
            // Versions: 32-bit raw first, gamma(zigzag(delta)+1) for rest.
            // Versions are parallel to offsets and NOT necessarily descending,
            // so we use zigzag encoding to handle negative deltas.
            writer.write(entry.versions[0], 32);
            for (uint64_t v = 1; v < num_entries; ++v) {
                int32_t delta = static_cast<int32_t>(entry.versions[v - 1]) -
                                static_cast<int32_t>(entry.versions[v]);
                writer.writeEliasGamma(zigzagEncode(delta) + 1);
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

size_t SegmentLSlotCodec::bitsNeeded(const LSlotContents& contents,
                                 uint8_t fp_bits) {
    size_t bits = contents.entries.size() + 1;  // unary(K)
    for (const auto& entry : contents.entries) {
        bits += fp_bits;                         // fingerprint
        bits += entry.offsets.size() + 1;        // unary(M)
        if (!entry.offsets.empty()) {
            // Offsets: 32-bit raw first + gamma(delta+1) deltas
            bits += 32;
            for (size_t v = 1; v < entry.offsets.size(); ++v) {
                uint32_t delta = entry.offsets[v - 1] - entry.offsets[v];
                bits += eliasGammaBits(delta + 1);
            }
            // Versions: 32-bit raw first + gamma(zigzag(delta)+1) deltas
            bits += 32;
            for (size_t v = 1; v < entry.versions.size(); ++v) {
                int32_t delta = static_cast<int32_t>(entry.versions[v - 1]) -
                                static_cast<int32_t>(entry.versions[v]);
                bits += eliasGammaBits(zigzagEncode(delta) + 1);
            }
        }
    }
    return bits;
}

size_t SegmentLSlotCodec::bitOffset(const uint8_t* data,
                                uint32_t target_lslot) const {
    size_t offset = 0;
    for (uint32_t s = 0; s < target_lslot; ++s) {
        decode(data, offset, &offset);
    }
    return offset;
}

size_t SegmentLSlotCodec::totalBits(const uint8_t* data,
                                uint32_t num_lslots) const {
    return bitOffset(data, num_lslots);
}

}  // namespace internal
}  // namespace kvlite
