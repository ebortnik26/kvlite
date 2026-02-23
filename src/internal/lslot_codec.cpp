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
        contents.entries[i].fingerprint = reader.read(fingerprint_bits_);
        uint64_t num_values = reader.readUnary();
        contents.entries[i].values.resize(num_values);
        if (num_values > 0) {
            contents.entries[i].values[0] = static_cast<uint32_t>(reader.read(32));
            for (uint64_t v = 1; v < num_values; ++v) {
                uint32_t delta = reader.readEliasGamma() - 1;
                contents.entries[i].values[v] =
                    contents.entries[i].values[v - 1] - delta;
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
        writer.write(contents.entries[i].fingerprint, fingerprint_bits_);
        uint64_t num_values = contents.entries[i].values.size();
        writer.writeUnary(num_values);
        if (num_values > 0) {
            writer.write(contents.entries[i].values[0], 32);
            for (uint64_t v = 1; v < num_values; ++v) {
                uint32_t delta =
                    contents.entries[i].values[v - 1] - contents.entries[i].values[v];
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

size_t LSlotCodec::bitsNeeded(const LSlotContents& contents,
                               uint8_t fp_bits) {
    size_t bits = contents.entries.size() + 1;  // unary(K)
    for (const auto& entry : contents.entries) {
        bits += fp_bits;                         // fingerprint
        bits += entry.values.size() + 1;         // unary(M)
        if (!entry.values.empty()) {
            bits += 32;                          // first value raw
            for (size_t v = 1; v < entry.values.size(); ++v) {
                uint32_t delta = entry.values[v - 1] - entry.values[v];
                bits += eliasGammaBits(delta + 1);
            }
        }
    }
    return bits;
}

size_t LSlotCodec::bitOffset(const uint8_t* data, uint32_t target_lslot) const {
    size_t offset = 0;
    for (uint32_t s = 0; s < target_lslot; ++s) {
        decode(data, offset, &offset);
    }
    return offset;
}

size_t LSlotCodec::totalBits(const uint8_t* data, uint32_t num_lslots) const {
    return bitOffset(data, num_lslots);
}

}  // namespace internal
}  // namespace kvlite
