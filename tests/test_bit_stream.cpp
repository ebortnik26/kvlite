#include <gtest/gtest.h>

#include "internal/bit_stream.h"

#include <cstring>

using namespace kvlite::internal;

TEST(BitStream, WriteAndReadBits) {
    uint8_t buf[16] = {};
    BitWriter writer(buf, 0);
    writer.write(0b10110, 5);
    writer.write(0xFF, 8);
    writer.write(0, 3);

    BitReader reader(buf, 0);
    EXPECT_EQ(reader.read(5), 0b10110u);
    EXPECT_EQ(reader.read(8), 0xFFu);
    EXPECT_EQ(reader.read(3), 0u);
}

TEST(BitStream, UnaryEncoding) {
    uint8_t buf[16] = {};
    BitWriter writer(buf, 0);
    writer.writeUnary(0);  // "0"
    writer.writeUnary(1);  // "10"
    writer.writeUnary(3);  // "1110"
    writer.writeUnary(0);  // "0"

    BitReader reader(buf, 0);
    EXPECT_EQ(reader.readUnary(), 0u);
    EXPECT_EQ(reader.readUnary(), 1u);
    EXPECT_EQ(reader.readUnary(), 3u);
    EXPECT_EQ(reader.readUnary(), 0u);
}

TEST(BitStream, OffsetReadWrite) {
    uint8_t buf[16] = {};
    BitWriter writer(buf, 7);  // start at bit offset 7
    writer.write(0xAB, 8);

    BitReader reader(buf, 7);
    EXPECT_EQ(reader.read(8), 0xABu);
}

TEST(BitStream, LargeValue) {
    uint8_t buf[16] = {};
    uint64_t val = 0x123456789ABCDEF0ULL;
    BitWriter writer(buf, 0);
    writer.write(val, 64);

    BitReader reader(buf, 0);
    EXPECT_EQ(reader.read(64), val);
}

// --- Non-aligned / cross-boundary tests ---

// 64-bit value written at every possible sub-byte offset (1..7).
// This forces the read/write to span a 9th byte.
TEST(BitStream, Read64BitsAtEveryOffset) {
    uint64_t val = 0xDEADBEEFCAFEBABEULL;
    for (uint8_t off = 0; off <= 7; ++off) {
        uint8_t buf[32] = {};
        BitWriter writer(buf, off);
        writer.write(val, 64);

        BitReader reader(buf, off);
        EXPECT_EQ(reader.read(64), val)
            << "failed at bit offset " << (int)off;
    }
}

// Write a 39-bit fingerprint at various non-aligned positions.
TEST(BitStream, Read39BitsNonAligned) {
    uint64_t fp = 0x5A5A5A5A5ULL;  // 39 bits
    fp &= (1ULL << 39) - 1;
    for (uint8_t off = 0; off <= 7; ++off) {
        uint8_t buf[32] = {};
        BitWriter writer(buf, off);
        writer.write(fp, 39);

        BitReader reader(buf, off);
        EXPECT_EQ(reader.read(39), fp)
            << "failed at bit offset " << (int)off;
    }
}

// Sequential non-aligned writes then reads of mixed sizes.
TEST(BitStream, MixedSizesNonAligned) {
    uint8_t buf[64] = {};
    BitWriter writer(buf, 3);  // start misaligned
    writer.write(0b101, 3);
    writer.write(0xABCD, 16);
    writer.write(1, 1);
    writer.write(0x123456789ULL, 39);
    writer.write(0, 1);
    writer.write(0xFFFFFFFFFFFFFFFFULL, 64);
    writer.write(0b11, 2);

    BitReader reader(buf, 3);
    EXPECT_EQ(reader.read(3),  0b101u);
    EXPECT_EQ(reader.read(16), 0xABCDu);
    EXPECT_EQ(reader.read(1),  1u);
    EXPECT_EQ(reader.read(39), 0x123456789ULL);
    EXPECT_EQ(reader.read(1),  0u);
    EXPECT_EQ(reader.read(64), 0xFFFFFFFFFFFFFFFFULL);
    EXPECT_EQ(reader.read(2),  0b11u);
}

// Write crosses the 8-byte word boundary at different points.
TEST(BitStream, CrossWordBoundary) {
    // Write starts inside one 8-byte word and ends in the next.
    // word boundary is at byte 8 = bit 64.
    for (uint8_t width = 1; width <= 64; ++width) {
        // Place the write so it straddles bit 64.
        // Start at bit (64 - width/2), so roughly half the bits are
        // in the first word and half in the second.
        size_t start = 64 - width / 2;
        uint64_t val = (width == 64) ? 0xA5A5A5A5A5A5A5A5ULL
                                     : ((1ULL << width) - 1) ^ ((1ULL << (width / 2)));
        val &= (width == 64) ? ~0ULL : (1ULL << width) - 1;

        uint8_t buf[32] = {};
        BitWriter writer(buf, start);
        writer.write(val, width);

        BitReader reader(buf, start);
        EXPECT_EQ(reader.read(width), val)
            << "width=" << (int)width << " start=" << start;
    }
}

// Single-bit reads and writes at every offset in the first two bytes.
TEST(BitStream, SingleBitEveryOffset) {
    uint8_t buf[16] = {};

    // Write alternating 1/0 bits
    for (size_t i = 0; i < 16; ++i) {
        BitWriter writer(buf, i);
        writer.write(i & 1, 1);
    }

    for (size_t i = 0; i < 16; ++i) {
        BitReader reader(buf, i);
        EXPECT_EQ(reader.read(1), i & 1)
            << "bit position " << i;
    }
}

// Unary at non-zero offsets and mixed with fixed-width reads.
TEST(BitStream, UnaryNonAligned) {
    uint8_t buf[32] = {};
    BitWriter writer(buf, 5);  // start at bit 5
    writer.writeUnary(0);      // "0"           -> 1 bit
    writer.writeUnary(7);      // "11111110"    -> 8 bits
    writer.write(0xAB, 8);     //               -> 8 bits
    writer.writeUnary(1);      // "10"          -> 2 bits
    writer.writeUnary(0);      // "0"           -> 1 bit

    BitReader reader(buf, 5);
    EXPECT_EQ(reader.readUnary(), 0u);
    EXPECT_EQ(reader.readUnary(), 7u);
    EXPECT_EQ(reader.read(8), 0xABu);
    EXPECT_EQ(reader.readUnary(), 1u);
    EXPECT_EQ(reader.readUnary(), 0u);
}

// Write does not corrupt adjacent bits.
TEST(BitStream, WritePreservesNeighbors) {
    uint8_t buf[32];
    memset(buf, 0xFF, sizeof(buf));  // all ones

    // Overwrite 8 bits in the middle with zeros, starting at bit 12.
    BitWriter writer(buf, 12);
    writer.write(0x00, 8);

    // Bits [0,11] should still be 1.
    BitReader r1(buf, 0);
    EXPECT_EQ(r1.read(12), 0xFFFu);

    // Bits [12,19] should be 0.
    BitReader r2(buf, 12);
    EXPECT_EQ(r2.read(8), 0x00u);

    // Bits [20,31] should still be 1.
    BitReader r3(buf, 20);
    EXPECT_EQ(r3.read(12), 0xFFFu);
}

// Overwrite existing data (read-modify-write correctness).
TEST(BitStream, OverwriteExistingData) {
    uint8_t buf[16] = {};

    // Write a value, then overwrite with a different value.
    BitWriter w1(buf, 3);
    w1.write(0x1FF, 9);  // 9 bits of ones

    BitWriter w2(buf, 3);
    w2.write(0x0AA, 9);  // different pattern

    BitReader reader(buf, 3);
    EXPECT_EQ(reader.read(9), 0x0AAu);
}

// Position tracking across many operations.
TEST(BitStream, PositionTracking) {
    uint8_t buf[64] = {};
    BitWriter writer(buf, 0);
    EXPECT_EQ(writer.position(), 0u);

    writer.write(0, 5);
    EXPECT_EQ(writer.position(), 5u);

    writer.writeUnary(3);  // 4 bits: "1110"
    EXPECT_EQ(writer.position(), 9u);

    writer.write(0, 64);
    EXPECT_EQ(writer.position(), 73u);

    BitReader reader(buf, 0);
    reader.read(5);
    EXPECT_EQ(reader.position(), 5u);

    reader.readUnary();  // reads "1110" = 4 bits
    EXPECT_EQ(reader.position(), 9u);

    reader.read(64);
    EXPECT_EQ(reader.position(), 73u);
}

// Exhaustive: write every width (1..64) at offset 0 and offset 5.
TEST(BitStream, AllWidthsAligned) {
    for (uint8_t w = 1; w <= 64; ++w) {
        uint64_t val = (w == 64) ? 0xC0FFEE0DEADBEEFULL : ((1ULL << w) - 1);
        val &= (w == 64) ? ~0ULL : (1ULL << w) - 1;

        uint8_t buf[32] = {};
        BitWriter writer(buf, 0);
        writer.write(val, w);

        BitReader reader(buf, 0);
        EXPECT_EQ(reader.read(w), val) << "width=" << (int)w;
    }
}

TEST(BitStream, AllWidthsMisaligned) {
    for (uint8_t w = 1; w <= 64; ++w) {
        uint64_t val = (w == 64) ? 0xC0FFEE0DEADBEEFULL : ((1ULL << w) - 1);
        val &= (w == 64) ? ~0ULL : (1ULL << w) - 1;

        uint8_t buf[32] = {};
        BitWriter writer(buf, 5);
        writer.write(val, w);

        BitReader reader(buf, 5);
        EXPECT_EQ(reader.read(w), val) << "width=" << (int)w << " offset=5";
    }
}
