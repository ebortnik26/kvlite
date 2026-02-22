#include <gtest/gtest.h>

#include <cstring>
#include <fcntl.h>
#include <set>
#include <unistd.h>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/segment_lslot_codec.h"
#include "internal/segment_delta_hash_table.h"
#include "internal/segment_index.h"
#include "internal/global_index.h"
#include "internal/gc_manager.h"
#include "internal/log_file.h"
#include "internal/segment.h"
#include "internal/delta_hash_table_base.h"

using namespace kvlite::internal;
using kvlite::Status;

// --- SegmentLSlotCodec Tests ---

TEST(SegmentLSlotCodec, EncodeDecodeEmpty) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;

    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);
    EXPECT_GT(end, 0u);

    size_t decoded_end;
    SegmentLSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    EXPECT_TRUE(decoded.entries.empty());
}

TEST(SegmentLSlotCodec, EncodeDecodeSingleEntry) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1234;
    entry.offsets = {1000};
    entry.versions = {5};
    contents.entries.push_back(entry);

    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    SegmentLSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 0x1234u);
    ASSERT_EQ(decoded.entries[0].offsets.size(), 1u);
    EXPECT_EQ(decoded.entries[0].offsets[0], 1000u);
    EXPECT_EQ(decoded.entries[0].versions[0], 5u);
}

TEST(SegmentLSlotCodec, EncodeDecodeMultiplePairs) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 42;
    entry.offsets = {3000, 2000, 1000};   // desc
    entry.versions = {30, 20, 10};        // desc
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    SegmentLSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    ASSERT_EQ(decoded.entries[0].offsets.size(), 3u);
    EXPECT_EQ(decoded.entries[0].offsets[0], 3000u);
    EXPECT_EQ(decoded.entries[0].offsets[1], 2000u);
    EXPECT_EQ(decoded.entries[0].offsets[2], 1000u);
    EXPECT_EQ(decoded.entries[0].versions[0], 30u);
    EXPECT_EQ(decoded.entries[0].versions[1], 20u);
    EXPECT_EQ(decoded.entries[0].versions[2], 10u);
}

TEST(SegmentLSlotCodec, EncodeDecodeMultipleFingerprints) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;

    SegmentLSlotCodec::TrieEntry e1;
    e1.fingerprint = 10;
    e1.offsets = {500, 400};
    e1.versions = {50, 40};
    contents.entries.push_back(e1);

    SegmentLSlotCodec::TrieEntry e2;
    e2.fingerprint = 20;
    e2.offsets = {900};
    e2.versions = {90};
    contents.entries.push_back(e2);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    SegmentLSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 2u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 10u);
    EXPECT_EQ(decoded.entries[1].fingerprint, 20u);
    ASSERT_EQ(decoded.entries[0].offsets.size(), 2u);
    ASSERT_EQ(decoded.entries[1].offsets.size(), 1u);
}

TEST(SegmentLSlotCodec, BitsNeeded) {
    SegmentLSlotCodec::LSlotContents empty;
    EXPECT_EQ(SegmentLSlotCodec::bitsNeeded(empty, 39), 1u);

    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 1;
    entry.offsets = {100};
    entry.versions = {5};
    contents.entries.push_back(entry);

    size_t bits = SegmentLSlotCodec::bitsNeeded(contents, 39);
    // unary(1)=2 + fp=39 + unary(1)=2 + offset_raw=32 + version_raw=32 = 107
    EXPECT_EQ(bits, 107u);
}

TEST(SegmentLSlotCodec, BitOffsetAndTotalBits) {
    SegmentLSlotCodec codec(39);

    uint8_t buf[512] = {};
    size_t offset = 0;

    SegmentLSlotCodec::LSlotContents s0;
    SegmentLSlotCodec::TrieEntry e0;
    e0.fingerprint = 1;
    e0.offsets = {10};
    e0.versions = {1};
    s0.entries.push_back(e0);
    offset = codec.encode(buf, offset, s0);

    SegmentLSlotCodec::LSlotContents s1;  // empty
    offset = codec.encode(buf, offset, s1);

    SegmentLSlotCodec::LSlotContents s2;
    SegmentLSlotCodec::TrieEntry e2;
    e2.fingerprint = 2;
    e2.offsets = {20};
    e2.versions = {2};
    s2.entries.push_back(e2);
    offset = codec.encode(buf, offset, s2);

    EXPECT_EQ(codec.bitOffset(buf, 0), 0u);
    EXPECT_GT(codec.bitOffset(buf, 1), 0u);
    EXPECT_EQ(codec.totalBits(buf, 3), offset);
}

// --- SegmentDeltaHashTable Tests ---

static SegmentDeltaHashTable::Config testConfig() {
    SegmentDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 512;
    return cfg;
}

TEST(SegmentDeltaHashTable, AddAndFindFirst) {
    SegmentDeltaHashTable dht(testConfig());

    dht.addEntry("hello", 100, 1);

    uint32_t off, ver;
    EXPECT_TRUE(dht.findFirst("hello", off, ver));
    EXPECT_EQ(off, 100u);
    EXPECT_EQ(ver, 1u);
    EXPECT_EQ(dht.size(), 1u);
}

TEST(SegmentDeltaHashTable, FindNonExistent) {
    SegmentDeltaHashTable dht(testConfig());
    uint32_t off, ver;
    EXPECT_FALSE(dht.findFirst("missing", off, ver));
}

TEST(SegmentDeltaHashTable, AddMultipleEntries) {
    SegmentDeltaHashTable dht(testConfig());

    dht.addEntry("key1", 100, 1);
    dht.addEntry("key1", 200, 2);
    dht.addEntry("key1", 300, 3);

    EXPECT_EQ(dht.size(), 3u);

    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(dht.findAll("key1", offsets, versions));
    ASSERT_EQ(offsets.size(), 3u);
    EXPECT_EQ(offsets[0], 300u);
    EXPECT_EQ(offsets[1], 200u);
    EXPECT_EQ(offsets[2], 100u);
    EXPECT_EQ(versions[0], 3u);
    EXPECT_EQ(versions[1], 2u);
    EXPECT_EQ(versions[2], 1u);
}

TEST(SegmentDeltaHashTable, FindFirstReturnsHighest) {
    SegmentDeltaHashTable dht(testConfig());

    dht.addEntry("key1", 100, 1);
    dht.addEntry("key1", 300, 3);
    dht.addEntry("key1", 200, 2);

    uint32_t off, ver;
    ASSERT_TRUE(dht.findFirst("key1", off, ver));
    EXPECT_EQ(off, 300u);
    EXPECT_EQ(ver, 3u);
}

TEST(SegmentDeltaHashTable, Contains) {
    SegmentDeltaHashTable dht(testConfig());

    EXPECT_FALSE(dht.contains("key1"));
    dht.addEntry("key1", 100, 1);
    EXPECT_TRUE(dht.contains("key1"));
}

TEST(SegmentDeltaHashTable, Clear) {
    SegmentDeltaHashTable dht(testConfig());

    for (int i = 0; i < 10; ++i) {
        dht.addEntry("key" + std::to_string(i),
                     static_cast<uint32_t>(i * 100 + 1),
                     static_cast<uint32_t>(i + 1));
    }
    EXPECT_EQ(dht.size(), 10u);

    dht.clear();
    EXPECT_EQ(dht.size(), 0u);

    for (int i = 0; i < 10; ++i) {
        EXPECT_FALSE(dht.contains("key" + std::to_string(i)));
    }
}

TEST(SegmentDeltaHashTable, ForEach) {
    SegmentDeltaHashTable dht(testConfig());

    dht.addEntry("a", 100, 1);
    dht.addEntry("b", 200, 2);
    dht.addEntry("c", 300, 3);

    std::set<uint32_t> all_offsets;
    dht.forEach([&](uint64_t, uint32_t offset, uint32_t) {
        all_offsets.insert(offset);
    });

    EXPECT_EQ(all_offsets.size(), 3u);
    EXPECT_EQ(all_offsets.count(100u), 1u);
    EXPECT_EQ(all_offsets.count(200u), 1u);
    EXPECT_EQ(all_offsets.count(300u), 1u);
}

TEST(SegmentDeltaHashTable, ForEachGroup) {
    SegmentDeltaHashTable dht(testConfig());

    dht.addEntry("a", 100, 1);
    dht.addEntry("a", 200, 2);
    dht.addEntry("b", 300, 3);

    size_t group_count = 0;
    dht.forEachGroup([&](uint64_t, const std::vector<uint32_t>& offsets,
                         const std::vector<uint32_t>& versions) {
        ++group_count;
        if (offsets.size() == 2) {
            EXPECT_EQ(offsets[0], 200u);
            EXPECT_EQ(offsets[1], 100u);
            EXPECT_EQ(versions[0], 2u);
            EXPECT_EQ(versions[1], 1u);
        } else {
            EXPECT_EQ(offsets.size(), 1u);
            EXPECT_EQ(offsets[0], 300u);
            EXPECT_EQ(versions[0], 3u);
        }
    });

    EXPECT_EQ(group_count, 2u);
}

TEST(SegmentDeltaHashTable, ManyKeys) {
    SegmentDeltaHashTable dht(testConfig());

    const int N = 200;
    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        dht.addEntry(key, static_cast<uint32_t>(i * 100 + 1),
                     static_cast<uint32_t>(i + 1));
    }

    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint32_t off, ver;
        ASSERT_TRUE(dht.findFirst(key, off, ver)) << "key not found: " << key;
        EXPECT_EQ(off, static_cast<uint32_t>(i * 100 + 1));
        EXPECT_EQ(ver, static_cast<uint32_t>(i + 1));
    }
}

TEST(SegmentDeltaHashTable, AddEntryByHash) {
    SegmentDeltaHashTable dht(testConfig());

    uint64_t hash = 0xDEADBEEF12345678ULL;
    dht.addEntryByHash(hash, 100, 1);

    size_t count = 0;
    dht.forEach([&](uint64_t, uint32_t offset, uint32_t version) {
        ++count;
        EXPECT_EQ(offset, 100u);
        EXPECT_EQ(version, 1u);
    });
    EXPECT_EQ(count, 1u);
}

// --- SegmentIndex Tests ---

TEST(SegmentIndex, PutAndGetLatest) {
    SegmentIndex index;

    index.put("key1", 100, 1);
    index.put("key1", 200, 2);
    index.put("key1", 300, 3);

    uint32_t off, ver;
    EXPECT_TRUE(index.getLatest("key1", off, ver));
    EXPECT_EQ(off, 300u);
    EXPECT_EQ(ver, 3u);

    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(index.get("key1", offsets, versions));
    ASSERT_EQ(offsets.size(), 3u);
    EXPECT_EQ(offsets[0], 300u);
    EXPECT_EQ(offsets[1], 200u);
    EXPECT_EQ(offsets[2], 100u);
    EXPECT_EQ(versions[0], 3u);
    EXPECT_EQ(versions[1], 2u);
    EXPECT_EQ(versions[2], 1u);
}

TEST(SegmentIndex, GetLatest) {
    SegmentIndex index;
    index.put("key1", 100, 1);
    index.put("key1", 200, 2);

    uint32_t off, ver;
    EXPECT_TRUE(index.getLatest("key1", off, ver));
    EXPECT_EQ(off, 200u);
    EXPECT_EQ(ver, 2u);

    EXPECT_FALSE(index.getLatest("missing", off, ver));
}

TEST(SegmentIndex, Contains) {
    SegmentIndex index;
    EXPECT_FALSE(index.contains("key1"));
    index.put("key1", 100, 1);
    EXPECT_TRUE(index.contains("key1"));
}

TEST(SegmentIndex, GetNonExistent) {
    SegmentIndex index;
    std::vector<uint32_t> offsets, versions;
    EXPECT_FALSE(index.get("missing", offsets, versions));
}

TEST(SegmentIndex, Clear) {
    SegmentIndex index;
    for (int i = 0; i < 50; ++i) {
        index.put("key" + std::to_string(i), i * 100, i);
    }
    EXPECT_EQ(index.keyCount(), 50u);

    index.clear();
    EXPECT_EQ(index.keyCount(), 0u);
    EXPECT_EQ(index.entryCount(), 0u);
}

TEST(SegmentIndex, LargeScale) {
    SegmentIndex index;
    const int N = 1000;

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        index.put(key, static_cast<uint32_t>(i * 100),
                  static_cast<uint32_t>(i + 1));
    }

    EXPECT_EQ(index.keyCount(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint32_t off, ver;
        ASSERT_TRUE(index.getLatest(key, off, ver));
        EXPECT_EQ(off, static_cast<uint32_t>(i * 100));
        EXPECT_EQ(ver, static_cast<uint32_t>(i + 1));
    }
}

// --- SegmentIndex Serialization Tests ---

class SegmentIndexSerializationTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = "/tmp/segment_index_test_" + std::to_string(getpid()) + ".idx";
    }
    void TearDown() override {
        ::unlink(path_.c_str());
    }
    std::string path_;
};

TEST_F(SegmentIndexSerializationTest, EmptyRoundTrip) {
    SegmentIndex src;

    LogFile wf;
    ASSERT_TRUE(wf.create(path_).ok());
    ASSERT_TRUE(src.writeTo(wf).ok());
    wf.close();

    SegmentIndex dst;
    LogFile rf;
    ASSERT_TRUE(rf.open(path_).ok());
    ASSERT_TRUE(dst.readFrom(rf).ok());
    rf.close();

    EXPECT_EQ(dst.entryCount(), 0u);
    EXPECT_EQ(dst.keyCount(), 0u);
}

TEST_F(SegmentIndexSerializationTest, SingleEntryRoundTrip) {
    SegmentIndex src;
    src.put("hello", 100, 1);

    LogFile wf;
    ASSERT_TRUE(wf.create(path_).ok());
    ASSERT_TRUE(src.writeTo(wf).ok());
    wf.close();

    SegmentIndex dst;
    LogFile rf;
    ASSERT_TRUE(rf.open(path_).ok());
    ASSERT_TRUE(dst.readFrom(rf).ok());
    rf.close();

    EXPECT_EQ(dst.entryCount(), 1u);
    EXPECT_EQ(dst.keyCount(), 1u);

    uint32_t off, ver;
    ASSERT_TRUE(dst.getLatest("hello", off, ver));
    EXPECT_EQ(off, 100u);
    EXPECT_EQ(ver, 1u);
}

TEST_F(SegmentIndexSerializationTest, MultiEntryRoundTrip) {
    SegmentIndex src;
    src.put("a", 100, 1);
    src.put("a", 200, 2);
    src.put("b", 300, 3);
    src.put("c", 400, 4);
    src.put("c", 500, 5);
    src.put("c", 600, 6);

    LogFile wf;
    ASSERT_TRUE(wf.create(path_).ok());
    ASSERT_TRUE(src.writeTo(wf).ok());
    wf.close();

    SegmentIndex dst;
    LogFile rf;
    ASSERT_TRUE(rf.open(path_).ok());
    ASSERT_TRUE(dst.readFrom(rf).ok());
    rf.close();

    EXPECT_EQ(dst.entryCount(), 6u);
    EXPECT_EQ(dst.keyCount(), 3u);

    // Verify all entries preserved via getLatest.
    uint32_t off, ver;
    ASSERT_TRUE(dst.getLatest("a", off, ver));
    EXPECT_EQ(off, 200u);
    EXPECT_EQ(ver, 2u);

    ASSERT_TRUE(dst.getLatest("b", off, ver));
    EXPECT_EQ(off, 300u);
    EXPECT_EQ(ver, 3u);

    ASSERT_TRUE(dst.getLatest("c", off, ver));
    EXPECT_EQ(off, 600u);
    EXPECT_EQ(ver, 6u);
}

TEST_F(SegmentIndexSerializationTest, CorruptedChecksum) {
    SegmentIndex src;
    src.put("key", 100, 1);

    LogFile wf;
    ASSERT_TRUE(wf.create(path_).ok());
    ASSERT_TRUE(src.writeTo(wf).ok());
    wf.close();

    // Corrupt a byte in the file.
    {
        LogFile f;
        ASSERT_TRUE(f.open(path_).ok());
        uint8_t byte = 0xFF;
        uint64_t dummy;
        // Overwrite byte 4 (inside the header entry_count field).
        f.append(&byte, 1, dummy);  // appends at end, won't help
        f.close();

        // Directly corrupt via POSIX write.
        int fd = ::open(path_.c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ssize_t w = ::pwrite(fd, &byte, 1, 4);
        ASSERT_EQ(w, 1);
        ::close(fd);
    }

    SegmentIndex dst;
    LogFile rf;
    ASSERT_TRUE(rf.open(path_).ok());
    Status s = dst.readFrom(rf);
    rf.close();

    // Could be checksum mismatch or a read error due to size change.
    EXPECT_FALSE(s.ok());
}

TEST_F(SegmentIndexSerializationTest, BadMagic) {
    // Write a file with bad magic.
    {
        LogFile f;
        ASSERT_TRUE(f.create(path_).ok());
        uint32_t bad_magic = 0xDEADBEEF;
        uint32_t zeros[3] = {0, 0, 0};
        uint64_t dummy;
        f.append(&bad_magic, sizeof(bad_magic), dummy);
        f.append(zeros, sizeof(zeros), dummy);
        // Write a CRC (won't match, but magic check comes first).
        uint32_t crc = 0;
        f.append(&crc, sizeof(crc), dummy);
        f.close();
    }

    SegmentIndex dst;
    LogFile rf;
    ASSERT_TRUE(rf.open(path_).ok());
    Status s = dst.readFrom(rf);
    rf.close();

    EXPECT_TRUE(s.isCorruption());
}

// --- GCManager Visible Count Tests ---

// One key, one version in seg1, snapshot >= version → 1.
TEST(VisibleCount, AllVisible) {
    GlobalIndex gi;
    gi.put("key1", /*version=*/10, /*segment_id=*/1);

    SegmentIndex si;
    si.put("key1", /*offset=*/0, /*version=*/10);

    std::vector<uint64_t> snapshots = {10};  // current version
    size_t count = GCManager::computeVisibleCount(gi, si, /*segment_id=*/1, snapshots);
    EXPECT_EQ(count, 1u);
}

// Key has v1(seg1), v2(seg2). Only current snapshot at v2 → seg1 gets 0.
TEST(VisibleCount, Superseded) {
    GlobalIndex gi;
    gi.put("key1", /*version=*/1, /*segment_id=*/1);
    gi.put("key1", /*version=*/2, /*segment_id=*/2);

    SegmentIndex si;
    si.put("key1", /*offset=*/0, /*version=*/1);

    std::vector<uint64_t> snapshots = {2};  // current version
    size_t count = GCManager::computeVisibleCount(gi, si, /*segment_id=*/1, snapshots);
    EXPECT_EQ(count, 0u);
}

// Key has v1(seg1), v2(seg2). Snapshot at v1, current at v2 → seg1 gets 1.
TEST(VisibleCount, SnapshotPins) {
    GlobalIndex gi;
    gi.put("key1", /*version=*/1, /*segment_id=*/1);
    gi.put("key1", /*version=*/2, /*segment_id=*/2);

    SegmentIndex si;
    si.put("key1", /*offset=*/0, /*version=*/1);

    std::vector<uint64_t> snapshots = {1, 2};  // snapshot at v1, current at v2
    size_t count = GCManager::computeVisibleCount(gi, si, /*segment_id=*/1, snapshots);
    EXPECT_EQ(count, 1u);
}

// Multiple keys, mixed visibility across segments.
TEST(VisibleCount, MultipleKeys) {
    GlobalIndex gi;
    // key1: v1 in seg1, v2 in seg2
    gi.put("key1", 1, 1);
    gi.put("key1", 2, 2);
    // key2: v3 in seg1 (latest)
    gi.put("key2", 3, 1);
    // key3: v4 in seg2
    gi.put("key3", 4, 2);

    SegmentIndex si;
    si.put("key1", 0, 1);
    si.put("key2", 100, 3);

    std::vector<uint64_t> snapshots = {4};  // current version
    size_t count = GCManager::computeVisibleCount(gi, si, /*segment_id=*/1, snapshots);
    // key1: latest is v2 in seg2, not seg1 → 0
    // key2: latest is v3 in seg1 → 1
    EXPECT_EQ(count, 1u);
}

// SegmentIndex entries not in GlobalIndex → 0.
TEST(VisibleCount, NoOverlap) {
    GlobalIndex gi;
    gi.put("other_key", 1, 2);

    SegmentIndex si;
    si.put("lonely_key", 0, 1);

    std::vector<uint64_t> snapshots = {1};
    size_t count = GCManager::computeVisibleCount(gi, si, /*segment_id=*/1, snapshots);
    EXPECT_EQ(count, 0u);
}

// Several snapshots pin different versions in the same segment.
TEST(VisibleCount, MultipleSnapshots) {
    GlobalIndex gi;
    // key1: v1(seg1), v3(seg1), v5(seg2)
    gi.put("key1", 1, 1);
    gi.put("key1", 3, 1);
    gi.put("key1", 5, 2);

    SegmentIndex si;
    si.put("key1", 0, 1);
    si.put("key1", 100, 3);

    // Snapshots at v2, v4, v5:
    //   snap=2: latest <= 2 is v1(seg1) → pinned
    //   snap=4: latest <= 4 is v3(seg1) → pinned
    //   snap=5: latest <= 5 is v5(seg2) → not in seg1
    std::vector<uint64_t> snapshots = {2, 4, 5};
    size_t count = GCManager::computeVisibleCount(gi, si, /*segment_id=*/1, snapshots);
    EXPECT_EQ(count, 2u);  // v1 and v3 are pinned
}

// Verify merge-join works correctly when GlobalIndex and SegmentIndex
// have different DHT bucket configurations (different bucket_bits).
// The reconstructed 64-bit hash is lossless in both cases
// (bucket_bits + lslot_bits + fingerprint_bits = 64), so hashes match.
TEST(VisibleCount, DifferentBucketCounts) {
    // GlobalIndex uses default Config (bucket_bits=20, lslot_bits=5)
    // SegmentIndex uses its own config (bucket_bits=16, lslot_bits=4)
    // Both produce the same 64-bit hash for the same key.

    GlobalIndex gi;
    SegmentIndex si;

    // Insert many keys to exercise different bucket assignments.
    const int N = 100;
    for (int i = 0; i < N; ++i) {
        std::string key = "dbc_key_" + std::to_string(i);
        gi.put(key, static_cast<uint64_t>(i + 1), /*segment_id=*/1);
        si.put(key, static_cast<uint32_t>(i * 64), static_cast<uint32_t>(i + 1));
    }

    // All keys have their latest version in seg1, snapshot covers all.
    std::vector<uint64_t> snapshots = {static_cast<uint64_t>(N)};
    size_t count = GCManager::computeVisibleCount(gi, si, /*segment_id=*/1, snapshots);
    EXPECT_EQ(count, static_cast<size_t>(N));
}

// Visible count setter/getter on SegmentIndex.
TEST(SegmentIndex, VisibleCount) {
    SegmentIndex si;
    EXPECT_EQ(si.visibleCount(), 0u);
    si.setVisibleCount(42);
    EXPECT_EQ(si.visibleCount(), 42u);
}

// --- VisibleVersionIterator Tests ---

class VisibleVersionIteratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_ = "/tmp/vvi_test_" + std::to_string(getpid()) + "_";
    }
    void TearDown() override {
        for (auto& seg : segments_) {
            seg.close();
        }
        for (auto& path : paths_) {
            ::unlink(path.c_str());
        }
    }

    // Create a segment, write entries, seal it, and register in GlobalIndex.
    // Returns the segment index in segments_.
    size_t createSegment(
        uint32_t segment_id,
        const std::vector<std::tuple<std::string, uint64_t, std::string, bool>>& entries) {
        std::string path = base_ + std::to_string(segment_id) + ".data";
        paths_.push_back(path);
        segments_.emplace_back();
        size_t idx = segments_.size() - 1;
        auto& seg = segments_[idx];

        EXPECT_TRUE(seg.create(path, segment_id).ok());
        for (const auto& [key, version, value, tombstone] : entries) {
            EXPECT_TRUE(seg.put(key, version, value, tombstone).ok());
            gi_.put(key, version, segment_id);
        }
        EXPECT_TRUE(seg.seal().ok());
        return idx;
    }

    GlobalIndex gi_;
    std::vector<Segment> segments_;
    std::string base_;
    std::vector<std::string> paths_;
};

// 1 key, 1 version, snapshot covers it → yields 1 entry with correct key/value.
TEST_F(VisibleVersionIteratorTest, IteratorAllVisible) {
    size_t idx = createSegment(1, {{"key1", 10, "val1", false}});
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 10);

    std::vector<uint64_t> snapshots = {10};
    auto iter = GCManager::getVisibleVersions(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    ASSERT_TRUE(iter.valid());
    EXPECT_EQ(iter.entry().log_entry.key, "key1");
    EXPECT_EQ(iter.entry().log_entry.value, "val1");
    EXPECT_EQ(iter.entry().log_entry.version(), 10u);
    EXPECT_FALSE(iter.entry().log_entry.tombstone());

    ASSERT_TRUE(iter.next().ok());
    EXPECT_FALSE(iter.valid());
}

// key v1(seg1), v2(seg2). Only current snapshot at v2 → seg1 yields 0.
TEST_F(VisibleVersionIteratorTest, IteratorSuperseded) {
    size_t idx = createSegment(1, {{"key1", 1, "val1", false}});
    createSegment(2, {{"key1", 2, "val2", false}});
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 1);

    std::vector<uint64_t> snapshots = {2};
    auto iter = GCManager::getVisibleVersions(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    EXPECT_FALSE(iter.valid());
}

// key v1(seg1), v2(seg2). Snapshot at v1 → seg1 yields v1.
TEST_F(VisibleVersionIteratorTest, IteratorSnapshotPins) {
    size_t idx = createSegment(1, {{"key1", 1, "val1", false}});
    createSegment(2, {{"key1", 2, "val2", false}});
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 1);

    std::vector<uint64_t> snapshots = {1, 2};
    auto iter = GCManager::getVisibleVersions(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    ASSERT_TRUE(iter.valid());
    EXPECT_EQ(iter.entry().log_entry.key, "key1");
    EXPECT_EQ(iter.entry().log_entry.version(), 1u);

    ASSERT_TRUE(iter.next().ok());
    EXPECT_FALSE(iter.valid());
}

// key v1,v3 in seg1, v5 in seg2. Snapshots pin both v1 and v3.
// File order is (hash asc, version desc), so yields v3 then v1.
TEST_F(VisibleVersionIteratorTest, IteratorMultipleVersionsDesc) {
    // Segment::put writes entries in call order; we write v3, v1
    // so file order = v3, v1 (version desc for same hash).
    size_t idx = createSegment(1, {
        {"key1", 3, "val3", false},
        {"key1", 1, "val1", false},
    });
    createSegment(2, {{"key1", 5, "val5", false}});
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 3);   // offset doesn't matter for visible-set check
    si.put("key1", 100, 1);

    // snap=2 → latest <= 2 is v1(seg1) → pinned
    // snap=4 → latest <= 4 is v3(seg1) → pinned
    // snap=5 → latest <= 5 is v5(seg2) → not in seg1
    std::vector<uint64_t> snapshots = {2, 4, 5};
    auto iter = GCManager::getVisibleVersions(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    ASSERT_TRUE(iter.valid());
    EXPECT_EQ(iter.entry().log_entry.version(), 3u);
    EXPECT_EQ(iter.entry().log_entry.value, "val3");

    ASSERT_TRUE(iter.next().ok());
    ASSERT_TRUE(iter.valid());
    EXPECT_EQ(iter.entry().log_entry.version(), 1u);
    EXPECT_EQ(iter.entry().log_entry.value, "val1");

    ASSERT_TRUE(iter.next().ok());
    EXPECT_FALSE(iter.valid());
}

// Multiple keys: verifies hash-asc then version-desc order.
TEST_F(VisibleVersionIteratorTest, IteratorMultipleKeys) {
    // We need two keys with known hash ordering. Write them
    // in hash-ascending order to the segment.
    std::string keyA = "key1";
    std::string keyB = "key2";
    uint64_t hashA = dhtHashBytes(keyA.data(), keyA.size());
    uint64_t hashB = dhtHashBytes(keyB.data(), keyB.size());

    // Ensure we know which hash is smaller; swap if needed.
    if (hashA > hashB) {
        std::swap(keyA, keyB);
        std::swap(hashA, hashB);
    }

    // Write entries: keyA(v2), keyA(v1), keyB(v4), keyB(v3)
    // File order: hash-asc, version-desc within same hash.
    size_t idx = createSegment(1, {
        {keyA, 2, "A_v2", false},
        {keyA, 1, "A_v1", false},
        {keyB, 4, "B_v4", false},
        {keyB, 3, "B_v3", false},
    });
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put(keyA, 0, 2);
    si.put(keyA, 100, 1);
    si.put(keyB, 200, 4);
    si.put(keyB, 300, 3);

    // Snapshots pin all versions.
    std::vector<uint64_t> snapshots = {1, 2, 3, 4};
    auto iter = GCManager::getVisibleVersions(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    // Expect: keyA v2, keyA v1, keyB v4, keyB v3
    ASSERT_TRUE(iter.valid());
    EXPECT_EQ(iter.entry().log_entry.key, keyA);
    EXPECT_EQ(iter.entry().log_entry.version(), 2u);

    ASSERT_TRUE(iter.next().ok());
    ASSERT_TRUE(iter.valid());
    EXPECT_EQ(iter.entry().log_entry.key, keyA);
    EXPECT_EQ(iter.entry().log_entry.version(), 1u);

    ASSERT_TRUE(iter.next().ok());
    ASSERT_TRUE(iter.valid());
    EXPECT_EQ(iter.entry().log_entry.key, keyB);
    EXPECT_EQ(iter.entry().log_entry.version(), 4u);

    ASSERT_TRUE(iter.next().ok());
    ASSERT_TRUE(iter.valid());
    EXPECT_EQ(iter.entry().log_entry.key, keyB);
    EXPECT_EQ(iter.entry().log_entry.version(), 3u);

    ASSERT_TRUE(iter.next().ok());
    EXPECT_FALSE(iter.valid());
}

// No visible entries → valid() false immediately.
TEST_F(VisibleVersionIteratorTest, IteratorEmpty) {
    size_t idx = createSegment(1, {{"key1", 1, "val1", false}});
    createSegment(2, {{"key1", 2, "val2", false}});
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 1);

    // Only snapshot at v2 → latest is v2(seg2), nothing visible in seg1.
    std::vector<uint64_t> snapshots = {2};
    auto iter = GCManager::getVisibleVersions(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    EXPECT_FALSE(iter.valid());
}
