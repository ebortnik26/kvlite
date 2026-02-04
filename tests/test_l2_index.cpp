#include <gtest/gtest.h>

#include <cstring>
#include <fcntl.h>
#include <set>
#include <unistd.h>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/l2_lslot_codec.h"
#include "internal/l2_delta_hash_table.h"
#include "internal/l2_index.h"
#include "internal/log_file.h"

using namespace kvlite::internal;
using kvlite::Status;

// --- L2LSlotCodec Tests ---

TEST(L2LSlotCodec, EncodeDecodeEmpty) {
    L2LSlotCodec codec(39);
    L2LSlotCodec::LSlotContents contents;

    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);
    EXPECT_GT(end, 0u);

    size_t decoded_end;
    L2LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    EXPECT_TRUE(decoded.entries.empty());
}

TEST(L2LSlotCodec, EncodeDecodeSingleEntry) {
    L2LSlotCodec codec(39);
    L2LSlotCodec::LSlotContents contents;
    L2LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1234;
    entry.offsets = {1000};
    entry.versions = {5};
    contents.entries.push_back(entry);

    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    L2LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 0x1234u);
    ASSERT_EQ(decoded.entries[0].offsets.size(), 1u);
    EXPECT_EQ(decoded.entries[0].offsets[0], 1000u);
    EXPECT_EQ(decoded.entries[0].versions[0], 5u);
}

TEST(L2LSlotCodec, EncodeDecodeMultiplePairs) {
    L2LSlotCodec codec(39);
    L2LSlotCodec::LSlotContents contents;
    L2LSlotCodec::TrieEntry entry;
    entry.fingerprint = 42;
    entry.offsets = {3000, 2000, 1000};   // desc
    entry.versions = {30, 20, 10};        // desc
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    L2LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
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

TEST(L2LSlotCodec, EncodeDecodeMultipleFingerprints) {
    L2LSlotCodec codec(39);
    L2LSlotCodec::LSlotContents contents;

    L2LSlotCodec::TrieEntry e1;
    e1.fingerprint = 10;
    e1.offsets = {500, 400};
    e1.versions = {50, 40};
    contents.entries.push_back(e1);

    L2LSlotCodec::TrieEntry e2;
    e2.fingerprint = 20;
    e2.offsets = {900};
    e2.versions = {90};
    contents.entries.push_back(e2);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    L2LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 2u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 10u);
    EXPECT_EQ(decoded.entries[1].fingerprint, 20u);
    ASSERT_EQ(decoded.entries[0].offsets.size(), 2u);
    ASSERT_EQ(decoded.entries[1].offsets.size(), 1u);
}

TEST(L2LSlotCodec, BitsNeeded) {
    L2LSlotCodec::LSlotContents empty;
    EXPECT_EQ(L2LSlotCodec::bitsNeeded(empty, 39), 1u);

    L2LSlotCodec::LSlotContents contents;
    L2LSlotCodec::TrieEntry entry;
    entry.fingerprint = 1;
    entry.offsets = {100};
    entry.versions = {5};
    contents.entries.push_back(entry);

    size_t bits = L2LSlotCodec::bitsNeeded(contents, 39);
    // unary(1)=2 + fp=39 + unary(1)=2 + offset_raw=32 + version_raw=32 = 107
    EXPECT_EQ(bits, 107u);
}

TEST(L2LSlotCodec, BitOffsetAndTotalBits) {
    L2LSlotCodec codec(39);

    uint8_t buf[512] = {};
    size_t offset = 0;

    L2LSlotCodec::LSlotContents s0;
    L2LSlotCodec::TrieEntry e0;
    e0.fingerprint = 1;
    e0.offsets = {10};
    e0.versions = {1};
    s0.entries.push_back(e0);
    offset = codec.encode(buf, offset, s0);

    L2LSlotCodec::LSlotContents s1;  // empty
    offset = codec.encode(buf, offset, s1);

    L2LSlotCodec::LSlotContents s2;
    L2LSlotCodec::TrieEntry e2;
    e2.fingerprint = 2;
    e2.offsets = {20};
    e2.versions = {2};
    s2.entries.push_back(e2);
    offset = codec.encode(buf, offset, s2);

    EXPECT_EQ(codec.bitOffset(buf, 0), 0u);
    EXPECT_GT(codec.bitOffset(buf, 1), 0u);
    EXPECT_EQ(codec.totalBits(buf, 3), offset);
}

// --- L2DeltaHashTable Tests ---

static L2DeltaHashTable::Config testConfig() {
    L2DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 512;
    return cfg;
}

TEST(L2DeltaHashTable, AddAndFindFirst) {
    L2DeltaHashTable dht(testConfig());

    dht.addEntry("hello", 100, 1);

    uint32_t off, ver;
    EXPECT_TRUE(dht.findFirst("hello", off, ver));
    EXPECT_EQ(off, 100u);
    EXPECT_EQ(ver, 1u);
    EXPECT_EQ(dht.size(), 1u);
}

TEST(L2DeltaHashTable, FindNonExistent) {
    L2DeltaHashTable dht(testConfig());
    uint32_t off, ver;
    EXPECT_FALSE(dht.findFirst("missing", off, ver));
}

TEST(L2DeltaHashTable, AddMultipleEntries) {
    L2DeltaHashTable dht(testConfig());

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

TEST(L2DeltaHashTable, FindFirstReturnsHighest) {
    L2DeltaHashTable dht(testConfig());

    dht.addEntry("key1", 100, 1);
    dht.addEntry("key1", 300, 3);
    dht.addEntry("key1", 200, 2);

    uint32_t off, ver;
    ASSERT_TRUE(dht.findFirst("key1", off, ver));
    EXPECT_EQ(off, 300u);
    EXPECT_EQ(ver, 3u);
}

TEST(L2DeltaHashTable, Contains) {
    L2DeltaHashTable dht(testConfig());

    EXPECT_FALSE(dht.contains("key1"));
    dht.addEntry("key1", 100, 1);
    EXPECT_TRUE(dht.contains("key1"));
}

TEST(L2DeltaHashTable, Clear) {
    L2DeltaHashTable dht(testConfig());

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

TEST(L2DeltaHashTable, ForEach) {
    L2DeltaHashTable dht(testConfig());

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

TEST(L2DeltaHashTable, ForEachGroup) {
    L2DeltaHashTable dht(testConfig());

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

TEST(L2DeltaHashTable, ManyKeys) {
    L2DeltaHashTable dht(testConfig());

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

TEST(L2DeltaHashTable, AddEntryByHash) {
    L2DeltaHashTable dht(testConfig());

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

// --- L2Index Tests ---

TEST(L2Index, PutAndGetLatest) {
    L2Index index;

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

TEST(L2Index, GetLatest) {
    L2Index index;
    index.put("key1", 100, 1);
    index.put("key1", 200, 2);

    uint32_t off, ver;
    EXPECT_TRUE(index.getLatest("key1", off, ver));
    EXPECT_EQ(off, 200u);
    EXPECT_EQ(ver, 2u);

    EXPECT_FALSE(index.getLatest("missing", off, ver));
}

TEST(L2Index, Contains) {
    L2Index index;
    EXPECT_FALSE(index.contains("key1"));
    index.put("key1", 100, 1);
    EXPECT_TRUE(index.contains("key1"));
}

TEST(L2Index, GetNonExistent) {
    L2Index index;
    std::vector<uint32_t> offsets, versions;
    EXPECT_FALSE(index.get("missing", offsets, versions));
}

TEST(L2Index, Clear) {
    L2Index index;
    for (int i = 0; i < 50; ++i) {
        index.put("key" + std::to_string(i), i * 100, i);
    }
    EXPECT_EQ(index.keyCount(), 50u);

    index.clear();
    EXPECT_EQ(index.keyCount(), 0u);
    EXPECT_EQ(index.entryCount(), 0u);
}

TEST(L2Index, LargeScale) {
    L2Index index;
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

// --- L2Index Serialization Tests ---

class L2IndexSerializationTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = "/tmp/l2_index_test_" + std::to_string(getpid()) + ".idx";
    }
    void TearDown() override {
        ::unlink(path_.c_str());
    }
    std::string path_;
};

TEST_F(L2IndexSerializationTest, EmptyRoundTrip) {
    L2Index src;

    LogFile wf;
    ASSERT_TRUE(wf.create(path_).ok());
    ASSERT_TRUE(src.writeTo(wf).ok());
    wf.close();

    L2Index dst;
    LogFile rf;
    ASSERT_TRUE(rf.open(path_).ok());
    ASSERT_TRUE(dst.readFrom(rf).ok());
    rf.close();

    EXPECT_EQ(dst.entryCount(), 0u);
    EXPECT_EQ(dst.keyCount(), 0u);
}

TEST_F(L2IndexSerializationTest, SingleEntryRoundTrip) {
    L2Index src;
    src.put("hello", 100, 1);

    LogFile wf;
    ASSERT_TRUE(wf.create(path_).ok());
    ASSERT_TRUE(src.writeTo(wf).ok());
    wf.close();

    L2Index dst;
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

TEST_F(L2IndexSerializationTest, MultiEntryRoundTrip) {
    L2Index src;
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

    L2Index dst;
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

TEST_F(L2IndexSerializationTest, CorruptedChecksum) {
    L2Index src;
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

    L2Index dst;
    LogFile rf;
    ASSERT_TRUE(rf.open(path_).ok());
    Status s = dst.readFrom(rf);
    rf.close();

    // Could be checksum mismatch or a read error due to size change.
    EXPECT_FALSE(s.ok());
}

TEST_F(L2IndexSerializationTest, BadMagic) {
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

    L2Index dst;
    LogFile rf;
    ASSERT_TRUE(rf.open(path_).ok());
    Status s = dst.readFrom(rf);
    rf.close();

    EXPECT_TRUE(s.isCorruption());
}
