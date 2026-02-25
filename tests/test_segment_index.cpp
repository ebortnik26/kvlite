#include <gtest/gtest.h>

#include <cstring>
#include <fcntl.h>
#include <set>
#include <unistd.h>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/lslot_codec.h"
#include "internal/read_only_delta_hash_table.h"
#include "internal/segment_index.h"
#include "internal/gc.h"
#include "internal/log_file.h"
#include "internal/segment.h"
#include "internal/delta_hash_table.h"

using namespace kvlite::internal;
using kvlite::Status;

// --- LSlotCodec Tests ---

TEST(LSlotCodec, EncodeDecodeEmpty) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;

    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);
    EXPECT_GT(end, 0u);

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    EXPECT_TRUE(decoded.entries.empty());
}

TEST(LSlotCodec, EncodeDecodeSingleEntry) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1234;
    entry.packed_versions = {1000};
    entry.ids = {5};
    contents.entries.push_back(entry);

    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 0x1234u);
    ASSERT_EQ(decoded.entries[0].packed_versions.size(), 1u);
    EXPECT_EQ(decoded.entries[0].packed_versions[0], 1000u);
    EXPECT_EQ(decoded.entries[0].ids[0], 5u);
}

TEST(LSlotCodec, EncodeDecodeMultiplePairs) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 42;
    entry.packed_versions = {3000, 2000, 1000};   // desc
    entry.ids = {30, 20, 10};                      // desc
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    ASSERT_EQ(decoded.entries[0].packed_versions.size(), 3u);
    EXPECT_EQ(decoded.entries[0].packed_versions[0], 3000u);
    EXPECT_EQ(decoded.entries[0].packed_versions[1], 2000u);
    EXPECT_EQ(decoded.entries[0].packed_versions[2], 1000u);
    EXPECT_EQ(decoded.entries[0].ids[0], 30u);
    EXPECT_EQ(decoded.entries[0].ids[1], 20u);
    EXPECT_EQ(decoded.entries[0].ids[2], 10u);
}

TEST(LSlotCodec, EncodeDecodeMultipleFingerprints) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;

    LSlotCodec::TrieEntry e1;
    e1.fingerprint = 10;
    e1.packed_versions = {500, 400};
    e1.ids = {50, 40};
    contents.entries.push_back(e1);

    LSlotCodec::TrieEntry e2;
    e2.fingerprint = 20;
    e2.packed_versions = {900};
    e2.ids = {90};
    contents.entries.push_back(e2);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 2u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 10u);
    EXPECT_EQ(decoded.entries[1].fingerprint, 20u);
    ASSERT_EQ(decoded.entries[0].packed_versions.size(), 2u);
    ASSERT_EQ(decoded.entries[1].packed_versions.size(), 1u);
}

TEST(LSlotCodec, BitsNeeded) {
    LSlotCodec::LSlotContents empty;
    EXPECT_EQ(LSlotCodec::bitsNeeded(empty, 39), 1u);

    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 1;
    entry.packed_versions = {100};
    entry.ids = {5};
    contents.entries.push_back(entry);

    size_t bits = LSlotCodec::bitsNeeded(contents, 39);
    // unary(1)=2 + fp=39 + unary(1)=2 + packed_version_raw=64 + id_raw=32 = 139
    EXPECT_EQ(bits, 139u);
}

TEST(LSlotCodec, BitOffsetAndTotalBits) {
    LSlotCodec codec(39);

    uint8_t buf[512] = {};
    size_t offset = 0;

    LSlotCodec::LSlotContents s0;
    LSlotCodec::TrieEntry e0;
    e0.fingerprint = 1;
    e0.packed_versions = {10};
    e0.ids = {1};
    s0.entries.push_back(e0);
    offset = codec.encode(buf, offset, s0);

    LSlotCodec::LSlotContents s1;  // empty
    offset = codec.encode(buf, offset, s1);

    LSlotCodec::LSlotContents s2;
    LSlotCodec::TrieEntry e2;
    e2.fingerprint = 2;
    e2.packed_versions = {20};
    e2.ids = {2};
    s2.entries.push_back(e2);
    offset = codec.encode(buf, offset, s2);

    EXPECT_EQ(codec.bitOffset(buf, 0), 0u);
    EXPECT_GT(codec.bitOffset(buf, 1), 0u);
    EXPECT_EQ(codec.totalBits(buf, 3), offset);
}

// --- ReadOnlyDeltaHashTable Tests ---

static ReadOnlyDeltaHashTable::Config testConfig() {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 512;
    return cfg;
}

TEST(ReadOnlyDeltaHashTable, AddAndFindFirst) {
    ReadOnlyDeltaHashTable dht(testConfig());

    dht.addEntry("hello", 100, 1);

    uint64_t pv;
    uint32_t id;
    EXPECT_TRUE(dht.findFirst("hello", pv, id));
    EXPECT_EQ(pv, 100u);
    EXPECT_EQ(id, 1u);
    EXPECT_EQ(dht.size(), 1u);
}

TEST(ReadOnlyDeltaHashTable, FindNonExistent) {
    ReadOnlyDeltaHashTable dht(testConfig());
    uint64_t pv;
    uint32_t id;
    EXPECT_FALSE(dht.findFirst("missing", pv, id));
}

TEST(ReadOnlyDeltaHashTable, AddMultipleEntries) {
    ReadOnlyDeltaHashTable dht(testConfig());

    dht.addEntry("key1", 100, 1);
    dht.addEntry("key1", 200, 2);
    dht.addEntry("key1", 300, 3);

    EXPECT_EQ(dht.size(), 3u);

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key1", packed_versions, ids));
    ASSERT_EQ(packed_versions.size(), 3u);
    EXPECT_EQ(packed_versions[0], 300u);
    EXPECT_EQ(packed_versions[1], 200u);
    EXPECT_EQ(packed_versions[2], 100u);
    EXPECT_EQ(ids[0], 3u);
    EXPECT_EQ(ids[1], 2u);
    EXPECT_EQ(ids[2], 1u);
}

TEST(ReadOnlyDeltaHashTable, FindFirstReturnsHighest) {
    ReadOnlyDeltaHashTable dht(testConfig());

    dht.addEntry("key1", 100, 1);
    dht.addEntry("key1", 300, 3);
    dht.addEntry("key1", 200, 2);

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key1", pv, id));
    EXPECT_EQ(pv, 300u);
    EXPECT_EQ(id, 3u);
}

TEST(ReadOnlyDeltaHashTable, Contains) {
    ReadOnlyDeltaHashTable dht(testConfig());

    EXPECT_FALSE(dht.contains("key1"));
    dht.addEntry("key1", 100, 1);
    EXPECT_TRUE(dht.contains("key1"));
}

TEST(ReadOnlyDeltaHashTable, Clear) {
    ReadOnlyDeltaHashTable dht(testConfig());

    for (int i = 0; i < 10; ++i) {
        dht.addEntry("key" + std::to_string(i),
                     static_cast<uint64_t>(i * 100 + 1),
                     static_cast<uint32_t>(i + 1));
    }
    EXPECT_EQ(dht.size(), 10u);

    dht.clear();
    EXPECT_EQ(dht.size(), 0u);

    for (int i = 0; i < 10; ++i) {
        EXPECT_FALSE(dht.contains("key" + std::to_string(i)));
    }
}

TEST(ReadOnlyDeltaHashTable, ForEach) {
    ReadOnlyDeltaHashTable dht(testConfig());

    dht.addEntry("a", 100, 1);
    dht.addEntry("b", 200, 2);
    dht.addEntry("c", 300, 3);

    std::set<uint32_t> all_ids;
    dht.forEach([&](uint64_t, uint64_t, uint32_t id) {
        all_ids.insert(id);
    });

    EXPECT_EQ(all_ids.size(), 3u);
    EXPECT_EQ(all_ids.count(1u), 1u);
    EXPECT_EQ(all_ids.count(2u), 1u);
    EXPECT_EQ(all_ids.count(3u), 1u);
}

TEST(ReadOnlyDeltaHashTable, ForEachGroup) {
    ReadOnlyDeltaHashTable dht(testConfig());

    dht.addEntry("a", 100, 1);
    dht.addEntry("a", 200, 2);
    dht.addEntry("b", 300, 3);

    size_t group_count = 0;
    dht.forEachGroup([&](uint64_t, const std::vector<uint64_t>& pvs,
                         const std::vector<uint32_t>& ids) {
        ++group_count;
        if (pvs.size() == 2) {
            EXPECT_EQ(pvs[0], 200u);
            EXPECT_EQ(pvs[1], 100u);
            EXPECT_EQ(ids[0], 2u);
            EXPECT_EQ(ids[1], 1u);
        } else {
            EXPECT_EQ(pvs.size(), 1u);
            EXPECT_EQ(pvs[0], 300u);
            EXPECT_EQ(ids[0], 3u);
        }
    });

    EXPECT_EQ(group_count, 2u);
}

TEST(ReadOnlyDeltaHashTable, ManyKeys) {
    ReadOnlyDeltaHashTable dht(testConfig());

    const int N = 200;
    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        dht.addEntry(key, static_cast<uint64_t>(i * 100 + 1),
                     static_cast<uint32_t>(i + 1));
    }

    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t id;
        ASSERT_TRUE(dht.findFirst(key, pv, id)) << "key not found: " << key;
        EXPECT_EQ(pv, static_cast<uint64_t>(i * 100 + 1));
        EXPECT_EQ(id, static_cast<uint32_t>(i + 1));
    }
}

TEST(ReadOnlyDeltaHashTable, AddEntryByHash) {
    ReadOnlyDeltaHashTable dht(testConfig());

    uint64_t hash = 0xDEADBEEF12345678ULL;
    dht.addEntryByHash(hash, 100, 1);

    size_t count = 0;
    dht.forEach([&](uint64_t, uint64_t pv, uint32_t id) {
        ++count;
        EXPECT_EQ(pv, 100u);
        EXPECT_EQ(id, 1u);
    });
    EXPECT_EQ(count, 1u);
}

// --- SegmentIndex Tests ---

TEST(SegmentIndex, PutAndGetLatest) {
    SegmentIndex index;

    index.put("key1", 100, 1);
    index.put("key1", 200, 2);
    index.put("key1", 300, 3);

    uint32_t off;
    uint64_t ver;
    EXPECT_TRUE(index.getLatest("key1", off, ver));
    EXPECT_EQ(off, 300u);
    EXPECT_EQ(ver, 3u);

    std::vector<uint32_t> offsets;
    std::vector<uint64_t> versions;
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

    uint32_t off;
    uint64_t ver;
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
    std::vector<uint32_t> offsets;
    std::vector<uint64_t> versions;
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
                  static_cast<uint64_t>(i + 1));
    }

    EXPECT_EQ(index.keyCount(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint32_t off;
        uint64_t ver;
        ASSERT_TRUE(index.getLatest(key, off, ver));
        EXPECT_EQ(off, static_cast<uint32_t>(i * 100));
        EXPECT_EQ(ver, static_cast<uint64_t>(i + 1));
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

    uint32_t off;
    uint64_t ver;
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
    uint32_t off;
    uint64_t ver;
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
        f.append(&byte, 1, dummy);
        f.close();

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

    EXPECT_FALSE(s.ok());
}

TEST_F(SegmentIndexSerializationTest, BadMagic) {
    {
        LogFile f;
        ASSERT_TRUE(f.create(path_).ok());
        uint32_t bad_magic = 0xDEADBEEF;
        uint32_t zeros[3] = {0, 0, 0};
        uint64_t dummy;
        f.append(&bad_magic, sizeof(bad_magic), dummy);
        f.append(zeros, sizeof(zeros), dummy);
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


// --- GC Merge Tests ---

class GCMergeTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_ = "/tmp/gc_test_" + std::to_string(getpid()) + "_";
        next_id_ = 100;
    }
    void TearDown() override {
        for (auto& seg : segments_) {
            seg.close();
        }
        for (auto& path : paths_) {
            ::unlink(path.c_str());
        }
        for (auto& out : last_result_.outputs) {
            out.close();
        }
        for (auto& path : output_paths_) {
            ::unlink(path.c_str());
        }
    }

    size_t createSegment(
        uint32_t segment_id,
        const std::vector<std::tuple<std::string, uint64_t, std::string, bool>>& entries) {
        std::string path = base_ + "input_" + std::to_string(segment_id) + ".data";
        paths_.push_back(path);
        segments_.emplace_back();
        size_t idx = segments_.size() - 1;
        auto& seg = segments_[idx];

        EXPECT_TRUE(seg.create(path, segment_id).ok());
        for (const auto& [key, version, value, tombstone] : entries) {
            EXPECT_TRUE(seg.put(key, version, value, tombstone).ok());
        }
        EXPECT_TRUE(seg.seal().ok());
        return idx;
    }

    std::string pathForOutput(uint32_t id) {
        std::string path = base_ + "output_" + std::to_string(id) + ".data";
        output_paths_.push_back(path);
        return path;
    }

    uint32_t allocateId() {
        return next_id_++;
    }

    std::vector<Segment> segments_;
    std::string base_;
    std::vector<std::string> paths_;
    std::vector<std::string> output_paths_;
    uint32_t next_id_ = 100;
    GC::Result last_result_;
};

TEST_F(GCMergeTest, MergeSingleSegmentAllVisible) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
        {"key3", 3, "val3", false},
    });
    auto& seg = segments_[idx];

    std::vector<uint64_t> snapshots = {3};
    std::vector<const Segment*> inputs = {&seg};

    std::vector<GC::Relocation> relocations;
    std::vector<GC::Elimination> eliminations;

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&](std::string_view key, uint64_t packed_version,
            uint32_t old_id, uint32_t new_id) {
            relocations.push_back({std::string(key), packed_version, old_id, new_id});
        },
        [&](std::string_view key, uint64_t packed_version, uint32_t old_id) {
            eliminations.push_back({std::string(key), packed_version, old_id});
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 3u);
    ASSERT_EQ(last_result_.outputs.size(), 1u);

    EXPECT_TRUE(eliminations.empty());
    EXPECT_EQ(relocations.size(), 3u);
    for (const auto& r : relocations) {
        EXPECT_EQ(r.old_segment_id, 1u);
        EXPECT_EQ(r.new_segment_id, last_result_.outputs[0].getId());
    }

    auto& out = last_result_.outputs[0];
    ASSERT_EQ(out.state(), Segment::State::kReadable);

    LogEntry entry;
    ASSERT_TRUE(out.getLatest("key1", entry).ok());
    EXPECT_EQ(entry.value, "val1");
    EXPECT_EQ(entry.version(), 1u);

    ASSERT_TRUE(out.getLatest("key2", entry).ok());
    EXPECT_EQ(entry.value, "val2");
    EXPECT_EQ(entry.version(), 2u);

    ASSERT_TRUE(out.getLatest("key3", entry).ok());
    EXPECT_EQ(entry.value, "val3");
    EXPECT_EQ(entry.version(), 3u);
}

TEST_F(GCMergeTest, MergeEliminatesInvisible) {
    size_t idx1 = createSegment(1, {{"key1", 1, "old", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "new", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg1, &seg2};

    std::vector<GC::Relocation> relocations;
    std::vector<GC::Elimination> eliminations;

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&](std::string_view key, uint64_t packed_version,
            uint32_t old_id, uint32_t new_id) {
            relocations.push_back({std::string(key), packed_version, old_id, new_id});
        },
        [&](std::string_view key, uint64_t packed_version, uint32_t old_id) {
            eliminations.push_back({std::string(key), packed_version, old_id});
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 1u);
    ASSERT_EQ(last_result_.outputs.size(), 1u);

    ASSERT_EQ(eliminations.size(), 1u);
    EXPECT_EQ(eliminations[0].key, "key1");
    EXPECT_EQ(eliminations[0].packed_version, PackedVersion(1, false).data);
    EXPECT_EQ(eliminations[0].old_segment_id, 1u);

    ASSERT_EQ(relocations.size(), 1u);
    EXPECT_EQ(relocations[0].key, "key1");
    EXPECT_EQ(relocations[0].packed_version, PackedVersion(2, false).data);
    EXPECT_EQ(relocations[0].old_segment_id, 2u);

    LogEntry entry;
    ASSERT_TRUE(last_result_.outputs[0].getLatest("key1", entry).ok());
    EXPECT_EQ(entry.value, "new");
    EXPECT_EQ(entry.version(), 2u);
}

TEST_F(GCMergeTest, MergePreservesOrder) {
    std::string keyA = "alpha";
    std::string keyB = "beta";
    uint64_t hashA = dhtHashBytes(keyA.data(), keyA.size());
    uint64_t hashB = dhtHashBytes(keyB.data(), keyB.size());
    if (hashA > hashB) {
        std::swap(keyA, keyB);
        std::swap(hashA, hashB);
    }

    size_t idx1 = createSegment(1, {{keyA, 2, "A_v2", false}});
    size_t idx2 = createSegment(2, {{keyB, 1, "B_v1", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg1, &seg2};

    auto noop_relocate = [](std::string_view, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](std::string_view, uint64_t, uint32_t) {};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        noop_relocate, noop_eliminate,
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 2u);
    ASSERT_EQ(last_result_.outputs.size(), 1u);

    auto& out = last_result_.outputs[0];
    LogEntry entry;
    ASSERT_TRUE(out.getLatest(keyA, entry).ok());
    EXPECT_EQ(entry.value, "A_v2");
    ASSERT_TRUE(out.getLatest(keyB, entry).ok());
    EXPECT_EQ(entry.value, "B_v1");
}

TEST_F(GCMergeTest, MergeSplitsOnSize) {
    std::string big_val(200, 'x');
    size_t idx = createSegment(1, {
        {"k1", 1, big_val, false},
        {"k2", 2, big_val, false},
        {"k3", 3, big_val, false},
        {"k4", 4, big_val, false},
    });
    auto& seg = segments_[idx];

    std::vector<uint64_t> snapshots = {4};
    std::vector<const Segment*> inputs = {&seg};

    auto noop_relocate = [](std::string_view, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](std::string_view, uint64_t, uint32_t) {};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/450,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        noop_relocate, noop_eliminate,
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 4u);
    EXPECT_GE(last_result_.outputs.size(), 2u);

    size_t found = 0;
    for (auto& out : last_result_.outputs) {
        ASSERT_EQ(out.state(), Segment::State::kReadable);
        LogEntry entry;
        if (out.getLatest("k1", entry).ok()) found++;
        if (out.getLatest("k2", entry).ok()) found++;
        if (out.getLatest("k3", entry).ok()) found++;
        if (out.getLatest("k4", entry).ok()) found++;
    }
    EXPECT_EQ(found, 4u);
}

TEST_F(GCMergeTest, MergeEmpty) {
    std::vector<uint64_t> snapshots = {1};
    std::vector<const Segment*> inputs;

    auto noop_relocate = [](std::string_view, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](std::string_view, uint64_t, uint32_t) {};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        noop_relocate, noop_eliminate,
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 0u);
    EXPECT_EQ(last_result_.outputs.size(), 0u);
}

TEST_F(GCMergeTest, MergeSnapshotPins) {
    size_t idx1 = createSegment(1, {{"key1", 1, "v1", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "v2", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<uint64_t> snapshots = {1, 2};
    std::vector<const Segment*> inputs = {&seg1, &seg2};

    auto noop_relocate = [](std::string_view, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](std::string_view, uint64_t, uint32_t) {};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        noop_relocate, noop_eliminate,
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 2u);
    ASSERT_EQ(last_result_.outputs.size(), 1u);

    auto& out = last_result_.outputs[0];
    std::vector<LogEntry> entries;
    ASSERT_TRUE(out.get("key1", entries).ok());
    ASSERT_EQ(entries.size(), 2u);
    std::set<uint64_t> versions;
    std::set<std::string> values;
    for (const auto& e : entries) {
        versions.insert(e.version());
        values.insert(e.value);
    }
    EXPECT_EQ(versions.count(1u), 1u);
    EXPECT_EQ(versions.count(2u), 1u);
    EXPECT_EQ(values.count("v1"), 1u);
    EXPECT_EQ(values.count("v2"), 1u);
}

TEST_F(GCMergeTest, MergeReleasesVersionsBelowOldestSnapshot) {
    size_t idx1 = createSegment(1, {{"key1", 1, "v1", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "v2", false}});
    size_t idx3 = createSegment(3, {{"key1", 3, "v3", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];
    auto& seg3 = segments_[idx3];

    std::vector<uint64_t> snapshots = {2, 3};
    std::vector<const Segment*> inputs = {&seg1, &seg2, &seg3};

    std::vector<GC::Relocation> relocations;
    std::vector<GC::Elimination> eliminations;

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&](std::string_view key, uint64_t packed_version,
            uint32_t old_id, uint32_t new_id) {
            relocations.push_back({std::string(key), packed_version, old_id, new_id});
        },
        [&](std::string_view key, uint64_t packed_version, uint32_t old_id) {
            eliminations.push_back({std::string(key), packed_version, old_id});
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 2u);
    ASSERT_EQ(last_result_.outputs.size(), 1u);

    ASSERT_EQ(eliminations.size(), 1u);
    EXPECT_EQ(eliminations[0].key, "key1");
    EXPECT_EQ(eliminations[0].packed_version, PackedVersion(1, false).data);
    EXPECT_EQ(eliminations[0].old_segment_id, 1u);

    EXPECT_EQ(relocations.size(), 2u);

    auto& out = last_result_.outputs[0];
    std::vector<LogEntry> entries;
    ASSERT_TRUE(out.get("key1", entries).ok());
    ASSERT_EQ(entries.size(), 2u);

    std::set<uint64_t> versions;
    for (const auto& e : entries) {
        versions.insert(e.version());
    }
    EXPECT_EQ(versions.count(1u), 0u);
    EXPECT_EQ(versions.count(2u), 1u);
    EXPECT_EQ(versions.count(3u), 1u);
}

TEST_F(GCMergeTest, MergeReleasesAllOldVersionsWhenNoSnapshot) {
    size_t idx1 = createSegment(1, {{"key1", 1, "v1", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "v2", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg1, &seg2};

    auto noop_relocate = [](std::string_view, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](std::string_view, uint64_t, uint32_t) {};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        noop_relocate, noop_eliminate,
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 1u);
    ASSERT_EQ(last_result_.outputs.size(), 1u);

    auto& out = last_result_.outputs[0];
    LogEntry entry;
    ASSERT_TRUE(out.getLatest("key1", entry).ok());
    EXPECT_EQ(entry.value, "v2");
    EXPECT_EQ(entry.version(), 2u);

    std::vector<LogEntry> entries;
    ASSERT_TRUE(out.get("key1", entries).ok());
    ASSERT_EQ(entries.size(), 1u);
}

// --- GC + GlobalIndex integration tests ---

#include "internal/global_index.h"

TEST_F(GCMergeTest, RelocateUpdatesGlobalIndex) {
    // Put entries into GlobalIndex with specific segment_ids.
    GlobalIndex gi;
    gi.put("key1", PackedVersion(1, false).data, 1);
    gi.put("key2", PackedVersion(2, false).data, 1);

    // Create segment with those entries.
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
    });
    auto& seg = segments_[idx];

    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&gi](std::string_view key, uint64_t packed_version,
              uint32_t old_id, uint32_t new_id) {
            gi.relocate(std::string(key), packed_version, old_id, new_id);
        },
        [&gi](std::string_view key, uint64_t packed_version, uint32_t old_id) {
            gi.eliminate(std::string(key), packed_version, old_id);
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    ASSERT_EQ(last_result_.outputs.size(), 1u);
    uint32_t new_seg_id = last_result_.outputs[0].getId();

    // Verify GlobalIndex entries now point to the new segment.
    uint64_t pv;
    uint32_t seg_id;
    ASSERT_TRUE(gi.getLatest("key1", pv, seg_id).ok());
    EXPECT_EQ(seg_id, new_seg_id);
    ASSERT_TRUE(gi.getLatest("key2", pv, seg_id).ok());
    EXPECT_EQ(seg_id, new_seg_id);

    EXPECT_EQ(gi.keyCount(), 2u);
    EXPECT_EQ(gi.entryCount(), 2u);
}

TEST_F(GCMergeTest, EliminateRemovesFromGlobalIndex) {
    GlobalIndex gi;
    gi.put("key1", PackedVersion(1, false).data, 1);
    gi.put("key1", PackedVersion(2, false).data, 2);

    size_t idx1 = createSegment(1, {{"key1", 1, "old", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "new", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg1, &seg2};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&gi](std::string_view key, uint64_t packed_version,
              uint32_t old_id, uint32_t new_id) {
            gi.relocate(std::string(key), packed_version, old_id, new_id);
        },
        [&gi](std::string_view key, uint64_t packed_version, uint32_t old_id) {
            gi.eliminate(std::string(key), packed_version, old_id);
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_eliminated, 1u);

    // Only version 2 should remain in GlobalIndex, pointing to new segment.
    EXPECT_EQ(gi.entryCount(), 1u);
    EXPECT_EQ(gi.keyCount(), 1u);

    uint64_t pv;
    uint32_t seg_id;
    ASSERT_TRUE(gi.getLatest("key1", pv, seg_id).ok());
    EXPECT_EQ(pv, PackedVersion(2, false).data);
    EXPECT_EQ(seg_id, last_result_.outputs[0].getId());
}

TEST_F(GCMergeTest, EliminateAllVersionsRemovesKey) {
    GlobalIndex gi;
    // Two versions of "key1" in same segment — both will be compacted,
    // but only the latest survives the single-snapshot classification.
    // To eliminate ALL versions, we need a tombstone as latest.
    gi.put("key1", PackedVersion(1, false).data, 1);
    gi.put("key1", PackedVersion(2, true).data, 1);  // tombstone

    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key1", 2, "", true},  // tombstone
    });
    auto& seg = segments_[idx];

    // Single snapshot at version 2 — classification keeps only the latest
    // entry per snapshot. The latest is the tombstone at version 2.
    // With only one snapshot, version 1 is eliminated.
    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&gi](std::string_view key, uint64_t packed_version,
              uint32_t old_id, uint32_t new_id) {
            gi.relocate(std::string(key), packed_version, old_id, new_id);
        },
        [&gi](std::string_view key, uint64_t packed_version, uint32_t old_id) {
            gi.eliminate(std::string(key), packed_version, old_id);
        },
        last_result_);

    ASSERT_TRUE(s.ok());

    // Version 1 was eliminated, version 2 (tombstone) was relocated.
    EXPECT_EQ(last_result_.entries_eliminated, 1u);
    EXPECT_EQ(last_result_.entries_written, 1u);

    // key1 still exists in GlobalIndex (tombstone entry remains).
    // This is correct: the tombstone must stay until the output segment
    // containing it is GC'ed in a future pass.
    EXPECT_EQ(gi.entryCount(), 1u);
    EXPECT_EQ(gi.keyCount(), 1u);
    EXPECT_TRUE(gi.contains("key1"));

    // Verify the remaining entry is the tombstone at version 2.
    uint64_t pv;
    uint32_t seg_id;
    ASSERT_TRUE(gi.getLatest("key1", pv, seg_id).ok());
    EXPECT_EQ(pv, PackedVersion(2, true).data);
    EXPECT_EQ(seg_id, last_result_.outputs[0].getId());
}

TEST_F(GCMergeTest, TombstoneOnlyKeyEliminatedOnSecondGC) {
    // Simulate: after a first GC pass, a key has only a tombstone left
    // in the output segment. A second GC pass eliminates it entirely.
    GlobalIndex gi;
    uint32_t first_seg_id = 50;
    gi.put("key1", PackedVersion(2, true).data, first_seg_id);  // tombstone only

    // Create a segment containing just the tombstone.
    size_t idx = createSegment(first_seg_id, {
        {"key1", 2, "", true},  // tombstone
    });
    auto& seg = segments_[idx];

    EXPECT_EQ(gi.keyCount(), 1u);
    EXPECT_EQ(gi.entryCount(), 1u);
    EXPECT_TRUE(gi.contains("key1"));

    // Second GC pass: single snapshot at version 2.
    // The tombstone is the only entry — it's the latest for the snapshot,
    // so it is kept (relocated), not eliminated.
    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg};

    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&gi](std::string_view key, uint64_t packed_version,
              uint32_t old_id, uint32_t new_id) {
            gi.relocate(std::string(key), packed_version, old_id, new_id);
        },
        [&gi](std::string_view key, uint64_t packed_version, uint32_t old_id) {
            gi.eliminate(std::string(key), packed_version, old_id);
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    // Tombstone is kept (relocated) — it's the latest visible entry.
    EXPECT_EQ(last_result_.entries_written, 1u);
    EXPECT_EQ(last_result_.entries_eliminated, 0u);

    // Key still exists — tombstone persists until no snapshot pins it.
    EXPECT_EQ(gi.keyCount(), 1u);
    EXPECT_TRUE(gi.contains("key1"));

    // Now simulate: eliminate the tombstone directly (as would happen
    // when all snapshots are released and a GC eliminates it).
    uint64_t tomb_pv = PackedVersion(2, true).data;
    uint32_t new_seg_id = last_result_.outputs[0].getId();
    gi.eliminate("key1", tomb_pv, new_seg_id);

    // Now the key is fully gone from GlobalIndex.
    EXPECT_EQ(gi.keyCount(), 0u);
    EXPECT_EQ(gi.entryCount(), 0u);
    EXPECT_FALSE(gi.contains("key1"));
}
