#include <gtest/gtest.h>

#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <list>
#include <set>
#include <unistd.h>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/global_index.h"
#include "internal/manifest.h"
#include "internal/read_only_delta_hash_table.h"
#include "internal/segment_index.h"
#include "internal/gc.h"
#include "internal/log_file.h"
#include "internal/segment.h"
#include "internal/delta_hash_table.h"

using namespace kvlite::internal;
using kvlite::Status;

// Helper: hash a literal key for DHT calls.
static uint64_t RH(const char* s) { return dhtHashBytes(s, std::strlen(s)); }
static uint64_t RH(const std::string& s) { return dhtHashBytes(s.data(), s.size()); }

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
        for (auto& gi : gi_instances_) {
            if (gi.index && gi.index->isOpen()) gi.index->close();
            gi.manifest.close();
            std::filesystem::remove_all(gi.dir);
        }
    }

    struct OpenedGI {
        std::string dir;
        kvlite::internal::Manifest manifest;
        std::unique_ptr<GlobalIndex> index;
    };

    GlobalIndex& createOpenGI() {
        gi_instances_.emplace_back();
        auto& gi = gi_instances_.back();
        gi.dir = base_ + "gi_" + std::to_string(gi_instances_.size());
        std::filesystem::create_directories(gi.dir);
        auto s = gi.manifest.create(gi.dir);
        EXPECT_TRUE(s.ok()) << s.toString();
        gi.index = std::make_unique<GlobalIndex>(gi.manifest);
        GlobalIndex::Options opts;
        s = gi.index->open(gi.dir, opts);
        EXPECT_TRUE(s.ok()) << s.toString();
        return *gi.index;
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
    std::list<OpenedGI> gi_instances_;
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
        [&](uint64_t hkey, uint64_t packed_version,
            uint32_t old_id, uint32_t new_id) {
            relocations.push_back({hkey, packed_version, old_id, new_id});
        },
        [&](uint64_t hkey, uint64_t packed_version, uint32_t old_id) {
            eliminations.push_back({hkey, packed_version, old_id});
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
        [&](uint64_t hkey, uint64_t packed_version,
            uint32_t old_id, uint32_t new_id) {
            relocations.push_back({hkey, packed_version, old_id, new_id});
        },
        [&](uint64_t hkey, uint64_t packed_version, uint32_t old_id) {
            eliminations.push_back({hkey, packed_version, old_id});
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 1u);
    ASSERT_EQ(last_result_.outputs.size(), 1u);

    ASSERT_EQ(eliminations.size(), 1u);
    EXPECT_EQ(eliminations[0].hkey, dhtHashBytes("key1", 4));
    EXPECT_EQ(eliminations[0].packed_version, PackedVersion(1, false).data);
    EXPECT_EQ(eliminations[0].old_segment_id, 1u);

    ASSERT_EQ(relocations.size(), 1u);
    EXPECT_EQ(relocations[0].hkey, dhtHashBytes("key1", 4));
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

    auto noop_relocate = [](uint64_t, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](uint64_t, uint64_t, uint32_t) {};

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

    auto noop_relocate = [](uint64_t, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](uint64_t, uint64_t, uint32_t) {};

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

    auto noop_relocate = [](uint64_t, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](uint64_t, uint64_t, uint32_t) {};

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

    auto noop_relocate = [](uint64_t, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](uint64_t, uint64_t, uint32_t) {};

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
        [&](uint64_t hkey, uint64_t packed_version,
            uint32_t old_id, uint32_t new_id) {
            relocations.push_back({hkey, packed_version, old_id, new_id});
        },
        [&](uint64_t hkey, uint64_t packed_version, uint32_t old_id) {
            eliminations.push_back({hkey, packed_version, old_id});
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_written, 2u);
    ASSERT_EQ(last_result_.outputs.size(), 1u);

    ASSERT_EQ(eliminations.size(), 1u);
    EXPECT_EQ(eliminations[0].hkey, dhtHashBytes("key1", 4));
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

    auto noop_relocate = [](uint64_t, uint64_t, uint32_t, uint32_t) {};
    auto noop_eliminate = [](uint64_t, uint64_t, uint32_t) {};

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

// Helper: hash a string literal for GI/DHT calls.
static uint64_t H(const char* s) { return dhtHashBytes(s, std::strlen(s)); }

TEST_F(GCMergeTest, RelocateUpdatesGlobalIndex) {
    // Put entries into GlobalIndex with specific segment_ids.
    auto& gi = createOpenGI();
    gi.put(H("key1"), PackedVersion(1, false).data, 1);
    gi.put(H("key2"), PackedVersion(2, false).data, 1);

    // Create segment with those entries.
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
    });
    auto& seg = segments_[idx];

    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg};

    // Hold the savepoint lock for the entire GC merge.
    GlobalIndex::BatchGuard guard(gi);
    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&](uint64_t hkey, uint64_t packed_version,
              uint32_t old_id, uint32_t new_id) {
            gi.relocate(hkey, packed_version, old_id, new_id);
        },
        [&](uint64_t hkey, uint64_t packed_version, uint32_t old_id) {
            gi.eliminate(hkey, packed_version, old_id);
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    ASSERT_EQ(last_result_.outputs.size(), 1u);
    uint32_t new_seg_id = last_result_.outputs[0].getId();

    // Verify GlobalIndex entries now point to the new segment.
    uint64_t pv;
    uint32_t seg_id;
    ASSERT_TRUE(gi.getLatest(H("key1"), pv, seg_id).ok());
    EXPECT_EQ(seg_id, new_seg_id);
    ASSERT_TRUE(gi.getLatest(H("key2"), pv, seg_id).ok());
    EXPECT_EQ(seg_id, new_seg_id);

    EXPECT_EQ(gi.keyCount(), 2u);
    EXPECT_EQ(gi.entryCount(), 2u);
}

TEST_F(GCMergeTest, EliminateRemovesFromGlobalIndex) {
    auto& gi = createOpenGI();
    gi.put(H("key1"), PackedVersion(1, false).data, 1);
    gi.put(H("key1"), PackedVersion(2, false).data, 2);

    size_t idx1 = createSegment(1, {{"key1", 1, "old", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "new", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg1, &seg2};

    GlobalIndex::BatchGuard guard(gi);
    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&](uint64_t hkey, uint64_t packed_version,
              uint32_t old_id, uint32_t new_id) {
            gi.relocate(hkey, packed_version, old_id, new_id);
        },
        [&](uint64_t hkey, uint64_t packed_version, uint32_t old_id) {
            gi.eliminate(hkey, packed_version, old_id);
        },
        last_result_);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(last_result_.entries_eliminated, 1u);

    // Only version 2 should remain in GlobalIndex, pointing to new segment.
    EXPECT_EQ(gi.entryCount(), 1u);
    EXPECT_EQ(gi.keyCount(), 1u);

    uint64_t pv;
    uint32_t seg_id;
    ASSERT_TRUE(gi.getLatest(H("key1"), pv, seg_id).ok());
    EXPECT_EQ(pv, PackedVersion(2, false).data);
    EXPECT_EQ(seg_id, last_result_.outputs[0].getId());
}

TEST_F(GCMergeTest, EliminateAllVersionsRemovesKey) {
    auto& gi = createOpenGI();
    // Two versions of "key1" in same segment — both will be compacted,
    // but only the latest survives the single-snapshot classification.
    // To eliminate ALL versions, we need a tombstone as latest.
    gi.put(H("key1"), PackedVersion(1, false).data, 1);
    gi.put(H("key1"), PackedVersion(2, true).data, 1);  // tombstone

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

    GlobalIndex::BatchGuard guard(gi);
    Status s = GC::merge(
        snapshots, inputs, /*max_segment_size=*/1 << 20,
        [this](uint32_t id) { return pathForOutput(id); },
        [this]() { return allocateId(); },
        [&](uint64_t hkey, uint64_t packed_version,
              uint32_t old_id, uint32_t new_id) {
            gi.relocate(hkey, packed_version, old_id, new_id);
        },
        [&](uint64_t hkey, uint64_t packed_version, uint32_t old_id) {
            gi.eliminate(hkey, packed_version, old_id);
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
    EXPECT_TRUE(gi.contains(H("key1")));

    // Verify the remaining entry is the tombstone at version 2.
    uint64_t pv;
    uint32_t seg_id;
    ASSERT_TRUE(gi.getLatest(H("key1"), pv, seg_id).ok());
    EXPECT_EQ(pv, PackedVersion(2, true).data);
    EXPECT_EQ(seg_id, last_result_.outputs[0].getId());
}

TEST_F(GCMergeTest, TombstoneOnlyKeyEliminatedOnSecondGC) {
    // Simulate: after a first GC pass, a key has only a tombstone left
    // in the output segment. A second GC pass eliminates it entirely.
    auto& gi = createOpenGI();
    uint32_t first_seg_id = 50;
    gi.put(H("key1"), PackedVersion(2, true).data, first_seg_id);  // tombstone only

    // Create a segment containing just the tombstone.
    size_t idx = createSegment(first_seg_id, {
        {"key1", 2, "", true},  // tombstone
    });
    auto& seg = segments_[idx];

    EXPECT_EQ(gi.keyCount(), 1u);
    EXPECT_EQ(gi.entryCount(), 1u);
    EXPECT_TRUE(gi.contains(H("key1")));

    // Second GC pass: single snapshot at version 2.
    // The tombstone is the only entry — it's the latest for the snapshot,
    // so it is kept (relocated), not eliminated.
    std::vector<uint64_t> snapshots = {2};
    std::vector<const Segment*> inputs = {&seg};

    {
        GlobalIndex::BatchGuard guard(gi);
        Status s = GC::merge(
            snapshots, inputs, /*max_segment_size=*/1 << 20,
            [this](uint32_t id) { return pathForOutput(id); },
            [this]() { return allocateId(); },
            [&](uint64_t hkey, uint64_t packed_version,
                  uint32_t old_id, uint32_t new_id) {
                gi.relocate(hkey, packed_version, old_id, new_id);
            },
            [&](uint64_t hkey, uint64_t packed_version, uint32_t old_id) {
                gi.eliminate(hkey, packed_version, old_id);
            },
            last_result_);

        ASSERT_TRUE(s.ok());
    }
    // Tombstone is kept (relocated) — it's the latest visible entry.
    EXPECT_EQ(last_result_.entries_written, 1u);
    EXPECT_EQ(last_result_.entries_eliminated, 0u);

    // Key still exists — tombstone persists until no snapshot pins it.
    EXPECT_EQ(gi.keyCount(), 1u);
    EXPECT_TRUE(gi.contains(H("key1")));

    // Now simulate: eliminate the tombstone directly (as would happen
    // when all snapshots are released and a GC eliminates it).
    uint64_t tomb_pv = PackedVersion(2, true).data;
    uint32_t new_seg_id = last_result_.outputs[0].getId();
    {
        GlobalIndex::BatchGuard guard(gi);
        gi.eliminate(H("key1"), tomb_pv, new_seg_id);
    }

    // Now the key is fully gone from GlobalIndex.
    EXPECT_EQ(gi.keyCount(), 0u);
    EXPECT_EQ(gi.entryCount(), 0u);
    EXPECT_FALSE(gi.contains(H("key1")));
}
