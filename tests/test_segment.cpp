#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "internal/segment.h"
#include "internal/log_entry.h"

using kvlite::Status;
using kvlite::internal::LogEntry;
using kvlite::internal::PackedVersion;
using kvlite::internal::Segment;

namespace {

class SegmentTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_ = ::testing::TempDir() + "/seg_test_" +
                std::to_string(reinterpret_cast<uintptr_t>(this));
        data_path_ = base_ + ".data";
        idx_path_ = base_ + ".idx";
    }

    void TearDown() override {
        if (seg_.isOpen()) seg_.close();
        std::remove(data_path_.c_str());
        std::remove(idx_path_.c_str());
    }

    // Write a LogEntry into the segment and record it in the index.
    // Returns the offset where the entry was written.
    uint64_t writeEntry(const std::string& key, uint64_t version,
                        const std::string& value, bool tombstone) {
        PackedVersion pv(version, tombstone);
        uint32_t key_len = static_cast<uint32_t>(key.size());
        uint32_t val_len = static_cast<uint32_t>(value.size());
        size_t entry_size = LogEntry::kHeaderSize + key_len + val_len +
                            LogEntry::kChecksumSize;

        std::vector<uint8_t> buf(entry_size);
        uint8_t* p = buf.data();
        std::memcpy(p, &pv.data, 8);   p += 8;
        std::memcpy(p, &key_len, 4);   p += 4;
        std::memcpy(p, &val_len, 4);   p += 4;
        std::memcpy(p, key.data(), key_len);   p += key_len;
        std::memcpy(p, value.data(), val_len);  p += val_len;
        // CRC placeholder (not verified in these tests).
        uint32_t crc = 0;
        std::memcpy(p, &crc, 4);

        uint64_t offset;
        Status s = seg_.append(buf.data(), entry_size, offset);
        EXPECT_TRUE(s.ok());
        seg_.addIndex(key, static_cast<uint32_t>(offset),
                      static_cast<uint32_t>(version));
        return offset;
    }

    // Read back key and version from a raw offset.
    void readBack(Segment& seg, uint64_t offset,
                  std::string& key, uint64_t& version) {
        uint8_t hdr[LogEntry::kHeaderSize];
        ASSERT_TRUE(seg.readAt(offset, hdr, LogEntry::kHeaderSize).ok());

        uint64_t pv_data;
        uint32_t kl;
        std::memcpy(&pv_data, hdr, 8);
        std::memcpy(&kl, hdr + 8, 4);

        version = PackedVersion(pv_data).version();
        key.resize(kl);
        if (kl > 0) {
            ASSERT_TRUE(seg.readAt(offset + LogEntry::kHeaderSize,
                                   key.data(), kl).ok());
        }
    }

    std::string base_;
    std::string data_path_;
    std::string idx_path_;
    Segment seg_;
};

}  // namespace

// --- Lifecycle tests ---

TEST_F(SegmentTest, DefaultState) {
    EXPECT_FALSE(seg_.isOpen());
    EXPECT_EQ(seg_.size(), 0u);
    EXPECT_EQ(seg_.entryCount(), 0u);
    EXPECT_EQ(seg_.keyCount(), 0u);
}

TEST_F(SegmentTest, CreateOpensFile) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    EXPECT_TRUE(seg_.isOpen());
    EXPECT_EQ(seg_.size(), 0u);
}

TEST_F(SegmentTest, CloseAfterCreate) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    ASSERT_TRUE(seg_.close().ok());
    EXPECT_FALSE(seg_.isOpen());
}

TEST_F(SegmentTest, CreateNonExistentDir) {
    Status s = seg_.create("/nonexistent_dir_xyz/file.data");
    EXPECT_FALSE(s.ok());
    EXPECT_FALSE(seg_.isOpen());
}

// --- Write + read tests ---

TEST_F(SegmentTest, AppendAndReadAt) {
    ASSERT_TRUE(seg_.create(data_path_).ok());

    const char data[] = "hello";
    uint64_t offset;
    ASSERT_TRUE(seg_.append(data, 5, offset).ok());
    EXPECT_EQ(offset, 0u);
    EXPECT_EQ(seg_.size(), 5u);

    char buf[5] = {};
    ASSERT_TRUE(seg_.readAt(0, buf, 5).ok());
    EXPECT_EQ(std::string(buf, 5), "hello");
}

TEST_F(SegmentTest, MultipleAppends) {
    ASSERT_TRUE(seg_.create(data_path_).ok());

    uint64_t off1, off2;
    ASSERT_TRUE(seg_.append("aaa", 3, off1).ok());
    ASSERT_TRUE(seg_.append("bbb", 3, off2).ok());
    EXPECT_EQ(off1, 0u);
    EXPECT_EQ(off2, 3u);
    EXPECT_EQ(seg_.size(), 6u);
}

// --- Index tests ---

TEST_F(SegmentTest, AddIndexAndQuery) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    writeEntry("key1", 1, "val1", false);
    writeEntry("key1", 2, "val2", false);
    writeEntry("key2", 5, "val5", true);

    EXPECT_EQ(seg_.entryCount(), 3u);
    EXPECT_EQ(seg_.keyCount(), 2u);

    EXPECT_TRUE(seg_.contains("key1"));
    EXPECT_TRUE(seg_.contains("key2"));
    EXPECT_FALSE(seg_.contains("key3"));

    uint32_t off, ver;
    ASSERT_TRUE(seg_.getLatest("key1", off, ver));
    EXPECT_EQ(ver, 2u);

    ASSERT_TRUE(seg_.getLatest("key2", off, ver));
    EXPECT_EQ(ver, 5u);
}

TEST_F(SegmentTest, GetAllVersions) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    writeEntry("k", 10, "a", false);
    writeEntry("k", 20, "b", false);
    writeEntry("k", 30, "c", false);

    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(seg_.get("k", offsets, versions));
    ASSERT_EQ(versions.size(), 3u);

    // Verify each offset points to the correct data.
    for (size_t i = 0; i < offsets.size(); ++i) {
        std::string key;
        uint64_t ver;
        readBack(seg_, offsets[i], key, ver);
        EXPECT_EQ(key, "k");
        EXPECT_EQ(ver, versions[i]);
    }
}

TEST_F(SegmentTest, GetByUpperBound) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    writeEntry("k", 10, "a", false);
    writeEntry("k", 20, "b", false);
    writeEntry("k", 30, "c", false);

    uint64_t off, ver;
    ASSERT_TRUE(seg_.get("k", 25u, off, ver));
    EXPECT_EQ(ver, 20u);

    ASSERT_TRUE(seg_.get("k", 10u, off, ver));
    EXPECT_EQ(ver, 10u);

    EXPECT_FALSE(seg_.get("k", 5u, off, ver));
}

// --- Save + load round-trip ---

TEST_F(SegmentTest, SaveAndLoadEmpty) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    ASSERT_TRUE(seg_.saveIndex(idx_path_).ok());
    seg_.close();

    Segment loaded;
    ASSERT_TRUE(loaded.load(data_path_, idx_path_).ok());
    EXPECT_TRUE(loaded.isOpen());
    EXPECT_EQ(loaded.size(), 0u);
    EXPECT_EQ(loaded.entryCount(), 0u);
    loaded.close();
}

TEST_F(SegmentTest, SaveAndLoadRoundTrip) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    writeEntry("alpha", 1, "v1", false);
    writeEntry("alpha", 2, "v2", false);
    writeEntry("beta", 10, "vb", true);

    ASSERT_TRUE(seg_.saveIndex(idx_path_).ok());
    uint64_t original_size = seg_.size();
    seg_.close();

    Segment loaded;
    ASSERT_TRUE(loaded.load(data_path_, idx_path_).ok());

    EXPECT_EQ(loaded.size(), original_size);
    EXPECT_EQ(loaded.entryCount(), 3u);
    EXPECT_EQ(loaded.keyCount(), 2u);

    // Verify index queries.
    uint32_t off, ver;
    ASSERT_TRUE(loaded.getLatest("alpha", off, ver));
    EXPECT_EQ(ver, 2u);

    ASSERT_TRUE(loaded.getLatest("beta", off, ver));
    EXPECT_EQ(ver, 10u);

    // Verify data readable via loaded segment.
    std::string key;
    uint64_t v;
    readBack(loaded, off, key, v);
    EXPECT_EQ(key, "beta");
    EXPECT_EQ(v, 10u);

    // Verify get with multiple versions.
    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(loaded.get("alpha", offsets, versions));
    ASSERT_EQ(offsets.size(), 2u);
    for (size_t i = 0; i < offsets.size(); ++i) {
        readBack(loaded, offsets[i], key, v);
        EXPECT_EQ(key, "alpha");
        EXPECT_EQ(v, versions[i]);
    }

    loaded.close();
}

TEST_F(SegmentTest, LoadMissingDataFile) {
    Segment loaded;
    Status s = loaded.load("/nonexistent.data", idx_path_);
    EXPECT_FALSE(s.ok());
    EXPECT_FALSE(loaded.isOpen());
}

TEST_F(SegmentTest, LoadMissingIndexFile) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    seg_.close();

    Segment loaded;
    Status s = loaded.load(data_path_, "/nonexistent.idx");
    EXPECT_FALSE(s.ok());
    EXPECT_FALSE(loaded.isOpen());
}

// --- Move semantics ---

TEST_F(SegmentTest, MoveConstruct) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    writeEntry("k", 1, "v", false);

    Segment moved(std::move(seg_));
    EXPECT_TRUE(moved.isOpen());
    EXPECT_EQ(moved.entryCount(), 1u);

    uint32_t off, ver;
    ASSERT_TRUE(moved.getLatest("k", off, ver));
    EXPECT_EQ(ver, 1u);

    moved.close();
}

TEST_F(SegmentTest, MoveAssign) {
    ASSERT_TRUE(seg_.create(data_path_).ok());
    writeEntry("k", 1, "v", false);

    Segment other;
    other = std::move(seg_);
    EXPECT_TRUE(other.isOpen());
    EXPECT_EQ(other.entryCount(), 1u);

    uint32_t off, ver;
    ASSERT_TRUE(other.getLatest("k", off, ver));
    EXPECT_EQ(ver, 1u);

    other.close();
}
