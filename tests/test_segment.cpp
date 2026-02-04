#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <unistd.h>
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
        path_ = ::testing::TempDir() + "/seg_test_" +
                std::to_string(reinterpret_cast<uintptr_t>(this)) + ".data";
    }

    void TearDown() override {
        if (seg_.isOpen()) seg_.close();
        std::remove(path_.c_str());
    }

    // Write a LogEntry into the segment and record it in the index.
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
        uint32_t crc = 0;
        std::memcpy(p, &crc, 4);

        uint64_t offset;
        EXPECT_TRUE(seg_.append(buf.data(), entry_size, offset).ok());
        EXPECT_TRUE(seg_.addIndex(key, static_cast<uint32_t>(offset),
                                  static_cast<uint32_t>(version)).ok());
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

    std::string path_;
    Segment seg_;
};

}  // namespace

// --- State machine tests ---

TEST_F(SegmentTest, DefaultState) {
    EXPECT_EQ(seg_.state(), Segment::State::kClosed);
    EXPECT_FALSE(seg_.isOpen());
    EXPECT_EQ(seg_.dataSize(), 0u);
    EXPECT_EQ(seg_.entryCount(), 0u);
    EXPECT_EQ(seg_.keyCount(), 0u);
}

TEST_F(SegmentTest, CreateTransitionsToWriting) {
    ASSERT_TRUE(seg_.create(path_).ok());
    EXPECT_EQ(seg_.state(), Segment::State::kWriting);
    EXPECT_TRUE(seg_.isOpen());
}

TEST_F(SegmentTest, SealTransitionsToReadable) {
    ASSERT_TRUE(seg_.create(path_).ok());
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_EQ(seg_.state(), Segment::State::kReadable);
}

TEST_F(SegmentTest, OpenTransitionsToReadable) {
    ASSERT_TRUE(seg_.create(path_).ok());
    ASSERT_TRUE(seg_.seal().ok());
    seg_.close();

    Segment loaded;
    ASSERT_TRUE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.state(), Segment::State::kReadable);
    loaded.close();
}

TEST_F(SegmentTest, CloseTransitionsToClosed) {
    ASSERT_TRUE(seg_.create(path_).ok());
    seg_.close();
    EXPECT_EQ(seg_.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, CloseSealedTransitionsToClosed) {
    ASSERT_TRUE(seg_.create(path_).ok());
    ASSERT_TRUE(seg_.seal().ok());
    seg_.close();
    EXPECT_EQ(seg_.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, CloseWhenAlreadyClosed) {
    EXPECT_TRUE(seg_.close().ok());
}

// --- State violations ---

TEST_F(SegmentTest, CreateWhileWritingFails) {
    ASSERT_TRUE(seg_.create(path_).ok());
    EXPECT_FALSE(seg_.create(path_).ok());
}

TEST_F(SegmentTest, CreateWhileReadableFails) {
    ASSERT_TRUE(seg_.create(path_).ok());
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_FALSE(seg_.create(path_).ok());
}

TEST_F(SegmentTest, OpenWhileWritingFails) {
    ASSERT_TRUE(seg_.create(path_).ok());
    EXPECT_FALSE(seg_.open(path_).ok());
}

TEST_F(SegmentTest, SealWhileClosedFails) {
    EXPECT_FALSE(seg_.seal().ok());
}

TEST_F(SegmentTest, SealWhileReadableFails) {
    ASSERT_TRUE(seg_.create(path_).ok());
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_FALSE(seg_.seal().ok());
}

TEST_F(SegmentTest, AppendWhileClosedFails) {
    uint64_t off;
    EXPECT_FALSE(seg_.append("x", 1, off).ok());
}

TEST_F(SegmentTest, AppendWhileReadableFails) {
    ASSERT_TRUE(seg_.create(path_).ok());
    ASSERT_TRUE(seg_.seal().ok());
    uint64_t off;
    EXPECT_FALSE(seg_.append("x", 1, off).ok());
}

TEST_F(SegmentTest, AddIndexWhileReadableFails) {
    ASSERT_TRUE(seg_.create(path_).ok());
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_FALSE(seg_.addIndex("k", 0, 1).ok());
}

TEST_F(SegmentTest, ReadAtWhileWritingFails) {
    ASSERT_TRUE(seg_.create(path_).ok());
    uint64_t off;
    ASSERT_TRUE(seg_.append("hello", 5, off).ok());
    char buf[5];
    EXPECT_FALSE(seg_.readAt(0, buf, 5).ok());
}

TEST_F(SegmentTest, ReadAtWhileClosedFails) {
    char buf[1];
    EXPECT_FALSE(seg_.readAt(0, buf, 1).ok());
}

TEST_F(SegmentTest, QueriesWhileWritingReturnFalse) {
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("k", 1, "v", false);

    uint32_t off, ver;
    EXPECT_FALSE(seg_.getLatest("k", off, ver));
    EXPECT_FALSE(seg_.contains("k"));

    std::vector<uint32_t> offsets, versions;
    EXPECT_FALSE(seg_.get("k", offsets, versions));

    uint64_t off64, ver64;
    EXPECT_FALSE(seg_.get("k", 100u, off64, ver64));
}

// --- Create error ---

TEST_F(SegmentTest, CreateNonExistentDir) {
    Status s = seg_.create("/nonexistent_dir_xyz/file.data");
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(seg_.state(), Segment::State::kClosed);
}

// --- Write tests ---

TEST_F(SegmentTest, AppendAndDataSize) {
    ASSERT_TRUE(seg_.create(path_).ok());

    uint64_t offset;
    ASSERT_TRUE(seg_.append("hello", 5, offset).ok());
    EXPECT_EQ(offset, 0u);
    EXPECT_EQ(seg_.dataSize(), 5u);
}

TEST_F(SegmentTest, MultipleAppends) {
    ASSERT_TRUE(seg_.create(path_).ok());

    uint64_t off1, off2;
    ASSERT_TRUE(seg_.append("aaa", 3, off1).ok());
    ASSERT_TRUE(seg_.append("bbb", 3, off2).ok());
    EXPECT_EQ(off1, 0u);
    EXPECT_EQ(off2, 3u);
    EXPECT_EQ(seg_.dataSize(), 6u);
}

// --- Seal + read tests ---

TEST_F(SegmentTest, SealThenReadAt) {
    ASSERT_TRUE(seg_.create(path_).ok());
    uint64_t off;
    ASSERT_TRUE(seg_.append("hello", 5, off).ok());
    ASSERT_TRUE(seg_.seal().ok());

    char buf[5] = {};
    ASSERT_TRUE(seg_.readAt(0, buf, 5).ok());
    EXPECT_EQ(std::string(buf, 5), "hello");
}

TEST_F(SegmentTest, SealThenQueryIndex) {
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("key1", 1, "val1", false);
    writeEntry("key1", 2, "val2", false);
    writeEntry("key2", 5, "val5", true);

    // Stats work in writing state.
    EXPECT_EQ(seg_.entryCount(), 3u);
    EXPECT_EQ(seg_.keyCount(), 2u);

    ASSERT_TRUE(seg_.seal().ok());

    // Queries work after seal.
    EXPECT_TRUE(seg_.contains("key1"));
    EXPECT_TRUE(seg_.contains("key2"));
    EXPECT_FALSE(seg_.contains("key3"));

    uint32_t off, ver;
    ASSERT_TRUE(seg_.getLatest("key1", off, ver));
    EXPECT_EQ(ver, 2u);

    ASSERT_TRUE(seg_.getLatest("key2", off, ver));
    EXPECT_EQ(ver, 5u);
}

TEST_F(SegmentTest, GetAllVersionsAfterSeal) {
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("k", 10, "a", false);
    writeEntry("k", 20, "b", false);
    writeEntry("k", 30, "c", false);
    ASSERT_TRUE(seg_.seal().ok());

    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(seg_.get("k", offsets, versions));
    ASSERT_EQ(versions.size(), 3u);

    for (size_t i = 0; i < offsets.size(); ++i) {
        std::string key;
        uint64_t ver;
        readBack(seg_, offsets[i], key, ver);
        EXPECT_EQ(key, "k");
        EXPECT_EQ(ver, versions[i]);
    }
}

TEST_F(SegmentTest, GetByUpperBoundAfterSeal) {
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("k", 10, "a", false);
    writeEntry("k", 20, "b", false);
    writeEntry("k", 30, "c", false);
    ASSERT_TRUE(seg_.seal().ok());

    uint64_t off, ver;
    ASSERT_TRUE(seg_.get("k", 25u, off, ver));
    EXPECT_EQ(ver, 20u);

    ASSERT_TRUE(seg_.get("k", 10u, off, ver));
    EXPECT_EQ(ver, 10u);

    EXPECT_FALSE(seg_.get("k", 5u, off, ver));
}

// --- Seal + open round-trip ---

TEST_F(SegmentTest, SealAndOpenEmpty) {
    ASSERT_TRUE(seg_.create(path_).ok());
    ASSERT_TRUE(seg_.seal().ok());
    seg_.close();

    Segment loaded;
    ASSERT_TRUE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.dataSize(), 0u);
    EXPECT_EQ(loaded.entryCount(), 0u);
    loaded.close();
}

TEST_F(SegmentTest, SealAndOpenRoundTrip) {
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("alpha", 1, "v1", false);
    writeEntry("alpha", 2, "v2", false);
    writeEntry("beta", 10, "vb", true);

    uint64_t data_size = seg_.dataSize();
    ASSERT_TRUE(seg_.seal().ok());
    seg_.close();

    Segment loaded;
    ASSERT_TRUE(loaded.open(path_).ok());

    EXPECT_EQ(loaded.dataSize(), data_size);
    EXPECT_EQ(loaded.entryCount(), 3u);
    EXPECT_EQ(loaded.keyCount(), 2u);

    uint32_t off, ver;
    ASSERT_TRUE(loaded.getLatest("alpha", off, ver));
    EXPECT_EQ(ver, 2u);

    ASSERT_TRUE(loaded.getLatest("beta", off, ver));
    EXPECT_EQ(ver, 10u);

    std::string key;
    uint64_t v;
    readBack(loaded, off, key, v);
    EXPECT_EQ(key, "beta");
    EXPECT_EQ(v, 10u);

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

// --- Open error paths ---

TEST_F(SegmentTest, OpenMissingFile) {
    Segment loaded;
    EXPECT_FALSE(loaded.open("/nonexistent.seg").ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenTruncatedFile) {
    ASSERT_TRUE(seg_.create(path_).ok());
    uint64_t off;
    ASSERT_TRUE(seg_.append("x", 1, off).ok());
    seg_.close();

    Segment loaded;
    EXPECT_FALSE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenBadMagic) {
    ASSERT_TRUE(seg_.create(path_).ok());
    uint8_t zeros[12] = {};
    uint64_t off;
    ASSERT_TRUE(seg_.append(zeros, 12, off).ok());
    seg_.close();

    Segment loaded;
    EXPECT_FALSE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenDataWrittenButNoIndex) {
    // Simulate a crash after data was flushed but before seal() ran:
    // the file contains valid log entries but no L2 index or footer.
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("key1", 1, "val1", false);
    writeEntry("key2", 2, "val2", false);
    seg_.close();  // close without seal

    Segment loaded;
    Status s = loaded.open(path_);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenDataWrittenWithFakeFooter) {
    // Data was flushed, then a valid-looking footer was written but
    // the index_offset points into the data region (no real L2 index).
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("key1", 1, "val1", false);
    // Append a footer that claims the index starts at offset 0
    // (inside the data region — readFrom should fail).
    uint8_t footer[12];
    uint64_t fake_index_offset = 0;
    uint32_t magic = 0x53454746;  // "SEGF"
    std::memcpy(footer, &fake_index_offset, 8);
    std::memcpy(footer + 8, &magic, 4);
    uint64_t off;
    ASSERT_TRUE(seg_.append(footer, 12, off).ok());
    seg_.close();

    Segment loaded;
    Status s = loaded.open(path_);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenIndexOffsetBeyondFile) {
    // Footer is valid but index_offset points past the end of the file.
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("key1", 1, "val1", false);

    uint8_t footer[12];
    uint64_t bad_offset = 999999;
    uint32_t magic = 0x53454746;
    std::memcpy(footer, &bad_offset, 8);
    std::memcpy(footer + 8, &magic, 4);
    uint64_t off;
    ASSERT_TRUE(seg_.append(footer, 12, off).ok());
    seg_.close();

    Segment loaded;
    Status s = loaded.open(path_);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenTruncatedIndex) {
    // Seal a valid segment, then truncate the file mid-index to simulate
    // a crash during seal (index partially written).
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("key1", 1, "val1", false);
    writeEntry("key2", 2, "val2", false);
    ASSERT_TRUE(seg_.seal().ok());
    uint64_t data_size = seg_.dataSize();
    seg_.close();

    // Truncate: keep data + a few bytes of the index, drop the rest.
    int fd = ::open(path_.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    int ret = ::ftruncate(fd, static_cast<off_t>(data_size + 4));
    ASSERT_EQ(ret, 0);
    ::close(fd);

    Segment loaded;
    Status s = loaded.open(path_);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

// --- Move semantics ---

TEST_F(SegmentTest, MoveConstructWriting) {
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("k", 1, "v", false);

    Segment moved(std::move(seg_));
    EXPECT_EQ(moved.state(), Segment::State::kWriting);
    EXPECT_EQ(moved.entryCount(), 1u);
    moved.close();
}

TEST_F(SegmentTest, MoveConstructReadable) {
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("k", 1, "v", false);
    ASSERT_TRUE(seg_.seal().ok());

    Segment moved(std::move(seg_));
    EXPECT_EQ(moved.state(), Segment::State::kReadable);

    uint32_t off, ver;
    ASSERT_TRUE(moved.getLatest("k", off, ver));
    EXPECT_EQ(ver, 1u);
    moved.close();
}

TEST_F(SegmentTest, MoveAssign) {
    ASSERT_TRUE(seg_.create(path_).ok());
    writeEntry("k", 1, "v", false);
    ASSERT_TRUE(seg_.seal().ok());

    Segment other;
    other = std::move(seg_);
    EXPECT_EQ(other.state(), Segment::State::kReadable);

    uint32_t off, ver;
    ASSERT_TRUE(other.getLatest("k", off, ver));
    EXPECT_EQ(ver, 1u);
    other.close();
}
