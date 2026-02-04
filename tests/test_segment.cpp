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

    // Write a LogEntry into the segment via the high-level put API.
    void writeEntry(const std::string& key, uint64_t version,
                    const std::string& value, bool tombstone) {
        ASSERT_TRUE(seg_.put(key, version, value, tombstone).ok());
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
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    EXPECT_EQ(seg_.state(), Segment::State::kWriting);
    EXPECT_TRUE(seg_.isOpen());
}

TEST_F(SegmentTest, SealTransitionsToReadable) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_EQ(seg_.state(), Segment::State::kReadable);
}

TEST_F(SegmentTest, OpenTransitionsToReadable) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.seal().ok());
    seg_.close();

    Segment loaded;
    ASSERT_TRUE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.state(), Segment::State::kReadable);
    loaded.close();
}

TEST_F(SegmentTest, CloseTransitionsToClosed) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    seg_.close();
    EXPECT_EQ(seg_.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, CloseSealedTransitionsToClosed) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.seal().ok());
    seg_.close();
    EXPECT_EQ(seg_.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, CloseWhenAlreadyClosed) {
    EXPECT_TRUE(seg_.close().ok());
}

// --- State violations ---

TEST_F(SegmentTest, CreateWhileWritingFails) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    EXPECT_FALSE(seg_.create(path_, 1).ok());
}

TEST_F(SegmentTest, CreateWhileReadableFails) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_FALSE(seg_.create(path_, 1).ok());
}

TEST_F(SegmentTest, OpenWhileWritingFails) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    EXPECT_FALSE(seg_.open(path_).ok());
}

TEST_F(SegmentTest, SealWhileClosedFails) {
    EXPECT_FALSE(seg_.seal().ok());
}

TEST_F(SegmentTest, SealWhileReadableFails) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_FALSE(seg_.seal().ok());
}

TEST_F(SegmentTest, PutWhileClosedFails) {
    EXPECT_FALSE(seg_.put("x", 1, "", false).ok());
}

TEST_F(SegmentTest, PutWhileReadableFails) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_FALSE(seg_.put("k", 1, "", false).ok());
}

TEST_F(SegmentTest, GetLatestWhileWritingFails) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("k", 1, "v", false);
    LogEntry entry;
    EXPECT_FALSE(seg_.getLatest("k", entry).ok());
}

TEST_F(SegmentTest, GetLatestWhileClosedFails) {
    LogEntry entry;
    EXPECT_FALSE(seg_.getLatest("k", entry).ok());
}

TEST_F(SegmentTest, QueriesWhileWritingFail) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("k", 1, "v", false);

    LogEntry entry;
    EXPECT_FALSE(seg_.getLatest("k", entry).ok());
    EXPECT_FALSE(seg_.contains("k"));

    std::vector<LogEntry> entries;
    EXPECT_FALSE(seg_.get("k", entries).ok());

    EXPECT_FALSE(seg_.get("k", 100u, entry).ok());
}

// --- Create error ---

TEST_F(SegmentTest, CreateNonExistentDir) {
    Status s = seg_.create("/nonexistent_dir_xyz/file.data", 1);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(seg_.state(), Segment::State::kClosed);
}

// --- Write tests ---

TEST_F(SegmentTest, PutAndDataSize) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.put("hello", 1, "world", false).ok());
    EXPECT_GT(seg_.dataSize(), 0u);
}

TEST_F(SegmentTest, MultiplePuts) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());

    ASSERT_TRUE(seg_.put("aaa", 1, "v1", false).ok());
    uint64_t size1 = seg_.dataSize();
    ASSERT_TRUE(seg_.put("bbb", 2, "v2", false).ok());
    EXPECT_GT(seg_.dataSize(), size1);
}

// --- Seal + read tests ---

TEST_F(SegmentTest, SealThenGetLatest) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("hello", 1, "world", false);
    ASSERT_TRUE(seg_.seal().ok());

    LogEntry entry;
    ASSERT_TRUE(seg_.getLatest("hello", entry).ok());
    EXPECT_EQ(entry.key, "hello");
    EXPECT_EQ(entry.value, "world");
    EXPECT_EQ(entry.version(), 1u);
}

TEST_F(SegmentTest, SealThenQueryIndex) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
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

    LogEntry entry;
    ASSERT_TRUE(seg_.getLatest("key1", entry).ok());
    EXPECT_EQ(entry.version(), 2u);
    EXPECT_EQ(entry.value, "val2");

    ASSERT_TRUE(seg_.getLatest("key2", entry).ok());
    EXPECT_EQ(entry.version(), 5u);
    EXPECT_TRUE(entry.tombstone());
}

TEST_F(SegmentTest, GetAllVersionsAfterSeal) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("k", 10, "a", false);
    writeEntry("k", 20, "b", false);
    writeEntry("k", 30, "c", false);
    ASSERT_TRUE(seg_.seal().ok());

    std::vector<LogEntry> entries;
    ASSERT_TRUE(seg_.get("k", entries).ok());
    ASSERT_EQ(entries.size(), 3u);

    for (const auto& e : entries) {
        EXPECT_EQ(e.key, "k");
    }
}

TEST_F(SegmentTest, GetByUpperBoundAfterSeal) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("k", 10, "a", false);
    writeEntry("k", 20, "b", false);
    writeEntry("k", 30, "c", false);
    ASSERT_TRUE(seg_.seal().ok());

    LogEntry entry;
    ASSERT_TRUE(seg_.get("k", 25u, entry).ok());
    EXPECT_EQ(entry.version(), 20u);
    EXPECT_EQ(entry.value, "b");

    ASSERT_TRUE(seg_.get("k", 10u, entry).ok());
    EXPECT_EQ(entry.version(), 10u);

    EXPECT_FALSE(seg_.get("k", 5u, entry).ok());
}

// --- Seal + open round-trip ---

TEST_F(SegmentTest, SealAndOpenEmpty) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.seal().ok());
    seg_.close();

    Segment loaded;
    ASSERT_TRUE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.dataSize(), 0u);
    EXPECT_EQ(loaded.entryCount(), 0u);
    loaded.close();
}

TEST_F(SegmentTest, SealAndOpenRoundTrip) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
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

    LogEntry entry;
    ASSERT_TRUE(loaded.getLatest("alpha", entry).ok());
    EXPECT_EQ(entry.version(), 2u);
    EXPECT_EQ(entry.value, "v2");

    ASSERT_TRUE(loaded.getLatest("beta", entry).ok());
    EXPECT_EQ(entry.version(), 10u);
    EXPECT_EQ(entry.key, "beta");
    EXPECT_TRUE(entry.tombstone());

    std::vector<LogEntry> entries;
    ASSERT_TRUE(loaded.get("alpha", entries).ok());
    ASSERT_EQ(entries.size(), 2u);
    for (const auto& e : entries) {
        EXPECT_EQ(e.key, "alpha");
    }

    loaded.close();
}

TEST_F(SegmentTest, GetIdAfterCreateAndOpenRoundTrip) {
    ASSERT_TRUE(seg_.create(path_, 42).ok());
    EXPECT_EQ(seg_.getId(), 42u);

    writeEntry("k", 1, "v", false);
    ASSERT_TRUE(seg_.seal().ok());
    EXPECT_EQ(seg_.getId(), 42u);
    seg_.close();

    Segment loaded;
    ASSERT_TRUE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.getId(), 42u);
    loaded.close();
}

// --- Open error paths ---

TEST_F(SegmentTest, OpenMissingFile) {
    Segment loaded;
    EXPECT_FALSE(loaded.open("/nonexistent.seg").ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenTruncatedFile) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    ASSERT_TRUE(seg_.put("x", 1, "v", false).ok());
    seg_.close();

    Segment loaded;
    EXPECT_FALSE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenBadMagic) {
    // Write 16 zero bytes (invalid footer) directly via POSIX I/O.
    int fd = ::open(path_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    uint8_t zeros[16] = {};
    ASSERT_EQ(::write(fd, zeros, 16), 16);
    ::close(fd);

    Segment loaded;
    EXPECT_FALSE(loaded.open(path_).ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenDataWrittenButNoIndex) {
    // Simulate a crash after data was flushed but before seal() ran:
    // the file contains valid log entries but no L2 index or footer.
    ASSERT_TRUE(seg_.create(path_, 1).ok());
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
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("key1", 1, "val1", false);
    seg_.close();

    // Append a fake footer that claims the index starts at offset 0
    // (inside the data region -- readFrom should fail).
    int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    uint8_t footer[16];
    uint32_t fake_id = 0;
    uint64_t fake_index_offset = 0;
    uint32_t magic = 0x53454746;  // "SEGF"
    std::memcpy(footer, &fake_id, 4);
    std::memcpy(footer + 4, &fake_index_offset, 8);
    std::memcpy(footer + 12, &magic, 4);
    ASSERT_EQ(::write(fd, footer, 16), 16);
    ::close(fd);

    Segment loaded;
    Status s = loaded.open(path_);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenIndexOffsetBeyondFile) {
    // Footer is valid but index_offset points past the end of the file.
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("key1", 1, "val1", false);
    seg_.close();

    // Append a footer with an out-of-bounds index offset.
    int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    uint8_t footer[16];
    uint32_t fake_id = 0;
    uint64_t bad_offset = 999999;
    uint32_t magic = 0x53454746;
    std::memcpy(footer, &fake_id, 4);
    std::memcpy(footer + 4, &bad_offset, 8);
    std::memcpy(footer + 12, &magic, 4);
    ASSERT_EQ(::write(fd, footer, 16), 16);
    ::close(fd);

    Segment loaded;
    Status s = loaded.open(path_);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(loaded.state(), Segment::State::kClosed);
}

TEST_F(SegmentTest, OpenTruncatedIndex) {
    // Seal a valid segment, then truncate the file mid-index to simulate
    // a crash during seal (index partially written).
    ASSERT_TRUE(seg_.create(path_, 1).ok());
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
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("k", 1, "v", false);

    Segment moved(std::move(seg_));
    EXPECT_EQ(moved.state(), Segment::State::kWriting);
    EXPECT_EQ(moved.entryCount(), 1u);
    moved.close();
}

TEST_F(SegmentTest, MoveConstructReadable) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("k", 1, "v", false);
    ASSERT_TRUE(seg_.seal().ok());

    Segment moved(std::move(seg_));
    EXPECT_EQ(moved.state(), Segment::State::kReadable);

    LogEntry entry;
    ASSERT_TRUE(moved.getLatest("k", entry).ok());
    EXPECT_EQ(entry.version(), 1u);
    moved.close();
}

TEST_F(SegmentTest, MoveAssign) {
    ASSERT_TRUE(seg_.create(path_, 1).ok());
    writeEntry("k", 1, "v", false);
    ASSERT_TRUE(seg_.seal().ok());

    Segment other;
    other = std::move(seg_);
    EXPECT_EQ(other.state(), Segment::State::kReadable);

    LogEntry entry;
    ASSERT_TRUE(other.getLatest("k", entry).ok());
    EXPECT_EQ(entry.version(), 1u);
    other.close();
}
