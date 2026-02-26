#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "internal/wal.h"

namespace fs = std::filesystem;

using kvlite::Status;
using kvlite::internal::WAL;

class WALTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() / "kvlite_wal_test";
        fs::create_directories(dir_);
        path_ = (dir_ / "test.wal").string();
    }

    void TearDown() override {
        fs::remove_all(dir_);
    }

    fs::path dir_;
    std::string path_;
};

TEST_F(WALTest, CreateAndClose) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());
    EXPECT_TRUE(wal.isOpen());
    EXPECT_EQ(wal.size(), 0u);
    ASSERT_TRUE(wal.close().ok());
    EXPECT_FALSE(wal.isOpen());
}

TEST_F(WALTest, OpenExisting) {
    {
        WAL wal;
        ASSERT_TRUE(wal.create(path_).ok());
        uint8_t data[] = {1, 2, 3};
        wal.put(data, 3);
        ASSERT_TRUE(wal.commit().ok());
        ASSERT_TRUE(wal.close().ok());
    }

    WAL wal;
    ASSERT_TRUE(wal.open(path_).ok());
    EXPECT_TRUE(wal.isOpen());
    EXPECT_GT(wal.size(), 0u);
    ASSERT_TRUE(wal.close().ok());
}

TEST_F(WALTest, PutAndCommitSingle) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t data[] = {0xAA, 0xBB, 0xCC};
    wal.put(data, 3);
    ASSERT_TRUE(wal.commit().ok());

    // File should have: data record (4+1+3+4=12) + commit record (4+5=9) = 21 bytes
    EXPECT_EQ(wal.size(), 21u);
    ASSERT_TRUE(wal.close().ok());
}

TEST_F(WALTest, PutMultipleAndCommit) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d1[] = {1};
    uint8_t d2[] = {2, 3};
    wal.put(d1, 1);
    wal.put(d2, 2);
    ASSERT_TRUE(wal.commit().ok());

    // d1: 4+1+1+4=10, d2: 4+1+2+4=11, commit: 4+5=9, total=30
    EXPECT_EQ(wal.size(), 30u);
    ASSERT_TRUE(wal.close().ok());
}

TEST_F(WALTest, Abort) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t data[] = {1, 2, 3};
    wal.put(data, 3);
    wal.abort();

    // Nothing written
    EXPECT_EQ(wal.size(), 0u);

    // Can still commit a new batch
    uint8_t d2[] = {4};
    wal.put(d2, 1);
    ASSERT_TRUE(wal.commit().ok());
    EXPECT_GT(wal.size(), 0u);

    ASSERT_TRUE(wal.close().ok());
}

TEST_F(WALTest, EmptyCommit) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    // Commit with no data records — just a commit record
    ASSERT_TRUE(wal.commit().ok());
    EXPECT_EQ(wal.size(), 9u); // just the commit record (4+5)

    ASSERT_TRUE(wal.close().ok());
}

TEST_F(WALTest, ReplaySingleBatch) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d1[] = {10, 20};
    uint8_t d2[] = {30, 40, 50};
    wal.put(d1, 2);
    wal.put(d2, 3);
    ASSERT_TRUE(wal.commit().ok());
    ASSERT_TRUE(wal.close().ok());

    // Replay
    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    int callback_count = 0;
    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>& records) -> Status {
            EXPECT_EQ(records.size(), 2u);
            EXPECT_EQ(records[0].size(), 2u);
            EXPECT_EQ(static_cast<uint8_t>(records[0][0]), 10);
            EXPECT_EQ(static_cast<uint8_t>(records[0][1]), 20);
            EXPECT_EQ(records[1].size(), 3u);
            EXPECT_EQ(static_cast<uint8_t>(records[1][0]), 30);
            EXPECT_EQ(static_cast<uint8_t>(records[1][1]), 40);
            EXPECT_EQ(static_cast<uint8_t>(records[1][2]), 50);
            ++callback_count;
            return Status::OK();
        },
        valid_end);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(valid_end, wal2.size());
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, ReplayMultipleBatches) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d1[] = {1};
    wal.put(d1, 1);
    ASSERT_TRUE(wal.commit().ok());

    uint8_t d2[] = {2};
    uint8_t d3[] = {3};
    wal.put(d2, 1);
    wal.put(d3, 1);
    ASSERT_TRUE(wal.commit().ok());

    ASSERT_TRUE(wal.close().ok());

    // Replay
    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    std::vector<size_t> record_counts;
    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>& records) -> Status {
            record_counts.push_back(records.size());
            return Status::OK();
        },
        valid_end);

    ASSERT_TRUE(s.ok());
    ASSERT_EQ(record_counts.size(), 2u);
    EXPECT_EQ(record_counts[0], 1u);
    EXPECT_EQ(record_counts[1], 2u);
    EXPECT_EQ(valid_end, wal2.size());
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, ReplayEmptyFile) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());
    ASSERT_TRUE(wal.close().ok());

    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    int callback_count = 0;
    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>&) -> Status {
            ++callback_count;
            return Status::OK();
        },
        valid_end);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(callback_count, 0);
    EXPECT_EQ(valid_end, 0u);
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, ReplayTruncatedRecord) {
    // Write one committed batch, then write partial data
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d1[] = {1, 2};
    wal.put(d1, 2);
    ASSERT_TRUE(wal.commit().ok());
    uint64_t good_size = wal.size();

    // Write a second batch
    uint8_t d2[] = {3, 4, 5};
    wal.put(d2, 3);
    ASSERT_TRUE(wal.commit().ok());
    ASSERT_TRUE(wal.close().ok());

    // Truncate the file to corrupt the second batch
    {
        kvlite::internal::LogFile lf;
        ASSERT_TRUE(lf.open(path_).ok());
        ASSERT_TRUE(lf.truncateTo(good_size + 5).ok()); // partial second batch
        ASSERT_TRUE(lf.close().ok());
    }

    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    int callback_count = 0;
    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>& records) -> Status {
            ++callback_count;
            EXPECT_EQ(records.size(), 1u);
            return Status::OK();
        },
        valid_end);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(callback_count, 1); // only the first batch
    EXPECT_EQ(valid_end, good_size);
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, ReplayAndTruncate) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d1[] = {1};
    wal.put(d1, 1);
    ASSERT_TRUE(wal.commit().ok());
    uint64_t good_size = wal.size();

    uint8_t d2[] = {2};
    wal.put(d2, 1);
    ASSERT_TRUE(wal.commit().ok());
    ASSERT_TRUE(wal.close().ok());

    // Truncate file to corrupt the second batch
    {
        kvlite::internal::LogFile lf;
        ASSERT_TRUE(lf.open(path_).ok());
        ASSERT_TRUE(lf.truncateTo(good_size + 3).ok());
        ASSERT_TRUE(lf.close().ok());
    }

    // replayAndTruncate should recover + truncate trailing garbage
    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    int callback_count = 0;
    Status s = wal2.replayAndTruncate(
        [&](const std::vector<std::string_view>&) -> Status {
            ++callback_count;
            return Status::OK();
        });

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(wal2.size(), good_size); // truncated to valid end
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, Truncate) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d[] = {1, 2, 3};
    wal.put(d, 3);
    ASSERT_TRUE(wal.commit().ok());
    EXPECT_GT(wal.size(), 0u);

    ASSERT_TRUE(wal.truncate().ok());
    EXPECT_EQ(wal.size(), 0u);

    // Can still write after truncate
    wal.put(d, 3);
    ASSERT_TRUE(wal.commit().ok());
    EXPECT_GT(wal.size(), 0u);

    ASSERT_TRUE(wal.close().ok());
}

TEST_F(WALTest, Sync) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d[] = {1};
    wal.put(d, 1);
    ASSERT_TRUE(wal.commit(/*sync=*/true).ok());

    ASSERT_TRUE(wal.sync().ok());
    ASSERT_TRUE(wal.close().ok());
}

TEST_F(WALTest, MoveConstruct) {
    WAL wal1;
    ASSERT_TRUE(wal1.create(path_).ok());
    uint8_t d[] = {1};
    wal1.put(d, 1);
    ASSERT_TRUE(wal1.commit().ok());

    WAL wal2(std::move(wal1));
    EXPECT_TRUE(wal2.isOpen());
    EXPECT_FALSE(wal1.isOpen());
    EXPECT_GT(wal2.size(), 0u);

    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, MoveAssign) {
    WAL wal1;
    ASSERT_TRUE(wal1.create(path_).ok());
    uint8_t d[] = {1};
    wal1.put(d, 1);
    ASSERT_TRUE(wal1.commit().ok());

    WAL wal2;
    wal2 = std::move(wal1);
    EXPECT_TRUE(wal2.isOpen());
    EXPECT_FALSE(wal1.isOpen());

    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, LargePayload) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    std::vector<uint8_t> big(100000);
    for (size_t i = 0; i < big.size(); ++i) {
        big[i] = static_cast<uint8_t>(i & 0xFF);
    }
    wal.put(big.data(), big.size());
    ASSERT_TRUE(wal.commit().ok());
    ASSERT_TRUE(wal.close().ok());

    // Replay and verify
    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>& records) -> Status {
            EXPECT_EQ(records.size(), 1u);
            EXPECT_EQ(records[0].size(), big.size());
            for (size_t i = 0; i < big.size(); ++i) {
                EXPECT_EQ(static_cast<uint8_t>(records[0][i]), big[i]);
            }
            return Status::OK();
        },
        valid_end);

    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, CorruptedCRC) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d1[] = {1};
    wal.put(d1, 1);
    ASSERT_TRUE(wal.commit().ok());

    uint8_t d2[] = {2};
    wal.put(d2, 1);
    ASSERT_TRUE(wal.commit().ok());
    ASSERT_TRUE(wal.close().ok());

    // Corrupt a byte in the middle of the second batch
    {
        kvlite::internal::LogFile lf;
        ASSERT_TRUE(lf.open(path_).ok());
        // First batch is 10 + 17 = 27 bytes. Corrupt byte 28 (start of second batch data).
        uint64_t offset;
        std::vector<uint8_t> buf(lf.size());
        ASSERT_TRUE(lf.readAt(0, buf.data(), buf.size()).ok());
        buf[31] ^= 0xFF; // flip bits in second batch
        ASSERT_TRUE(lf.truncateTo(0).ok());
        ASSERT_TRUE(lf.append(buf.data(), buf.size(), offset).ok());
        ASSERT_TRUE(lf.close().ok());
    }

    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    int callback_count = 0;
    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>&) -> Status {
            ++callback_count;
            return Status::OK();
        },
        valid_end);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(callback_count, 1); // only first batch recovered
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, CallbackError) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    uint8_t d[] = {1};
    wal.put(d, 1);
    ASSERT_TRUE(wal.commit().ok());
    ASSERT_TRUE(wal.close().ok());

    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>&) -> Status {
            return Status::Corruption("test error");
        },
        valid_end);

    EXPECT_TRUE(s.isCorruption());
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, MultipleCommitsReplay) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    for (int i = 0; i < 5; ++i) {
        uint8_t d = static_cast<uint8_t>(i);
        wal.put(&d, 1);
        ASSERT_TRUE(wal.commit().ok());
    }
    ASSERT_TRUE(wal.close().ok());

    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    int batch_count = 0;
    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>& records) -> Status {
            EXPECT_EQ(records.size(), 1u);
            EXPECT_EQ(static_cast<uint8_t>(records[0][0]),
                       static_cast<uint8_t>(batch_count));
            ++batch_count;
            return Status::OK();
        },
        valid_end);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(batch_count, 5);
    ASSERT_TRUE(wal2.close().ok());
}

TEST_F(WALTest, UncommittedDataDiscarded) {
    WAL wal;
    ASSERT_TRUE(wal.create(path_).ok());

    // Committed batch
    uint8_t d1[] = {1};
    wal.put(d1, 1);
    ASSERT_TRUE(wal.commit().ok());

    // Uncommitted data (pending when close is called)
    uint8_t d2[] = {2};
    wal.put(d2, 1);
    ASSERT_TRUE(wal.close().ok()); // close discards uncommitted

    WAL wal2;
    ASSERT_TRUE(wal2.open(path_).ok());

    int callback_count = 0;
    uint64_t valid_end = 0;
    Status s = wal2.replay(
        [&](const std::vector<std::string_view>& records) -> Status {
            ++callback_count;
            EXPECT_EQ(records.size(), 1u);
            EXPECT_EQ(static_cast<uint8_t>(records[0][0]), 1);
            return Status::OK();
        },
        valid_end);

    ASSERT_TRUE(s.ok());
    EXPECT_EQ(callback_count, 1);
    ASSERT_TRUE(wal2.close().ok());
}
