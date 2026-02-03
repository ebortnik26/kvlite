#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "internal/log_file.h"

namespace kvlite {
namespace internal {
namespace {

class LogFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "kvlite_logfile_test";
        std::filesystem::create_directories(test_dir_);
        test_path_ = (test_dir_ / "test.data").string();
    }

    void TearDown() override {
        std::filesystem::remove_all(test_dir_);
    }

    std::filesystem::path test_dir_;
    std::string test_path_;
};

TEST_F(LogFileTest, CreateAndAppendReadBack) {
    LogFile lf;
    ASSERT_TRUE(lf.create(test_path_).ok());
    ASSERT_TRUE(lf.isOpen());
    ASSERT_EQ(lf.size(), 0u);

    const char* data = "hello world";
    size_t len = std::strlen(data);
    uint64_t offset = 0;
    ASSERT_TRUE(lf.append(data, len, offset).ok());
    EXPECT_EQ(offset, 0u);
    EXPECT_EQ(lf.size(), len);

    char buf[64] = {};
    ASSERT_TRUE(lf.readAt(0, buf, len).ok());
    EXPECT_EQ(std::string(buf, len), "hello world");
}

TEST_F(LogFileTest, MultipleAppendsSequentialOffsets) {
    LogFile lf;
    ASSERT_TRUE(lf.create(test_path_).ok());

    const char* a = "AAA";
    const char* b = "BBBBB";
    const char* c = "CC";

    uint64_t off1, off2, off3;
    ASSERT_TRUE(lf.append(a, 3, off1).ok());
    ASSERT_TRUE(lf.append(b, 5, off2).ok());
    ASSERT_TRUE(lf.append(c, 2, off3).ok());

    EXPECT_EQ(off1, 0u);
    EXPECT_EQ(off2, 3u);
    EXPECT_EQ(off3, 8u);
    EXPECT_EQ(lf.size(), 10u);

    // Read each segment back
    char buf[16] = {};
    ASSERT_TRUE(lf.readAt(off1, buf, 3).ok());
    EXPECT_EQ(std::memcmp(buf, "AAA", 3), 0);

    ASSERT_TRUE(lf.readAt(off2, buf, 5).ok());
    EXPECT_EQ(std::memcmp(buf, "BBBBB", 5), 0);

    ASSERT_TRUE(lf.readAt(off3, buf, 2).ok());
    EXPECT_EQ(std::memcmp(buf, "CC", 2), 0);
}

TEST_F(LogFileTest, CloseAndReopenPreservesData) {
    const char* data = "persistent data";
    size_t len = std::strlen(data);

    {
        LogFile lf;
        ASSERT_TRUE(lf.create(test_path_).ok());
        uint64_t offset;
        ASSERT_TRUE(lf.append(data, len, offset).ok());
        ASSERT_TRUE(lf.sync().ok());
        ASSERT_TRUE(lf.close().ok());
        EXPECT_FALSE(lf.isOpen());
    }

    {
        LogFile lf;
        ASSERT_TRUE(lf.open(test_path_).ok());
        EXPECT_TRUE(lf.isOpen());
        EXPECT_EQ(lf.size(), len);

        char buf[64] = {};
        ASSERT_TRUE(lf.readAt(0, buf, len).ok());
        EXPECT_EQ(std::string(buf, len), "persistent data");
    }
}

TEST_F(LogFileTest, ReadAtRandomOffsets) {
    LogFile lf;
    ASSERT_TRUE(lf.create(test_path_).ok());

    // Write 256 bytes: 0x00, 0x01, ..., 0xFF
    std::vector<uint8_t> pattern(256);
    for (int i = 0; i < 256; ++i) pattern[i] = static_cast<uint8_t>(i);

    uint64_t offset;
    ASSERT_TRUE(lf.append(pattern.data(), pattern.size(), offset).ok());

    // Read single bytes at various offsets
    uint8_t val;
    for (uint64_t i = 0; i < 256; i += 17) {
        ASSERT_TRUE(lf.readAt(i, &val, 1).ok());
        EXPECT_EQ(val, static_cast<uint8_t>(i));
    }

    // Read a range in the middle
    uint8_t buf[10];
    ASSERT_TRUE(lf.readAt(100, buf, 10).ok());
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(buf[i], static_cast<uint8_t>(100 + i));
    }
}

TEST_F(LogFileTest, ReadBeyondEOFReturnsError) {
    LogFile lf;
    ASSERT_TRUE(lf.create(test_path_).ok());

    const char* data = "short";
    uint64_t offset;
    ASSERT_TRUE(lf.append(data, 5, offset).ok());

    char buf[64];
    Status s = lf.readAt(3, buf, 10);  // only 2 bytes available from offset 3
    EXPECT_TRUE(s.isIOError());
}

TEST_F(LogFileTest, SyncDoesNotCrash) {
    LogFile lf;
    ASSERT_TRUE(lf.create(test_path_).ok());

    const char* data = "sync test";
    uint64_t offset;
    ASSERT_TRUE(lf.append(data, 9, offset).ok());
    EXPECT_TRUE(lf.sync().ok());
}

TEST_F(LogFileTest, MoveConstruct) {
    LogFile lf1;
    ASSERT_TRUE(lf1.create(test_path_).ok());

    const char* data = "moved";
    uint64_t offset;
    ASSERT_TRUE(lf1.append(data, 5, offset).ok());

    LogFile lf2(std::move(lf1));
    EXPECT_FALSE(lf1.isOpen());
    EXPECT_TRUE(lf2.isOpen());
    EXPECT_EQ(lf2.size(), 5u);
    EXPECT_EQ(lf2.path(), test_path_);

    char buf[8] = {};
    ASSERT_TRUE(lf2.readAt(0, buf, 5).ok());
    EXPECT_EQ(std::string(buf, 5), "moved");
}

TEST_F(LogFileTest, MoveAssign) {
    LogFile lf1;
    ASSERT_TRUE(lf1.create(test_path_).ok());

    const char* data = "assign";
    uint64_t offset;
    ASSERT_TRUE(lf1.append(data, 6, offset).ok());

    LogFile lf2;
    lf2 = std::move(lf1);
    EXPECT_FALSE(lf1.isOpen());
    EXPECT_TRUE(lf2.isOpen());
    EXPECT_EQ(lf2.size(), 6u);

    char buf[8] = {};
    ASSERT_TRUE(lf2.readAt(0, buf, 6).ok());
    EXPECT_EQ(std::string(buf, 6), "assign");
}

TEST_F(LogFileTest, DoubleOpenFails) {
    LogFile lf;
    ASSERT_TRUE(lf.create(test_path_).ok());
    Status s = lf.create(test_path_);
    EXPECT_TRUE(s.isIOError());
}

TEST_F(LogFileTest, OperationsOnClosedFileFail) {
    LogFile lf;
    char buf[8];
    uint64_t offset;
    EXPECT_TRUE(lf.append("x", 1, offset).isIOError());
    EXPECT_TRUE(lf.readAt(0, buf, 1).isIOError());
    EXPECT_TRUE(lf.sync().isIOError());
}

TEST_F(LogFileTest, PathAccessor) {
    LogFile lf;
    ASSERT_TRUE(lf.create(test_path_).ok());
    EXPECT_EQ(lf.path(), test_path_);
}

// Static helpers

TEST(LogFileStaticTest, MakeDataPath) {
    EXPECT_EQ(LogFile::makeDataPath("/db", 1), "/db/log_00000001.data");
    EXPECT_EQ(LogFile::makeDataPath("/db", 42), "/db/log_00000042.data");
    EXPECT_EQ(LogFile::makeDataPath("/db", 99999999), "/db/log_99999999.data");
}

TEST(LogFileStaticTest, MakeIndexPath) {
    EXPECT_EQ(LogFile::makeIndexPath("/db", 1), "/db/log_00000001.idx");
    EXPECT_EQ(LogFile::makeIndexPath("/db", 42), "/db/log_00000042.idx");
}

TEST(LogFileStaticTest, ParseFileId) {
    EXPECT_EQ(LogFile::parseFileId("log_00000001.data"), 1u);
    EXPECT_EQ(LogFile::parseFileId("log_00000042.idx"), 42u);
    EXPECT_EQ(LogFile::parseFileId("log_99999999.data"), 99999999u);
    EXPECT_EQ(LogFile::parseFileId("garbage"), 0u);
}

} // namespace
} // namespace internal
} // namespace kvlite
