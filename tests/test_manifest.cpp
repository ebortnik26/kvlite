#include <gtest/gtest.h>

#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <string>

#include "internal/manifest.h"

using namespace kvlite::internal;
using kvlite::Status;

class ManifestTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = "/tmp/manifest_test_" + std::to_string(getpid());
        std::filesystem::remove_all(path_);
    }
    void TearDown() override {
        std::filesystem::remove_all(path_);
    }
    std::string path_;
};

TEST_F(ManifestTest, CreateAndOpen) {
    Manifest m;
    ASSERT_FALSE(m.isOpen());
    ASSERT_TRUE(m.create(path_).ok());
    ASSERT_TRUE(m.isOpen());
    ASSERT_TRUE(m.close().ok());
    ASSERT_FALSE(m.isOpen());
}

TEST_F(ManifestTest, OpenNonExistentFails) {
    Manifest m;
    Status s = m.open(path_);
    EXPECT_TRUE(s.isNotFound());
}

TEST_F(ManifestTest, DoubleCreateFails) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());
    EXPECT_FALSE(m.create(path_).ok());
    ASSERT_TRUE(m.close().ok());
}

TEST_F(ManifestTest, SetAndGet) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());

    std::string val;
    EXPECT_FALSE(m.get("key1", val));

    ASSERT_TRUE(m.set("key1", "value1").ok());
    ASSERT_TRUE(m.get("key1", val));
    EXPECT_EQ(val, "value1");

    ASSERT_TRUE(m.set("key2", "value2").ok());
    ASSERT_TRUE(m.get("key2", val));
    EXPECT_EQ(val, "value2");

    ASSERT_TRUE(m.close().ok());
}

TEST_F(ManifestTest, SetThenRecover) {
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val));
        EXPECT_EQ(val, "v2");
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, OverwriteAndRecover) {
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "original").ok());
        ASSERT_TRUE(m.set("k1", "updated").ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "updated");
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, CompactReducesFileSize) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());

    // Write many overwrites to accumulate log entries.
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(m.set("key", std::string(100, 'a' + (i % 26))).ok());
    }

    // Get file size before compaction.
    std::string manifest_path = path_ + "/MANIFEST";
    auto size_before = std::filesystem::file_size(manifest_path);

    ASSERT_TRUE(m.compact().ok());

    auto size_after = std::filesystem::file_size(manifest_path);
    EXPECT_LT(size_after, size_before);

    // Verify last value survived.
    std::string val;
    ASSERT_TRUE(m.get("key", val));
    EXPECT_EQ(val, std::string(100, 'a' + (99 % 26)));

    ASSERT_TRUE(m.close().ok());
}

TEST_F(ManifestTest, CompactThenRecover) {
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.set("k2", "v2_updated").ok());
        ASSERT_TRUE(m.compact().ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val));
        EXPECT_EQ(val, "v2_updated");
        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, PartialWriteRecovery) {
    // Write valid records, then append garbage to simulate a partial write.
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.close().ok());
    }

    // Append partial/garbage data to the file.
    std::string manifest_path = path_ + "/MANIFEST";
    int fd = ::open(manifest_path.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    const char garbage[] = {'\x0a', '\x00', '\x00', '\x00', '\x01', '\xff'};
    ssize_t n = ::write(fd, garbage, sizeof(garbage));
    ASSERT_EQ(n, static_cast<ssize_t>(sizeof(garbage)));
    ::close(fd);

    // Recover should truncate the garbage and preserve valid records.
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m.get("k2", val));
        EXPECT_EQ(val, "v2");

        // Should be able to write new records after recovery.
        ASSERT_TRUE(m.set("k3", "v3").ok());
        ASSERT_TRUE(m.get("k3", val));
        EXPECT_EQ(val, "v3");

        ASSERT_TRUE(m.close().ok());
    }
}

TEST_F(ManifestTest, AppendAfterCompact) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());

    ASSERT_TRUE(m.set("k1", "v1").ok());
    ASSERT_TRUE(m.compact().ok());

    // Append after compaction should work.
    ASSERT_TRUE(m.set("k2", "v2").ok());

    std::string val;
    ASSERT_TRUE(m.get("k1", val));
    EXPECT_EQ(val, "v1");
    ASSERT_TRUE(m.get("k2", val));
    EXPECT_EQ(val, "v2");

    ASSERT_TRUE(m.close().ok());

    // Reopen and verify both records survived.
    {
        Manifest m2;
        ASSERT_TRUE(m2.open(path_).ok());
        ASSERT_TRUE(m2.get("k1", val));
        EXPECT_EQ(val, "v1");
        ASSERT_TRUE(m2.get("k2", val));
        EXPECT_EQ(val, "v2");
        ASSERT_TRUE(m2.close().ok());
    }
}

TEST_F(ManifestTest, EmptyRecovery) {
    // Create a manifest, close, reopen with no records.
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        EXPECT_FALSE(m.get("anything", val));
        ASSERT_TRUE(m.close().ok());
    }
}
