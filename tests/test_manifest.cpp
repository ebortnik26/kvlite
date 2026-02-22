#include <gtest/gtest.h>

#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <vector>

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

TEST_F(ManifestTest, DeleteAndRecover) {
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.remove("k1").ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        EXPECT_FALSE(m.get("k1", val));
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

    // Write many records then delete most.
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(m.set("key" + std::to_string(i), std::string(100, 'x')).ok());
    }
    for (int i = 0; i < 90; ++i) {
        ASSERT_TRUE(m.remove("key" + std::to_string(i)).ok());
    }

    // Get file size before compaction.
    std::string manifest_path = path_ + "/MANIFEST";
    auto size_before = std::filesystem::file_size(manifest_path);

    ASSERT_TRUE(m.compact().ok());

    auto size_after = std::filesystem::file_size(manifest_path);
    EXPECT_LT(size_after, size_before);

    // Verify state is intact.
    for (int i = 0; i < 90; ++i) {
        std::string val;
        EXPECT_FALSE(m.get("key" + std::to_string(i), val))
            << "key" << i << " should be deleted";
    }
    for (int i = 90; i < 100; ++i) {
        std::string val;
        ASSERT_TRUE(m.get("key" + std::to_string(i), val))
            << "key" << i << " should exist";
        EXPECT_EQ(val, std::string(100, 'x'));
    }

    ASSERT_TRUE(m.close().ok());
}

TEST_F(ManifestTest, CompactThenRecover) {
    {
        Manifest m;
        ASSERT_TRUE(m.create(path_).ok());
        ASSERT_TRUE(m.set("k1", "v1").ok());
        ASSERT_TRUE(m.set("k2", "v2").ok());
        ASSERT_TRUE(m.set("k3", "v3").ok());
        ASSERT_TRUE(m.remove("k2").ok());
        ASSERT_TRUE(m.compact().ok());
        ASSERT_TRUE(m.close().ok());
    }
    {
        Manifest m;
        ASSERT_TRUE(m.open(path_).ok());
        std::string val;
        ASSERT_TRUE(m.get("k1", val));
        EXPECT_EQ(val, "v1");
        EXPECT_FALSE(m.get("k2", val));
        ASSERT_TRUE(m.get("k3", val));
        EXPECT_EQ(val, "v3");
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

TEST_F(ManifestTest, GetKeysWithPrefix) {
    Manifest m;
    ASSERT_TRUE(m.create(path_).ok());

    ASSERT_TRUE(m.set("segment.1", "").ok());
    ASSERT_TRUE(m.set("segment.2", "").ok());
    ASSERT_TRUE(m.set("segment.10", "").ok());
    ASSERT_TRUE(m.set("next_version_id", "100").ok());
    ASSERT_TRUE(m.set("next_segment_id", "11").ok());

    auto keys = m.getKeysWithPrefix("segment.");
    ASSERT_EQ(keys.size(), 3u);
    // std::map is sorted, so keys should be in order.
    EXPECT_EQ(keys[0], "segment.1");
    EXPECT_EQ(keys[1], "segment.10");
    EXPECT_EQ(keys[2], "segment.2");

    auto version_keys = m.getKeysWithPrefix("next_version");
    ASSERT_EQ(version_keys.size(), 1u);
    EXPECT_EQ(version_keys[0], "next_version_id");

    auto empty_keys = m.getKeysWithPrefix("nonexistent");
    EXPECT_TRUE(empty_keys.empty());

    ASSERT_TRUE(m.close().ok());
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
