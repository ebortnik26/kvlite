// Versioning and snapshot tests for kvlite
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

class VersioningTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "kvlite_test_versioning";
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);

        kvlite::Options opts;
        opts.create_if_missing = true;
        ASSERT_TRUE(db_.open(test_dir_.string(), opts).ok());
    }

    void TearDown() override {
        if (db_.isOpen()) {
            db_.close();
        }
        fs::remove_all(test_dir_);
    }

    fs::path test_dir_;
    kvlite::DB db_;
};

// --- Version Monotonicity Tests ---

TEST_F(VersioningTest, VersionIncreases) {
    std::vector<uint64_t> versions;

    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(db_.put("key", "value" + std::to_string(i)).ok());

        std::string value;
        uint64_t version;
        ASSERT_TRUE(db_.get("key", value, version).ok());
        versions.push_back(version);
    }

    // Versions should be strictly increasing
    for (size_t i = 1; i < versions.size(); ++i) {
        EXPECT_GT(versions[i], versions[i - 1]);
    }
}

TEST_F(VersioningTest, DifferentKeysGetDifferentVersions) {
    ASSERT_TRUE(db_.put("key1", "value1").ok());
    ASSERT_TRUE(db_.put("key2", "value2").ok());

    std::string v1, v2;
    uint64_t ver1, ver2;
    ASSERT_TRUE(db_.get("key1", v1, ver1).ok());
    ASSERT_TRUE(db_.get("key2", v2, ver2).ok());

    EXPECT_NE(ver1, ver2);
    EXPECT_LT(ver1, ver2);
}

// --- getByVersion Tests ---

TEST_F(VersioningTest, GetByVersionBasic) {
    ASSERT_TRUE(db_.put("key", "v1").ok());
    std::string tmp;
    uint64_t ver1;
    ASSERT_TRUE(db_.get("key", tmp, ver1).ok());

    ASSERT_TRUE(db_.put("key", "v2").ok());
    uint64_t ver2;
    ASSERT_TRUE(db_.get("key", tmp, ver2).ok());

    ASSERT_TRUE(db_.put("key", "v3").ok());
    uint64_t ver3;
    ASSERT_TRUE(db_.get("key", tmp, ver3).ok());

    // Get value at different versions
    std::string value;

    // Version < ver2 should return v1
    ASSERT_TRUE(db_.getByVersion("key", ver2, value).ok());
    EXPECT_EQ(value, "v1");

    // Version < ver3 should return v2
    ASSERT_TRUE(db_.getByVersion("key", ver3, value).ok());
    EXPECT_EQ(value, "v2");

    // Version < ver1 should return NotFound
    EXPECT_TRUE(db_.getByVersion("key", ver1, value).isNotFound());
}

TEST_F(VersioningTest, GetByVersionWithVersionOutput) {
    ASSERT_TRUE(db_.put("key", "v1").ok());
    std::string tmp;
    uint64_t ver1;
    ASSERT_TRUE(db_.get("key", tmp, ver1).ok());

    ASSERT_TRUE(db_.put("key", "v2").ok());
    uint64_t ver2;
    ASSERT_TRUE(db_.get("key", tmp, ver2).ok());

    std::string value;
    uint64_t entry_version;
    ASSERT_TRUE(db_.getByVersion("key", ver2, value, entry_version).ok());
    EXPECT_EQ(value, "v1");
    EXPECT_EQ(entry_version, ver1);
}

TEST_F(VersioningTest, GetByVersionAfterDelete) {
    ASSERT_TRUE(db_.put("key", "v1").ok());
    std::string tmp;
    uint64_t ver1;
    ASSERT_TRUE(db_.get("key", tmp, ver1).ok());

    ASSERT_TRUE(db_.remove("key").ok());
    uint64_t ver_del;
    bool exists;
    db_.exists("key", exists);  // Get current version somehow

    ASSERT_TRUE(db_.put("key", "v2").ok());
    uint64_t ver2;
    ASSERT_TRUE(db_.get("key", tmp, ver2).ok());

    // After the delete, before v2, key should not exist
    std::string value;
    EXPECT_TRUE(db_.getByVersion("key", ver2, value).isNotFound());
}

// --- Snapshot Tests ---

TEST_F(VersioningTest, SnapshotBasic) {
    ASSERT_TRUE(db_.put("key", "original").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    ASSERT_TRUE(db_.createSnapshot(snapshot).ok());
    EXPECT_GT(snapshot->version(), 0u);

    // Modify after snapshot
    ASSERT_TRUE(db_.put("key", "modified").ok());

    // Snapshot still sees original
    std::string snap_value;
    ASSERT_TRUE(snapshot->get("key", snap_value).ok());
    EXPECT_EQ(snap_value, "original");

    // Current DB sees modified
    std::string current_value;
    ASSERT_TRUE(db_.get("key", current_value).ok());
    EXPECT_EQ(current_value, "modified");

    db_.releaseSnapshot(std::move(snapshot));
}

TEST_F(VersioningTest, SnapshotGetWithVersion) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    ASSERT_TRUE(db_.createSnapshot(snapshot).ok());

    std::string value;
    uint64_t entry_version;
    ASSERT_TRUE(snapshot->get("key", value, entry_version).ok());
    EXPECT_EQ(value, "value");
    EXPECT_GT(entry_version, 0u);
    EXPECT_LE(entry_version, snapshot->version());

    db_.releaseSnapshot(std::move(snapshot));
}

TEST_F(VersioningTest, SnapshotExists) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    ASSERT_TRUE(db_.createSnapshot(snapshot).ok());

    bool exists;
    ASSERT_TRUE(snapshot->exists("key", exists).ok());
    EXPECT_TRUE(exists);

    ASSERT_TRUE(snapshot->exists("nonexistent", exists).ok());
    EXPECT_FALSE(exists);

    db_.releaseSnapshot(std::move(snapshot));
}

TEST_F(VersioningTest, SnapshotSeesDeletedKey) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    ASSERT_TRUE(db_.createSnapshot(snapshot).ok());

    ASSERT_TRUE(db_.remove("key").ok());

    // Snapshot still sees the key
    std::string value;
    ASSERT_TRUE(snapshot->get("key", value).ok());
    EXPECT_EQ(value, "value");

    // Current DB doesn't see it
    EXPECT_TRUE(db_.get("key", value).isNotFound());

    db_.releaseSnapshot(std::move(snapshot));
}

TEST_F(VersioningTest, SnapshotSeesNewKeyAsNotFound) {
    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    ASSERT_TRUE(db_.createSnapshot(snapshot).ok());

    ASSERT_TRUE(db_.put("new_key", "value").ok());

    // Snapshot doesn't see new key
    std::string value;
    EXPECT_TRUE(snapshot->get("new_key", value).isNotFound());

    // Current DB sees it
    ASSERT_TRUE(db_.get("new_key", value).ok());
    EXPECT_EQ(value, "value");

    db_.releaseSnapshot(std::move(snapshot));
}

TEST_F(VersioningTest, MultipleConcurrentSnapshots) {
    ASSERT_TRUE(db_.put("key", "v1").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snap1;
    ASSERT_TRUE(db_.createSnapshot(snap1).ok());

    ASSERT_TRUE(db_.put("key", "v2").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snap2;
    ASSERT_TRUE(db_.createSnapshot(snap2).ok());

    ASSERT_TRUE(db_.put("key", "v3").ok());

    // Each snapshot sees different values
    std::string val1, val2, val_current;
    ASSERT_TRUE(snap1->get("key", val1).ok());
    ASSERT_TRUE(snap2->get("key", val2).ok());
    ASSERT_TRUE(db_.get("key", val_current).ok());

    EXPECT_EQ(val1, "v1");
    EXPECT_EQ(val2, "v2");
    EXPECT_EQ(val_current, "v3");

    // Snapshot versions should be ordered
    EXPECT_LT(snap1->version(), snap2->version());

    db_.releaseSnapshot(std::move(snap1));
    db_.releaseSnapshot(std::move(snap2));
}

TEST_F(VersioningTest, SnapshotIsValid) {
    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    ASSERT_TRUE(db_.createSnapshot(snapshot).ok());

    EXPECT_TRUE(snapshot->isValid());

    // After release, snapshot should be invalid (moved from)
    auto* raw_ptr = snapshot.get();
    db_.releaseSnapshot(std::move(snapshot));

    // snapshot is now nullptr after move
    EXPECT_EQ(snapshot.get(), nullptr);
}

TEST_F(VersioningTest, SnapshotAfterManyWrites) {
    // Write many versions
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(db_.put("key", "value" + std::to_string(i)).ok());
    }

    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    ASSERT_TRUE(db_.createSnapshot(snapshot).ok());

    // Write more
    for (int i = 100; i < 200; ++i) {
        ASSERT_TRUE(db_.put("key", "value" + std::to_string(i)).ok());
    }

    // Snapshot sees value99
    std::string value;
    ASSERT_TRUE(snapshot->get("key", value).ok());
    EXPECT_EQ(value, "value99");

    // Current sees value199
    ASSERT_TRUE(db_.get("key", value).ok());
    EXPECT_EQ(value, "value199");

    db_.releaseSnapshot(std::move(snapshot));
}
