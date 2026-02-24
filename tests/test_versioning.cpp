// Versioning and snapshot tests for kvlite
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static kvlite::ReadOptions snapOpts(const kvlite::Snapshot& snap) {
    kvlite::ReadOptions opts;
    opts.snapshot = &snap;
    return opts;
}

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
    // Write distinct keys so iterator can return all of them.
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(db_.put("key" + std::to_string(i),
                            "value" + std::to_string(i)).ok());
    }

    // Collect versions via iterator.
    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::map<int, uint64_t> idx_to_ver;
    std::string key, value;
    uint64_t version;
    while (iter->next(key, value, version).ok()) {
        int idx = std::stoi(key.substr(3));  // "key0" → 0
        idx_to_ver[idx] = version;
    }
    ASSERT_EQ(idx_to_ver.size(), 10u);

    // Versions should be strictly increasing with insertion order.
    for (int i = 1; i < 10; ++i) {
        EXPECT_GT(idx_to_ver[i], idx_to_ver[i - 1]);
    }
}

TEST_F(VersioningTest, DifferentKeysGetDifferentVersions) {
    ASSERT_TRUE(db_.put("key1", "value1").ok());
    ASSERT_TRUE(db_.put("key2", "value2").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::map<std::string, uint64_t> key_versions;
    std::string key, value;
    uint64_t version;
    while (iter->next(key, value, version).ok()) {
        key_versions[key] = version;
    }

    ASSERT_EQ(key_versions.size(), 2u);
    EXPECT_NE(key_versions["key1"], key_versions["key2"]);
    EXPECT_LT(key_versions["key1"], key_versions["key2"]);
}

// --- Snapshot-based point-in-time read tests ---

TEST_F(VersioningTest, SnapshotPointInTimeBasic) {
    ASSERT_TRUE(db_.put("key", "v1").ok());
    kvlite::Snapshot snap1 = db_.createSnapshot();

    ASSERT_TRUE(db_.put("key", "v2").ok());
    kvlite::Snapshot snap2 = db_.createSnapshot();

    ASSERT_TRUE(db_.put("key", "v3").ok());
    kvlite::Snapshot snap3 = db_.createSnapshot();

    // Each snapshot sees the value that was current at its creation time.
    std::string value;

    ASSERT_TRUE(db_.get("key", value, snapOpts(snap1)).ok());
    EXPECT_EQ(value, "v1");

    ASSERT_TRUE(db_.get("key", value, snapOpts(snap2)).ok());
    EXPECT_EQ(value, "v2");

    ASSERT_TRUE(db_.get("key", value, snapOpts(snap3)).ok());
    EXPECT_EQ(value, "v3");

    db_.releaseSnapshot(snap1);
    db_.releaseSnapshot(snap2);
    db_.releaseSnapshot(snap3);
}

TEST_F(VersioningTest, SnapshotPointInTimeWithVersionOutput) {
    ASSERT_TRUE(db_.put("key", "v1").ok());
    uint64_t ver1 = db_.getLatestVersion();
    kvlite::Snapshot snap1 = db_.createSnapshot();

    ASSERT_TRUE(db_.put("key", "v2").ok());
    uint64_t ver2 = db_.getLatestVersion();
    kvlite::Snapshot snap2 = db_.createSnapshot();

    // Verify versions via snapshot-based iterators.
    {
        std::unique_ptr<kvlite::DB::Iterator> iter;
        ASSERT_TRUE(db_.createIterator(iter, snapOpts(snap1)).ok());
        std::string key, value;
        uint64_t entry_version;
        ASSERT_TRUE(iter->next(key, value, entry_version).ok());
        EXPECT_EQ(value, "v1");
        EXPECT_EQ(entry_version, ver1);
    }

    {
        std::unique_ptr<kvlite::DB::Iterator> iter;
        ASSERT_TRUE(db_.createIterator(iter, snapOpts(snap2)).ok());
        std::string key, value;
        uint64_t entry_version;
        ASSERT_TRUE(iter->next(key, value, entry_version).ok());
        EXPECT_EQ(value, "v2");
        EXPECT_EQ(entry_version, ver2);
    }

    db_.releaseSnapshot(snap1);
    db_.releaseSnapshot(snap2);
}

TEST_F(VersioningTest, SnapshotPointInTimeAfterDelete) {
    ASSERT_TRUE(db_.put("key", "v1").ok());
    kvlite::Snapshot snap1 = db_.createSnapshot();

    // Delete the key. Snapshot taken after delete sees tombstone.
    ASSERT_TRUE(db_.remove("key").ok());
    kvlite::Snapshot snapDel = db_.createSnapshot();

    ASSERT_TRUE(db_.put("key", "v2").ok());
    kvlite::Snapshot snap2 = db_.createSnapshot();

    // Snapshot before delete returns v1
    std::string value;
    ASSERT_TRUE(db_.get("key", value, snapOpts(snap1)).ok());
    EXPECT_EQ(value, "v1");

    // Snapshot after delete sees tombstone -> NotFound
    EXPECT_TRUE(db_.get("key", value, snapOpts(snapDel)).isNotFound());

    // Snapshot after re-put returns v2
    ASSERT_TRUE(db_.get("key", value, snapOpts(snap2)).ok());
    EXPECT_EQ(value, "v2");

    db_.releaseSnapshot(snap1);
    db_.releaseSnapshot(snapDel);
    db_.releaseSnapshot(snap2);
}

// --- Snapshot Tests ---

TEST_F(VersioningTest, SnapshotBasic) {
    ASSERT_TRUE(db_.put("key", "original").ok());

    kvlite::Snapshot snap = db_.createSnapshot();
    EXPECT_GT(snap.version(), 0u);

    // Modify after snapshot
    ASSERT_TRUE(db_.put("key", "modified").ok());

    // Snapshot still sees original
    std::string snap_value;
    ASSERT_TRUE(db_.get("key", snap_value, snapOpts(snap)).ok());
    EXPECT_EQ(snap_value, "original");

    // Current DB sees modified
    std::string current_value;
    ASSERT_TRUE(db_.get("key", current_value).ok());
    EXPECT_EQ(current_value, "modified");

    db_.releaseSnapshot(snap);
}

TEST_F(VersioningTest, SnapshotGetWithVersion) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    kvlite::Snapshot snap = db_.createSnapshot();

    // Verify value via get
    std::string value;
    ASSERT_TRUE(db_.get("key", value, snapOpts(snap)).ok());
    EXPECT_EQ(value, "value");

    // Verify version via snapshot iterator
    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter, snapOpts(snap)).ok());
    std::string key;
    uint64_t entry_version;
    ASSERT_TRUE(iter->next(key, value, entry_version).ok());
    EXPECT_GT(entry_version, 0u);
    EXPECT_LE(entry_version, snap.version());

    db_.releaseSnapshot(snap);
}

TEST_F(VersioningTest, SnapshotSeesDeletedKey) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    kvlite::Snapshot snap = db_.createSnapshot();

    ASSERT_TRUE(db_.remove("key").ok());

    // Snapshot still sees the key
    std::string value;
    ASSERT_TRUE(db_.get("key", value, snapOpts(snap)).ok());
    EXPECT_EQ(value, "value");

    // Current DB doesn't see it
    EXPECT_TRUE(db_.get("key", value).isNotFound());

    db_.releaseSnapshot(snap);
}

TEST_F(VersioningTest, SnapshotSeesNewKeyAsNotFound) {
    kvlite::Snapshot snap = db_.createSnapshot();

    ASSERT_TRUE(db_.put("new_key", "value").ok());

    // Snapshot doesn't see new key
    std::string value;
    EXPECT_TRUE(db_.get("new_key", value, snapOpts(snap)).isNotFound());

    // Current DB sees it
    ASSERT_TRUE(db_.get("new_key", value).ok());
    EXPECT_EQ(value, "value");

    db_.releaseSnapshot(snap);
}

TEST_F(VersioningTest, MultipleConcurrentSnapshots) {
    ASSERT_TRUE(db_.put("key", "v1").ok());

    kvlite::Snapshot snap1 = db_.createSnapshot();

    ASSERT_TRUE(db_.put("key", "v2").ok());

    kvlite::Snapshot snap2 = db_.createSnapshot();

    ASSERT_TRUE(db_.put("key", "v3").ok());

    // Each snapshot sees different values
    std::string val1, val2, val_current;
    ASSERT_TRUE(db_.get("key", val1, snapOpts(snap1)).ok());
    ASSERT_TRUE(db_.get("key", val2, snapOpts(snap2)).ok());
    ASSERT_TRUE(db_.get("key", val_current).ok());

    EXPECT_EQ(val1, "v1");
    EXPECT_EQ(val2, "v2");
    EXPECT_EQ(val_current, "v3");

    // Snapshot versions should be ordered
    EXPECT_LT(snap1.version(), snap2.version());

    db_.releaseSnapshot(snap1);
    db_.releaseSnapshot(snap2);
}

TEST_F(VersioningTest, SnapshotAfterManyWrites) {
    // Write many versions
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(db_.put("key", "value" + std::to_string(i)).ok());
    }

    kvlite::Snapshot snap = db_.createSnapshot();

    // Write more
    for (int i = 100; i < 200; ++i) {
        ASSERT_TRUE(db_.put("key", "value" + std::to_string(i)).ok());
    }

    // Snapshot sees value99
    std::string value;
    ASSERT_TRUE(db_.get("key", value, snapOpts(snap)).ok());
    EXPECT_EQ(value, "value99");

    // Current sees value199
    ASSERT_TRUE(db_.get("key", value).ok());
    EXPECT_EQ(value, "value199");

    db_.releaseSnapshot(snap);
}

// --- Version Retention Tests ---

TEST_F(VersioningTest, OldestVersionPinnedBySnapshot) {
    ASSERT_TRUE(db_.put("key", "v1").ok());

    // Take a snapshot -- pins the oldest version.
    kvlite::Snapshot snap = db_.createSnapshot();
    uint64_t pinned = snap.version();

    // Write more -- versions advance, but oldest stays pinned.
    ASSERT_TRUE(db_.put("key", "v2").ok());
    ASSERT_TRUE(db_.put("key", "v3").ok());

    EXPECT_EQ(db_.getOldestVersion(), pinned);

    db_.releaseSnapshot(snap);

    // After release, oldest advances to latest.
    EXPECT_GT(db_.getOldestVersion(), pinned);
}

TEST_F(VersioningTest, OldestVersionPinnedByEarliestSnapshot) {
    ASSERT_TRUE(db_.put("key", "v1").ok());

    kvlite::Snapshot snap1 = db_.createSnapshot();
    uint64_t pinned1 = snap1.version();

    ASSERT_TRUE(db_.put("key", "v2").ok());

    kvlite::Snapshot snap2 = db_.createSnapshot();
    uint64_t pinned2 = snap2.version();
    EXPECT_GT(pinned2, pinned1);

    ASSERT_TRUE(db_.put("key", "v3").ok());

    // Oldest is pinned by the earliest snapshot.
    EXPECT_EQ(db_.getOldestVersion(), pinned1);

    // Release the earlier snapshot -- oldest advances to snap2.
    db_.releaseSnapshot(snap1);
    EXPECT_EQ(db_.getOldestVersion(), pinned2);

    // Release the last snapshot -- oldest advances past snap2.
    db_.releaseSnapshot(snap2);
    EXPECT_GT(db_.getOldestVersion(), pinned2);
}

TEST_F(VersioningTest, SnapshotRetainsVersionsForReads) {
    // Verify that data at the snapshot version remains readable even
    // after newer writes overwrite the same key.
    ASSERT_TRUE(db_.put("key", "original").ok());

    kvlite::Snapshot snap = db_.createSnapshot();

    // Overwrite the key a few times.
    ASSERT_TRUE(db_.put("key", "overwrite1").ok());
    ASSERT_TRUE(db_.put("key", "overwrite2").ok());

    // The snapshot must still see the original value.
    std::string value;
    ASSERT_TRUE(db_.get("key", value, snapOpts(snap)).ok());
    EXPECT_EQ(value, "original");

    // And the oldest version must be <= the snapshot version.
    EXPECT_LE(db_.getOldestVersion(), snap.version());

    db_.releaseSnapshot(snap);
}
