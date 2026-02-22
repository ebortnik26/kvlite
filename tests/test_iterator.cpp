// Iterator tests for kvlite
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include <filesystem>
#include <string>
#include <set>
#include <map>

namespace fs = std::filesystem;

class IteratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "kvlite_test_iterator";
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

TEST_F(IteratorTest, EmptyDatabase) {
    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::string key, value;
    kvlite::Status s = iter->next(key, value);
    EXPECT_TRUE(s.isNotFound());
}

TEST_F(IteratorTest, SingleKey) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::string key, value;
    ASSERT_TRUE(iter->next(key, value).ok());
    EXPECT_EQ(key, "key");
    EXPECT_EQ(value, "value");

    // No more entries
    EXPECT_TRUE(iter->next(key, value).isNotFound());
}

TEST_F(IteratorTest, MultipleKeys) {
    std::map<std::string, std::string> expected;
    for (int i = 0; i < 100; ++i) {
        std::string k = "key" + std::to_string(i);
        std::string v = "value" + std::to_string(i);
        ASSERT_TRUE(db_.put(k, v).ok());
        expected[k] = v;
    }

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::map<std::string, std::string> found;
    std::string key, value;
    while (iter->next(key, value).ok()) {
        found[key] = value;
    }

    EXPECT_EQ(found, expected);
}

TEST_F(IteratorTest, IteratorWithVersion) {
    ASSERT_TRUE(db_.put("key1", "value1").ok());
    ASSERT_TRUE(db_.put("key2", "value2").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::string key, value;
    uint64_t version;

    ASSERT_TRUE(iter->next(key, value, version).ok());
    EXPECT_GT(version, 0u);
    EXPECT_LE(version, iter->snapshot().version());

    ASSERT_TRUE(iter->next(key, value, version).ok());
    EXPECT_GT(version, 0u);
    EXPECT_LE(version, iter->snapshot().version());
}

TEST_F(IteratorTest, OnlyLatestVersionPerKey) {
    // Write multiple versions of the same key
    ASSERT_TRUE(db_.put("key", "v1").ok());
    ASSERT_TRUE(db_.put("key", "v2").ok());
    ASSERT_TRUE(db_.put("key", "v3").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::string key, value;
    int count = 0;
    while (iter->next(key, value).ok()) {
        EXPECT_EQ(key, "key");
        EXPECT_EQ(value, "v3");  // Latest version only
        count++;
    }
    EXPECT_EQ(count, 1);  // Only one entry per key
}

TEST_F(IteratorTest, SkipsDeletedKeys) {
    ASSERT_TRUE(db_.put("key1", "value1").ok());
    ASSERT_TRUE(db_.put("key2", "value2").ok());
    ASSERT_TRUE(db_.put("key3", "value3").ok());
    ASSERT_TRUE(db_.remove("key2").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::set<std::string> found_keys;
    std::string key, value;
    while (iter->next(key, value).ok()) {
        found_keys.insert(key);
    }

    EXPECT_EQ(found_keys.count("key1"), 1u);
    EXPECT_EQ(found_keys.count("key2"), 0u);  // Deleted
    EXPECT_EQ(found_keys.count("key3"), 1u);
}

TEST_F(IteratorTest, Snapshot) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    EXPECT_GT(iter->snapshot().version(), 0u);
    EXPECT_TRUE(iter->snapshot().isValid());
}

TEST_F(IteratorTest, ConsistentSnapshot) {
    ASSERT_TRUE(db_.put("key1", "initial1").ok());
    ASSERT_TRUE(db_.put("key2", "initial2").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    // Modify database after iterator creation
    ASSERT_TRUE(db_.put("key1", "modified1").ok());
    ASSERT_TRUE(db_.put("key3", "new3").ok());
    ASSERT_TRUE(db_.remove("key2").ok());

    // Iterator should see original state
    std::map<std::string, std::string> found;
    std::string key, value;
    while (iter->next(key, value).ok()) {
        found[key] = value;
    }

    EXPECT_EQ(found["key1"], "initial1");  // Not modified
    EXPECT_EQ(found["key2"], "initial2");  // Not deleted
    EXPECT_EQ(found.count("key3"), 0u);    // Not added yet
}

TEST_F(IteratorTest, LargeDataset) {
    const int num_keys = 10000;
    std::set<std::string> expected_keys;

    for (int i = 0; i < num_keys; ++i) {
        std::string k = "key" + std::to_string(i);
        ASSERT_TRUE(db_.put(k, "value" + std::to_string(i)).ok());
        expected_keys.insert(k);
    }

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::set<std::string> found_keys;
    std::string key, value;
    while (iter->next(key, value).ok()) {
        found_keys.insert(key);
    }

    EXPECT_EQ(found_keys, expected_keys);
}

TEST_F(IteratorTest, IteratorAfterReopen) {
    ASSERT_TRUE(db_.put("key1", "value1").ok());
    ASSERT_TRUE(db_.put("key2", "value2").ok());

    ASSERT_TRUE(db_.close().ok());

    kvlite::Options opts;
    opts.create_if_missing = false;
    ASSERT_TRUE(db_.open(test_dir_.string(), opts).ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::set<std::string> found_keys;
    std::string key, value;
    while (iter->next(key, value).ok()) {
        found_keys.insert(key);
    }

    EXPECT_EQ(found_keys.size(), 2u);
    EXPECT_EQ(found_keys.count("key1"), 1u);
    EXPECT_EQ(found_keys.count("key2"), 1u);
}

TEST_F(IteratorTest, MultipleIterators) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter1;
    std::unique_ptr<kvlite::DB::Iterator> iter2;

    ASSERT_TRUE(db_.createIterator(iter1).ok());
    ASSERT_TRUE(db_.createIterator(iter2).ok());

    std::string key, value;

    ASSERT_TRUE(iter1->next(key, value).ok());
    EXPECT_EQ(value, "value");

    ASSERT_TRUE(iter2->next(key, value).ok());
    EXPECT_EQ(value, "value");
}

TEST_F(IteratorTest, IteratorMoveSemantics) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    // Move iterator
    std::unique_ptr<kvlite::DB::Iterator> iter2 = std::move(iter);

    std::string key, value;
    ASSERT_TRUE(iter2->next(key, value).ok());
    EXPECT_EQ(key, "key");
    EXPECT_EQ(value, "value");
}

// --- Snapshot-based iterator tests ---

TEST_F(IteratorTest, SnapshotBasedIteratorSeesSnapshotState) {
    ASSERT_TRUE(db_.put("key1", "before_snap").ok());
    ASSERT_TRUE(db_.put("key2", "before_snap").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snap;
    ASSERT_TRUE(db_.createSnapshot(snap).ok());

    // Write more data after the snapshot
    ASSERT_TRUE(db_.put("key1", "after_snap").ok());
    ASSERT_TRUE(db_.put("key3", "after_snap").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(*snap, iter).ok());

    std::map<std::string, std::string> found;
    std::string key, value;
    while (iter->next(key, value).ok()) {
        found[key] = value;
    }

    EXPECT_EQ(found["key1"], "before_snap");  // Not the post-snapshot version
    EXPECT_EQ(found["key2"], "before_snap");
    EXPECT_EQ(found.count("key3"), 0u);       // Written after snapshot
}

TEST_F(IteratorTest, SnapshotBasedIteratorSnapshot) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snap;
    ASSERT_TRUE(db_.createSnapshot(snap).ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(*snap, iter).ok());

    EXPECT_EQ(iter->snapshot().version(), snap->version());
}

TEST_F(IteratorTest, SnapshotBasedIteratorDoesNotReleaseSnapshot) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snap;
    ASSERT_TRUE(db_.createSnapshot(snap).ok());

    {
        std::unique_ptr<kvlite::DB::Iterator> iter;
        ASSERT_TRUE(db_.createIterator(*snap, iter).ok());

        std::string key, value;
        ASSERT_TRUE(iter->next(key, value).ok());
        // iter destroyed here
    }

    // Snapshot should still be valid and usable after iterator destruction
    EXPECT_TRUE(snap->isValid());
    std::string value;
    ASSERT_TRUE(snap->get("key", value).ok());
    EXPECT_EQ(value, "value");
}

TEST_F(IteratorTest, SnapshotBasedIteratorDeduplicates) {
    // Write multiple versions of the same key
    ASSERT_TRUE(db_.put("key", "v1").ok());
    ASSERT_TRUE(db_.put("key", "v2").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snap;
    ASSERT_TRUE(db_.createSnapshot(snap).ok());

    ASSERT_TRUE(db_.put("key", "v3").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(*snap, iter).ok());

    std::string key, value;
    int count = 0;
    while (iter->next(key, value).ok()) {
        EXPECT_EQ(key, "key");
        EXPECT_EQ(value, "v2");  // Latest version at snapshot time
        count++;
    }
    EXPECT_EQ(count, 1);
}

TEST_F(IteratorTest, DefaultAndSnapshotIteratorsCoexist) {
    ASSERT_TRUE(db_.put("key1", "v1").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snap;
    ASSERT_TRUE(db_.createSnapshot(snap).ok());

    ASSERT_TRUE(db_.put("key1", "v2").ok());
    ASSERT_TRUE(db_.put("key2", "v2").ok());

    // Snapshot-based iterator sees pre-snapshot state
    std::unique_ptr<kvlite::DB::Iterator> snap_iter;
    ASSERT_TRUE(db_.createIterator(*snap, snap_iter).ok());

    // Default iterator sees current state
    std::unique_ptr<kvlite::DB::Iterator> default_iter;
    ASSERT_TRUE(db_.createIterator(default_iter).ok());

    // Collect snapshot iterator results
    std::map<std::string, std::string> snap_found;
    std::string key, value;
    while (snap_iter->next(key, value).ok()) {
        snap_found[key] = value;
    }

    // Collect default iterator results
    std::map<std::string, std::string> default_found;
    while (default_iter->next(key, value).ok()) {
        default_found[key] = value;
    }

    // Snapshot iterator: only key1=v1
    EXPECT_EQ(snap_found.size(), 1u);
    EXPECT_EQ(snap_found["key1"], "v1");

    // Default iterator: key1=v2, key2=v2
    EXPECT_EQ(default_found.size(), 2u);
    EXPECT_EQ(default_found["key1"], "v2");
    EXPECT_EQ(default_found["key2"], "v2");
}
