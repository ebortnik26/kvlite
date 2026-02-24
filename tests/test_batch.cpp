// Batch operation tests for kvlite
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

class BatchTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "kvlite_test_batch";
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

// --- WriteBatch Tests ---

TEST_F(BatchTest, WriteBatchBasic) {
    kvlite::WriteBatch batch;
    batch.put("key1", "value1");
    batch.put("key2", "value2");
    batch.put("key3", "value3");

    ASSERT_TRUE(db_.write(batch).ok());

    std::string value;
    ASSERT_TRUE(db_.get("key1", value).ok());
    EXPECT_EQ(value, "value1");
    ASSERT_TRUE(db_.get("key2", value).ok());
    EXPECT_EQ(value, "value2");
    ASSERT_TRUE(db_.get("key3", value).ok());
    EXPECT_EQ(value, "value3");
}

TEST_F(BatchTest, WriteBatchOverwrite) {
    // Pre-populate
    ASSERT_TRUE(db_.put("key1", "old1").ok());

    kvlite::WriteBatch batch;
    batch.put("key1", "new1");
    batch.put("key2", "value2");

    ASSERT_TRUE(db_.write(batch).ok());

    std::string value;
    ASSERT_TRUE(db_.get("key1", value).ok());
    EXPECT_EQ(value, "new1");

    ASSERT_TRUE(db_.get("key2", value).ok());
    EXPECT_EQ(value, "value2");
}

TEST_F(BatchTest, WriteBatchEmpty) {
    kvlite::WriteBatch batch;
    EXPECT_TRUE(db_.write(batch).ok());
}

TEST_F(BatchTest, WriteBatchSameVersion) {
    kvlite::WriteBatch batch;
    batch.put("key1", "value1");
    batch.put("key2", "value2");
    batch.put("key3", "value3");

    ASSERT_TRUE(db_.write(batch).ok());

    // All keys should have the same version
    std::string v1, v2, v3;
    uint64_t ver1, ver2, ver3;

    ASSERT_TRUE(db_.get("key1", v1, ver1).ok());
    ASSERT_TRUE(db_.get("key2", v2, ver2).ok());
    ASSERT_TRUE(db_.get("key3", v3, ver3).ok());

    EXPECT_EQ(ver1, ver2);
    EXPECT_EQ(ver2, ver3);
}

TEST_F(BatchTest, WriteBatchLarge) {
    kvlite::WriteBatch batch;
    for (int i = 0; i < 1000; ++i) {
        batch.put("key" + std::to_string(i), "value" + std::to_string(i));
    }

    ASSERT_TRUE(db_.write(batch).ok());

    for (int i = 0; i < 1000; ++i) {
        std::string value;
        ASSERT_TRUE(db_.get("key" + std::to_string(i), value).ok());
        EXPECT_EQ(value, "value" + std::to_string(i));
    }
}

// --- ReadBatch Tests ---

TEST_F(BatchTest, ReadBatchBasic) {
    ASSERT_TRUE(db_.put("key1", "value1").ok());
    ASSERT_TRUE(db_.put("key2", "value2").ok());
    ASSERT_TRUE(db_.put("key3", "value3").ok());

    kvlite::ReadBatch batch;
    batch.get("key1");
    batch.get("key2");
    batch.get("key3");

    ASSERT_TRUE(db_.read(batch).ok());

    const auto& results = batch.results();
    ASSERT_EQ(results.size(), 3u);

    EXPECT_TRUE(results[0].status.ok());
    EXPECT_EQ(results[0].key, "key1");
    EXPECT_EQ(results[0].value, "value1");

    EXPECT_TRUE(results[1].status.ok());
    EXPECT_EQ(results[1].key, "key2");
    EXPECT_EQ(results[1].value, "value2");

    EXPECT_TRUE(results[2].status.ok());
    EXPECT_EQ(results[2].key, "key3");
    EXPECT_EQ(results[2].value, "value3");
}

TEST_F(BatchTest, ReadBatchWithMissing) {
    ASSERT_TRUE(db_.put("key1", "value1").ok());

    kvlite::ReadBatch batch;
    batch.get("key1");
    batch.get("nonexistent");

    ASSERT_TRUE(db_.read(batch).ok());

    const auto& results = batch.results();
    ASSERT_EQ(results.size(), 2u);

    EXPECT_TRUE(results[0].status.ok());
    EXPECT_EQ(results[0].value, "value1");

    EXPECT_TRUE(results[1].status.isNotFound());
}

TEST_F(BatchTest, ReadBatchConsistentSnapshot) {
    ASSERT_TRUE(db_.put("key1", "initial1").ok());
    ASSERT_TRUE(db_.put("key2", "initial2").ok());

    kvlite::ReadBatch batch;
    batch.get("key1");
    batch.get("key2");

    // All reads in a batch should see the same snapshot version
    ASSERT_TRUE(db_.read(batch).ok());

    const auto& results = batch.results();
    EXPECT_EQ(results[0].version, results[1].version);
}

TEST_F(BatchTest, ReadBatchEmpty) {
    kvlite::ReadBatch batch;
    EXPECT_TRUE(db_.read(batch).ok());
    EXPECT_TRUE(batch.results().empty());
}

TEST_F(BatchTest, ReadBatchSnapshotVersion) {
    ASSERT_TRUE(db_.put("key", "value").ok());

    kvlite::ReadBatch batch;
    batch.get("key");

    ASSERT_TRUE(db_.read(batch).ok());
    EXPECT_GT(batch.snapshotVersion(), 0u);
}
