// Basic operation tests for kvlite
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

class BasicTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "kvlite_test_basic";
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
    }

    void TearDown() override {
        if (db_.isOpen()) {
            db_.close();
        }
        fs::remove_all(test_dir_);
    }

    kvlite::Status openDB(bool create = true) {
        kvlite::Options opts;
        opts.create_if_missing = create;
        return db_.open(test_dir_.string(), opts);
    }

    fs::path test_dir_;
    kvlite::DB db_;
};

// --- Open/Close Tests ---

TEST_F(BasicTest, OpenCreateIfMissing) {
    ASSERT_TRUE(openDB(true).ok());
    EXPECT_TRUE(db_.isOpen());
}

TEST_F(BasicTest, OpenFailsIfNotExists) {
    kvlite::Options opts;
    opts.create_if_missing = false;
    EXPECT_FALSE(db_.open(test_dir_.string(), opts).ok());
}

TEST_F(BasicTest, CloseAndReopen) {
    ASSERT_TRUE(openDB().ok());

    // Write some data
    ASSERT_TRUE(db_.put("key1", "value1").ok());

    // Close
    ASSERT_TRUE(db_.close().ok());
    EXPECT_FALSE(db_.isOpen());

    // Reopen
    ASSERT_TRUE(openDB().ok());

    // Data should persist
    std::string value;
    ASSERT_TRUE(db_.get("key1", value).ok());
    EXPECT_EQ(value, "value1");
}

// --- Put/Get Tests ---

TEST_F(BasicTest, PutAndGet) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("key", "value").ok());

    std::string value;
    ASSERT_TRUE(db_.get("key", value).ok());
    EXPECT_EQ(value, "value");
}

TEST_F(BasicTest, GetNonExistent) {
    ASSERT_TRUE(openDB().ok());

    std::string value;
    kvlite::Status s = db_.get("nonexistent", value);
    EXPECT_TRUE(s.isNotFound());
}

TEST_F(BasicTest, PutOverwrite) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("key", "value1").ok());
    ASSERT_TRUE(db_.put("key", "value2").ok());

    std::string value;
    ASSERT_TRUE(db_.get("key", value).ok());
    EXPECT_EQ(value, "value2");
}

TEST_F(BasicTest, VersionIsPositive) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("key", "value").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::string key, value;
    uint64_t version;
    ASSERT_TRUE(iter->next(key, value, version).ok());
    EXPECT_EQ(key, "key");
    EXPECT_EQ(value, "value");
    EXPECT_GT(version, 0u);
}

TEST_F(BasicTest, MultipleKeys) {
    ASSERT_TRUE(openDB().ok());

    for (int i = 0; i < 100; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val = "value" + std::to_string(i);
        ASSERT_TRUE(db_.put(key, val).ok());
    }

    for (int i = 0; i < 100; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string expected = "value" + std::to_string(i);
        std::string value;
        ASSERT_TRUE(db_.get(key, value).ok());
        EXPECT_EQ(value, expected);
    }
}

// --- Remove Tests ---

TEST_F(BasicTest, Remove) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("key", "value").ok());
    ASSERT_TRUE(db_.remove("key").ok());

    std::string value;
    EXPECT_TRUE(db_.get("key", value).isNotFound());
}

TEST_F(BasicTest, RemoveNonExistent) {
    ASSERT_TRUE(openDB().ok());

    // Removing non-existent key should succeed (creates tombstone)
    EXPECT_TRUE(db_.remove("nonexistent").ok());
}

TEST_F(BasicTest, PutAfterRemove) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("key", "value1").ok());
    ASSERT_TRUE(db_.remove("key").ok());
    ASSERT_TRUE(db_.put("key", "value2").ok());

    std::string value;
    ASSERT_TRUE(db_.get("key", value).ok());
    EXPECT_EQ(value, "value2");
}

// --- Exists Tests ---

TEST_F(BasicTest, Exists) {
    ASSERT_TRUE(openDB().ok());

    bool exists;
    ASSERT_TRUE(db_.exists("key", exists).ok());
    EXPECT_FALSE(exists);

    ASSERT_TRUE(db_.put("key", "value").ok());
    ASSERT_TRUE(db_.exists("key", exists).ok());
    EXPECT_TRUE(exists);

    ASSERT_TRUE(db_.remove("key").ok());
    ASSERT_TRUE(db_.exists("key", exists).ok());
    EXPECT_FALSE(exists);
}

// --- Large Value Tests ---

TEST_F(BasicTest, LargeValue) {
    ASSERT_TRUE(openDB().ok());

    // 1MB value
    std::string large_value(1024 * 1024, 'x');
    ASSERT_TRUE(db_.put("large", large_value).ok());

    std::string retrieved;
    ASSERT_TRUE(db_.get("large", retrieved).ok());
    EXPECT_EQ(retrieved, large_value);
}

TEST_F(BasicTest, EmptyValue) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("empty", "").ok());

    std::string value;
    ASSERT_TRUE(db_.get("empty", value).ok());
    EXPECT_EQ(value, "");
}

TEST_F(BasicTest, EmptyKey) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("", "value").ok());

    std::string value;
    ASSERT_TRUE(db_.get("", value).ok());
    EXPECT_EQ(value, "value");
}

// --- Binary Data Tests ---

TEST_F(BasicTest, BinaryKey) {
    ASSERT_TRUE(openDB().ok());

    std::string binary_key = "key\x00with\x01null\x02bytes";
    binary_key.resize(20);  // Include null bytes

    ASSERT_TRUE(db_.put(binary_key, "value").ok());

    std::string value;
    ASSERT_TRUE(db_.get(binary_key, value).ok());
    EXPECT_EQ(value, "value");
}

TEST_F(BasicTest, BinaryValue) {
    ASSERT_TRUE(openDB().ok());

    std::string binary_value = "value\x00with\x01null\x02bytes";
    binary_value.resize(25);

    ASSERT_TRUE(db_.put("key", binary_value).ok());

    std::string value;
    ASSERT_TRUE(db_.get("key", value).ok());
    EXPECT_EQ(value, binary_value);
}
