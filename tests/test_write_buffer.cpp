#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

#include "internal/write_buffer.h"

using kvlite::internal::WriteBuffer;

TEST(WriteBuffer, PutAndGetSingleEntry) {
    WriteBuffer wb;
    wb.put("key1", 1, "value1", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("key1", value, version, tombstone));
    EXPECT_EQ(value, "value1");
    EXPECT_EQ(version, 1u);
    EXPECT_FALSE(tombstone);
}

TEST(WriteBuffer, GetMissing) {
    WriteBuffer wb;
    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.get("missing", value, version, tombstone));
}

TEST(WriteBuffer, GetReturnsLatestVersion) {
    WriteBuffer wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key1", 5, "v5", false);
    wb.put("key1", 3, "v3", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    // Latest = highest version (5), regardless of insertion order
    ASSERT_TRUE(wb.get("key1", value, version, tombstone));
    EXPECT_EQ(value, "v5");
    EXPECT_EQ(version, 5u);
}

TEST(WriteBuffer, Tombstone) {
    WriteBuffer wb;
    wb.put("key1", 1, "", true);

    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("key1", value, version, tombstone));
    EXPECT_TRUE(tombstone);
    EXPECT_EQ(version, 1u);
}

TEST(WriteBuffer, GetByVersionExact) {
    WriteBuffer wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key1", 3, "v3", false);
    wb.put("key1", 5, "v5", false);

    std::string value;
    uint64_t version;
    bool tombstone;

    ASSERT_TRUE(wb.getByVersion("key1", 3, value, version, tombstone));
    EXPECT_EQ(value, "v3");
    EXPECT_EQ(version, 3u);
}

TEST(WriteBuffer, GetByVersionBetween) {
    WriteBuffer wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key1", 3, "v3", false);
    wb.put("key1", 5, "v5", false);

    std::string value;
    uint64_t version;
    bool tombstone;

    ASSERT_TRUE(wb.getByVersion("key1", 4, value, version, tombstone));
    EXPECT_EQ(value, "v3");
    EXPECT_EQ(version, 3u);
}

TEST(WriteBuffer, GetByVersionTooLow) {
    WriteBuffer wb;
    wb.put("key1", 5, "v5", false);

    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.getByVersion("key1", 3, value, version, tombstone));
}

TEST(WriteBuffer, GetByVersionMissingKey) {
    WriteBuffer wb;
    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.getByVersion("missing", 100, value, version, tombstone));
}

TEST(WriteBuffer, ForEach) {
    WriteBuffer wb;
    wb.put("a", 1, "va", false);
    wb.put("b", 2, "vb", false);
    wb.put("a", 3, "va2", false);

    std::vector<std::pair<std::string, size_t>> collected;
    wb.forEach([&](const std::string& key, const std::vector<WriteBuffer::Entry>& entries) {
        collected.push_back({key, entries.size()});
    });

    ASSERT_EQ(collected.size(), 2u);

    // Sort for deterministic comparison
    std::sort(collected.begin(), collected.end());
    EXPECT_EQ(collected[0].first, "a");
    EXPECT_EQ(collected[0].second, 2u);
    EXPECT_EQ(collected[1].first, "b");
    EXPECT_EQ(collected[1].second, 1u);
}

TEST(WriteBuffer, ClearResetsEverything) {
    WriteBuffer wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key2", 2, "v2", false);

    EXPECT_EQ(wb.keyCount(), 2u);
    EXPECT_EQ(wb.entryCount(), 2u);
    EXPECT_FALSE(wb.empty());
    EXPECT_GT(wb.memoryUsage(), 0u);

    wb.clear();

    EXPECT_EQ(wb.keyCount(), 0u);
    EXPECT_EQ(wb.entryCount(), 0u);
    EXPECT_TRUE(wb.empty());
    EXPECT_EQ(wb.memoryUsage(), 0u);

    std::string value;
    uint64_t version;
    bool tombstone;
    EXPECT_FALSE(wb.get("key1", value, version, tombstone));
}

TEST(WriteBuffer, Statistics) {
    WriteBuffer wb;
    EXPECT_TRUE(wb.empty());
    EXPECT_EQ(wb.keyCount(), 0u);
    EXPECT_EQ(wb.entryCount(), 0u);

    wb.put("k1", 1, "val1", false);
    EXPECT_EQ(wb.keyCount(), 1u);
    EXPECT_EQ(wb.entryCount(), 1u);
    EXPECT_FALSE(wb.empty());

    wb.put("k1", 2, "val2", false);
    EXPECT_EQ(wb.keyCount(), 1u);
    EXPECT_EQ(wb.entryCount(), 2u);

    wb.put("k2", 3, "val3", false);
    EXPECT_EQ(wb.keyCount(), 2u);
    EXPECT_EQ(wb.entryCount(), 3u);
}

TEST(WriteBuffer, ManyKeys) {
    WriteBuffer wb;
    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        wb.put("key_" + std::to_string(i), static_cast<uint64_t>(i), "val_" + std::to_string(i), false);
    }
    EXPECT_EQ(wb.keyCount(), static_cast<size_t>(N));
    EXPECT_EQ(wb.entryCount(), static_cast<size_t>(N));

    // Spot check
    std::string value;
    uint64_t version;
    bool tombstone;
    ASSERT_TRUE(wb.get("key_42", value, version, tombstone));
    EXPECT_EQ(value, "val_42");
    EXPECT_EQ(version, 42u);
}

TEST(WriteBuffer, ConcurrentPuts) {
    WriteBuffer wb;
    const int kThreads = 4;
    const int kPerThread = 5000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&wb, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                wb.put(key, static_cast<uint64_t>(i), "v" + std::to_string(i), false);
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(wb.keyCount(), static_cast<size_t>(kThreads * kPerThread));
    EXPECT_EQ(wb.entryCount(), static_cast<size_t>(kThreads * kPerThread));

    // Verify all keys readable
    std::string value;
    uint64_t version;
    bool tombstone;
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kPerThread; i += 500) {
            std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
            ASSERT_TRUE(wb.get(key, value, version, tombstone));
        }
    }
}

TEST(WriteBuffer, ConcurrentPutsSameKey) {
    WriteBuffer wb;
    const int kThreads = 4;
    const int kPerThread = 1000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&wb, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                uint64_t ver = static_cast<uint64_t>(t * kPerThread + i);
                wb.put("shared_key", ver, "v" + std::to_string(ver), false);
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(wb.keyCount(), 1u);
    EXPECT_EQ(wb.entryCount(), static_cast<size_t>(kThreads * kPerThread));
}
