#include <gtest/gtest.h>

#include "internal/delta_hash_table.h"

using namespace kvlite::internal;

// --- DeltaHashTable Tests ---

// Use small config for tests to keep memory usage low
static DeltaHashTable::Config testConfig() {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;      // 16 buckets
    cfg.lslot_bits = 2;       // 4 lslots per bucket
    cfg.bucket_bytes = 512;   // plenty of room
    return cfg;
}

TEST(DeltaHashTable, InsertAndFind) {
    DeltaHashTable dht(testConfig());

    KeyRecord* rec = dht.insert("hello");
    ASSERT_NE(rec, nullptr);
    EXPECT_EQ(rec->key, "hello");
    EXPECT_TRUE(rec->file_ids.empty());

    // Add a file_id
    rec->file_ids.push_back(100);

    KeyRecord* found = dht.find("hello");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->key, "hello");
    ASSERT_EQ(found->file_ids.size(), 1u);
    EXPECT_EQ(found->file_ids[0], 100u);

    EXPECT_EQ(dht.size(), 1u);
}

TEST(DeltaHashTable, FindNonExistent) {
    DeltaHashTable dht(testConfig());
    EXPECT_EQ(dht.find("missing"), nullptr);
}

TEST(DeltaHashTable, InsertDuplicate) {
    DeltaHashTable dht(testConfig());

    KeyRecord* r1 = dht.insert("key1");
    r1->file_ids.push_back(10);

    KeyRecord* r2 = dht.insert("key1");
    EXPECT_EQ(r1, r2);  // same pointer
    EXPECT_EQ(dht.size(), 1u);
    ASSERT_EQ(r2->file_ids.size(), 1u);
}

TEST(DeltaHashTable, Remove) {
    DeltaHashTable dht(testConfig());

    dht.insert("key1");
    dht.insert("key2");
    EXPECT_EQ(dht.size(), 2u);

    EXPECT_TRUE(dht.remove("key1"));
    EXPECT_EQ(dht.size(), 1u);
    EXPECT_EQ(dht.find("key1"), nullptr);
    EXPECT_NE(dht.find("key2"), nullptr);
}

TEST(DeltaHashTable, RemoveNonExistent) {
    DeltaHashTable dht(testConfig());
    EXPECT_FALSE(dht.remove("missing"));
}

TEST(DeltaHashTable, Clear) {
    DeltaHashTable dht(testConfig());

    for (int i = 0; i < 10; ++i) {
        dht.insert("key" + std::to_string(i));
    }
    EXPECT_EQ(dht.size(), 10u);

    dht.clear();
    EXPECT_EQ(dht.size(), 0u);

    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(dht.find("key" + std::to_string(i)), nullptr);
    }
}

TEST(DeltaHashTable, ForEach) {
    DeltaHashTable dht(testConfig());

    dht.insert("a")->file_ids.push_back(10);
    dht.insert("b")->file_ids.push_back(20);
    dht.insert("c")->file_ids.push_back(30);

    std::map<std::string, std::vector<uint32_t>> collected;
    dht.forEach([&](const KeyRecord& rec) {
        collected[rec.key] = rec.file_ids;
    });

    EXPECT_EQ(collected.size(), 3u);
    EXPECT_EQ(collected["a"][0], 10u);
    EXPECT_EQ(collected["b"][0], 20u);
    EXPECT_EQ(collected["c"][0], 30u);
}

TEST(DeltaHashTable, ManyKeys) {
    DeltaHashTable dht(testConfig());

    const int N = 200;
    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        KeyRecord* rec = dht.insert(key);
        rec->file_ids.push_back(static_cast<uint32_t>(i * 10));
    }

    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    // Verify all keys are findable
    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        KeyRecord* rec = dht.find(key);
        ASSERT_NE(rec, nullptr) << "key not found: " << key;
        EXPECT_EQ(rec->key, key);
        ASSERT_EQ(rec->file_ids.size(), 1u);
        EXPECT_EQ(rec->file_ids[0], static_cast<uint32_t>(i * 10));
    }
}

TEST(DeltaHashTable, InsertAfterRemove) {
    DeltaHashTable dht(testConfig());

    dht.insert("key1")->file_ids.push_back(10);
    EXPECT_TRUE(dht.remove("key1"));
    EXPECT_EQ(dht.find("key1"), nullptr);

    // Re-insert
    KeyRecord* rec = dht.insert("key1");
    ASSERT_NE(rec, nullptr);
    EXPECT_TRUE(rec->file_ids.empty());  // fresh record
    EXPECT_EQ(dht.size(), 1u);
}

TEST(DeltaHashTable, MemoryUsage) {
    DeltaHashTable dht(testConfig());
    size_t empty_usage = dht.memoryUsage();
    EXPECT_GT(empty_usage, 0u);

    for (int i = 0; i < 50; ++i) {
        dht.insert("key_" + std::to_string(i));
    }

    EXPECT_GT(dht.memoryUsage(), empty_usage);
}

TEST(DeltaHashTable, EmptyKeyAndBinaryKey) {
    DeltaHashTable dht(testConfig());

    // Empty key
    KeyRecord* r1 = dht.insert("");
    ASSERT_NE(r1, nullptr);
    EXPECT_EQ(r1->key, "");

    // Binary key with null bytes
    std::string binary_key("\x00\x01\x02\x03", 4);
    KeyRecord* r2 = dht.insert(binary_key);
    ASSERT_NE(r2, nullptr);
    EXPECT_EQ(r2->key, binary_key);

    EXPECT_EQ(dht.size(), 2u);
    EXPECT_NE(dht.find(""), nullptr);
    EXPECT_NE(dht.find(binary_key), nullptr);
}

// --- L1Index via DHT Tests ---

#include "internal/l1_index.h"
#include <filesystem>

using kvlite::Status;

TEST(L1IndexDHT, PutAndGetLatest) {
    L1Index index;

    index.put("key1", 100);
    index.put("key1", 200);
    index.put("key1", 300);

    uint32_t file_id;
    EXPECT_TRUE(index.getLatest("key1", file_id));
    EXPECT_EQ(file_id, 300u);  // latest (prepended last)

    // Check all file_ids
    auto* fids = index.getFileIds("key1");
    ASSERT_NE(fids, nullptr);
    ASSERT_EQ(fids->size(), 3u);
    EXPECT_EQ((*fids)[0], 300u);  // latest first
    EXPECT_EQ((*fids)[1], 200u);
    EXPECT_EQ((*fids)[2], 100u);
}

TEST(L1IndexDHT, PutDuplicateFileId) {
    L1Index index;

    index.put("key1", 100);
    index.put("key1", 200);
    // Putting 200 again should be a no-op (already at front)
    index.put("key1", 200);

    auto* fids = index.getFileIds("key1");
    ASSERT_NE(fids, nullptr);
    ASSERT_EQ(fids->size(), 2u);
    EXPECT_EQ(index.entryCount(), 2u);
}

TEST(L1IndexDHT, GetLatest) {
    L1Index index;
    index.put("key1", 100);
    index.put("key1", 200);

    uint32_t file_id;
    EXPECT_TRUE(index.getLatest("key1", file_id));
    EXPECT_EQ(file_id, 200u);

    EXPECT_FALSE(index.getLatest("missing", file_id));
}

TEST(L1IndexDHT, Contains) {
    L1Index index;
    EXPECT_FALSE(index.contains("key1"));
    index.put("key1", 100);
    EXPECT_TRUE(index.contains("key1"));
}

TEST(L1IndexDHT, Remove) {
    L1Index index;
    index.put("key1", 100);
    index.put("key1", 200);
    EXPECT_EQ(index.entryCount(), 2u);

    index.remove("key1");
    EXPECT_FALSE(index.contains("key1"));
    EXPECT_EQ(index.keyCount(), 0u);
    EXPECT_EQ(index.entryCount(), 0u);
}

TEST(L1IndexDHT, RemoveFile) {
    L1Index index;
    index.put("key1", 100);
    index.put("key1", 200);
    index.put("key1", 300);

    index.removeFile("key1", 200);

    auto* fids = index.getFileIds("key1");
    ASSERT_NE(fids, nullptr);
    ASSERT_EQ(fids->size(), 2u);
    EXPECT_EQ((*fids)[0], 300u);
    EXPECT_EQ((*fids)[1], 100u);
    EXPECT_EQ(index.entryCount(), 2u);
}

TEST(L1IndexDHT, RemoveFileRemovesKey) {
    L1Index index;
    index.put("key1", 100);
    index.removeFile("key1", 100);
    EXPECT_FALSE(index.contains("key1"));
    EXPECT_EQ(index.keyCount(), 0u);
    EXPECT_EQ(index.entryCount(), 0u);
}

TEST(L1IndexDHT, ForEach) {
    L1Index index;
    index.put("a", 10);
    index.put("b", 20);

    std::map<std::string, size_t> seen;
    index.forEach([&](const std::string& key, const std::vector<uint32_t>& file_ids) {
        seen[key] = file_ids.size();
    });

    EXPECT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen["a"], 1u);
    EXPECT_EQ(seen["b"], 1u);
}

TEST(L1IndexDHT, Snapshot) {
    std::string path = "/tmp/test_l1_snapshot_dht.dat";

    {
        L1Index index;
        index.put("key1", 100);
        index.put("key1", 200);
        index.put("key2", 300);

        Status s = index.saveSnapshot(path);
        ASSERT_TRUE(s.ok()) << s.toString();
    }

    {
        L1Index index;
        Status s = index.loadSnapshot(path);
        ASSERT_TRUE(s.ok()) << s.toString();

        EXPECT_EQ(index.keyCount(), 2u);
        EXPECT_EQ(index.entryCount(), 3u);

        uint32_t file_id;
        EXPECT_TRUE(index.getLatest("key1", file_id));
        EXPECT_EQ(file_id, 200u);

        EXPECT_TRUE(index.getLatest("key2", file_id));
        EXPECT_EQ(file_id, 300u);
    }

    std::filesystem::remove(path);
}

TEST(L1IndexDHT, Clear) {
    L1Index index;
    for (int i = 0; i < 50; ++i) {
        index.put("key" + std::to_string(i), i * 10);
    }
    EXPECT_EQ(index.keyCount(), 50u);

    index.clear();
    EXPECT_EQ(index.keyCount(), 0u);
    EXPECT_EQ(index.entryCount(), 0u);
}

// --- Concurrency Tests ---

#include <thread>

TEST(DeltaHashTable, ConcurrentInsertAndFind) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 10;     // 1024 buckets
    cfg.lslot_bits = 3;       // 8 lslots
    cfg.bucket_bytes = 512;
    DeltaHashTable dht(cfg);

    const int NUM_THREADS = 4;
    const int KEYS_PER_THREAD = 500;

    // Phase 1: concurrent inserts
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&dht, t]() {
            for (int i = 0; i < KEYS_PER_THREAD; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                KeyRecord* rec = dht.insert(key);
                ASSERT_NE(rec, nullptr);
                rec->file_ids.push_back(static_cast<uint32_t>(t));
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(dht.size(), static_cast<size_t>(NUM_THREADS * KEYS_PER_THREAD));

    // Phase 2: concurrent finds
    threads.clear();
    std::atomic<int> found_count{0};
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&dht, &found_count, t]() {
            for (int i = 0; i < KEYS_PER_THREAD; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                if (dht.find(key)) {
                    found_count.fetch_add(1);
                }
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(found_count.load(), NUM_THREADS * KEYS_PER_THREAD);
}

TEST(DeltaHashTable, ConcurrentInsertSameKeys) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 8;
    cfg.lslot_bits = 3;
    cfg.bucket_bytes = 512;
    DeltaHashTable dht(cfg);

    const int NUM_THREADS = 4;
    const int NUM_KEYS = 200;

    // Multiple threads inserting the same keys - should not crash or corrupt
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&dht]() {
            for (int i = 0; i < NUM_KEYS; ++i) {
                std::string key = "shared_key_" + std::to_string(i);
                KeyRecord* rec = dht.insert(key);
                ASSERT_NE(rec, nullptr);
                EXPECT_EQ(rec->key, key);
            }
        });
    }
    for (auto& t : threads) t.join();

    // Each key should exist exactly once
    EXPECT_EQ(dht.size(), static_cast<size_t>(NUM_KEYS));
}

TEST(DeltaHashTable, ConcurrentInsertAndRemove) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 8;
    cfg.lslot_bits = 3;
    cfg.bucket_bytes = 512;
    DeltaHashTable dht(cfg);

    const int NUM_KEYS = 200;

    // Pre-populate
    for (int i = 0; i < NUM_KEYS; ++i) {
        dht.insert("key_" + std::to_string(i));
    }

    // Concurrently: thread 0 removes even keys, thread 1 inserts new keys
    std::thread remover([&dht]() {
        for (int i = 0; i < NUM_KEYS; i += 2) {
            dht.remove("key_" + std::to_string(i));
        }
    });

    std::thread inserter([&dht]() {
        for (int i = NUM_KEYS; i < NUM_KEYS * 2; ++i) {
            dht.insert("key_" + std::to_string(i));
        }
    });

    remover.join();
    inserter.join();

    // Verify odd keys still exist
    for (int i = 1; i < NUM_KEYS; i += 2) {
        EXPECT_NE(dht.find("key_" + std::to_string(i)), nullptr)
            << "missing odd key: " << i;
    }

    // Verify new keys exist
    for (int i = NUM_KEYS; i < NUM_KEYS * 2; ++i) {
        EXPECT_NE(dht.find("key_" + std::to_string(i)), nullptr)
            << "missing new key: " << i;
    }
}

TEST(L1IndexDHT, LargeScale) {
    L1Index index;
    const int N = 1000;

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        index.put(key, static_cast<uint32_t>(i * 10));
    }

    EXPECT_EQ(index.keyCount(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint32_t file_id;
        ASSERT_TRUE(index.getLatest(key, file_id));
        EXPECT_EQ(file_id, static_cast<uint32_t>(i * 10));
    }
}
