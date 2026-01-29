#include <gtest/gtest.h>

#include <map>
#include <set>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/delta_hash_table.h"
#include "internal/l1_index.h"

using namespace kvlite::internal;

// --- Elias Gamma Round-Trip Tests ---

TEST(EliasGamma, RoundTrip) {
    // Test a range of values including edge cases.
    std::vector<uint32_t> test_values = {
        1, 2, 3, 4, 5, 6, 7, 8, 15, 16, 17, 31, 32, 100, 255, 256,
        1000, 10000, 65535, 65536, 100000, 1000000, 0x7FFFFFFFu
    };

    // Buffer with padding for BitReader/BitWriter (needs 8 bytes beyond data).
    uint8_t buf[256] = {};

    // Write all values.
    BitWriter writer(buf, 0);
    for (uint32_t v : test_values) {
        writer.writeEliasGamma(v);
    }

    // Read them back.
    BitReader reader(buf, 0);
    for (uint32_t expected : test_values) {
        uint32_t got = reader.readEliasGamma();
        EXPECT_EQ(got, expected) << "mismatch for value " << expected;
    }

    // Positions should match.
    EXPECT_EQ(reader.position(), writer.position());
}

// --- DeltaHashTable Tests ---

// Use small config for tests to keep memory usage low
static DeltaHashTable::Config testConfig() {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;      // 16 buckets
    cfg.lslot_bits = 2;       // 4 lslots per bucket
    cfg.bucket_bytes = 512;   // plenty of room
    return cfg;
}

TEST(DeltaHashTable, AddAndFindFirst) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("hello", 42);

    uint32_t val;
    EXPECT_TRUE(dht.findFirst("hello", val));
    EXPECT_EQ(val, 42u);

    EXPECT_EQ(dht.size(), 1u);
}

TEST(DeltaHashTable, FindNonExistent) {
    DeltaHashTable dht(testConfig());
    uint32_t val;
    EXPECT_FALSE(dht.findFirst("missing", val));
}

TEST(DeltaHashTable, AddDuplicateValue) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("key1", 10);
    dht.addEntry("key1", 10);  // duplicate

    EXPECT_EQ(dht.size(), 1u);

    std::vector<uint32_t> out;
    ASSERT_TRUE(dht.findAll("key1", out));
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], 10u);
}

TEST(DeltaHashTable, AddMultipleValues) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("key1", 10);
    dht.addEntry("key1", 20);
    dht.addEntry("key1", 30);

    EXPECT_EQ(dht.size(), 3u);

    std::vector<uint32_t> out;
    ASSERT_TRUE(dht.findAll("key1", out));
    ASSERT_EQ(out.size(), 3u);
    // Values sorted desc (highest first)
    EXPECT_EQ(out[0], 30u);
    EXPECT_EQ(out[1], 20u);
    EXPECT_EQ(out[2], 10u);
}

TEST(DeltaHashTable, FindFirstReturnsHighest) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("key1", 10);
    dht.addEntry("key1", 30);
    dht.addEntry("key1", 20);

    uint32_t val;
    ASSERT_TRUE(dht.findFirst("key1", val));
    EXPECT_EQ(val, 30u);
}

TEST(DeltaHashTable, Contains) {
    DeltaHashTable dht(testConfig());

    EXPECT_FALSE(dht.contains("key1"));
    dht.addEntry("key1", 10);
    EXPECT_TRUE(dht.contains("key1"));
}

TEST(DeltaHashTable, RemoveEntry) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("key1", 10);
    dht.addEntry("key1", 20);
    dht.addEntry("key2", 30);
    EXPECT_EQ(dht.size(), 3u);

    EXPECT_TRUE(dht.removeEntry("key1", 10));
    EXPECT_EQ(dht.size(), 2u);

    std::vector<uint32_t> out;
    ASSERT_TRUE(dht.findAll("key1", out));
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], 20u);

    ASSERT_TRUE(dht.findAll("key2", out));
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], 30u);
}

TEST(DeltaHashTable, RemoveEntryNonExistent) {
    DeltaHashTable dht(testConfig());
    EXPECT_FALSE(dht.removeEntry("missing", 10));
}

TEST(DeltaHashTable, RemoveEntryRemovesFingerprint) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("key1", 10);
    EXPECT_TRUE(dht.removeEntry("key1", 10));
    EXPECT_EQ(dht.size(), 0u);
    EXPECT_FALSE(dht.contains("key1"));
}

TEST(DeltaHashTable, RemoveAll) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("key1", 10);
    dht.addEntry("key1", 20);
    dht.addEntry("key1", 30);

    size_t removed = dht.removeAll("key1");
    EXPECT_EQ(removed, 3u);
    EXPECT_EQ(dht.size(), 0u);
    EXPECT_FALSE(dht.contains("key1"));
}

TEST(DeltaHashTable, RemoveAllNonExistent) {
    DeltaHashTable dht(testConfig());
    EXPECT_EQ(dht.removeAll("missing"), 0u);
}

TEST(DeltaHashTable, Clear) {
    DeltaHashTable dht(testConfig());

    for (int i = 0; i < 10; ++i) {
        dht.addEntry("key" + std::to_string(i), static_cast<uint32_t>(i + 1));
    }
    EXPECT_EQ(dht.size(), 10u);

    dht.clear();
    EXPECT_EQ(dht.size(), 0u);

    for (int i = 0; i < 10; ++i) {
        EXPECT_FALSE(dht.contains("key" + std::to_string(i)));
    }
}

TEST(DeltaHashTable, ForEach) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("a", 10);
    dht.addEntry("b", 20);
    dht.addEntry("c", 30);

    std::set<uint32_t> all_values;
    dht.forEach([&](uint64_t /*hash*/, uint32_t value) {
        all_values.insert(value);
    });

    EXPECT_EQ(all_values.size(), 3u);
    EXPECT_EQ(all_values.count(10u), 1u);
    EXPECT_EQ(all_values.count(20u), 1u);
    EXPECT_EQ(all_values.count(30u), 1u);
}

TEST(DeltaHashTable, ForEachGroup) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("a", 10);
    dht.addEntry("a", 20);
    dht.addEntry("b", 30);

    size_t group_count = 0;
    dht.forEachGroup([&](uint64_t /*hash*/, const std::vector<uint32_t>& values) {
        ++group_count;
        if (values.size() == 2) {
            EXPECT_EQ(values[0], 20u);
            EXPECT_EQ(values[1], 10u);
        } else {
            EXPECT_EQ(values.size(), 1u);
            EXPECT_EQ(values[0], 30u);
        }
    });

    EXPECT_EQ(group_count, 2u);
}

TEST(DeltaHashTable, ManyKeys) {
    DeltaHashTable dht(testConfig());

    const int N = 200;
    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        dht.addEntry(key, static_cast<uint32_t>(i * 10 + 1));
    }

    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint32_t val;
        ASSERT_TRUE(dht.findFirst(key, val)) << "key not found: " << key;
        EXPECT_EQ(val, static_cast<uint32_t>(i * 10 + 1));
    }
}

TEST(DeltaHashTable, AddAfterRemoveAll) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("key1", 10);
    dht.removeAll("key1");

    EXPECT_FALSE(dht.contains("key1"));

    // Re-add
    dht.addEntry("key1", 20);
    uint32_t val;
    ASSERT_TRUE(dht.findFirst("key1", val));
    EXPECT_EQ(val, 20u);
    EXPECT_EQ(dht.size(), 1u);
}

TEST(DeltaHashTable, MemoryUsage) {
    DeltaHashTable dht(testConfig());
    size_t empty_usage = dht.memoryUsage();
    EXPECT_GT(empty_usage, 0u);

    for (int i = 0; i < 50; ++i) {
        dht.addEntry("key_" + std::to_string(i), static_cast<uint32_t>(i + 1));
    }

    EXPECT_GE(dht.memoryUsage(), empty_usage);
}

TEST(DeltaHashTable, EmptyKeyAndBinaryKey) {
    DeltaHashTable dht(testConfig());

    dht.addEntry("", 1);

    std::string binary_key("\x00\x01\x02\x03", 4);
    dht.addEntry(binary_key, 2);

    EXPECT_EQ(dht.size(), 2u);

    uint32_t val;
    ASSERT_TRUE(dht.findFirst("", val));
    EXPECT_EQ(val, 1u);

    ASSERT_TRUE(dht.findFirst(binary_key, val));
    EXPECT_EQ(val, 2u);
}

TEST(DeltaHashTable, AddEntryByHash) {
    DeltaHashTable dht(testConfig());

    uint64_t hash = 0xDEADBEEF12345678ULL;
    dht.addEntryByHash(hash, 42);

    // Can't find by key, but forEach should see it
    size_t count = 0;
    dht.forEach([&](uint64_t h, uint32_t value) {
        ++count;
        EXPECT_EQ(value, 42u);
    });
    EXPECT_EQ(count, 1u);
}

// --- L1Index via DHT Tests ---

using kvlite::Status;

TEST(L1IndexDHT, PutAndGetLatest) {
    L1Index index;

    index.put("key1", 100);
    index.put("key1", 200);
    index.put("key1", 300);

    uint32_t file_id;
    EXPECT_TRUE(index.getLatest("key1", file_id));
    EXPECT_EQ(file_id, 300u);

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 3u);
    EXPECT_EQ(fids[0], 300u);
    EXPECT_EQ(fids[1], 200u);
    EXPECT_EQ(fids[2], 100u);
}

TEST(L1IndexDHT, PutDuplicateFileId) {
    L1Index index;

    index.put("key1", 100);
    index.put("key1", 200);
    // Putting 200 again should be a no-op (already at front)
    index.put("key1", 200);

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 2u);
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

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 2u);
    EXPECT_EQ(fids[0], 300u);
    EXPECT_EQ(fids[1], 100u);
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

TEST(L1IndexDHT, RemoveFileFromMultiple) {
    L1Index index;
    index.put("key1", 100);
    index.put("key1", 200);

    // Remove second (older)
    index.removeFile("key1", 100);

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 1u);
    EXPECT_EQ(fids[0], 200u);
    EXPECT_EQ(index.entryCount(), 1u);

    // Remove first (latest)
    index.removeFile("key1", 200);
    EXPECT_FALSE(index.contains("key1"));
    EXPECT_EQ(index.entryCount(), 0u);
}

TEST(L1IndexDHT, RemoveFileFromThree) {
    L1Index index;

    index.put("key1", 100);
    index.put("key1", 200);
    index.put("key1", 300);
    EXPECT_EQ(index.entryCount(), 3u);

    index.removeFile("key1", 200);
    EXPECT_EQ(index.entryCount(), 2u);

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 2u);
    EXPECT_EQ(fids[0], 300u);
    EXPECT_EQ(fids[1], 100u);

    index.removeFile("key1", 100);
    EXPECT_EQ(index.entryCount(), 1u);

    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 1u);
    EXPECT_EQ(fids[0], 300u);

    index.removeFile("key1", 300);
    EXPECT_EQ(index.entryCount(), 0u);
    EXPECT_FALSE(index.contains("key1"));
}

TEST(L1IndexDHT, ForEach) {
    L1Index index;
    index.put("a", 10);
    index.put("b", 20);

    size_t count = 0;
    index.forEach([&](const std::vector<uint32_t>& file_ids) {
        EXPECT_EQ(file_ids.size(), 1u);
        ++count;
    });

    EXPECT_EQ(count, 2u);
}

TEST(L1IndexDHT, ForEachMixed) {
    L1Index index;
    index.put("a", 10);
    index.put("b", 20);
    index.put("b", 30);
    index.put("c", 40);
    index.put("c", 50);
    index.put("c", 60);

    std::set<size_t> sizes;
    index.forEach([&](const std::vector<uint32_t>& file_ids) {
        sizes.insert(file_ids.size());
    });

    EXPECT_EQ(sizes.count(1u), 1u);
    EXPECT_EQ(sizes.count(2u), 1u);
    EXPECT_EQ(sizes.count(3u), 1u);
}

TEST(L1IndexDHT, GetFileIdsNonExistent) {
    L1Index index;
    std::vector<uint32_t> fids;
    EXPECT_FALSE(index.getFileIds("missing", fids));
}

#include <filesystem>

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

TEST(L1IndexDHT, SnapshotWithManyFileIds) {
    std::string path = "/tmp/test_l1_snapshot_many.dat";

    {
        L1Index index;
        index.put("key1", 100);
        index.put("key1", 200);
        index.put("key1", 300);
        index.put("key2", 400);

        Status s = index.saveSnapshot(path);
        ASSERT_TRUE(s.ok()) << s.toString();
    }

    {
        L1Index index;
        Status s = index.loadSnapshot(path);
        ASSERT_TRUE(s.ok()) << s.toString();

        EXPECT_EQ(index.keyCount(), 2u);
        EXPECT_EQ(index.entryCount(), 4u);

        std::vector<uint32_t> fids;
        ASSERT_TRUE(index.getFileIds("key1", fids));
        ASSERT_EQ(fids.size(), 3u);
        EXPECT_EQ(fids[0], 300u);
        EXPECT_EQ(fids[1], 200u);
        EXPECT_EQ(fids[2], 100u);

        uint32_t file_id;
        EXPECT_TRUE(index.getLatest("key2", file_id));
        EXPECT_EQ(file_id, 400u);
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

TEST(DeltaHashTable, ConcurrentAddAndFind) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 10;     // 1024 buckets
    cfg.lslot_bits = 3;       // 8 lslots
    cfg.bucket_bytes = 512;
    DeltaHashTable dht(cfg);

    const int NUM_THREADS = 4;
    const int KEYS_PER_THREAD = 500;

    // Phase 1: concurrent adds
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&dht, t]() {
            for (int i = 0; i < KEYS_PER_THREAD; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                dht.addEntry(key, static_cast<uint32_t>(t));
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
                uint32_t val;
                if (dht.findFirst(key, val)) {
                    found_count.fetch_add(1);
                }
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(found_count.load(), NUM_THREADS * KEYS_PER_THREAD);
}

TEST(DeltaHashTable, ConcurrentAddSameKeys) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 8;
    cfg.lslot_bits = 3;
    cfg.bucket_bytes = 512;
    DeltaHashTable dht(cfg);

    const int NUM_THREADS = 4;
    const int NUM_KEYS = 200;

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&dht, t]() {
            for (int i = 0; i < NUM_KEYS; ++i) {
                std::string key = "shared_key_" + std::to_string(i);
                // Each thread adds its own value
                dht.addEntry(key, static_cast<uint32_t>(t));
            }
        });
    }
    for (auto& t : threads) t.join();

    // Each key should have up to NUM_THREADS values
    EXPECT_EQ(dht.size(), static_cast<size_t>(NUM_KEYS * NUM_THREADS));
}

TEST(DeltaHashTable, ConcurrentAddAndRemove) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 8;
    cfg.lslot_bits = 3;
    cfg.bucket_bytes = 512;
    DeltaHashTable dht(cfg);

    const int NUM_KEYS = 200;

    // Pre-populate
    for (int i = 0; i < NUM_KEYS; ++i) {
        dht.addEntry("key_" + std::to_string(i), static_cast<uint32_t>(i));
    }

    std::thread remover([&dht]() {
        for (int i = 0; i < NUM_KEYS; i += 2) {
            dht.removeAll("key_" + std::to_string(i));
        }
    });

    std::thread inserter([&dht]() {
        for (int i = NUM_KEYS; i < NUM_KEYS * 2; ++i) {
            dht.addEntry("key_" + std::to_string(i), static_cast<uint32_t>(i));
        }
    });

    remover.join();
    inserter.join();

    // Verify odd keys still exist
    for (int i = 1; i < NUM_KEYS; i += 2) {
        EXPECT_TRUE(dht.contains("key_" + std::to_string(i)))
            << "missing odd key: " << i;
    }

    // Verify new keys exist
    for (int i = NUM_KEYS; i < NUM_KEYS * 2; ++i) {
        EXPECT_TRUE(dht.contains("key_" + std::to_string(i)))
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
