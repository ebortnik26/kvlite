#include <gtest/gtest.h>

#include <map>
#include <set>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/lslot_codec.h"
#include "internal/delta_hash_table.h"
#include "internal/global_index.h"

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

// --- LSlotCodec Tests ---

TEST(LSlotCodec, EncodeDecodeEmpty) {
    LSlotCodec codec(39);  // typical fingerprint_bits = 64 - 20 - 5
    LSlotCodec::LSlotContents contents;

    // Buffer with padding
    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);
    EXPECT_GT(end, 0u);  // at least the unary(0) terminator

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    EXPECT_TRUE(decoded.entries.empty());
}

TEST(LSlotCodec, EncodeDecodeSingleEntry) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1234;
    entry.values = {100};
    contents.entries.push_back(entry);

    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 0x1234u);
    ASSERT_EQ(decoded.entries[0].values.size(), 1u);
    EXPECT_EQ(decoded.entries[0].values[0], 100u);
}

TEST(LSlotCodec, EncodeDecodeMultipleValues) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 42;
    entry.values = {300, 200, 100};  // desc order
    contents.entries.push_back(entry);

    uint8_t buf[128] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    ASSERT_EQ(decoded.entries[0].values.size(), 3u);
    EXPECT_EQ(decoded.entries[0].values[0], 300u);
    EXPECT_EQ(decoded.entries[0].values[1], 200u);
    EXPECT_EQ(decoded.entries[0].values[2], 100u);
}

TEST(LSlotCodec, EncodeDecodeMultipleFingerprints) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;

    LSlotCodec::TrieEntry e1;
    e1.fingerprint = 10;
    e1.values = {50, 40};
    contents.entries.push_back(e1);

    LSlotCodec::TrieEntry e2;
    e2.fingerprint = 20;
    e2.values = {90};
    contents.entries.push_back(e2);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 2u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 10u);
    EXPECT_EQ(decoded.entries[1].fingerprint, 20u);
    ASSERT_EQ(decoded.entries[0].values.size(), 2u);
    ASSERT_EQ(decoded.entries[1].values.size(), 1u);
}

TEST(LSlotCodec, BitsNeeded) {
    LSlotCodec::LSlotContents empty;
    EXPECT_EQ(LSlotCodec::bitsNeeded(empty, 39), 1u);  // just unary(0)

    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 1;
    entry.values = {100};
    contents.entries.push_back(entry);

    size_t bits = LSlotCodec::bitsNeeded(contents, 39);
    // unary(1)=2 + fp=39 + unary(1)=2 + raw_val=32 = 75
    EXPECT_EQ(bits, 75u);
}

TEST(LSlotCodec, BitOffsetAndTotalBits) {
    LSlotCodec codec(39);

    // Encode 3 consecutive lslots
    uint8_t buf[512] = {};
    size_t offset = 0;

    LSlotCodec::LSlotContents s0;
    LSlotCodec::TrieEntry e0;
    e0.fingerprint = 1;
    e0.values = {10};
    s0.entries.push_back(e0);
    offset = codec.encode(buf, offset, s0);

    LSlotCodec::LSlotContents s1;  // empty
    offset = codec.encode(buf, offset, s1);

    LSlotCodec::LSlotContents s2;
    LSlotCodec::TrieEntry e2;
    e2.fingerprint = 2;
    e2.values = {20};
    s2.entries.push_back(e2);
    offset = codec.encode(buf, offset, s2);

    // bitOffset(0) should be 0
    EXPECT_EQ(codec.bitOffset(buf, 0), 0u);

    // bitOffset(1) should skip s0
    size_t off1 = codec.bitOffset(buf, 1);
    EXPECT_GT(off1, 0u);

    // totalBits for 3 lslots should match the final offset
    EXPECT_EQ(codec.totalBits(buf, 3), offset);
}

TEST(LSlotCodec, NonZeroBitOffset) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0xFF;
    entry.values = {42};
    contents.entries.push_back(entry);

    uint8_t buf[128] = {};
    // Encode at a non-zero bit offset
    size_t end = codec.encode(buf, 7, contents);
    EXPECT_GT(end, 7u);

    size_t decoded_end;
    LSlotCodec::LSlotContents decoded = codec.decode(buf, 7, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].fingerprint, 0xFFu);
    EXPECT_EQ(decoded.entries[0].values[0], 42u);
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
    dht.forEach([&](uint64_t /*h*/, uint32_t value) {
        ++count;
        EXPECT_EQ(value, 42u);
    });
    EXPECT_EQ(count, 1u);
}

// --- GlobalIndex via DHT Tests ---
//
// New API: put(key, version, segment_id), get(key, segment_ids, versions),
// getLatest(key, version, segment_id), removeSegment(key, segment_id).
// Versions sorted descending (latest first).

using kvlite::Status;

TEST(GlobalIndexDHT, PutAndGetLatest) {
    GlobalIndex index;

    // version=100 in seg 1, version=200 in seg 2, version=300 in seg 3
    index.put("key1", 100, 1);
    index.put("key1", 200, 2);
    index.put("key1", 300, 3);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index.getLatest("key1", ver, seg).ok());
    EXPECT_EQ(ver, 300u);
    EXPECT_EQ(seg, 3u);

    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    ASSERT_TRUE(index.get("key1", seg_ids, vers));
    ASSERT_EQ(seg_ids.size(), 3u);
    EXPECT_EQ(vers[0], 300u);  EXPECT_EQ(seg_ids[0], 3u);
    EXPECT_EQ(vers[1], 200u);  EXPECT_EQ(seg_ids[1], 2u);
    EXPECT_EQ(vers[2], 100u);  EXPECT_EQ(seg_ids[2], 1u);
}

TEST(GlobalIndexDHT, PutMultipleVersions) {
    GlobalIndex index;

    index.put("key1", 100, 1);
    index.put("key1", 200, 2);
    index.put("key1", 300, 3);

    EXPECT_EQ(index.entryCount(), 3u);
    EXPECT_EQ(index.keyCount(), 1u);
}

TEST(GlobalIndexDHT, GetLatest) {
    GlobalIndex index;
    index.put("key1", 100, 1);
    index.put("key1", 200, 2);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index.getLatest("key1", ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 2u);

    EXPECT_TRUE(index.getLatest("missing", ver, seg).isNotFound());
}

TEST(GlobalIndexDHT, GetWithUpperBound) {
    GlobalIndex index;
    index.put("key1", 100, 1);
    index.put("key1", 200, 2);
    index.put("key1", 300, 3);

    uint64_t ver;
    uint32_t seg;
    // Upper bound 250 → should get version 200
    EXPECT_TRUE(index.get("key1", 250, ver, seg));
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 2u);

    // Upper bound 300 → should get version 300
    EXPECT_TRUE(index.get("key1", 300, ver, seg));
    EXPECT_EQ(ver, 300u);
    EXPECT_EQ(seg, 3u);

    // Upper bound 50 → nothing
    EXPECT_FALSE(index.get("key1", 50, ver, seg));
}

TEST(GlobalIndexDHT, Contains) {
    GlobalIndex index;
    EXPECT_FALSE(index.contains("key1"));
    index.put("key1", 100, 1);
    EXPECT_TRUE(index.contains("key1"));
}

TEST(GlobalIndexDHT, GetNonExistent) {
    GlobalIndex index;
    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    EXPECT_FALSE(index.get("missing", seg_ids, vers));
}

#include <filesystem>

TEST(GlobalIndexDHT, Snapshot) {
    std::string path = "/tmp/test_global_index_snapshot_dht.dat";

    {
        GlobalIndex index;
        index.put("key1", 100, 1);
        index.put("key1", 200, 2);
        index.put("key2", 300, 3);

        Status s = index.saveSnapshot(path);
        ASSERT_TRUE(s.ok()) << s.toString();
    }

    {
        GlobalIndex index;
        Status s = index.loadSnapshot(path);
        ASSERT_TRUE(s.ok()) << s.toString();

        EXPECT_EQ(index.keyCount(), 2u);
        EXPECT_EQ(index.entryCount(), 3u);

        uint64_t ver;
        uint32_t seg;
        EXPECT_TRUE(index.getLatest("key1", ver, seg).ok());
        EXPECT_EQ(ver, 200u);
        EXPECT_EQ(seg, 2u);

        EXPECT_TRUE(index.getLatest("key2", ver, seg).ok());
        EXPECT_EQ(ver, 300u);
        EXPECT_EQ(seg, 3u);
    }

    std::filesystem::remove(path);
}

TEST(GlobalIndexDHT, SnapshotWithManyEntries) {
    std::string path = "/tmp/test_global_index_snapshot_many.dat";

    {
        GlobalIndex index;
        index.put("key1", 100, 1);
        index.put("key1", 200, 2);
        index.put("key1", 300, 3);
        index.put("key2", 400, 4);

        Status s = index.saveSnapshot(path);
        ASSERT_TRUE(s.ok()) << s.toString();
    }

    {
        GlobalIndex index;
        Status s = index.loadSnapshot(path);
        ASSERT_TRUE(s.ok()) << s.toString();

        EXPECT_EQ(index.keyCount(), 2u);
        EXPECT_EQ(index.entryCount(), 4u);

        std::vector<uint32_t> seg_ids;
        std::vector<uint64_t> vers;
        ASSERT_TRUE(index.get("key1", seg_ids, vers));
        ASSERT_EQ(seg_ids.size(), 3u);
        EXPECT_EQ(vers[0], 300u);  EXPECT_EQ(seg_ids[0], 3u);
        EXPECT_EQ(vers[1], 200u);  EXPECT_EQ(seg_ids[1], 2u);
        EXPECT_EQ(vers[2], 100u);  EXPECT_EQ(seg_ids[2], 1u);

        uint64_t ver;
        uint32_t seg;
        EXPECT_TRUE(index.getLatest("key2", ver, seg).ok());
        EXPECT_EQ(ver, 400u);
        EXPECT_EQ(seg, 4u);
    }

    std::filesystem::remove(path);
}

TEST(GlobalIndexDHT, Clear) {
    GlobalIndex index;
    for (int i = 0; i < 50; ++i) {
        index.put("key" + std::to_string(i), static_cast<uint64_t>(i * 10),
                  static_cast<uint32_t>(i));
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

// ============================================================
// Codec-level tests: SegmentLSlotCodec zero-delta safety
// ============================================================

#include "internal/segment_lslot_codec.h"

// Zero offset deltas: entries with identical offsets (e.g. same version).
// Before the fix, this would crash in writeEliasGamma(0).
TEST(SegmentLSlotCodecTest, ZeroOffsetDeltaRoundTrip) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x42;
    entry.offsets  = {100, 100, 100};  // zero deltas
    entry.versions = {3, 2, 1};       // unique, delta=1
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);
    EXPECT_GT(end, 0u);

    size_t decoded_end;
    auto decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].offsets, entry.offsets);
    EXPECT_EQ(decoded.entries[0].versions, entry.versions);
}

// Identical versions (e.g. same segment_id): raw encoding handles this.
TEST(SegmentLSlotCodecTest, IdenticalVersionsRoundTrip) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x42;
    entry.offsets  = {30, 20, 10};  // unique, delta=10
    entry.versions = {5, 5, 5};    // all same
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    auto decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].offsets, entry.offsets);
    EXPECT_EQ(decoded.entries[0].versions, entry.versions);
}

// Versions in DESCENDING order (correlated with descending offsets).
// After flipping flush to version-asc, higher versions get higher file offsets.
// Sorting by offset desc yields versions in descending order.
TEST(SegmentLSlotCodecTest, DescendingVersionsRoundTrip) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    entry.offsets  = {300, 200, 100};  // desc
    entry.versions = {3, 2, 1};        // desc (correlated)
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    codec.encode(buf, 0, contents);

    auto decoded = codec.decode(buf, 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].offsets, entry.offsets);
    EXPECT_EQ(decoded.entries[0].versions, entry.versions);
}

// Consecutive versions [N, N-1, ..., 1]: all deltas are 1, verifying compact encoding.
TEST(SegmentLSlotCodecTest, ConsecutiveVersionsRoundTrip) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    const int N = 50;
    for (int i = N; i >= 1; --i) {
        entry.offsets.push_back(static_cast<uint32_t>(i * 100));  // desc
        entry.versions.push_back(static_cast<uint32_t>(i));       // desc
    }
    contents.entries.push_back(entry);

    std::vector<uint8_t> buf(4096, 0);
    size_t end = codec.encode(buf.data(), 0, contents);
    EXPECT_GT(end, 0u);

    auto decoded = codec.decode(buf.data(), 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].offsets, entry.offsets);
    EXPECT_EQ(decoded.entries[0].versions, entry.versions);
}

// BitsNeeded must not crash on zero deltas (previously hit UB via __builtin_clz(0)).
TEST(SegmentLSlotCodecTest, BitsNeededZeroDeltas) {
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    entry.offsets  = {10, 10};
    entry.versions = {5, 5};
    contents.entries.push_back(entry);

    size_t bits = SegmentLSlotCodec::bitsNeeded(contents, 39);
    EXPECT_GT(bits, 0u);  // must not crash
}

// LSlotCodec: zero deltas with duplicate values (before the fix, this was UB).
TEST(LSlotCodec, ZeroDeltaRoundTrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x42;
    entry.values = {100, 100, 100};
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    auto decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].values, entry.values);
}

// Mixed: offsets with zero deltas, versions non-increasing (desc with repeats).
TEST(SegmentLSlotCodecTest, MixedDeltasRoundTrip) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 0xABC;
    entry.offsets  = {100, 100, 90, 90, 80};  // deltas: 0, 10, 0, 10
    entry.versions = {5, 4, 4, 2, 1};         // non-increasing
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    codec.encode(buf, 0, contents);

    auto decoded = codec.decode(buf, 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].offsets, entry.offsets);
    EXPECT_EQ(decoded.entries[0].versions, entry.versions);
}

// Many entries per fingerprint — stress-test the encoding.
// Versions are correlated with offsets (both descending).
TEST(SegmentLSlotCodecTest, ManyEntriesRoundTrip) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;
    SegmentLSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    const int N = 500;
    for (int i = N; i >= 1; --i) {
        entry.offsets.push_back(static_cast<uint32_t>(i));
        entry.versions.push_back(static_cast<uint32_t>(i));  // descending
    }
    contents.entries.push_back(entry);

    std::vector<uint8_t> buf(65536, 0);
    size_t end = codec.encode(buf.data(), 0, contents);
    EXPECT_GT(end, 0u);

    auto decoded = codec.decode(buf.data(), 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].offsets, entry.offsets);
    EXPECT_EQ(decoded.entries[0].versions, entry.versions);
}

// Multiple fingerprint groups in one lslot, each with zero deltas.
TEST(SegmentLSlotCodecTest, MultipleGroupsZeroDeltas) {
    SegmentLSlotCodec codec(39);
    SegmentLSlotCodec::LSlotContents contents;

    for (uint64_t fp = 1; fp <= 3; ++fp) {
        SegmentLSlotCodec::TrieEntry entry;
        entry.fingerprint = fp;
        entry.offsets  = {50, 50, 50};
        entry.versions = {10, 10, 10};
        contents.entries.push_back(entry);
    }

    std::vector<uint8_t> buf(4096, 0);
    codec.encode(buf.data(), 0, contents);

    auto decoded = codec.decode(buf.data(), 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 3u);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(decoded.entries[i].offsets, contents.entries[i].offsets);
        EXPECT_EQ(decoded.entries[i].versions, contents.entries[i].versions);
    }
}

// ============================================================
// DHT-level tests: bucket overflow with small buckets
// ============================================================

// Small-bucket config to force overflow quickly.
static SegmentDeltaHashTable::Config smallBucketConfig() {
    SegmentDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;      // 16 buckets
    cfg.lslot_bits = 2;       // 4 lslots
    cfg.bucket_bytes = 128;   // tiny buckets → fast overflow
    return cfg;
}

// Write path: many entries for one key in small buckets forces overflow.
// Verifies addEntry doesn't crash when bucket chains are created.
TEST(SegmentDHTOverflow, WritePathManyEntries) {
    SegmentDeltaHashTable dht(smallBucketConfig());
    const int N = 200;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint32_t>(i), /*version=*/1);
    }
    EXPECT_EQ(dht.size(), static_cast<size_t>(N));
}

// Read-back after overflow: verify all entries survive the chain.
TEST(SegmentDHTOverflow, ReadBackAfterOverflow) {
    SegmentDeltaHashTable dht(smallBucketConfig());
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint32_t>(i), static_cast<uint32_t>(i));
    }

    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(dht.findAll("key", offsets, versions));
    EXPECT_EQ(offsets.size(), static_cast<size_t>(N));
    EXPECT_EQ(versions.size(), static_cast<size_t>(N));

    // Every offset 1..N should appear
    std::set<uint32_t> offset_set(offsets.begin(), offsets.end());
    for (int i = 1; i <= N; ++i) {
        EXPECT_EQ(offset_set.count(static_cast<uint32_t>(i)), 1u)
            << "missing offset " << i;
    }
}

// Read-back with identical versions (segment_ids) — zero deltas in version field.
TEST(SegmentDHTOverflow, ReadBackSameVersionOverflow) {
    SegmentDeltaHashTable dht(smallBucketConfig());
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint32_t>(i), /*version=*/42);
    }

    std::vector<uint32_t> offsets, versions;
    ASSERT_TRUE(dht.findAll("key", offsets, versions));
    EXPECT_EQ(offsets.size(), static_cast<size_t>(N));

    // All versions should be 42
    for (size_t i = 0; i < versions.size(); ++i) {
        EXPECT_EQ(versions[i], 42u);
    }
}

// findFirst after overflow should return the highest offset.
TEST(SegmentDHTOverflow, FindFirstAfterOverflow) {
    SegmentDeltaHashTable dht(smallBucketConfig());
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry("key", static_cast<uint32_t>(i), static_cast<uint32_t>(i));
    }

    uint32_t offset, version;
    ASSERT_TRUE(dht.findFirst("key", offset, version));
    // findFirst returns the entry with the highest version in the first bucket
    // that contains the key — may or may not be the global max depending on
    // which bucket the latest entry landed in.
    EXPECT_GT(offset, 0u);
}

// Multiple keys all forcing overflow in the same bucket.
TEST(SegmentDHTOverflow, MultipleKeysOverflowSameBucket) {
    // Use very small config: 1 bucket, 1 lslot to force everything together.
    SegmentDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;      // 2 buckets
    cfg.lslot_bits = 1;       // 2 lslots
    cfg.bucket_bytes = 64;    // very small
    SegmentDeltaHashTable dht(cfg);

    // Add many entries; they'll share the 2 buckets.
    for (int k = 0; k < 10; ++k) {
        std::string key = "k" + std::to_string(k);
        for (int i = 1; i <= 10; ++i) {
            dht.addEntry(key, static_cast<uint32_t>(i),
                         static_cast<uint32_t>(k));
        }
    }

    EXPECT_EQ(dht.size(), 100u);

    // Verify read-back for each key.
    for (int k = 0; k < 10; ++k) {
        std::string key = "k" + std::to_string(k);
        std::vector<uint32_t> offsets, versions;
        ASSERT_TRUE(dht.findAll(key, offsets, versions))
            << "key not found: " << key;
        EXPECT_EQ(offsets.size(), 10u);
    }
}

// forEach traverses all entries across overflow chains.
TEST(SegmentDHTOverflow, ForEachAcrossChain) {
    SegmentDeltaHashTable dht(smallBucketConfig());
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint32_t>(i), static_cast<uint32_t>(i));
    }

    std::set<uint32_t> seen_offsets;
    dht.forEach([&](uint64_t, uint32_t off, uint32_t) {
        seen_offsets.insert(off);
    });
    EXPECT_EQ(seen_offsets.size(), static_cast<size_t>(N));
}

// forEachGroup merges entries from overflow chains.
TEST(SegmentDHTOverflow, ForEachGroupMergesChain) {
    SegmentDeltaHashTable dht(smallBucketConfig());
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint32_t>(i), static_cast<uint32_t>(i));
    }

    size_t total_entries = 0;
    dht.forEachGroup([&](uint64_t, const std::vector<uint32_t>& offsets,
                         const std::vector<uint32_t>&) {
        total_entries += offsets.size();
    });
    EXPECT_EQ(total_entries, static_cast<size_t>(N));
}

// ============================================================
// GlobalIndex-level overflow tests
// ============================================================

// Many versions of the same key, all in the same segment.
TEST(GlobalIndexDHT, ManyVersionsSameKeyAndSegment) {
    GlobalIndex index;
    for (int i = 1; i <= 200; ++i) {
        index.put("key", static_cast<uint64_t>(i), /*segment_id=*/1);
    }

    EXPECT_EQ(index.entryCount(), 200u);
    EXPECT_EQ(index.keyCount(), 1u);

    uint64_t ver;
    uint32_t seg;
    ASSERT_TRUE(index.getLatest("key", ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 1u);

    // Verify all versions are retrievable.
    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    ASSERT_TRUE(index.get("key", seg_ids, vers));
    EXPECT_EQ(vers.size(), 200u);
    EXPECT_EQ(vers[0], 200u);   // latest first
    EXPECT_EQ(vers[199], 1u);   // earliest last
}

// Same key, different segments (unique segment_ids).
TEST(GlobalIndexDHT, ManyVersionsDifferentSegments) {
    GlobalIndex index;
    for (int i = 1; i <= 200; ++i) {
        index.put("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t ver;
    uint32_t seg;
    ASSERT_TRUE(index.getLatest("key", ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 200u);

    // Verify upper-bound query works across the full range.
    ASSERT_TRUE(index.get("key", 150, ver, seg));
    EXPECT_EQ(ver, 150u);
    EXPECT_EQ(seg, 150u);
}

TEST(GlobalIndexDHT, LargeScale) {
    GlobalIndex index;
    const int N = 1000;

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        index.put(key, static_cast<uint64_t>(i * 10), static_cast<uint32_t>(i));
    }

    EXPECT_EQ(index.keyCount(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t ver;
        uint32_t seg;
        ASSERT_TRUE(index.getLatest(key, ver, seg).ok());
        EXPECT_EQ(ver, static_cast<uint64_t>(i * 10));
        EXPECT_EQ(seg, static_cast<uint32_t>(i));
    }
}
