#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <thread>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/global_index.h"
#include "internal/manifest.h"
#include "internal/segment_storage_manager.h"
#include "internal/read_only_delta_hash_table.h"
#include "internal/read_write_delta_hash_table.h"

using namespace kvlite::internal;

static uint64_t H(const std::string& s) {
    return dhtHashBytes(s.data(), s.size());
}

// --- GlobalIndex via DHT Tests ---
//
// New API: put(key, version, segment_id), get(key, segment_ids, versions),
// getLatest(key, version, segment_id), removeSegment(key, segment_id).
// Versions sorted descending (latest first).

using kvlite::Status;

class GlobalIndexDHT : public ::testing::Test {
protected:
    void SetUp() override {
        db_dir_ = ::testing::TempDir() + "/gi_dht_test_" +
                  std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::create_directories(db_dir_);
        ASSERT_TRUE(manifest_.create(db_dir_).ok());
        storage_ = std::make_unique<SegmentStorageManager>(manifest_);
        ASSERT_TRUE(storage_->open(db_dir_).ok());
        index = std::make_unique<GlobalIndex>(manifest_);
        GlobalIndex::Options opts;
        ASSERT_TRUE(index->open(db_dir_, opts, *storage_).ok());
    }

    void TearDown() override {
        if (index && index->isOpen()) index->close();
        index.reset();
        if (storage_) storage_->close();
        storage_.reset();
        manifest_.close();
        std::filesystem::remove_all(db_dir_);
    }

    void put(uint64_t hkey, uint64_t packed_version, uint32_t segment_id) {
        index->applyPut(hkey, packed_version, segment_id);
    }

    std::string db_dir_;
    kvlite::internal::Manifest manifest_;
    std::unique_ptr<SegmentStorageManager> storage_;
    std::unique_ptr<GlobalIndex> index;
};

TEST_F(GlobalIndexDHT, PutAndGetLatest) {

    uint64_t hkey1 = H("key1");
    put(hkey1, 100, 1);
    put(hkey1, 200, 2);
    put(hkey1, 300, 3);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index->getLatest(hkey1, ver, seg).ok());
    EXPECT_EQ(ver, 300u);
    EXPECT_EQ(seg, 3u);

    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    ASSERT_TRUE(index->get(hkey1, seg_ids, vers));
    ASSERT_EQ(seg_ids.size(), 3u);
    EXPECT_EQ(vers[0], 300u);  EXPECT_EQ(seg_ids[0], 3u);
    EXPECT_EQ(vers[1], 200u);  EXPECT_EQ(seg_ids[1], 2u);
    EXPECT_EQ(vers[2], 100u);  EXPECT_EQ(seg_ids[2], 1u);
}

TEST_F(GlobalIndexDHT, PutMultipleVersions) {
    uint64_t hkey1 = H("key1");
    put(hkey1, 100, 1);
    put(hkey1, 200, 2);
    put(hkey1, 300, 3);

    EXPECT_EQ(index->entryCount(), 3u);
    EXPECT_EQ(index->keyCount(), 1u);
}

TEST_F(GlobalIndexDHT, GetLatest) {
    uint64_t hkey1 = H("key1");
    put(hkey1, 100, 1);
    put(hkey1, 200, 2);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index->getLatest(hkey1, ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 2u);

    EXPECT_TRUE(index->getLatest(H("missing"), ver, seg).isNotFound());
}

TEST_F(GlobalIndexDHT, GetWithUpperBound) {
    uint64_t hkey1 = H("key1");
    put(hkey1, 100, 1);
    put(hkey1, 200, 2);
    put(hkey1, 300, 3);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index->get(hkey1, 250, ver, seg));
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 2u);

    EXPECT_TRUE(index->get(hkey1, 300, ver, seg));
    EXPECT_EQ(ver, 300u);
    EXPECT_EQ(seg, 3u);

    EXPECT_FALSE(index->get(hkey1, 50, ver, seg));
}

TEST_F(GlobalIndexDHT, Contains) {
    uint64_t hkey1 = H("key1");
    EXPECT_FALSE(index->contains(hkey1));
    put(hkey1, 100, 1);
    EXPECT_TRUE(index->contains(hkey1));
}

TEST_F(GlobalIndexDHT, GetNonExistent) {
    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    EXPECT_FALSE(index->get(H("missing"), seg_ids, vers));
}

TEST_F(GlobalIndexDHT, Savepoint) {
    uint64_t hkey1 = H("key1");
    uint64_t hkey2 = H("key2");
    put(hkey1, 100, 1);
    put(hkey1, 200, 2);
    put(hkey2, 300, 3);

    ASSERT_TRUE(index->storeSavepoint(0).ok());
    ASSERT_TRUE(index->close().ok());

    // Re-open and verify data is loaded from savepoint.
    storage_->close();
    manifest_.close();
    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    SegmentStorageManager storage2(manifest2);
    ASSERT_TRUE(storage2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts, storage2).ok());

    EXPECT_EQ(index2.keyCount(), 2u);
    EXPECT_EQ(index2.entryCount(), 3u);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index2.getLatest(hkey1, ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 2u);

    EXPECT_TRUE(index2.getLatest(hkey2, ver, seg).ok());
    EXPECT_EQ(ver, 300u);
    EXPECT_EQ(seg, 3u);

    index2.close();
    storage2.close();
    manifest2.close();
}

TEST_F(GlobalIndexDHT, SavepointWithManyEntries) {
    uint64_t hkey1 = H("key1");
    uint64_t hkey2 = H("key2");
    put(hkey1, 100, 1);
    put(hkey1, 200, 2);
    put(hkey1, 300, 3);
    put(hkey2, 400, 4);

    ASSERT_TRUE(index->storeSavepoint(0).ok());
    ASSERT_TRUE(index->close().ok());

    // Re-open and verify data is loaded from savepoint.
    storage_->close();
    manifest_.close();
    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    SegmentStorageManager storage2(manifest2);
    ASSERT_TRUE(storage2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts, storage2).ok());

    EXPECT_EQ(index2.keyCount(), 2u);
    EXPECT_EQ(index2.entryCount(), 4u);

    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    ASSERT_TRUE(index2.get(hkey1, seg_ids, vers));
    ASSERT_EQ(seg_ids.size(), 3u);
    EXPECT_EQ(vers[0], 300u);  EXPECT_EQ(seg_ids[0], 3u);
    EXPECT_EQ(vers[1], 200u);  EXPECT_EQ(seg_ids[1], 2u);
    EXPECT_EQ(vers[2], 100u);  EXPECT_EQ(seg_ids[2], 1u);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index2.getLatest(hkey2, ver, seg).ok());
    EXPECT_EQ(ver, 400u);
    EXPECT_EQ(seg, 4u);

    index2.close();
    storage2.close();
    manifest2.close();
}

TEST_F(GlobalIndexDHT, Clear) {
    for (int i = 0; i < 50; ++i) {
        std::string key = "key" + std::to_string(i);
        put(H(key), static_cast<uint64_t>(i * 10),
            static_cast<uint32_t>(i));
    }
    EXPECT_EQ(index->keyCount(), 50u);

    index->clear();
    EXPECT_EQ(index->keyCount(), 0u);
    EXPECT_EQ(index->entryCount(), 0u);
}

// ============================================================
// DHT-level tests: bucket overflow with small buckets
// ============================================================

// Small-bucket config to force overflow quickly.
static ReadOnlyDeltaHashTable::Config smallBucketConfig() {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;      // 16 buckets
    cfg.bucket_bytes = 128;   // tiny buckets → fast overflow
    return cfg;
}

// Write path: many entries for one key in small buckets forces overflow.
TEST(ReadOnlyDHTOverflow, WritePathManyEntries) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    const int N = 200;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), /*id=*/1);
    }
    EXPECT_EQ(dht.size(), static_cast<size_t>(N));
}

// Read-back after overflow: verify all entries survive the chain.
TEST(ReadOnlyDHTOverflow, ReadBackAfterOverflow) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, packed_versions, ids));
    EXPECT_EQ(packed_versions.size(), static_cast<size_t>(N));
    EXPECT_EQ(ids.size(), static_cast<size_t>(N));

    std::set<uint32_t> id_set(ids.begin(), ids.end());
    for (int i = 1; i <= N; ++i) {
        EXPECT_EQ(id_set.count(static_cast<uint32_t>(i)), 1u)
            << "missing id " << i;
    }
}

// Read-back with identical ids — zero deltas in id field.
TEST(ReadOnlyDHTOverflow, ReadBackSameIdOverflow) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), /*id=*/42);
    }

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, packed_versions, ids));
    EXPECT_EQ(packed_versions.size(), static_cast<size_t>(N));

    for (size_t i = 0; i < ids.size(); ++i) {
        EXPECT_EQ(ids[i], 42u);
    }
}

// findFirst after overflow should return the highest packed_version.
TEST(ReadOnlyDHTOverflow, FindFirstAfterOverflow) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, packed_version, id));
    EXPECT_GT(packed_version, 0u);
}

// Multiple keys all forcing overflow in the same bucket.
TEST(ReadOnlyDHTOverflow, MultipleKeysOverflowSameBucket) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;      // 2 buckets
    cfg.bucket_bytes = 64;    // very small
    ReadOnlyDeltaHashTable dht(cfg);

    for (int k = 0; k < 10; ++k) {
        std::string key = "k" + std::to_string(k);
        uint64_t hkey = H(key);
        for (int i = 1; i <= 10; ++i) {
            dht.addEntry(hkey, static_cast<uint64_t>(i),
                         static_cast<uint32_t>(k));
        }
    }

    EXPECT_EQ(dht.size(), 100u);

    for (int k = 0; k < 10; ++k) {
        std::string key = "k" + std::to_string(k);
        std::vector<uint64_t> packed_versions;
        std::vector<uint32_t> ids;
        ASSERT_TRUE(dht.findAll(H(key), packed_versions, ids))
            << "key not found: " << key;
        EXPECT_EQ(packed_versions.size(), 10u);
    }
}

// forEach traverses all entries across overflow chains.
TEST(ReadOnlyDHTOverflow, ForEachAcrossChain) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    std::set<uint32_t> seen_ids;
    dht.forEach([&](uint64_t, uint64_t, uint32_t id) {
        seen_ids.insert(id);
    });
    EXPECT_EQ(seen_ids.size(), static_cast<size_t>(N));
}

// forEachGroup merges entries from overflow chains.
TEST(ReadOnlyDHTOverflow, ForEachGroupMergesChain) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    size_t total_entries = 0;
    dht.forEachGroup([&](uint64_t, const std::vector<uint64_t>& pvs,
                         const std::vector<uint32_t>&) {
        total_entries += pvs.size();
    });
    EXPECT_EQ(total_entries, static_cast<size_t>(N));
}

// ============================================================
// GlobalIndex-level overflow tests
// ============================================================

// Many versions of the same key, all in the same segment.
TEST_F(GlobalIndexDHT, ManyVersionsSameKeyAndSegment) {
    uint64_t hkey = H("key");
    for (int i = 1; i <= 200; ++i) {
        put(hkey, static_cast<uint64_t>(i), /*segment_id=*/1);
    }

    EXPECT_EQ(index->entryCount(), 200u);
    EXPECT_EQ(index->keyCount(), 1u);

    uint64_t ver;
    uint32_t seg;
    ASSERT_TRUE(index->getLatest(hkey, ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 1u);

    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    ASSERT_TRUE(index->get(hkey, seg_ids, vers));
    EXPECT_EQ(vers.size(), 200u);
    EXPECT_EQ(vers[0], 200u);
    EXPECT_EQ(vers[199], 1u);
}

// Same key, different segments (unique segment_ids).
TEST_F(GlobalIndexDHT, ManyVersionsDifferentSegments) {
    uint64_t hkey = H("key");
    for (int i = 1; i <= 200; ++i) {
        put(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t ver;
    uint32_t seg;
    ASSERT_TRUE(index->getLatest(hkey, ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 200u);

    ASSERT_TRUE(index->get(hkey, 150, ver, seg));
    EXPECT_EQ(ver, 150u);
    EXPECT_EQ(seg, 150u);
}

TEST_F(GlobalIndexDHT, LargeScale) {
    const int N = 1000;

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        put(H(key), static_cast<uint64_t>(i * 10), static_cast<uint32_t>(i));
    }

    EXPECT_EQ(index->keyCount(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t ver;
        uint32_t seg;
        ASSERT_TRUE(index->getLatest(H(key), ver, seg).ok());
        EXPECT_EQ(ver, static_cast<uint64_t>(i * 10));
        EXPECT_EQ(seg, static_cast<uint32_t>(i));
    }
}

// ============================================================
// addEntryIsNew correctness tests
// ============================================================

TEST(ReadOnlyDHT, AddEntryIsNewFirstAdd) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    EXPECT_TRUE(dht.addEntryIsNew(H("key1"), 100, 1));
}

TEST(ReadOnlyDHT, AddEntryIsNewDuplicateKey) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey1 = H("key1");
    EXPECT_TRUE(dht.addEntryIsNew(hkey1, 100, 1));
    EXPECT_FALSE(dht.addEntryIsNew(hkey1, 200, 2));
}

TEST(ReadOnlyDHT, AddEntryIsNewDifferentKeys) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    EXPECT_TRUE(dht.addEntryIsNew(H("key1"), 100, 1));
    EXPECT_TRUE(dht.addEntryIsNew(H("key2"), 200, 2));
}

TEST(ReadOnlyDHT, AddEntryIsNewAfterOverflow) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 64;
    ReadOnlyDeltaHashTable dht(cfg);

    uint64_t hkey1 = H("key1");
    EXPECT_TRUE(dht.addEntryIsNew(hkey1, 1, 1));
    for (int i = 2; i <= 50; ++i) {
        EXPECT_FALSE(dht.addEntryIsNew(hkey1, static_cast<uint64_t>(i),
                                        static_cast<uint32_t>(i)));
    }
}

TEST_F(GlobalIndexDHT, PutKeyCountWithAddEntryIsNew) {
    const int K = 50;
    const int V = 5;
    for (int k = 0; k < K; ++k) {
        std::string key = "key" + std::to_string(k);
        uint64_t hkey = H(key);
        for (int v = 0; v < V; ++v) {
            put(hkey,
                static_cast<uint64_t>(k * V + v),
                static_cast<uint32_t>(k));
        }
    }
    EXPECT_EQ(index->keyCount(), static_cast<size_t>(K));
    EXPECT_EQ(index->entryCount(), static_cast<size_t>(K * V));
}

// ============================================================
// findFirst across overflow chains (strengthen coverage)
// ============================================================

TEST(ReadOnlyDHTOverflow, FindFirstReturnsExactMax) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, packed_version, id));
    EXPECT_EQ(packed_version, 100u);
    EXPECT_EQ(id, 100u);
}

TEST(ReadOnlyDHTOverflow, FindFirstWithDescendingInsertOrder) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    for (int i = 100; i >= 1; --i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, packed_version, id));
    EXPECT_EQ(packed_version, 100u);
    EXPECT_EQ(id, 100u);
}

TEST(ReadOnlyDHTOverflow, FindFirstSingleEntryPerBucket) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;
    ReadOnlyDeltaHashTable dht(cfg);

    uint64_t hkey = H("key");
    for (int i = 1; i <= 20; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, packed_version, id));
    EXPECT_EQ(packed_version, 20u);
}

// findFirst must return the highest version across the ENTIRE bucket chain,
// not just the first bucket that contains the suffix.  Regression test for
// a bug where findKeyInChain stopped at the first match.
TEST(ReadOnlyDHTOverflow, FindFirstWalksFullChain) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;  // tiny buckets → many extensions
    ReadOnlyDeltaHashTable dht(cfg);

    uint64_t hkey = H("chain_key");

    // Insert versions 1..50.  With 32-byte buckets, entries spill across
    // many extension buckets.  The highest version (50) may land in any bucket.
    for (int i = 1; i <= 50; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, pv, id));
    EXPECT_EQ(pv, 50u) << "findFirst must return highest version across all buckets";
    EXPECT_EQ(id, 50u);
}

// findFirstBounded must return the highest version <= bound, even when
// the key spans multiple extension buckets.
TEST(ReadOnlyDHTOverflow, FindFirstBoundedAcrossChain) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;
    ReadOnlyDeltaHashTable dht(cfg);

    uint64_t hkey = H("bounded_key");
    for (int i = 1; i <= 50; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    // Bound at 35 — should return version 35, not the first version found
    // in the primary bucket that happens to be <= 35.
    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirstBounded(hkey, 35, pv, id));
    EXPECT_EQ(pv, 35u);
    EXPECT_EQ(id, 35u);

    // Bound at 1 — should return version 1.
    ASSERT_TRUE(dht.findFirstBounded(hkey, 1, pv, id));
    EXPECT_EQ(pv, 1u);

    // Bound at 0 — no version <= 0.
    EXPECT_FALSE(dht.findFirstBounded(hkey, 0, pv, id));
}

// Same tests on ReadWriteDeltaHashTable (locked path).
TEST(ReadWriteDHT, FindFirstWalksFullChain) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;
    ReadWriteDeltaHashTable dht(cfg);

    uint64_t hkey = H("rw_chain");
    for (int i = 1; i <= 50; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, pv, id));
    EXPECT_EQ(pv, 50u);
}

TEST(ReadWriteDHT, FindFirstBoundedAcrossChain) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;
    ReadWriteDeltaHashTable dht(cfg);

    uint64_t hkey = H("rw_bounded");
    for (int i = 1; i <= 50; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirstBounded(hkey, 35, pv, id));
    EXPECT_EQ(pv, 35u);

    ASSERT_TRUE(dht.findFirstBounded(hkey, 1, pv, id));
    EXPECT_EQ(pv, 1u);

    EXPECT_FALSE(dht.findFirstBounded(hkey, 0, pv, id));
}

// ============================================================
// findAll ordering across overflow chains
// ============================================================

TEST(ReadOnlyDHTOverflow, FindAllDescOrderAcrossChain) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, packed_versions, ids));
    EXPECT_EQ(packed_versions.size(), 100u);

    std::set<uint64_t> pv_set(packed_versions.begin(), packed_versions.end());
    for (int i = 1; i <= 100; ++i) {
        EXPECT_EQ(pv_set.count(static_cast<uint64_t>(i)), 1u)
            << "missing packed_version " << i;
    }
}

TEST(ReadOnlyDHTOverflow, FindAllCompleteAcrossChain) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i * 10),
                     static_cast<uint32_t>(i));
    }

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, packed_versions, ids));
    ASSERT_EQ(ids.size(), 100u);

    std::set<uint32_t> id_set(ids.begin(), ids.end());
    for (int i = 1; i <= 100; ++i) {
        EXPECT_EQ(id_set.count(static_cast<uint32_t>(i)), 1u)
            << "missing id " << i;
    }
}

// ============================================================
// forEachGroup correctness with overflow
// ============================================================

TEST(ReadOnlyDHTOverflow, ForEachGroupMergesCorrectly) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 64;
    ReadOnlyDeltaHashTable dht(cfg);

    for (int k = 0; k < 5; ++k) {
        std::string key = "grp" + std::to_string(k);
        uint64_t hkey = H(key);
        for (int i = 1; i <= 20; ++i) {
            dht.addEntry(hkey, static_cast<uint64_t>(k * 100 + i),
                         static_cast<uint32_t>(k * 100 + i));
        }
    }

    // Collect all groups via forEachGroup
    size_t total_entries = 0;
    std::map<uint64_t, size_t> group_sizes;
    dht.forEachGroup([&](uint64_t hash, const std::vector<uint64_t>& pvs,
                         const std::vector<uint32_t>& ids) {
        EXPECT_EQ(pvs.size(), ids.size());
        total_entries += pvs.size();
        group_sizes[hash] = pvs.size();

        // Verify all ids are present
        std::set<uint32_t> id_set(ids.begin(), ids.end());
        EXPECT_EQ(id_set.size(), ids.size());
    });

    EXPECT_EQ(total_entries, 100u);
}

// ============================================================
// ReadWriteDeltaHashTable concurrent operations
// ============================================================

TEST(ReadWriteDHT, ConcurrentAddAndFindFirst) {
    ReadWriteDeltaHashTable dht;
    const int threads = 4;
    const int per_thread = 1000;
    uint64_t hkey = H("key");

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&dht, t, per_thread, hkey]() {
            for (int i = 0; i < per_thread; ++i) {
                uint64_t pv = static_cast<uint64_t>(t * per_thread + i + 1);
                dht.addEntry(hkey, pv, static_cast<uint32_t>(pv));
            }
        });
    }
    for (auto& w : workers) w.join();

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, packed_version, id));
    EXPECT_EQ(packed_version, static_cast<uint64_t>(threads * per_thread));

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, pvs, ids));
    EXPECT_EQ(pvs.size(), static_cast<size_t>(threads * per_thread));
}

TEST(ReadWriteDHT, ConcurrentAddDifferentKeys) {
    ReadWriteDeltaHashTable dht;
    const int threads = 4;
    const int per_thread = 1000;

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&dht, t, per_thread]() {
            std::string key = "key" + std::to_string(t);
            uint64_t hkey = H(key);
            for (int i = 0; i < per_thread; ++i) {
                dht.addEntry(hkey, static_cast<uint64_t>(i + 1),
                             static_cast<uint32_t>(i + 1));
            }
        });
    }
    for (auto& w : workers) w.join();

    EXPECT_EQ(dht.size(), static_cast<size_t>(threads * per_thread));

    for (int t = 0; t < threads; ++t) {
        std::string key = "key" + std::to_string(t);
        std::vector<uint64_t> pvs;
        std::vector<uint32_t> ids;
        ASSERT_TRUE(dht.findAll(H(key), pvs, ids));
        EXPECT_EQ(pvs.size(), static_cast<size_t>(per_thread));
    }
}

TEST(ReadWriteDHT, ConcurrentOverflowSameBucket) {
    ReadWriteDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 64;
    ReadWriteDeltaHashTable dht(cfg);

    const int threads = 4;
    const int per_thread = 50;

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&dht, t, per_thread]() {
            std::string key = "overflow_key" + std::to_string(t);
            uint64_t hkey = H(key);
            for (int i = 0; i < per_thread; ++i) {
                dht.addEntry(hkey, static_cast<uint64_t>(t * per_thread + i + 1),
                             static_cast<uint32_t>(t * per_thread + i + 1));
            }
        });
    }
    for (auto& w : workers) w.join();

    for (int t = 0; t < threads; ++t) {
        std::string key = "overflow_key" + std::to_string(t);
        std::vector<uint64_t> pvs;
        std::vector<uint32_t> ids;
        ASSERT_TRUE(dht.findAll(H(key), pvs, ids))
            << "key not found: " << key;
        EXPECT_EQ(pvs.size(), static_cast<size_t>(per_thread));
    }
}

TEST(ReadWriteDHT, ConcurrentAddAndContains) {
    ReadWriteDeltaHashTable dht;
    std::atomic<bool> done{false};

    std::thread writer([&dht, &done]() {
        for (int i = 0; i < 5000; ++i) {
            std::string key = "key" + std::to_string(i % 100);
            dht.addEntry(H(key),
                         static_cast<uint64_t>(i + 1),
                         static_cast<uint32_t>(i + 1));
        }
        done.store(true, std::memory_order_release);
    });

    std::vector<std::thread> readers;
    for (int t = 0; t < 3; ++t) {
        readers.emplace_back([&dht, &done]() {
            while (!done.load(std::memory_order_acquire)) {
                for (int k = 0; k < 100; ++k) {
                    std::string key = "key" + std::to_string(k);
                    uint64_t hkey = H(key);
                    dht.contains(hkey);
                    uint64_t pv;
                    uint32_t id;
                    dht.findFirst(hkey, pv, id);
                }
            }
        });
    }

    writer.join();
    for (auto& r : readers) r.join();
}

TEST(ReadWriteDHT, FindFirstDuringConcurrentAdd) {
    ReadWriteDeltaHashTable dht;
    const int N = 10000;
    std::atomic<bool> done{false};
    uint64_t hkey = H("key");

    std::thread writer([&dht, &done, N, hkey]() {
        for (int i = 1; i <= N; ++i) {
            dht.addEntry(hkey, static_cast<uint64_t>(i),
                         static_cast<uint32_t>(i));
        }
        done.store(true, std::memory_order_release);
    });

    std::atomic<bool> reader_ok{true};
    std::vector<std::thread> readers;
    for (int t = 0; t < 3; ++t) {
        readers.emplace_back([&dht, &done, &reader_ok, N, hkey]() {
            while (!done.load(std::memory_order_acquire)) {
                uint64_t pv;
                uint32_t id;
                if (dht.findFirst(hkey, pv, id)) {
                    if (pv < 1 || pv > static_cast<uint64_t>(N)) {
                        reader_ok.store(false, std::memory_order_relaxed);
                    }
                }
            }
        });
    }

    writer.join();
    for (auto& r : readers) r.join();
    EXPECT_TRUE(reader_ok.load());
}

// ============================================================
// Edge cases
// ============================================================

TEST(ReadOnlyDHT, EmptyTableFindFirst) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t pv;
    uint32_t id;
    EXPECT_FALSE(dht.findFirst(H("key"), pv, id));
}

TEST(ReadOnlyDHT, EmptyTableFindAll) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    EXPECT_FALSE(dht.findAll(H("key"), pvs, ids));
    EXPECT_TRUE(pvs.empty());
    EXPECT_TRUE(ids.empty());
}

TEST(ReadOnlyDHT, SingleEntryFindFirst) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    dht.addEntry(hkey, 42, 7);
    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, pv, id));
    EXPECT_EQ(pv, 42u);
    EXPECT_EQ(id, 7u);
}

TEST(ReadOnlyDHT, TwoEntriesSameSuffix) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 256;
    ReadOnlyDeltaHashTable dht(cfg);

    uint64_t hkey_a = H("key_a");
    dht.addEntry(hkey_a, 100, 1);
    dht.addEntry(hkey_a, 200, 2);

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey_a, pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey_a, pv, id));
    EXPECT_EQ(pv, 200u);
    EXPECT_EQ(id, 2u);
}

// ============================================================
// ReadWriteDeltaHashTable removeEntry / updateEntryId tests
// ============================================================

TEST(ReadWriteDHT, RemoveEntryBasic) {
    ReadWriteDeltaHashTable dht;
    uint64_t hkey = H("key");
    dht.addEntry(hkey, 100, 1);
    dht.addEntry(hkey, 200, 2);
    dht.addEntry(hkey, 300, 3);
    EXPECT_EQ(dht.size(), 3u);

    bool group_empty = dht.removeEntry(hkey, 200, 2);
    EXPECT_FALSE(group_empty);
    EXPECT_EQ(dht.size(), 2u);

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);
    std::set<uint64_t> pv_set(pvs.begin(), pvs.end());
    EXPECT_EQ(pv_set.count(100u), 1u);
    EXPECT_EQ(pv_set.count(300u), 1u);
    EXPECT_EQ(pv_set.count(200u), 0u);
}

TEST(ReadWriteDHT, RemoveEntryLastInGroup) {
    ReadWriteDeltaHashTable dht;
    uint64_t hkey = H("key");
    dht.addEntry(hkey, 100, 1);
    EXPECT_EQ(dht.size(), 1u);

    bool group_empty = dht.removeEntry(hkey, 100, 1);
    EXPECT_TRUE(group_empty);
    EXPECT_EQ(dht.size(), 0u);

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    EXPECT_FALSE(dht.findAll(hkey, pvs, ids));
}

TEST(ReadWriteDHT, RemoveEntryFromOverflowChain) {
    ReadWriteDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    ReadWriteDeltaHashTable dht(cfg);

    uint64_t hkey = H("key");
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    bool group_empty = dht.removeEntry(hkey, 50, 50);
    EXPECT_FALSE(group_empty);
    EXPECT_EQ(dht.size(), static_cast<size_t>(N - 1));

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, pvs, ids));
    EXPECT_EQ(pvs.size(), static_cast<size_t>(N - 1));
    std::set<uint32_t> id_set(ids.begin(), ids.end());
    EXPECT_EQ(id_set.count(50u), 0u);
    for (int i = 1; i <= N; ++i) {
        if (i == 50) continue;
        EXPECT_EQ(id_set.count(static_cast<uint32_t>(i)), 1u) << "missing id " << i;
    }
}

TEST(ReadWriteDHT, UpdateEntryIdBasic) {
    ReadWriteDeltaHashTable dht;
    uint64_t hkey = H("key");
    dht.addEntry(hkey, 100, 100);
    EXPECT_EQ(dht.size(), 1u);

    bool found = dht.updateEntryId(hkey, 100, 100, 200);
    EXPECT_TRUE(found);
    EXPECT_EQ(dht.size(), 1u);

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, pv, id));
    EXPECT_EQ(pv, 100u);
    EXPECT_EQ(id, 200u);
}

TEST(ReadWriteDHT, UpdateEntryIdOverflow) {
    ReadWriteDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    ReadWriteDeltaHashTable dht(cfg);

    uint64_t hkey = H("key");
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    bool found = dht.updateEntryId(hkey, 50, 50, 999999);
    EXPECT_TRUE(found);
    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, pvs, ids));
    EXPECT_EQ(pvs.size(), static_cast<size_t>(N));
    bool found_updated = false;
    for (size_t i = 0; i < pvs.size(); ++i) {
        if (pvs[i] == 50) {
            EXPECT_EQ(ids[i], 999999u);
            found_updated = true;
        }
    }
    EXPECT_TRUE(found_updated);
}

TEST(ReadWriteDHT, ConcurrentRemoveAndFind) {
    ReadWriteDeltaHashTable dht;
    const int N = 2000;
    uint64_t hkey = H("key");

    for (int i = 1; i <= N; ++i) {
        dht.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    std::atomic<bool> done{false};

    std::vector<std::thread> writers;
    for (int t = 0; t < 2; ++t) {
        writers.emplace_back([&dht, &done, t, N, hkey]() {
            for (int i = t + 1; i <= N; i += 2) {
                dht.removeEntry(hkey, static_cast<uint64_t>(i),
                                static_cast<uint32_t>(i));
            }
        });
    }

    std::vector<std::thread> readers;
    for (int t = 0; t < 2; ++t) {
        readers.emplace_back([&dht, &done, hkey]() {
            for (int i = 0; i < 1000; ++i) {
                uint64_t pv;
                uint32_t id;
                dht.findFirst(hkey, pv, id);
            }
        });
    }

    for (auto& w : writers) w.join();
    for (auto& r : readers) r.join();
    EXPECT_EQ(dht.size(), 0u);
}

TEST(ReadOnlyDHT, MaxPackedVersion) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t hkey = H("key");
    uint64_t max_pv = UINT64_MAX - 1;
    dht.addEntry(hkey, max_pv, 42);

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, pv, id));
    EXPECT_EQ(pv, max_pv);
    EXPECT_EQ(id, 42u);
}

// ============================================================
// BucketCodec round-trip tests
// ============================================================

// Helper: expose protected decodeBucket/encodeBucket for testing.
class TestableDHT : public ReadOnlyDeltaHashTable {
public:
    using ReadOnlyDeltaHashTable::ReadOnlyDeltaHashTable;
    using DeltaHashTable::BucketContents;
    using DeltaHashTable::KeyEntry;

    BucketContents testDecodeBucket(uint32_t bi) const {
        return codec_.decodeBucket(buckets_[bi]);
    }
    size_t testEncodeBucket(uint32_t bi, const BucketContents& contents) {
        return codec_.encodeBucket(const_cast<Bucket&>(buckets_[bi]), contents);
    }
    uint8_t testSuffixBits() const { return suffix_bits_; }
};

TEST(BucketCodec, EmptyBucketRoundTrip) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 64;
    TestableDHT dht(cfg);

    auto contents = dht.testDecodeBucket(0);
    EXPECT_TRUE(contents.keys.empty());

    size_t bits = dht.testEncodeBucket(0, contents);
    EXPECT_EQ(bits, 16u);  // just the uint16_t N_k header

    auto rt = dht.testDecodeBucket(0);
    EXPECT_TRUE(rt.keys.empty());
}

TEST(BucketCodec, SingleKeyRoundTrip) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    TestableDHT dht(cfg);

    // Write a single key with one version/id pair.
    DeltaHashTable::BucketContents contents;
    DeltaHashTable::KeyEntry ke;
    ke.suffix = 0x12345;
    ke.packed_versions = {1000};
    ke.ids = {42};
    contents.keys.push_back(ke);

    dht.testEncodeBucket(0, contents);

    auto rt = dht.testDecodeBucket(0);
    ASSERT_EQ(rt.keys.size(), 1u);
    EXPECT_EQ(rt.keys[0].suffix, 0x12345u);
    ASSERT_EQ(rt.keys[0].packed_versions.size(), 1u);
    EXPECT_EQ(rt.keys[0].packed_versions[0], 1000u);
    ASSERT_EQ(rt.keys[0].ids.size(), 1u);
    EXPECT_EQ(rt.keys[0].ids[0], 42u);
}

TEST(BucketCodec, MultipleKeysRoundTrip) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 256;
    TestableDHT dht(cfg);

    DeltaHashTable::BucketContents contents;

    // Three keys with different suffixes, sorted ascending.
    DeltaHashTable::KeyEntry k1, k2, k3;
    k1.suffix = 100; k1.packed_versions = {500}; k1.ids = {1};
    k2.suffix = 200; k2.packed_versions = {600, 400}; k2.ids = {2, 3};
    k3.suffix = 300; k3.packed_versions = {700}; k3.ids = {4};

    contents.keys = {k1, k2, k3};
    dht.testEncodeBucket(0, contents);

    auto rt = dht.testDecodeBucket(0);
    ASSERT_EQ(rt.keys.size(), 3u);

    EXPECT_EQ(rt.keys[0].suffix, 100u);
    EXPECT_EQ(rt.keys[1].suffix, 200u);
    EXPECT_EQ(rt.keys[2].suffix, 300u);

    ASSERT_EQ(rt.keys[1].packed_versions.size(), 2u);
    EXPECT_EQ(rt.keys[1].packed_versions[0], 600u);
    EXPECT_EQ(rt.keys[1].packed_versions[1], 400u);
    ASSERT_EQ(rt.keys[1].ids.size(), 2u);
    EXPECT_EQ(rt.keys[1].ids[0], 2u);
    EXPECT_EQ(rt.keys[1].ids[1], 3u);
}

TEST(BucketCodec, ManyVersionsRoundTrip) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 512;
    TestableDHT dht(cfg);

    DeltaHashTable::BucketContents contents;
    DeltaHashTable::KeyEntry ke;
    ke.suffix = 0xABCDE;

    // 20 versions descending, with various ids.
    for (int i = 20; i >= 1; --i) {
        ke.packed_versions.push_back(static_cast<uint64_t>(i) * 100);
        ke.ids.push_back(static_cast<uint32_t>(i));
    }
    contents.keys.push_back(ke);
    dht.testEncodeBucket(0, contents);

    auto rt = dht.testDecodeBucket(0);
    ASSERT_EQ(rt.keys.size(), 1u);
    ASSERT_EQ(rt.keys[0].packed_versions.size(), 20u);
    ASSERT_EQ(rt.keys[0].ids.size(), 20u);

    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(rt.keys[0].packed_versions[i], static_cast<uint64_t>(20 - i) * 100);
        EXPECT_EQ(rt.keys[0].ids[i], static_cast<uint32_t>(20 - i));
    }
}

TEST(BucketCodec, ZigzagDeltaRoundTrip) {
    // Test that ids with non-monotonic deltas round-trip correctly.
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 256;
    TestableDHT dht(cfg);

    DeltaHashTable::BucketContents contents;
    DeltaHashTable::KeyEntry ke;
    ke.suffix = 0x42;
    ke.packed_versions = {1000, 900, 800};
    ke.ids = {50, 10, 100};  // non-monotonic: 50, then -40, then +90
    contents.keys.push_back(ke);

    dht.testEncodeBucket(0, contents);
    auto rt = dht.testDecodeBucket(0);

    ASSERT_EQ(rt.keys.size(), 1u);
    ASSERT_EQ(rt.keys[0].ids.size(), 3u);
    EXPECT_EQ(rt.keys[0].ids[0], 50u);
    EXPECT_EQ(rt.keys[0].ids[1], 10u);
    EXPECT_EQ(rt.keys[0].ids[2], 100u);
}

TEST(BucketCodec, LargePackedVersionRoundTrip) {
    // Test with very large packed_version values (near UINT64_MAX).
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 256;
    TestableDHT dht(cfg);

    DeltaHashTable::BucketContents contents;
    DeltaHashTable::KeyEntry ke;
    ke.suffix = 0xFF;
    ke.packed_versions = {UINT64_MAX - 1, UINT64_MAX - 100};
    ke.ids = {999, 1};
    contents.keys.push_back(ke);

    dht.testEncodeBucket(0, contents);
    auto rt = dht.testDecodeBucket(0);

    ASSERT_EQ(rt.keys.size(), 1u);
    EXPECT_EQ(rt.keys[0].packed_versions[0], UINT64_MAX - 1);
    EXPECT_EQ(rt.keys[0].packed_versions[1], UINT64_MAX - 100);
    EXPECT_EQ(rt.keys[0].ids[0], 999u);
    EXPECT_EQ(rt.keys[0].ids[1], 1u);
}

// TestableRWDHT for memory leak tests — exposes extension count/arena.
class TestableRWDHT : public ReadWriteDeltaHashTable {
public:
    using ReadWriteDeltaHashTable::ReadWriteDeltaHashTable;

    size_t extensionCount() const { return ext_arena_->size(); }
    const BucketArena& arena() const { return *ext_arena_; }
};











// ============================================================
// Memory leak tests
// ============================================================

// Helper: ReadOnly DHT with exposed extension count.
class TestableRODHT : public ReadOnlyDeltaHashTable {
public:
    using ReadOnlyDeltaHashTable::ReadOnlyDeltaHashTable;
    size_t extensionCount() const { return ext_arena_->size(); }
    const BucketArena& arena() const { return *ext_arena_; }
};

// Compute base memoryUsage for a config (no extensions).
static size_t baseMemory(const DeltaHashTable::Config& cfg) {
    size_t stride = cfg.bucket_bytes;  // no padding, stride == bucket_bytes
    return (1u << cfg.bucket_bits) * stride;
}

TEST(MemoryLeak, ReadOnlyClearReleasesExtensions) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    for (int i = 1; i <= 500; ++i) {
        std::string key = "key" + std::to_string(i % 5);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    ASSERT_GT(dht.extensionCount(), 0u);
    ASSERT_GT(dht.memoryUsage(), baseMemory(cfg));

    dht.clear();

    EXPECT_EQ(dht.extensionCount(), 0u);
    EXPECT_EQ(dht.memoryUsage(), baseMemory(cfg));
    EXPECT_EQ(dht.size(), 0u);
}

TEST(MemoryLeak, ReadOnlyMoveTransfersOwnership) {
    auto cfg = smallBucketConfig();
    TestableRODHT src(cfg);
    for (int i = 1; i <= 300; ++i) {
        std::string key = "k" + std::to_string(i % 3);
        src.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    size_t ext_before = src.extensionCount();
    size_t mem_before = src.memoryUsage();
    ASSERT_GT(ext_before, 0u);

    TestableRODHT dst(std::move(src));
    EXPECT_EQ(dst.extensionCount(), ext_before);
    EXPECT_EQ(dst.memoryUsage(), mem_before);

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    EXPECT_TRUE(dst.findAll(H("k0"), pvs, ids));
    EXPECT_FALSE(pvs.empty());
}

TEST(MemoryLeak, ReadWriteClearReleasesExtensions) {
    ReadWriteDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;

    // Use TestableRWDHT to inspect extension count, but note it can't be
    // moved (deleted move ctor), so we test clear() only.
    DeltaHashTable::Config dcfg;
    dcfg.bucket_bits = cfg.bucket_bits;
    dcfg.bucket_bytes = cfg.bucket_bytes;
    TestableRWDHT dht(dcfg);

    for (int i = 1; i <= 500; ++i) {
        std::string key = "key" + std::to_string(i % 5);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    ASSERT_GT(dht.extensionCount(), 0u);

    dht.clear();

    EXPECT_EQ(dht.extensionCount(), 0u);
    EXPECT_EQ(dht.size(), 0u);
}

TEST(MemoryLeak, RepeatedAddRemoveCyclesStableMemory) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 64;
    TestableRWDHT dht(cfg);

    for (int cycle = 0; cycle < 5; ++cycle) {
        const int N = 200;
        for (int i = 1; i <= N; ++i) {
            std::string key = "key" + std::to_string(i);
            dht.addEntry(H(key),
                         static_cast<uint64_t>(i), static_cast<uint32_t>(i));
        }
        ASSERT_GT(dht.extensionCount(), 0u);

        for (int i = 1; i <= N; ++i) {
            std::string key = "key" + std::to_string(i);
            dht.removeEntry(H(key),
                            static_cast<uint64_t>(i), static_cast<uint32_t>(i));
        }
    }

    for (int i = 1; i <= 200; ++i) {
        std::string key = "key" + std::to_string(i);
        EXPECT_FALSE(dht.contains(H(key)));
    }
    EXPECT_EQ(dht.size(), 0u);
}

TEST(MemoryLeak, PruneEmptyExtensionOnRemove) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;  // very small → forces extensions quickly
    TestableRWDHT dht(cfg);

    // Add enough entries across multiple keys so different extension chain
    // tails become empty during removal (pruneEmptyExtension unlinks empty
    // tail extensions). Using multiple keys spreads entries across buckets,
    // meaning some extension buckets become entirely empty sooner.
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        std::string key = "key" + std::to_string(i % 10);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    size_t ext_after_add = dht.extensionCount();
    ASSERT_GT(ext_after_add, 0u);

    for (int i = 1; i <= N; ++i) {
        std::string key = "key" + std::to_string(i % 10);
        dht.removeEntry(H(key),
                        static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    for (int i = 0; i < 10; ++i) {
        std::string key = "key" + std::to_string(i);
        EXPECT_FALSE(dht.contains(H(key)));
    }
    EXPECT_EQ(dht.size(), 0u);
}

TEST(MemoryLeak, ClearAfterOverflowThenReuseWorks) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    for (int i = 1; i <= 300; ++i) {
        std::string key = "k" + std::to_string(i);
        dht.addEntry(H(key), static_cast<uint64_t>(i),
                     static_cast<uint32_t>(i));
    }
    ASSERT_GT(dht.extensionCount(), 0u);

    dht.clear();
    ASSERT_EQ(dht.extensionCount(), 0u);

    for (int i = 1; i <= 300; ++i) {
        std::string key = "k" + std::to_string(i);
        dht.addEntry(H(key), static_cast<uint64_t>(i + 1000),
                     static_cast<uint32_t>(i));
    }

    for (int i = 1; i <= 300; ++i) {
        std::string key = "k" + std::to_string(i);
        ASSERT_TRUE(dht.contains(H(key))) << "missing after reuse: " << key;
    }
}

// ============================================================
// BucketArena tests
// ============================================================

// Basic allocation: size and dataBytes track correctly.
TEST(BucketArena, SizeAndDataBytesTrackAllocations) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    EXPECT_EQ(dht.arena().size(), 0u);
    EXPECT_EQ(dht.arena().dataBytes(), 0u);

    for (int i = 1; i <= 300; ++i) {
        std::string key = "k" + std::to_string(i);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    uint32_t ext_count = dht.arena().size();
    ASSERT_GT(ext_count, 0u);

    size_t stride = cfg.bucket_bytes;  // no padding, stride == bucket_bytes
    EXPECT_EQ(dht.arena().dataBytes(), static_cast<size_t>(ext_count) * stride);
}

// Pointer stability: pointers from early allocations remain valid after
// further allocations that span multiple chunks.
TEST(BucketArena, PointerStabilityAcrossChunks) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    for (int i = 1; i <= 2000; ++i) {
        std::string key = "k" + std::to_string(i);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    uint32_t ext_count = dht.arena().size();
    ASSERT_GT(ext_count, 64u) << "need multiple chunks for this test";

    // Collect all Bucket* pointers.
    std::vector<const void*> ptrs;
    for (uint32_t i = 1; i <= ext_count; ++i) {
        ptrs.push_back(dht.arena().get(i));
    }

    // All pointers must be non-null and unique.
    for (uint32_t i = 0; i < ext_count; ++i) {
        ASSERT_NE(ptrs[i], nullptr) << "null pointer at index " << (i + 1);
    }
    std::set<const void*> unique_ptrs(ptrs.begin(), ptrs.end());
    EXPECT_EQ(unique_ptrs.size(), ptrs.size()) << "duplicate Bucket* pointers";

    for (int i = 1; i <= 2000; ++i) {
        std::string key = "k" + std::to_string(i);
        ASSERT_TRUE(dht.contains(H(key)))
            << "missing key after multi-chunk allocation: k" << i;
    }
}

// Clear resets the arena completely; subsequent allocations work.
TEST(BucketArena, ClearResetsAndReallocWorks) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    for (int i = 1; i <= 500; ++i) {
        std::string key = "k" + std::to_string(i);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    uint32_t ext_before = dht.arena().size();
    ASSERT_GT(ext_before, 0u);

    dht.clear();
    EXPECT_EQ(dht.arena().size(), 0u);
    EXPECT_EQ(dht.arena().dataBytes(), 0u);

    for (int i = 1; i <= 500; ++i) {
        std::string key = "k" + std::to_string(i);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i + 1000), static_cast<uint32_t>(i));
    }
    EXPECT_GT(dht.arena().size(), 0u);

    for (int i = 1; i <= 500; ++i) {
        std::string key = "k" + std::to_string(i);
        ASSERT_TRUE(dht.contains(H(key)));
    }
}

// Repeated clear/fill cycles don't corrupt data or leak (ASAN will catch leaks).
TEST(BucketArena, RepeatedClearFillCycles) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    for (int cycle = 0; cycle < 10; ++cycle) {
        for (int i = 1; i <= 200; ++i) {
            std::string key = "k" + std::to_string(i);
            dht.addEntry(H(key),
                         static_cast<uint64_t>(cycle * 1000 + i),
                         static_cast<uint32_t>(i));
        }
        ASSERT_GT(dht.arena().size(), 0u)
            << "no extensions on cycle " << cycle;

        for (int i = 1; i <= 200; ++i) {
            std::string key = "k" + std::to_string(i);
            uint64_t pv;
            uint32_t id;
            ASSERT_TRUE(dht.findFirst(H(key), pv, id))
                << "missing on cycle " << cycle << " key k" << i;
        }

        dht.clear();
        EXPECT_EQ(dht.arena().size(), 0u);
    }
}

// memoryUsage is consistent with arena dataBytes.
TEST(BucketArena, MemoryUsageConsistency) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    size_t base = baseMemory(cfg);
    EXPECT_EQ(dht.memoryUsage(), base);

    for (int i = 1; i <= 500; ++i) {
        std::string key = "k" + std::to_string(i);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    EXPECT_EQ(dht.memoryUsage(), base + dht.arena().dataBytes());

    dht.clear();
    EXPECT_EQ(dht.memoryUsage(), base);
}

// Extensions created via ReadWrite DHT also use the arena correctly.
TEST(BucketArena, ReadWriteArenaTracking) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;
    TestableRWDHT dht(cfg);

    EXPECT_EQ(dht.arena().size(), 0u);

    for (int i = 1; i <= 500; ++i) {
        std::string key = "key" + std::to_string(i % 5);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    uint32_t ext_after_fill = dht.arena().size();
    ASSERT_GT(ext_after_fill, 0u);

    for (int i = 1; i <= 500; ++i) {
        std::string key = "key" + std::to_string(i % 5);
        dht.removeEntry(H(key),
                        static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    EXPECT_EQ(dht.size(), 0u);
    for (int i = 0; i < 5; ++i) {
        std::string key = "key" + std::to_string(i);
        EXPECT_FALSE(dht.contains(H(key)));
    }

    // clear() resets the arena.
    dht.clear();
    EXPECT_EQ(dht.arena().size(), 0u);
    EXPECT_EQ(dht.arena().dataBytes(), 0u);
}

// Chunk boundary: exactly kBucketsPerChunk extensions, then one more.
TEST(BucketArena, ChunkBoundaryTransition) {
    // Use a very small bucket to force many extensions quickly.
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 2;   // 4 buckets
    cfg.bucket_bytes = 32; // tiny
    TestableRODHT dht(cfg);

    int i = 1;
    while (dht.arena().size() <= 64) {
        std::string key = "k" + std::to_string(i);
        dht.addEntry(H(key),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
        ++i;
    }
    ASSERT_GT(dht.arena().size(), 64u)
        << "need >64 extensions to test chunk boundary";

    for (int j = 1; j < i; ++j) {
        std::string key = "k" + std::to_string(j);
        ASSERT_TRUE(dht.contains(H(key)))
            << "missing key k" << j << " after crossing chunk boundary";
    }
}

// Concurrent allocate: N threads each call allocate() M times.
// All returned indices must be unique and size() == N*M.
TEST(BucketArena, ConcurrentAllocate) {
    const uint32_t slot_size = 64;
    BucketArena arena(slot_size, /*concurrent=*/true);

    constexpr int N = 8;   // threads
    constexpr int M = 500; // allocations per thread

    std::vector<std::vector<uint32_t>> per_thread(N);
    std::vector<std::thread> threads;
    threads.reserve(N);

    for (int t = 0; t < N; ++t) {
        threads.emplace_back([&arena, &per_thread, t]() {
            per_thread[t].reserve(M);
            for (int i = 0; i < M; ++i) {
                per_thread[t].push_back(arena.allocate());
            }
        });
    }
    for (auto& th : threads) th.join();

    EXPECT_EQ(arena.size(), static_cast<uint32_t>(N * M));

    // All indices must be unique.
    std::set<uint32_t> all_indices;
    for (int t = 0; t < N; ++t) {
        for (uint32_t idx : per_thread[t]) {
            ASSERT_TRUE(all_indices.insert(idx).second)
                << "duplicate index " << idx;
        }
    }
    EXPECT_EQ(all_indices.size(), static_cast<size_t>(N * M));

    // All pointers must be valid and unique.
    std::set<const void*> all_ptrs;
    for (uint32_t idx : all_indices) {
        const Bucket* b = arena.get(idx);
        ASSERT_NE(b, nullptr);
        ASSERT_TRUE(all_ptrs.insert(b).second)
            << "duplicate pointer for index " << idx;
    }
}

// ============================================================
// Binary savepoint tests
// ============================================================

class SavepointTest : public ::testing::Test {
protected:
    void SetUp() override {
        db_dir_ = ::testing::TempDir() + "/gi_snap_v9_" +
                  std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::create_directories(db_dir_);
        ASSERT_TRUE(manifest_.create(db_dir_).ok());
        storage_ = std::make_unique<SegmentStorageManager>(manifest_);
        ASSERT_TRUE(storage_->open(db_dir_).ok());
        index_ = std::make_unique<GlobalIndex>(manifest_);
        GlobalIndex::Options opts;
        ASSERT_TRUE(index_->open(db_dir_, opts, *storage_).ok());
    }

    void TearDown() override {
        if (index_ && index_->isOpen()) index_->close();
        index_.reset();
        if (storage_) storage_->close();
        storage_.reset();
        manifest_.close();
        std::filesystem::remove_all(db_dir_);
    }

    void put(uint64_t hkey, uint64_t packed_version, uint32_t segment_id) {
        index_->applyPut(hkey, packed_version, segment_id);
    }

    std::string db_dir_;
    Manifest manifest_;
    std::unique_ptr<SegmentStorageManager> storage_;
    std::unique_ptr<GlobalIndex> index_;
};

TEST_F(SavepointTest, RoundTripBasic) {
    const int N = 1000;
    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        put(H(key), i * 10 + 1, i + 1);
    }

    ASSERT_EQ(index_->entryCount(), static_cast<size_t>(N));
    ASSERT_EQ(index_->keyCount(), static_cast<size_t>(N));

    ASSERT_TRUE(index_->storeSavepoint(0).ok());

    ASSERT_TRUE(index_->close().ok());
    storage_->close();
    manifest_.close();

    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    SegmentStorageManager storage2(manifest2);
    ASSERT_TRUE(storage2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts, storage2).ok());
    // Savepoint is loaded automatically during open()/recover()

    EXPECT_EQ(index2.entryCount(), static_cast<size_t>(N));
    EXPECT_EQ(index2.keyCount(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(index2.getLatest(H(key), pv, seg).ok()) << "missing key: " << key;
        EXPECT_EQ(pv, static_cast<uint64_t>(i * 10 + 1));
        EXPECT_EQ(seg, static_cast<uint32_t>(i + 1));
    }

    index2.close();
    storage2.close();
    manifest2.close();
}

TEST_F(SavepointTest, RoundTripMultiVersion) {
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t hkey = H(key);
        for (int v = 0; v < 5; ++v) {
            put(hkey, v * 100 + i + 1, v * 10 + i);
        }
    }

    ASSERT_EQ(index_->entryCount(), 500u);
    ASSERT_EQ(index_->keyCount(), 100u);

    ASSERT_TRUE(index_->storeSavepoint(0).ok());
    ASSERT_TRUE(index_->close().ok());
    storage_->close();
    manifest_.close();

    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    SegmentStorageManager storage2(manifest2);
    ASSERT_TRUE(storage2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts, storage2).ok());
    // Savepoint is loaded automatically during open()/recover()

    EXPECT_EQ(index2.entryCount(), 500u);
    EXPECT_EQ(index2.keyCount(), 100u);

    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(index2.getLatest(H(key), pv, seg).ok());
        EXPECT_EQ(pv, static_cast<uint64_t>(4 * 100 + i + 1));
    }

    index2.close();
    storage2.close();
    manifest2.close();
}

TEST_F(SavepointTest, SavepointWithExtensions) {
    const int N = 200;
    for (int i = 0; i < N; ++i) {
        std::string key = "extkey_" + std::to_string(i);
        uint64_t hkey = H(key);
        for (int v = 0; v < 50; ++v) {
            put(hkey, v * 1000 + i + 1, v);
        }
    }

    size_t entry_count = index_->entryCount();
    size_t key_count = index_->keyCount();
    ASSERT_GT(entry_count, 0u);

    ASSERT_TRUE(index_->storeSavepoint(0).ok());
    ASSERT_TRUE(index_->close().ok());
    storage_->close();
    manifest_.close();

    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    SegmentStorageManager storage2(manifest2);
    ASSERT_TRUE(storage2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts, storage2).ok());
    // Savepoint is loaded automatically during open()/recover()

    EXPECT_EQ(index2.entryCount(), entry_count);
    EXPECT_EQ(index2.keyCount(), key_count);

    for (int i = 0; i < N; ++i) {
        std::string key = "extkey_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(index2.getLatest(H(key), pv, seg).ok()) << "missing key: " << key;
    }

    index2.close();
    storage2.close();
    manifest2.close();
}

TEST_F(SavepointTest, SavepointBlocksConcurrentPut) {
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        put(H(key), i + 1, i);
    }

    std::atomic<bool> savepoint_started{false};
    std::atomic<bool> savepoint_done{false};
    std::atomic<bool> put_started{false};
    std::atomic<bool> put_done{false};

    uint64_t hblocked = H("blocked_key");
    std::thread writer([&]() {
        while (!savepoint_started.load(std::memory_order_acquire)) {
        }
        put_started.store(true, std::memory_order_release);
        index_->applyPut(hblocked, 999, 42);
        put_done.store(true, std::memory_order_release);
    });

    savepoint_started.store(true, std::memory_order_release);
    ASSERT_TRUE(index_->storeSavepoint(0).ok());
    savepoint_done.store(true, std::memory_order_release);

    writer.join();

    EXPECT_TRUE(put_done.load());

    uint64_t pv;
    uint32_t seg;
    EXPECT_TRUE(index_->getLatest(hblocked, pv, seg).ok());
    EXPECT_EQ(pv, 999u);
    EXPECT_EQ(seg, 42u);
}

// applyPut (flush path) runs concurrently with savepoint.
// After both complete, the flushed entries should be visible.
TEST_F(SavepointTest, SavepointConcurrentWithFlush) {
    // Pre-populate so the savepoint has work to do.
    for (int i = 0; i < 200; ++i) {
        std::string key = "pre_" + std::to_string(i);
        put(H(key), i + 1, i);
    }

    std::atomic<bool> go{false};
    std::atomic<bool> flush_done{false};

    // Flush thread: applies entries via applyPut (no BatchGuard needed).
    const int flush_count = 100;
    std::thread flusher([&]() {
        while (!go.load(std::memory_order_acquire)) {}
        for (int i = 0; i < flush_count; ++i) {
            std::string key = "flush_" + std::to_string(i);
            uint64_t hkey = H(key);
            uint64_t pv = static_cast<uint64_t>(1000 + i);
            index_->applyPut(hkey, pv, 99);
        }
        flush_done.store(true, std::memory_order_release);
    });

    // Savepoint thread: run concurrently.
    go.store(true, std::memory_order_release);
    ASSERT_TRUE(index_->storeSavepoint(0).ok());
    flusher.join();
    EXPECT_TRUE(flush_done.load());

    // All flushed entries must be visible.
    for (int i = 0; i < flush_count; ++i) {
        std::string key = "flush_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        EXPECT_TRUE(index_->getLatest(H(key), pv, seg).ok()) << "missing: " << key;
        EXPECT_EQ(pv, static_cast<uint64_t>(1000 + i));
        EXPECT_EQ(seg, 99u);
    }

    // Reopen and verify entries survived.
    ASSERT_TRUE(index_->close().ok());
    storage_->close();
    manifest_.close();

    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    SegmentStorageManager storage2(manifest2);
    ASSERT_TRUE(storage2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts, storage2).ok());

    for (int i = 0; i < flush_count; ++i) {
        std::string key = "flush_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        EXPECT_TRUE(index2.getLatest(H(key), pv, seg).ok()) << "missing after reopen: " << key;
    }

    // Pre-populated entries should also survive.
    EXPECT_GE(index2.keyCount(), 200u);

    index2.close();
    storage2.close();
    manifest2.close();
}

TEST_F(SavepointTest, AtomicSwapLeavesValidDir) {
    for (int i = 0; i < 10; ++i) {
        std::string key = "key_" + std::to_string(i);
        put(H(key), i + 1, i);
    }

    ASSERT_TRUE(index_->storeSavepoint(0).ok());

    std::string valid_dir = db_dir_ + "/gi/savepoint";
    std::string tmp_dir = valid_dir + ".tmp";
    std::string old_dir = valid_dir + ".old";

    EXPECT_TRUE(std::filesystem::exists(valid_dir));
    EXPECT_TRUE(std::filesystem::is_directory(valid_dir));
    EXPECT_FALSE(std::filesystem::exists(tmp_dir));
    EXPECT_FALSE(std::filesystem::exists(old_dir));

    for (int i = 10; i < 20; ++i) {
        std::string key = "key_" + std::to_string(i);
        put(H(key), i + 1, i);
    }
    ASSERT_TRUE(index_->storeSavepoint(0).ok());

    EXPECT_TRUE(std::filesystem::exists(valid_dir));
    EXPECT_FALSE(std::filesystem::exists(tmp_dir));
    EXPECT_FALSE(std::filesystem::exists(old_dir));
}

TEST_F(SavepointTest, WriteSavepointThenLoad) {
    for (int i = 0; i < 50; ++i) {
        std::string key = "key_" + std::to_string(i);
        index_->applyPut(H(key), i + 1, i);
    }

    ASSERT_TRUE(index_->storeSavepoint(0).ok());
    ASSERT_TRUE(index_->close().ok());

    std::string valid_dir = db_dir_ + "/gi/savepoint";
    EXPECT_TRUE(std::filesystem::exists(valid_dir));

    // Re-open and verify data is loaded from savepoint.
    storage_->close();
    manifest_.close();
    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    SegmentStorageManager storage2(manifest2);
    ASSERT_TRUE(storage2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts, storage2).ok());

    EXPECT_EQ(index2.entryCount(), 50u);
    EXPECT_EQ(index2.keyCount(), 50u);

    for (int i = 0; i < 50; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(index2.getLatest(H(key), pv, seg).ok());
    }

    index2.close();
    storage2.close();
    manifest2.close();
}

TEST_F(SavepointTest, EmptyIndexSavepoint) {
    // Savepoint an empty index — should produce valid savepoint with 0 entries.
    ASSERT_TRUE(index_->storeSavepoint(0).ok());

    ASSERT_TRUE(index_->close().ok());
    storage_->close();
    manifest_.close();

    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    SegmentStorageManager storage2(manifest2);
    ASSERT_TRUE(storage2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts, storage2).ok());

    EXPECT_EQ(index2.entryCount(), 0u);
    EXPECT_EQ(index2.keyCount(), 0u);

    index2.close();
    storage2.close();
    manifest2.close();
}

// ============================================================
// RWDHT snapshotBucketChain / loadBucketChain tests
// ============================================================

TEST(RWDHTChainSnapshot, RoundTripSingleBucket) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;      // 16 buckets
    cfg.bucket_bytes = 64;    // small buckets to force extensions
    ReadWriteDeltaHashTable src(cfg);

    // Insert entries that will land in the same bucket and force extensions.
    // Use a single key with many versions.
    uint64_t hkey = H("chain_test_key");
    for (int i = 1; i <= 100; ++i) {
        src.addEntry(hkey, static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    ASSERT_EQ(src.size(), 100u);

    // Snapshot the bucket containing our key.
    uint32_t bi = hkey >> (64 - cfg.bucket_bits);
    std::vector<uint8_t> buf;
    uint32_t chain_len = src.snapshotBucketChain(bi, buf);
    ASSERT_GE(chain_len, 1u);
    ASSERT_EQ(buf.size(), static_cast<size_t>(chain_len) * src.bucketStride());

    // Load into a fresh DHT.
    ReadWriteDeltaHashTable dst(cfg);
    dst.loadBucketChain(bi, buf.data(), static_cast<uint8_t>(chain_len));

    // Verify all entries round-tripped.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dst.findAll(hkey, pvs, ids));
    EXPECT_EQ(pvs.size(), 100u);

    std::set<uint32_t> id_set(ids.begin(), ids.end());
    for (int i = 1; i <= 100; ++i) {
        EXPECT_EQ(id_set.count(static_cast<uint32_t>(i)), 1u) << "missing id " << i;
    }
}

TEST(RWDHTChainSnapshot, EmptyBucket) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    ReadWriteDeltaHashTable dht(cfg);

    // Bucket 0 should be empty.
    std::vector<uint8_t> buf;
    uint32_t chain_len = dht.snapshotBucketChain(0, buf);
    EXPECT_EQ(chain_len, 0u);
    EXPECT_TRUE(buf.empty());
}

TEST(RWDHTChainSnapshot, MultipleKeysSameBucket) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;      // 2 buckets — most keys collide
    cfg.bucket_bytes = 64;
    ReadWriteDeltaHashTable src(cfg);

    // Insert many distinct keys to force extensions.
    for (int k = 0; k < 20; ++k) {
        uint64_t hkey = H("mk_" + std::to_string(k));
        for (int v = 1; v <= 3; ++v) {
            src.addEntry(hkey, static_cast<uint64_t>(v), static_cast<uint32_t>(k));
        }
    }

    // Snapshot + load all buckets into a fresh DHT.
    ReadWriteDeltaHashTable dst(cfg);
    std::vector<uint8_t> buf;
    for (uint32_t bi = 0; bi < dst.numBuckets(); ++bi) {
        uint32_t cl = src.snapshotBucketChain(bi, buf);
        if (cl > 0) {
            dst.loadBucketChain(bi, buf.data(), static_cast<uint8_t>(cl));
        }
    }
    dst.setSize(src.size());

    // Verify all keys round-tripped.
    for (int k = 0; k < 20; ++k) {
        uint64_t hkey = H("mk_" + std::to_string(k));
        std::vector<uint64_t> pvs;
        std::vector<uint32_t> ids;
        ASSERT_TRUE(dst.findAll(hkey, pvs, ids)) << "missing key mk_" << k;
        EXPECT_EQ(pvs.size(), 3u);
    }
}

// ============================================================
// GlobalIndex recovery tests
// ============================================================

class RecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        db_dir_ = ::testing::TempDir() + "/gi_recovery_" +
                  std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::create_directories(db_dir_);
        ASSERT_TRUE(manifest_.create(db_dir_).ok());
        storage_ = std::make_unique<SegmentStorageManager>(manifest_);
        ASSERT_TRUE(storage_->open(db_dir_).ok());
    }

    void TearDown() override {
        if (storage_) storage_->close();
        storage_.reset();
        manifest_.close();
        std::filesystem::remove_all(db_dir_);
    }

    void put(GlobalIndex& idx, uint64_t hkey, uint64_t packed_version, uint32_t segment_id) {
        idx.applyPut(hkey, packed_version, segment_id);
    }

    // Populate index with N keys, each with packed_version = i+1, segment_id = i.
    void populate(GlobalIndex& idx, int N) {
        for (int i = 0; i < N; ++i) {
            std::string key = "key_" + std::to_string(i);
            idx.applyPut(H(key), i + 1, i);
        }
        // Write savepoint so data survives close+reopen.
        if (N > 0) idx.storeSavepoint(static_cast<uint32_t>(N - 1));
    }

    // Verify index contains N keys with expected packed_version = i+1, segment_id = i.
    void verify(GlobalIndex& idx, int N) {
        EXPECT_EQ(idx.keyCount(), static_cast<size_t>(N));
        EXPECT_EQ(idx.entryCount(), static_cast<size_t>(N));
        for (int i = 0; i < N; ++i) {
            std::string key = "key_" + std::to_string(i);
            uint64_t pv;
            uint32_t seg;
            ASSERT_TRUE(idx.getLatest(H(key), pv, seg).ok()) << "missing: " << key;
            EXPECT_EQ(pv, static_cast<uint64_t>(i + 1));
            EXPECT_EQ(seg, static_cast<uint32_t>(i));
        }
    }

    // Open a fresh GlobalIndex on db_dir_.
    std::unique_ptr<GlobalIndex> openIndex() {
        auto idx = std::make_unique<GlobalIndex>(manifest_);
        GlobalIndex::Options opts;
        Status s = idx->open(db_dir_, opts, *storage_);
        EXPECT_TRUE(s.ok()) << s.toString();
        return idx;
    }

    // Reopen manifest and storage (close + open).
    void reopenManifest() {
        if (storage_) storage_->close();
        manifest_.close();
        ASSERT_TRUE(manifest_.open(db_dir_).ok());
        storage_ = std::make_unique<SegmentStorageManager>(manifest_);
        ASSERT_TRUE(storage_->open(db_dir_).ok());
    }

    std::string savepointDir() { return db_dir_ + "/gi/savepoint"; }
    std::string savepointTmp() { return savepointDir() + ".tmp"; }
    std::string savepointOld() { return savepointDir() + ".old"; }

    std::string db_dir_;
    Manifest manifest_;
    std::unique_ptr<SegmentStorageManager> storage_;
};

// Clean close and reopen: savepoint is written on close, loaded on open.
TEST_F(RecoveryTest, CleanCloseAndReopen) {
    auto idx = openIndex();
    populate(*idx, 100);
    ASSERT_TRUE(idx->close().ok());
    reopenManifest();

    auto idx2 = openIndex();
    verify(*idx2, 100);
    idx2->close();
}

// Recovery converges: close + reopen produces consistent state.
TEST_F(RecoveryTest, RecoveryConverges) {
    auto idx = openIndex();
    populate(*idx, 100);
    ASSERT_TRUE(idx->close().ok());
    reopenManifest();

    auto idx2 = openIndex();
    verify(*idx2, 100);
    idx2->close();
}

// Simulate crash leaving .tmp dir (incomplete savepoint write).
// Recovery should discard .tmp and load the valid savepoint.
TEST_F(RecoveryTest, CrashLeavingTmpDir) {
    auto idx = openIndex();
    populate(*idx, 50);
    ASSERT_TRUE(idx->storeSavepoint(0).ok());

    // Add more data and savepoint again.
    for (int i = 50; i < 80; ++i) {
        std::string key = "key_" + std::to_string(i);
        put(*idx, H(key), i + 1, i);
    }
    ASSERT_TRUE(idx->storeSavepoint(79).ok());
    ASSERT_TRUE(idx->close().ok());
    reopenManifest();

    // Simulate a crashed .tmp dir from a partial savepoint write.
    std::filesystem::create_directories(savepointTmp());
    std::ofstream(savepointTmp() + "/garbage.dat") << "corrupt";

    auto idx2 = openIndex();
    // Should have all 80 keys (savepoint captured all 80).
    verify(*idx2, 80);
    // .tmp should be cleaned up.
    EXPECT_FALSE(std::filesystem::exists(savepointTmp()));
    idx2->close();
}

// Simulate crash between the two renames: valid→old succeeded,
// but tmp→valid didn't. Only .old exists.
// Recovery should restore .old as the valid savepoint.
TEST_F(RecoveryTest, CrashAfterFirstRename) {
    auto idx = openIndex();
    populate(*idx, 50);
    ASSERT_TRUE(idx->storeSavepoint(0).ok());
    ASSERT_TRUE(idx->close().ok());
    reopenManifest();

    // Simulate: rename valid → old (as storeSavepoint would do).
    ASSERT_TRUE(std::filesystem::exists(savepointDir()));
    std::filesystem::rename(savepointDir(), savepointOld());
    ASSERT_FALSE(std::filesystem::exists(savepointDir()));
    ASSERT_TRUE(std::filesystem::exists(savepointOld()));

    auto idx2 = openIndex();
    // Recovery should restore .old → valid and load data.
    verify(*idx2, 50);
    EXPECT_TRUE(std::filesystem::exists(savepointDir()));
    EXPECT_FALSE(std::filesystem::exists(savepointOld()));
    idx2->close();
}

// Simulate crash after swap completes but before .old cleanup.
// Both valid and .old exist. Recovery should clean up .old.
TEST_F(RecoveryTest, CrashBeforeOldCleanup) {
    auto idx = openIndex();
    populate(*idx, 50);
    ASSERT_TRUE(idx->storeSavepoint(0).ok());

    // Add more and savepoint again.
    for (int i = 50; i < 70; ++i) {
        std::string key = "key_" + std::to_string(i);
        put(*idx, H(key), i + 1, i);
    }
    ASSERT_TRUE(idx->storeSavepoint(0).ok());
    ASSERT_TRUE(idx->close().ok());
    reopenManifest();

    // Simulate leftover .old dir.
    std::filesystem::create_directories(savepointOld());
    std::ofstream(savepointOld() + "/stale.dat") << "old data";

    auto idx2 = openIndex();
    verify(*idx2, 70);
    // .old should be cleaned up.
    EXPECT_FALSE(std::filesystem::exists(savepointOld()));
    idx2->close();
}

// Recovery is idempotent: close + reopen doesn't corrupt the index.
TEST_F(RecoveryTest, IdempotentRecovery) {
    auto idx = openIndex();
    populate(*idx, 100);
    // Explicit savepoint captures all 100 keys.
    ASSERT_TRUE(idx->storeSavepoint(0).ok());

    // Write the same keys again with different values, then savepoint.
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        put(*idx, H(key), (i + 1) * 10, i + 100);
    }
    ASSERT_TRUE(idx->storeSavepoint(199).ok());
    ASSERT_TRUE(idx->close().ok());
    reopenManifest();

    auto idx2 = openIndex();
    // Each key should have 2 entries (original + update).
    EXPECT_EQ(idx2->keyCount(), 100u);
    EXPECT_EQ(idx2->entryCount(), 200u);
    // Latest for each key should be the update.
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(idx2->getLatest(H(key), pv, seg).ok());
        EXPECT_EQ(pv, static_cast<uint64_t>((i + 1) * 10));
        EXPECT_EQ(seg, static_cast<uint32_t>(i + 100));
    }
    idx2->close();
}

// Recovery with no savepoint — delete savepoint and reopen.
// Should recover empty (no lineage to replay).
TEST_F(RecoveryTest, RecoveryWithoutSavepoint) {
    auto idx = openIndex();
    populate(*idx, 30);
    ASSERT_TRUE(idx->storeSavepoint(0).ok());
    ASSERT_TRUE(idx->close().ok());
    reopenManifest();

    // Delete the savepoint to force recovery without it.
    std::filesystem::remove_all(savepointDir());

    auto idx2 = openIndex();
    // The key test is that open() doesn't fail.
    EXPECT_TRUE(idx2->isOpen());
    idx2->close();
}

// Multiple close/reopen cycles should be stable.
TEST_F(RecoveryTest, MultipleReopenCycles) {
    for (int cycle = 0; cycle < 3; ++cycle) {
        reopenManifest();
        auto idx = openIndex();
        int base = cycle * 20;
        for (int i = base; i < base + 20; ++i) {
            std::string key = "key_" + std::to_string(i);
            put(*idx, H(key), i + 1, i);
        }
        ASSERT_TRUE(idx->storeSavepoint(static_cast<uint32_t>(base + 19)).ok());
        ASSERT_TRUE(idx->close().ok());
    }

    reopenManifest();
    auto idx = openIndex();
    // Should have all 60 keys across 3 cycles.
    EXPECT_EQ(idx->keyCount(), 60u);
    EXPECT_EQ(idx->entryCount(), 60u);
    for (int i = 0; i < 60; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(idx->getLatest(H(key), pv, seg).ok()) << "missing: " << key;
    }
    idx->close();
}

// ============================================================
// Testable DHT for optimization tests — exposes protected helpers.
// ============================================================

class OptTestDHT : public ReadOnlyDeltaHashTable {
public:
    using ReadOnlyDeltaHashTable::ReadOnlyDeltaHashTable;
    using DeltaHashTable::BucketContents;
    using DeltaHashTable::KeyEntry;
    using DeltaHashTable::SuffixScanResult;

    SuffixScanResult testDecodeSuffixes(uint32_t bi) const {
        return codec_.decodeSuffixes(buckets_[bi]);
    }
    BucketContents testDecodeBucket(uint32_t bi) const {
        return codec_.decodeBucket(buckets_[bi]);
    }
    size_t testEncodeBucket(uint32_t bi, const BucketContents& contents) {
        return codec_.encodeBucket(const_cast<Bucket&>(buckets_[bi]), contents);
    }
    size_t testDecodeBucketUsedBits(uint32_t bi) const {
        return codec_.decodeBucketUsedBits(buckets_[bi]);
    }
    size_t testContentsBitsNeeded(const BucketContents& contents) const {
        return codec_.contentsBitsNeeded(contents);
    }
    uint8_t testSuffixBits() const { return suffix_bits_; }
    uint32_t testBucketIndex(uint64_t hash) const { return bucketIndex(hash); }
    uint64_t testSuffixFromHash(uint64_t hash) const { return suffixFromHash(hash); }
    const Bucket& testBucket(uint32_t bi) const { return buckets_[bi]; }

    KeyEntry testDecodeKeyAt(uint32_t bi, uint16_t key_index, uint64_t suffix) const {
        auto scan = codec_.decodeSuffixes(buckets_[bi]);
        return codec_.decodeKeyAt(buckets_[bi], key_index, suffix, scan.data_start_bit);
    }
};

class OptTestRWDHT : public ReadWriteDeltaHashTable {
public:
    using ReadWriteDeltaHashTable::ReadWriteDeltaHashTable;
    using DeltaHashTable::SuffixScanResult;

    SuffixScanResult testDecodeSuffixes(uint32_t bi) const {
        return codec_.decodeSuffixes(buckets_[bi]);
    }
    size_t testDecodeBucketUsedBits(uint32_t bi) const {
        return codec_.decodeBucketUsedBits(buckets_[bi]);
    }
    uint32_t testBucketIndex(uint64_t hash) const { return bucketIndex(hash); }
    uint64_t testSuffixFromHash(uint64_t hash) const { return suffixFromHash(hash); }
    bool testContainsByHash(uint32_t bi, uint64_t suffix) const {
        return containsByHash(bi, suffix);
    }
};

// ============================================================
// Suite: DHTTargetedScan (improvement 1)
// ============================================================

TEST(DHTTargetedScan, DecodeSuffixesEmpty) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    OptTestDHT dht(cfg);

    auto result = dht.testDecodeSuffixes(0);
    EXPECT_TRUE(result.suffixes.empty());
    EXPECT_EQ(result.data_start_bit, 16u);
}

TEST(DHTTargetedScan, DecodeSuffixesSingle) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    OptTestDHT dht(cfg);

    // Insert a single key.
    uint64_t hash = (0ULL << 60) | 0x12345ULL;  // bucket 0, suffix 0x12345
    dht.addEntry(hash, 1000, 42);

    auto result = dht.testDecodeSuffixes(0);
    ASSERT_EQ(result.suffixes.size(), 1u);
    EXPECT_EQ(result.suffixes[0], 0x12345u);
    // data_start_bit should be > 16 (16-bit header + suffix bits).
    EXPECT_GT(result.data_start_bit, 16u);
}

TEST(DHTTargetedScan, DecodeSuffixesMultiple) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 256;
    OptTestDHT dht(cfg);

    // Manually encode three keys in bucket 0.
    DeltaHashTable::BucketContents contents;
    DeltaHashTable::KeyEntry k1, k2, k3;
    k1.suffix = 100; k1.packed_versions = {500}; k1.ids = {1};
    k2.suffix = 200; k2.packed_versions = {600}; k2.ids = {2};
    k3.suffix = 300; k3.packed_versions = {700}; k3.ids = {3};
    contents.keys = {k1, k2, k3};
    dht.testEncodeBucket(0, contents);

    auto result = dht.testDecodeSuffixes(0);
    ASSERT_EQ(result.suffixes.size(), 3u);
    EXPECT_EQ(result.suffixes[0], 100u);
    EXPECT_EQ(result.suffixes[1], 200u);
    EXPECT_EQ(result.suffixes[2], 300u);
}

TEST(DHTTargetedScan, DecodeKeyAtRoundTrip) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 256;
    OptTestDHT dht(cfg);

    // Encode three keys with different version counts.
    DeltaHashTable::BucketContents contents;
    DeltaHashTable::KeyEntry k1, k2, k3;
    k1.suffix = 100; k1.packed_versions = {500, 400}; k1.ids = {1, 2};
    k2.suffix = 200; k2.packed_versions = {800, 700, 600}; k2.ids = {10, 20, 30};
    k3.suffix = 300; k3.packed_versions = {900}; k3.ids = {99};
    contents.keys = {k1, k2, k3};
    dht.testEncodeBucket(0, contents);

    // Decode each key by index.
    auto key1 = dht.testDecodeKeyAt(0, 0, 100);
    ASSERT_EQ(key1.packed_versions.size(), 2u);
    EXPECT_EQ(key1.packed_versions[0], 500u);
    EXPECT_EQ(key1.packed_versions[1], 400u);
    EXPECT_EQ(key1.ids[0], 1u);
    EXPECT_EQ(key1.ids[1], 2u);

    auto key2 = dht.testDecodeKeyAt(0, 1, 200);
    ASSERT_EQ(key2.packed_versions.size(), 3u);
    EXPECT_EQ(key2.packed_versions[0], 800u);
    EXPECT_EQ(key2.packed_versions[1], 700u);
    EXPECT_EQ(key2.packed_versions[2], 600u);
    EXPECT_EQ(key2.ids[0], 10u);
    EXPECT_EQ(key2.ids[1], 20u);
    EXPECT_EQ(key2.ids[2], 30u);

    auto key3 = dht.testDecodeKeyAt(0, 2, 300);
    ASSERT_EQ(key3.packed_versions.size(), 1u);
    EXPECT_EQ(key3.packed_versions[0], 900u);
    EXPECT_EQ(key3.ids[0], 99u);
}

TEST(DHTTargetedScan, FindFirstTargetedMatchesFull) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 512;
    OptTestDHT dht(cfg);

    // Insert 50 keys into the same bucket.
    std::vector<uint64_t> hashes;
    for (int i = 0; i < 50; ++i) {
        // Use bucket 0: high 4 bits = 0.
        uint64_t hash = static_cast<uint64_t>(i * 7 + 1);  // suffix only
        dht.addEntry(hash, 1000 + i, 100 + i);
        hashes.push_back(hash);
    }

    // Verify findFirst returns correct results for each.
    for (int i = 0; i < 50; ++i) {
        uint64_t pv;
        uint32_t id;
        ASSERT_TRUE(dht.findFirst(hashes[i], pv, id))
            << "Key " << i << " not found";
        EXPECT_EQ(pv, static_cast<uint64_t>(1000 + i));
        EXPECT_EQ(id, static_cast<uint32_t>(100 + i));
    }
}

TEST(DHTTargetedScan, FindAllTargetedMatchesFull) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 512;
    OptTestDHT dht(cfg);

    // Insert one key with 5 versions.
    uint64_t hash = 42;
    for (int v = 0; v < 5; ++v) {
        dht.addEntry(hash, 500 - v * 10, 100 + v);
    }

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hash, pvs, ids));
    ASSERT_EQ(pvs.size(), 5u);
    // Versions should be sorted descending.
    EXPECT_EQ(pvs[0], 500u);
    EXPECT_EQ(pvs[4], 460u);
    EXPECT_EQ(ids[0], 100u);
    EXPECT_EQ(ids[4], 104u);
}

// ============================================================
// Suite: DHTOverflowOpt (improvement 2)
// ============================================================

TEST(DHTOverflowOpt, AddToChainOverflowPreserves) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;  // 2 buckets
    cfg.bucket_bytes = 64; // tiny
    OptTestDHT dht(cfg);

    // Fill bucket 0 near capacity.
    std::vector<uint64_t> hashes;
    for (int i = 0; i < 20; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 3 + 1);  // bucket 0
        dht.addEntry(hash, 1000 + i, i);
        hashes.push_back(hash);
    }

    // Verify all entries are findable (some will be in extensions).
    for (int i = 0; i < 20; ++i) {
        uint64_t pv;
        uint32_t id;
        ASSERT_TRUE(dht.findFirst(hashes[i], pv, id))
            << "Key " << i << " not found after overflow";
        EXPECT_EQ(pv, static_cast<uint64_t>(1000 + i));
        EXPECT_EQ(id, static_cast<uint32_t>(i));
    }
}

TEST(DHTOverflowOpt, AddToChainMultipleLevelOverflow) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 32; // very tiny — triggers multiple extension levels
    OptTestDHT dht(cfg);

    // Insert enough keys to overflow through 3+ extension levels.
    std::vector<uint64_t> hashes;
    for (int i = 0; i < 30; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 5 + 1);  // bucket 0
        dht.addEntry(hash, 2000 + i, i);
        hashes.push_back(hash);
    }

    for (int i = 0; i < 30; ++i) {
        uint64_t pv;
        uint32_t id;
        ASSERT_TRUE(dht.findFirst(hashes[i], pv, id))
            << "Key " << i << " not found after multi-level overflow";
        EXPECT_EQ(pv, static_cast<uint64_t>(2000 + i));
    }
}

TEST(DHTOverflowOpt, AddToChainOverflowIsNew) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 32;
    OptTestDHT dht(cfg);

    // Fill bucket 0 until overflow happens.
    for (int i = 0; i < 5; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 3 + 1);
        bool is_new = dht.addEntryIsNew(hash, 1000 + i, i);
        EXPECT_TRUE(is_new) << "Key " << i << " should be new";
    }

    // Add another unique key that will overflow to extension.
    uint64_t overflow_hash = 999;  // bucket 0 (high bit 0)
    bool is_new = dht.addEntryIsNew(overflow_hash, 5000, 50);
    EXPECT_TRUE(is_new) << "Overflow key should be new";
}

TEST(DHTOverflowOpt, AddToChainAppendOverflowIsNotNew) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 32;
    OptTestDHT dht(cfg);

    // Insert some keys to fill bucket.
    uint64_t target_hash = 1;  // bucket 0
    dht.addEntry(target_hash, 1000, 1);

    for (int i = 1; i < 5; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 3 + 1);
        dht.addEntry(hash, 2000 + i, 10 + i);
    }

    // Now add another version of the same key — should overflow but is_new = false.
    bool is_new = dht.addEntryIsNew(target_hash, 900, 2);
    EXPECT_FALSE(is_new) << "Existing key with new version should not be new";
}

// ============================================================
// Suite: DHTForEachGroupOpt (improvement 3)
// ============================================================

TEST(DHTForEachGroupOpt, ForEachGroupSorted) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 64;
    OptTestDHT dht(cfg);

    // Add key with versions across primary + extension.
    uint64_t hash = 1;
    for (int v = 0; v < 8; ++v) {
        dht.addEntry(hash, 1000 - v * 10, v);
    }

    int group_count = 0;
    dht.forEachGroup([&](uint64_t h, const std::vector<uint64_t>& pvs,
                         const std::vector<uint32_t>& ids) {
        if (h == hash) {
            group_count++;
            // All 8 versions should be merged into one group.
            EXPECT_EQ(pvs.size(), 8u);
        }
    });
    EXPECT_EQ(group_count, 1);
}

TEST(DHTForEachGroupOpt, ForEachGroupManyKeys) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 64;  // small — forces extensions
    OptTestDHT dht(cfg);

    // Insert 100 distinct keys.
    std::set<uint64_t> inserted_hashes;
    for (int i = 0; i < 100; ++i) {
        uint64_t hash = H("forEachGroupKey_" + std::to_string(i));
        dht.addEntry(hash, 5000 + i, i);
        inserted_hashes.insert(hash);
    }

    std::set<uint64_t> seen_hashes;
    dht.forEachGroup([&](uint64_t h, const std::vector<uint64_t>& pvs,
                         const std::vector<uint32_t>& ids) {
        seen_hashes.insert(h);
        EXPECT_EQ(pvs.size(), ids.size());
    });
    EXPECT_EQ(seen_hashes.size(), 100u);
    EXPECT_EQ(seen_hashes, inserted_hashes);
}

TEST(DHTForEachGroupOpt, ForEachGroupNoExtensions) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 512;  // large — no overflow
    OptTestDHT dht(cfg);

    // Insert 3 keys in same bucket.
    DeltaHashTable::BucketContents contents;
    DeltaHashTable::KeyEntry k1, k2, k3;
    k1.suffix = 10; k1.packed_versions = {100}; k1.ids = {1};
    k2.suffix = 20; k2.packed_versions = {200}; k2.ids = {2};
    k3.suffix = 30; k3.packed_versions = {300}; k3.ids = {3};
    contents.keys = {k1, k2, k3};
    dht.testEncodeBucket(0, contents);

    int group_count = 0;
    dht.forEachGroup([&](uint64_t h, const std::vector<uint64_t>& pvs,
                         const std::vector<uint32_t>& ids) {
        group_count++;
        EXPECT_EQ(pvs.size(), 1u);
    });
    EXPECT_EQ(group_count, 3);
}

TEST(DHTForEachGroupOpt, ForEachGroupDuplicateSuffix) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 32;  // very small — forces extensions
    OptTestDHT dht(cfg);

    // Insert same key with multiple versions — will spread across extensions.
    uint64_t hash = 1;
    for (int v = 0; v < 10; ++v) {
        dht.addEntry(hash, 5000 - v * 100, v);
    }

    int group_count = 0;
    size_t total_versions = 0;
    dht.forEachGroup([&](uint64_t h, const std::vector<uint64_t>& pvs,
                         const std::vector<uint32_t>& ids) {
        if (h == hash) {
            group_count++;
            total_versions = pvs.size();
        }
    });
    EXPECT_EQ(group_count, 1) << "Duplicate suffix should merge into one group";
    EXPECT_EQ(total_versions, 10u);
}

// ============================================================
// Suite: DHTContainsOpt (improvement 4)
// ============================================================

TEST(DHTContainsOpt, ContainsEmpty) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    OptTestDHT dht(cfg);

    EXPECT_FALSE(dht.contains(12345));
    EXPECT_FALSE(dht.contains(0));
    EXPECT_FALSE(dht.contains(~0ULL));
}

TEST(DHTContainsOpt, ContainsSingleKey) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    OptTestDHT dht(cfg);

    uint64_t hash = H("the_only_key");
    dht.addEntry(hash, 100, 1);

    EXPECT_TRUE(dht.contains(hash));
    EXPECT_FALSE(dht.contains(hash + 1));
    EXPECT_FALSE(dht.contains(hash ^ (1ULL << 63)));
}

TEST(DHTContainsOpt, ContainsAfterOverflow) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.bucket_bytes = 32;
    OptTestDHT dht(cfg);

    // Fill until overflow, then check contains for a key in extension.
    std::vector<uint64_t> hashes;
    for (int i = 0; i < 15; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 3 + 1);
        dht.addEntry(hash, 1000 + i, i);
        hashes.push_back(hash);
    }

    for (auto h : hashes) {
        EXPECT_TRUE(dht.contains(h)) << "contains failed for hash " << h;
    }
}

TEST(DHTContainsOpt, ContainsManyKeys) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 8;
    cfg.bucket_bytes = 256;
    OptTestRWDHT dht(cfg);

    std::vector<uint64_t> existing;
    for (int i = 0; i < 200; ++i) {
        uint64_t hash = H("contains_key_" + std::to_string(i));
        dht.addEntry(hash, 1000 + i, i);
        existing.push_back(hash);
    }

    for (auto h : existing) {
        EXPECT_TRUE(dht.contains(h));
    }
    for (int i = 200; i < 400; ++i) {
        uint64_t hash = H("contains_key_" + std::to_string(i));
        EXPECT_FALSE(dht.contains(hash));
    }
}

TEST(DHTContainsOpt, ConcurrentContainsDuringWrites) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 8;
    cfg.bucket_bytes = 256;
    ReadWriteDeltaHashTable dht(cfg);

    std::atomic<bool> stop{false};
    std::atomic<int> written{0};

    // 4 writer threads.
    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([&, t]() {
            for (int i = 0; i < 100; ++i) {
                uint64_t hash = H("concurrent_key_" + std::to_string(t * 100 + i));
                dht.addEntry(hash, 1000 + i, i);
                written.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // 4 reader threads calling contains.
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&, t]() {
            while (!stop.load(std::memory_order_relaxed)) {
                // Just exercise contains without crashing.
                uint64_t hash = H("concurrent_key_" + std::to_string(t * 50));
                dht.contains(hash);
            }
        });
    }

    for (auto& w : writers) w.join();
    stop.store(true, std::memory_order_relaxed);
    for (auto& r : readers) r.join();

    EXPECT_EQ(written.load(), 400);
    // All written keys should be findable.
    for (int t = 0; t < 4; ++t) {
        for (int i = 0; i < 100; ++i) {
            uint64_t hash = H("concurrent_key_" + std::to_string(t * 100 + i));
            EXPECT_TRUE(dht.contains(hash));
        }
    }
}

// ============================================================
// Suite: DHTIncrementalBits (improvement 5)
// ============================================================

TEST(DHTIncrementalBits, BitsForNewEntryEmptyBucket) {
    // First key in empty bucket → raw suffix_bits + gamma(1) + 64 + 32.
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;   // suffix_bits = 60
    cfg.bucket_bytes = 256;
    OptTestDHT dht(cfg);

    // Compute bitsForNewEntry on the empty bucket 0.
    size_t bits = dht.testContentsBitsNeeded(DeltaHashTable::BucketContents{});
    // Empty bucket: just 16-bit header.
    EXPECT_EQ(bits, 16u);
}

TEST(DHTIncrementalBits, IncrementalMatchesFull) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 512;
    OptTestDHT dht(cfg);

    // Insert 20 keys and verify incremental bit computation matches full decode.
    for (int i = 0; i < 20; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 7 + 1);
        dht.addEntry(hash, 1000 + i, i);

        // Get the bucket index.
        uint32_t bi = dht.testBucketIndex(hash);
        // Decode full bucket to compute contentsBitsNeeded.
        auto contents = dht.testDecodeBucket(bi);
        // Re-encode to get exact bit count.
        size_t encode_bits = dht.testEncodeBucket(bi, contents);
        // Compare with decodeBucketUsedBits.
        size_t used_bits = dht.testDecodeBucketUsedBits(bi);
        EXPECT_EQ(encode_bits, used_bits)
            << "Mismatch at key " << i << " for bucket " << bi;
    }
}

TEST(DHTIncrementalBits, AddToChainUsesIncrementalBits) {
    // End-to-end: add entries, verify they're stored correctly.
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 128;
    OptTestDHT dht(cfg);

    // Add 15 unique keys.
    for (int i = 0; i < 15; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 11 + 1);
        dht.addEntry(hash, 3000 + i, 50 + i);
    }

    // Add multiple versions to some keys.
    for (int i = 0; i < 5; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 11 + 1);
        dht.addEntry(hash, 2000 + i, 60 + i);
    }

    // Verify all entries are retrievable.
    for (int i = 0; i < 15; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 11 + 1);
        uint64_t pv;
        uint32_t id;
        ASSERT_TRUE(dht.findFirst(hash, pv, id));
        EXPECT_EQ(pv, static_cast<uint64_t>(3000 + i));
    }

    // Verify multi-version keys have all versions.
    for (int i = 0; i < 5; ++i) {
        uint64_t hash = static_cast<uint64_t>(i * 11 + 1);
        std::vector<uint64_t> pvs;
        std::vector<uint32_t> ids;
        ASSERT_TRUE(dht.findAll(hash, pvs, ids));
        EXPECT_EQ(pvs.size(), 2u);
    }
}

// ============================================================
// Suite: DHTChaos (concurrency stress)
// ============================================================

TEST(DHTChaos, ConcurrentContainsAndAdds) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 8;
    cfg.bucket_bytes = 256;
    ReadWriteDeltaHashTable dht(cfg);

    std::atomic<bool> stop{false};

    // 4 writer threads adding keys.
    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([&, t]() {
            for (int i = 0; i < 200; ++i) {
                uint64_t hash = H("chaos_key_" + std::to_string(t * 1000 + i));
                dht.addEntry(hash, 1000 + i, i);
            }
        });
    }

    // 4 reader threads calling contains on random keys.
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&, t]() {
            int checks = 0;
            while (!stop.load(std::memory_order_relaxed) && checks < 10000) {
                uint64_t hash = H("chaos_key_" + std::to_string((t * 37 + checks) % 800));
                dht.contains(hash);
                checks++;
            }
        });
    }

    for (auto& w : writers) w.join();
    stop.store(true, std::memory_order_relaxed);
    for (auto& r : readers) r.join();

    // All 800 written keys should be findable.
    for (int t = 0; t < 4; ++t) {
        for (int i = 0; i < 200; ++i) {
            uint64_t hash = H("chaos_key_" + std::to_string(t * 1000 + i));
            EXPECT_TRUE(dht.contains(hash));
        }
    }
}

TEST(DHTChaos, ConcurrentFindFirstDuringOverflow) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 64;
    ReadWriteDeltaHashTable dht(cfg);

    std::atomic<bool> stop{false};

    // Pre-populate some data.
    for (int i = 0; i < 20; ++i) {
        uint64_t hash = H("overflow_key_" + std::to_string(i));
        dht.addEntry(hash, 1000 + i, i);
    }

    // 4 writer threads causing overflow on same buckets.
    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([&, t]() {
            for (int i = 0; i < 50; ++i) {
                uint64_t hash = H("overflow_key_" + std::to_string(t * 50 + i + 20));
                dht.addEntry(hash, 2000 + t * 50 + i, t * 50 + i);
            }
        });
    }

    // 4 reader threads doing findFirst.
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&, t]() {
            while (!stop.load(std::memory_order_relaxed)) {
                uint64_t hash = H("overflow_key_" + std::to_string(t % 20));
                uint64_t pv;
                uint32_t id;
                dht.findFirst(hash, pv, id);
            }
        });
    }

    for (auto& w : writers) w.join();
    stop.store(true, std::memory_order_relaxed);
    for (auto& r : readers) r.join();

    // Pre-populated keys should still be findable.
    for (int i = 0; i < 20; ++i) {
        uint64_t hash = H("overflow_key_" + std::to_string(i));
        EXPECT_TRUE(dht.contains(hash));
    }
}

TEST(DHTChaos, ConcurrentForEachGroupDuringWrites) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 8;
    cfg.bucket_bytes = 256;
    ReadWriteDeltaHashTable dht(cfg);

    // Pre-populate.
    for (int i = 0; i < 50; ++i) {
        uint64_t hash = H("fegroup_key_" + std::to_string(i));
        dht.addEntry(hash, 1000 + i, i);
    }

    std::atomic<bool> stop{false};

    // 4 writer threads.
    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([&, t]() {
            for (int i = 0; i < 100; ++i) {
                uint64_t hash = H("fegroup_key_" + std::to_string(t * 100 + i + 50));
                dht.addEntry(hash, 2000 + i, i);
            }
        });
    }

    // Background forEachGroup — just exercise it without crashing.
    std::thread reader([&]() {
        int iters = 0;
        while (!stop.load(std::memory_order_relaxed) && iters < 5) {
            int count = 0;
            dht.forEachGroup([&](uint64_t, const std::vector<uint64_t>&,
                                  const std::vector<uint32_t>&) {
                count++;
            });
            EXPECT_GT(count, 0);
            iters++;
        }
    });

    for (auto& w : writers) w.join();
    stop.store(true, std::memory_order_relaxed);
    reader.join();
}

// ============================================================
// Version pruning tests
// ============================================================

TEST(ReadWriteDHT, PruneStaleVersionsBasic) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 256;
    ReadWriteDeltaHashTable dht(cfg);

    uint64_t hkey = H("prunable");

    // Insert 5 versions for the same key.
    for (int v = 1; v <= 5; ++v) {
        dht.addEntry(hkey, static_cast<uint64_t>(v * 2), static_cast<uint32_t>(v));
    }
    EXPECT_EQ(dht.size(), 5u);

    // Prune with snapshot at latest version only (version 5).
    // Should keep only version 10 (latest = highest packed_version).
    std::vector<uint64_t> snapshots = {5};  // logical version 5 → packed = 10
    size_t removed = dht.pruneStaleVersions(snapshots);
    EXPECT_EQ(removed, 4u);
    EXPECT_EQ(dht.size(), 1u);

    // The remaining entry should be the latest.
    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, pv, id));
    EXPECT_EQ(pv, 10u);
    EXPECT_EQ(id, 5u);
}

TEST(ReadWriteDHT, PrunePreservesSnapshotVersions) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 256;
    ReadWriteDeltaHashTable dht(cfg);

    uint64_t hkey = H("snapped");

    // Versions: 2, 4, 6, 8, 10 (packed = logical * 2)
    for (int v = 1; v <= 5; ++v) {
        dht.addEntry(hkey, static_cast<uint64_t>(v * 2), static_cast<uint32_t>(v));
    }

    // Snapshots at logical versions 2 and 4 → packed 4 and 8.
    // Plus latest (logical 5 → packed 10).
    // Should keep: v4 (highest <= snap 2*2=4), v8 (highest <= snap 4*2=8), v10 (latest).
    std::vector<uint64_t> snapshots = {2, 4, 5};
    size_t removed = dht.pruneStaleVersions(snapshots);
    EXPECT_EQ(removed, 2u);  // v2 and v6 eliminated
    EXPECT_EQ(dht.size(), 3u);

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(hkey, pvs, ids));
    EXPECT_EQ(pvs.size(), 3u);
    // Remaining: 10, 8, 4 (descending)
    EXPECT_EQ(pvs[0], 10u);
    EXPECT_EQ(pvs[1], 8u);
    EXPECT_EQ(pvs[2], 4u);
}

TEST(ReadWriteDHT, PruneSkipsSingleVersionKeys) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 256;
    ReadWriteDeltaHashTable dht(cfg);

    // Insert 10 distinct keys, each with 1 version.
    for (int i = 0; i < 10; ++i) {
        std::string key = "single_" + std::to_string(i);
        dht.addEntry(H(key), static_cast<uint64_t>(i + 1), static_cast<uint32_t>(i));
    }
    EXPECT_EQ(dht.size(), 10u);

    std::vector<uint64_t> snapshots = {10};
    size_t removed = dht.pruneStaleVersions(snapshots);
    EXPECT_EQ(removed, 0u);
    EXPECT_EQ(dht.size(), 10u);
}

TEST(ReadWriteDHT, PruneMultipleKeysInSameBucket) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;  // 2 buckets — forces collisions
    cfg.bucket_bytes = 256;
    ReadWriteDeltaHashTable dht(cfg);

    // Insert multiple keys, each with multiple versions.
    for (int k = 0; k < 5; ++k) {
        uint64_t hkey = H("multi_" + std::to_string(k));
        for (int v = 1; v <= 3; ++v) {
            dht.addEntry(hkey, static_cast<uint64_t>(v * 2), static_cast<uint32_t>(k * 10 + v));
        }
    }
    EXPECT_EQ(dht.size(), 15u);

    // Keep only latest for each key.
    std::vector<uint64_t> snapshots = {3};  // logical 3 → packed 6
    size_t removed = dht.pruneStaleVersions(snapshots);
    EXPECT_EQ(removed, 10u);  // 2 stale per key × 5 keys
    EXPECT_EQ(dht.size(), 5u);

    // Each key should have exactly 1 version remaining.
    for (int k = 0; k < 5; ++k) {
        uint64_t hkey = H("multi_" + std::to_string(k));
        uint64_t pv;
        uint32_t id;
        ASSERT_TRUE(dht.findFirst(hkey, pv, id));
        EXPECT_EQ(pv, 6u);  // latest packed version
    }
}

TEST(ReadWriteDHT, PruneAcrossExtensionBuckets) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.bucket_bytes = 32;  // tiny buckets → extensions
    ReadWriteDeltaHashTable dht(cfg);

    uint64_t hkey = H("overflow_prune");
    for (int v = 1; v <= 30; ++v) {
        dht.addEntry(hkey, static_cast<uint64_t>(v * 2), static_cast<uint32_t>(v));
    }
    EXPECT_EQ(dht.size(), 30u);

    std::vector<uint64_t> snapshots = {30};  // logical 30 → packed 60
    size_t removed = dht.pruneStaleVersions(snapshots);
    EXPECT_EQ(removed, 29u);
    EXPECT_EQ(dht.size(), 1u);

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst(hkey, pv, id));
    EXPECT_EQ(pv, 60u);
    EXPECT_EQ(id, 30u);
}

TEST(ReadWriteDHT, BucketUtilization) {
    ReadWriteDeltaHashTable dht;  // default config

    // Empty DHT should have ~0% utilization.
    EXPECT_LT(dht.bucketUtilization(), 0.01);

    // Add some entries.
    for (int i = 0; i < 10000; ++i) {
        std::string key = "util_" + std::to_string(i);
        dht.addEntry(H(key), static_cast<uint64_t>(i + 1), static_cast<uint32_t>(i));
    }

    double util = dht.bucketUtilization();
    EXPECT_GT(util, 0.0);
    EXPECT_LE(util, 1.0);
}
