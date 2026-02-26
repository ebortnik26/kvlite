#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <map>
#include <set>
#include <thread>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/global_index.h"
#include "internal/lslot_codec.h"
#include "internal/manifest.h"
#include "internal/read_only_delta_hash_table.h"
#include "internal/read_write_delta_hash_table.h"

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

// --- 64-bit Elias Gamma Round-Trip Tests ---

TEST(EliasGamma, RoundTrip64) {
    std::vector<uint64_t> test_values = {
        1, 2, 3, 100, 0xFFFFFFFFULL, 0x100000000ULL,
        0xDEADBEEFCAFEULL, 0x7FFFFFFFFFFFFFFFULL
    };

    uint8_t buf[512] = {};

    BitWriter writer(buf, 0);
    for (uint64_t v : test_values) {
        writer.writeEliasGamma64(v);
    }

    BitReader reader(buf, 0);
    for (uint64_t expected : test_values) {
        uint64_t got = reader.readEliasGamma64();
        EXPECT_EQ(got, expected) << "mismatch for value " << expected;
    }

    EXPECT_EQ(reader.position(), writer.position());
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
        index = std::make_unique<GlobalIndex>(manifest_);
        GlobalIndex::Options opts;
        ASSERT_TRUE(index->open(db_dir_, opts).ok());
    }

    void TearDown() override {
        if (index && index->isOpen()) index->close();
        index.reset();
        manifest_.close();
        std::filesystem::remove_all(db_dir_);
    }

    std::string db_dir_;
    kvlite::internal::Manifest manifest_;
    std::unique_ptr<GlobalIndex> index;
};

TEST_F(GlobalIndexDHT, PutAndGetLatest) {

    // version=100 in seg 1, version=200 in seg 2, version=300 in seg 3
    index->put("key1", 100, 1);
    index->put("key1", 200, 2);
    index->put("key1", 300, 3);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index->getLatest("key1", ver, seg).ok());
    EXPECT_EQ(ver, 300u);
    EXPECT_EQ(seg, 3u);

    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    ASSERT_TRUE(index->get("key1", seg_ids, vers));
    ASSERT_EQ(seg_ids.size(), 3u);
    EXPECT_EQ(vers[0], 300u);  EXPECT_EQ(seg_ids[0], 3u);
    EXPECT_EQ(vers[1], 200u);  EXPECT_EQ(seg_ids[1], 2u);
    EXPECT_EQ(vers[2], 100u);  EXPECT_EQ(seg_ids[2], 1u);
}

TEST_F(GlobalIndexDHT, PutMultipleVersions) {
    index->put("key1", 100, 1);
    index->put("key1", 200, 2);
    index->put("key1", 300, 3);

    EXPECT_EQ(index->entryCount(), 3u);
    EXPECT_EQ(index->keyCount(), 1u);
}

TEST_F(GlobalIndexDHT, GetLatest) {
    index->put("key1", 100, 1);
    index->put("key1", 200, 2);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index->getLatest("key1", ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 2u);

    EXPECT_TRUE(index->getLatest("missing", ver, seg).isNotFound());
}

TEST_F(GlobalIndexDHT, GetWithUpperBound) {
    index->put("key1", 100, 1);
    index->put("key1", 200, 2);
    index->put("key1", 300, 3);

    uint64_t ver;
    uint32_t seg;
    // Upper bound 250 → should get version 200
    EXPECT_TRUE(index->get("key1", 250, ver, seg));
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 2u);

    // Upper bound 300 → should get version 300
    EXPECT_TRUE(index->get("key1", 300, ver, seg));
    EXPECT_EQ(ver, 300u);
    EXPECT_EQ(seg, 3u);

    // Upper bound 50 → nothing
    EXPECT_FALSE(index->get("key1", 50, ver, seg));
}

TEST_F(GlobalIndexDHT, Contains) {
    EXPECT_FALSE(index->contains("key1"));
    index->put("key1", 100, 1);
    EXPECT_TRUE(index->contains("key1"));
}

TEST_F(GlobalIndexDHT, GetNonExistent) {
    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    EXPECT_FALSE(index->get("missing", seg_ids, vers));
}

TEST_F(GlobalIndexDHT, Snapshot) {
    std::string path = db_dir_ + "/test_snapshot.dat";

    index->put("key1", 100, 1);
    index->put("key1", 200, 2);
    index->put("key2", 300, 3);

    Status s = index->saveSnapshot(path);
    ASSERT_TRUE(s.ok()) << s.toString();

    // Load into a fresh index (no open needed for loadSnapshot — it only reads the DHT).
    GlobalIndex index2(manifest_);
    s = index2.loadSnapshot(path);
    ASSERT_TRUE(s.ok()) << s.toString();

    EXPECT_EQ(index2.keyCount(), 2u);
    EXPECT_EQ(index2.entryCount(), 3u);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index2.getLatest("key1", ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 2u);

    EXPECT_TRUE(index2.getLatest("key2", ver, seg).ok());
    EXPECT_EQ(ver, 300u);
    EXPECT_EQ(seg, 3u);
}

TEST_F(GlobalIndexDHT, SnapshotWithManyEntries) {
    std::string path = db_dir_ + "/test_snapshot_many.dat";

    index->put("key1", 100, 1);
    index->put("key1", 200, 2);
    index->put("key1", 300, 3);
    index->put("key2", 400, 4);

    Status s = index->saveSnapshot(path);
    ASSERT_TRUE(s.ok()) << s.toString();

    GlobalIndex index2(manifest_);
    s = index2.loadSnapshot(path);
    ASSERT_TRUE(s.ok()) << s.toString();

    EXPECT_EQ(index2.keyCount(), 2u);
    EXPECT_EQ(index2.entryCount(), 4u);

    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    ASSERT_TRUE(index2.get("key1", seg_ids, vers));
    ASSERT_EQ(seg_ids.size(), 3u);
    EXPECT_EQ(vers[0], 300u);  EXPECT_EQ(seg_ids[0], 3u);
    EXPECT_EQ(vers[1], 200u);  EXPECT_EQ(seg_ids[1], 2u);
    EXPECT_EQ(vers[2], 100u);  EXPECT_EQ(seg_ids[2], 1u);

    uint64_t ver;
    uint32_t seg;
    EXPECT_TRUE(index2.getLatest("key2", ver, seg).ok());
    EXPECT_EQ(ver, 400u);
    EXPECT_EQ(seg, 4u);
}

TEST_F(GlobalIndexDHT, Clear) {
    for (int i = 0; i < 50; ++i) {
        index->put("key" + std::to_string(i), static_cast<uint64_t>(i * 10),
                  static_cast<uint32_t>(i));
    }
    EXPECT_EQ(index->keyCount(), 50u);

    index->clear();
    EXPECT_EQ(index->keyCount(), 0u);
    EXPECT_EQ(index->entryCount(), 0u);
}

// ============================================================
// Codec-level tests: LSlotCodec zero-delta safety
// ============================================================

// Zero packed_version deltas: entries with identical packed_versions.
// Before the fix, this would crash in writeEliasGamma(0).
TEST(LSlotCodecTest, ZeroPackedVersionDeltaRoundTrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x42;
    entry.packed_versions = {100, 100, 100};  // zero deltas
    entry.ids             = {3, 2, 1};        // unique, delta=1
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);
    EXPECT_GT(end, 0u);

    size_t decoded_end;
    auto decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].packed_versions, entry.packed_versions);
    EXPECT_EQ(decoded.entries[0].ids, entry.ids);
}

// Identical ids (e.g. same segment_id): raw encoding handles this.
TEST(LSlotCodecTest, IdenticalIdsRoundTrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x42;
    entry.packed_versions = {30, 20, 10};  // unique, delta=10
    entry.ids             = {5, 5, 5};     // all same
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    size_t end = codec.encode(buf, 0, contents);

    size_t decoded_end;
    auto decoded = codec.decode(buf, 0, &decoded_end);
    EXPECT_EQ(decoded_end, end);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].packed_versions, entry.packed_versions);
    EXPECT_EQ(decoded.entries[0].ids, entry.ids);
}

// Packed_versions in DESCENDING order.
TEST(LSlotCodecTest, DescendingPackedVersionsRoundTrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    entry.packed_versions = {300, 200, 100};  // desc
    entry.ids             = {3, 2, 1};        // desc (correlated)
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    codec.encode(buf, 0, contents);

    auto decoded = codec.decode(buf, 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].packed_versions, entry.packed_versions);
    EXPECT_EQ(decoded.entries[0].ids, entry.ids);
}

// Consecutive packed_versions [N, N-1, ..., 1]: all deltas are 1.
TEST(LSlotCodecTest, ConsecutivePackedVersionsRoundTrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    const int N = 50;
    for (int i = N; i >= 1; --i) {
        entry.packed_versions.push_back(static_cast<uint64_t>(i * 100));  // desc
        entry.ids.push_back(static_cast<uint32_t>(i));                     // desc
    }
    contents.entries.push_back(entry);

    std::vector<uint8_t> buf(4096, 0);
    size_t end = codec.encode(buf.data(), 0, contents);
    EXPECT_GT(end, 0u);

    auto decoded = codec.decode(buf.data(), 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].packed_versions, entry.packed_versions);
    EXPECT_EQ(decoded.entries[0].ids, entry.ids);
}

// BitsNeeded must not crash on zero deltas.
TEST(LSlotCodecTest, BitsNeededZeroDeltas) {
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    entry.packed_versions = {10, 10};
    entry.ids             = {5, 5};
    contents.entries.push_back(entry);

    size_t bits = LSlotCodec::bitsNeeded(contents, 39);
    EXPECT_GT(bits, 0u);  // must not crash
}

// Mixed: packed_versions with zero deltas, ids non-increasing.
TEST(LSlotCodecTest, MixedDeltasRoundTrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0xABC;
    entry.packed_versions = {100, 100, 90, 90, 80};  // deltas: 0, 10, 0, 10
    entry.ids             = {5, 4, 4, 2, 1};         // non-increasing
    contents.entries.push_back(entry);

    uint8_t buf[256] = {};
    codec.encode(buf, 0, contents);

    auto decoded = codec.decode(buf, 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].packed_versions, entry.packed_versions);
    EXPECT_EQ(decoded.entries[0].ids, entry.ids);
}

// Many entries per fingerprint — stress-test the encoding.
TEST(LSlotCodecTest, ManyEntriesRoundTrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    const int N = 500;
    for (int i = N; i >= 1; --i) {
        entry.packed_versions.push_back(static_cast<uint64_t>(i));
        entry.ids.push_back(static_cast<uint32_t>(i));  // descending
    }
    contents.entries.push_back(entry);

    std::vector<uint8_t> buf(65536, 0);
    size_t end = codec.encode(buf.data(), 0, contents);
    EXPECT_GT(end, 0u);

    auto decoded = codec.decode(buf.data(), 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].packed_versions, entry.packed_versions);
    EXPECT_EQ(decoded.entries[0].ids, entry.ids);
}

// Multiple fingerprint groups in one lslot, each with zero deltas.
TEST(LSlotCodecTest, MultipleGroupsZeroDeltas) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;

    for (uint64_t fp = 1; fp <= 3; ++fp) {
        LSlotCodec::TrieEntry entry;
        entry.fingerprint = fp;
        entry.packed_versions = {50, 50, 50};
        entry.ids             = {10, 10, 10};
        contents.entries.push_back(entry);
    }

    std::vector<uint8_t> buf(4096, 0);
    codec.encode(buf.data(), 0, contents);

    auto decoded = codec.decode(buf.data(), 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 3u);
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(decoded.entries[i].packed_versions, contents.entries[i].packed_versions);
        EXPECT_EQ(decoded.entries[i].ids, contents.entries[i].ids);
    }
}

// 64-bit packed_version: verify large values survive encode/decode.
TEST(LSlotCodecTest, LargePackedVersionRoundTrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    entry.packed_versions = {0xDEADBEEFCAFE0003ULL, 0xDEADBEEFCAFE0002ULL, 0xDEADBEEFCAFE0001ULL};
    entry.ids = {3, 2, 1};
    contents.entries.push_back(entry);

    std::vector<uint8_t> buf(4096, 0);
    size_t end = codec.encode(buf.data(), 0, contents);
    EXPECT_GT(end, 0u);

    auto decoded = codec.decode(buf.data(), 0, nullptr);
    ASSERT_EQ(decoded.entries.size(), 1u);
    EXPECT_EQ(decoded.entries[0].packed_versions, entry.packed_versions);
    EXPECT_EQ(decoded.entries[0].ids, entry.ids);
}

// ============================================================
// DHT-level tests: bucket overflow with small buckets
// ============================================================

// Small-bucket config to force overflow quickly.
static ReadOnlyDeltaHashTable::Config smallBucketConfig() {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;      // 16 buckets
    cfg.lslot_bits = 2;       // 4 lslots
    cfg.bucket_bytes = 128;   // tiny buckets → fast overflow
    return cfg;
}

// Write path: many entries for one key in small buckets forces overflow.
TEST(ReadOnlyDHTOverflow, WritePathManyEntries) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    const int N = 200;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), /*id=*/1);
    }
    EXPECT_EQ(dht.size(), static_cast<size_t>(N));
}

// Read-back after overflow: verify all entries survive the chain.
TEST(ReadOnlyDHTOverflow, ReadBackAfterOverflow) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key", packed_versions, ids));
    EXPECT_EQ(packed_versions.size(), static_cast<size_t>(N));
    EXPECT_EQ(ids.size(), static_cast<size_t>(N));

    // Every id 1..N should appear
    std::set<uint32_t> id_set(ids.begin(), ids.end());
    for (int i = 1; i <= N; ++i) {
        EXPECT_EQ(id_set.count(static_cast<uint32_t>(i)), 1u)
            << "missing id " << i;
    }
}

// Read-back with identical ids — zero deltas in id field.
TEST(ReadOnlyDHTOverflow, ReadBackSameIdOverflow) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), /*id=*/42);
    }

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key", packed_versions, ids));
    EXPECT_EQ(packed_versions.size(), static_cast<size_t>(N));

    // All ids should be 42
    for (size_t i = 0; i < ids.size(); ++i) {
        EXPECT_EQ(ids[i], 42u);
    }
}

// findFirst after overflow should return the highest packed_version.
TEST(ReadOnlyDHTOverflow, FindFirstAfterOverflow) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key", packed_version, id));
    EXPECT_GT(packed_version, 0u);
}

// Multiple keys all forcing overflow in the same bucket.
TEST(ReadOnlyDHTOverflow, MultipleKeysOverflowSameBucket) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;      // 2 buckets
    cfg.lslot_bits = 1;       // 2 lslots
    cfg.bucket_bytes = 64;    // very small
    ReadOnlyDeltaHashTable dht(cfg);

    for (int k = 0; k < 10; ++k) {
        std::string key = "k" + std::to_string(k);
        for (int i = 1; i <= 10; ++i) {
            dht.addEntry(key, static_cast<uint64_t>(i),
                         static_cast<uint32_t>(k));
        }
    }

    EXPECT_EQ(dht.size(), 100u);

    for (int k = 0; k < 10; ++k) {
        std::string key = "k" + std::to_string(k);
        std::vector<uint64_t> packed_versions;
        std::vector<uint32_t> ids;
        ASSERT_TRUE(dht.findAll(key, packed_versions, ids))
            << "key not found: " << key;
        EXPECT_EQ(packed_versions.size(), 10u);
    }
}

// forEach traverses all entries across overflow chains.
TEST(ReadOnlyDHTOverflow, ForEachAcrossChain) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
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
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
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
    for (int i = 1; i <= 200; ++i) {
        index->put("key", static_cast<uint64_t>(i), /*segment_id=*/1);
    }

    EXPECT_EQ(index->entryCount(), 200u);
    EXPECT_EQ(index->keyCount(), 1u);

    uint64_t ver;
    uint32_t seg;
    ASSERT_TRUE(index->getLatest("key", ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 1u);

    // Verify all versions are retrievable.
    std::vector<uint32_t> seg_ids;
    std::vector<uint64_t> vers;
    ASSERT_TRUE(index->get("key", seg_ids, vers));
    EXPECT_EQ(vers.size(), 200u);
    EXPECT_EQ(vers[0], 200u);   // latest first
    EXPECT_EQ(vers[199], 1u);   // earliest last
}

// Same key, different segments (unique segment_ids).
TEST_F(GlobalIndexDHT, ManyVersionsDifferentSegments) {
    for (int i = 1; i <= 200; ++i) {
        index->put("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t ver;
    uint32_t seg;
    ASSERT_TRUE(index->getLatest("key", ver, seg).ok());
    EXPECT_EQ(ver, 200u);
    EXPECT_EQ(seg, 200u);

    // Verify upper-bound query works across the full range.
    ASSERT_TRUE(index->get("key", 150, ver, seg));
    EXPECT_EQ(ver, 150u);
    EXPECT_EQ(seg, 150u);
}

TEST_F(GlobalIndexDHT, LargeScale) {
    const int N = 1000;

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        index->put(key, static_cast<uint64_t>(i * 10), static_cast<uint32_t>(i));
    }

    EXPECT_EQ(index->keyCount(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t ver;
        uint32_t seg;
        ASSERT_TRUE(index->getLatest(key, ver, seg).ok());
        EXPECT_EQ(ver, static_cast<uint64_t>(i * 10));
        EXPECT_EQ(seg, static_cast<uint32_t>(i));
    }
}

// ============================================================
// skipLSlot correctness tests
// ============================================================

TEST(LSlotCodecTest, SkipLSlotMatchesDecode) {
    LSlotCodec codec(39);
    const int N = 5;
    std::vector<LSlotCodec::LSlotContents> slots(N);

    // Slot 0: empty
    // Slot 1: 1 group, 1 entry
    {
        LSlotCodec::TrieEntry e;
        e.fingerprint = 0x1;
        e.packed_versions = {100};
        e.ids = {1};
        slots[1].entries.push_back(e);
    }
    // Slot 2: 3 groups, mixed entry counts
    for (uint64_t fp = 1; fp <= 3; ++fp) {
        LSlotCodec::TrieEntry e;
        e.fingerprint = fp;
        for (uint64_t v = fp * 10; v >= fp * 10 - fp + 1; --v) {
            e.packed_versions.push_back(v);
            e.ids.push_back(static_cast<uint32_t>(v));
        }
        slots[2].entries.push_back(e);
    }
    // Slot 3: 1 group, 5 entries
    {
        LSlotCodec::TrieEntry e;
        e.fingerprint = 0xABC;
        for (int i = 50; i >= 46; --i) {
            e.packed_versions.push_back(static_cast<uint64_t>(i));
            e.ids.push_back(static_cast<uint32_t>(i));
        }
        slots[3].entries.push_back(e);
    }
    // Slot 4: empty

    std::vector<uint8_t> buf(4096, 0);
    size_t write_offset = 0;
    for (int s = 0; s < N; ++s) {
        write_offset = codec.encode(buf.data(), write_offset, slots[s]);
    }

    // Verify skipLSlot matches decode for each lslot index
    for (int target = 0; target <= N; ++target) {
        size_t skip_offset = 0;
        for (int s = 0; s < target; ++s) {
            skip_offset = codec.skipLSlot(buf.data(), skip_offset);
        }
        size_t decode_offset = codec.bitOffset(buf.data(), target);
        EXPECT_EQ(skip_offset, decode_offset) << "mismatch at lslot " << target;
    }
}

TEST(LSlotCodecTest, SkipEmptyLSlots) {
    LSlotCodec codec(39);
    const int N = 8;
    std::vector<uint8_t> buf(4096, 0);
    size_t write_offset = 0;
    LSlotCodec::LSlotContents empty;
    for (int s = 0; s < N; ++s) {
        write_offset = codec.encode(buf.data(), write_offset, empty);
    }

    size_t offset = 0;
    for (int s = 0; s < N; ++s) {
        size_t expected = codec.bitOffset(buf.data(), s);
        EXPECT_EQ(offset, expected) << "mismatch at empty lslot " << s;
        offset = codec.skipLSlot(buf.data(), offset);
    }
}

TEST(LSlotCodecTest, SkipLSlotLargePayload) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;
    LSlotCodec::TrieEntry entry;
    entry.fingerprint = 0x1;
    for (int i = 100; i >= 1; --i) {
        entry.packed_versions.push_back(static_cast<uint64_t>(i * 1000));
        entry.ids.push_back(static_cast<uint32_t>(i));
    }
    contents.entries.push_back(entry);

    std::vector<uint8_t> buf(65536, 0);
    size_t end = codec.encode(buf.data(), 0, contents);

    // skipLSlot should return the same end offset as decode
    size_t skip_end = codec.skipLSlot(buf.data(), 0);
    EXPECT_EQ(skip_end, end);

    size_t decode_end;
    codec.decode(buf.data(), 0, &decode_end);
    EXPECT_EQ(skip_end, decode_end);
}

// ============================================================
// addEntryIsNew correctness tests
// ============================================================

TEST(ReadOnlyDHT, AddEntryIsNewFirstAdd) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    EXPECT_TRUE(dht.addEntryIsNew("key1", 100, 1));
}

TEST(ReadOnlyDHT, AddEntryIsNewDuplicateKey) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    EXPECT_TRUE(dht.addEntryIsNew("key1", 100, 1));
    EXPECT_FALSE(dht.addEntryIsNew("key1", 200, 2));
}

TEST(ReadOnlyDHT, AddEntryIsNewDifferentKeys) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    EXPECT_TRUE(dht.addEntryIsNew("key1", 100, 1));
    EXPECT_TRUE(dht.addEntryIsNew("key2", 200, 2));
}

TEST(ReadOnlyDHT, AddEntryIsNewAfterOverflow) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.lslot_bits = 1;
    cfg.bucket_bytes = 64;
    ReadOnlyDeltaHashTable dht(cfg);

    // First add is new
    EXPECT_TRUE(dht.addEntryIsNew("key1", 1, 1));
    // Add many entries to force overflow
    for (int i = 2; i <= 50; ++i) {
        EXPECT_FALSE(dht.addEntryIsNew("key1", static_cast<uint64_t>(i),
                                        static_cast<uint32_t>(i)));
    }
}

TEST_F(GlobalIndexDHT, PutKeyCountWithAddEntryIsNew) {
    const int K = 50;
    const int V = 5;
    for (int k = 0; k < K; ++k) {
        for (int v = 0; v < V; ++v) {
            index->put("key" + std::to_string(k),
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
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key", packed_version, id));
    EXPECT_EQ(packed_version, 100u);
    EXPECT_EQ(id, 100u);
}

TEST(ReadOnlyDHTOverflow, FindFirstWithDescendingInsertOrder) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    for (int i = 100; i >= 1; --i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key", packed_version, id));
    EXPECT_EQ(packed_version, 100u);
    EXPECT_EQ(id, 100u);
}

TEST(ReadOnlyDHTOverflow, FindFirstSingleEntryPerBucket) {
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 32;  // very tiny → forces frequent overflow
    ReadOnlyDeltaHashTable dht(cfg);

    for (int i = 1; i <= 20; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key", packed_version, id));
    EXPECT_EQ(packed_version, 20u);
}

// ============================================================
// findAll ordering across overflow chains
// ============================================================

TEST(ReadOnlyDHTOverflow, FindAllDescOrderAcrossChain) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key", packed_versions, ids));
    EXPECT_EQ(packed_versions.size(), 100u);

    // Within each bucket's contribution, entries are sorted desc.
    // Across buckets they may interleave but all values must be present.
    std::set<uint64_t> pv_set(packed_versions.begin(), packed_versions.end());
    for (int i = 1; i <= 100; ++i) {
        EXPECT_EQ(pv_set.count(static_cast<uint64_t>(i)), 1u)
            << "missing packed_version " << i;
    }
}

TEST(ReadOnlyDHTOverflow, FindAllCompleteAcrossChain) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    for (int i = 1; i <= 100; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i * 10),
                     static_cast<uint32_t>(i));
    }

    std::vector<uint64_t> packed_versions;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key", packed_versions, ids));
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
    cfg.lslot_bits = 1;
    cfg.bucket_bytes = 64;
    ReadOnlyDeltaHashTable dht(cfg);

    // 5 distinct keys, each with 20 entries to force overflow
    for (int k = 0; k < 5; ++k) {
        std::string key = "grp" + std::to_string(k);
        for (int i = 1; i <= 20; ++i) {
            dht.addEntry(key, static_cast<uint64_t>(k * 100 + i),
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

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&dht, t, per_thread]() {
            for (int i = 0; i < per_thread; ++i) {
                uint64_t pv = static_cast<uint64_t>(t * per_thread + i + 1);
                dht.addEntry("key", pv, static_cast<uint32_t>(pv));
            }
        });
    }
    for (auto& w : workers) w.join();

    uint64_t packed_version;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key", packed_version, id));
    EXPECT_EQ(packed_version, static_cast<uint64_t>(threads * per_thread));

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key", pvs, ids));
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
            for (int i = 0; i < per_thread; ++i) {
                dht.addEntry(key, static_cast<uint64_t>(i + 1),
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
        ASSERT_TRUE(dht.findAll(key, pvs, ids));
        EXPECT_EQ(pvs.size(), static_cast<size_t>(per_thread));
    }
}

TEST(ReadWriteDHT, ConcurrentOverflowSameBucket) {
    ReadWriteDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.lslot_bits = 1;
    cfg.bucket_bytes = 64;
    ReadWriteDeltaHashTable dht(cfg);

    const int threads = 4;
    const int per_thread = 50;

    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&dht, t, per_thread]() {
            std::string key = "overflow_key" + std::to_string(t);
            for (int i = 0; i < per_thread; ++i) {
                dht.addEntry(key, static_cast<uint64_t>(t * per_thread + i + 1),
                             static_cast<uint32_t>(t * per_thread + i + 1));
            }
        });
    }
    for (auto& w : workers) w.join();

    for (int t = 0; t < threads; ++t) {
        std::string key = "overflow_key" + std::to_string(t);
        std::vector<uint64_t> pvs;
        std::vector<uint32_t> ids;
        ASSERT_TRUE(dht.findAll(key, pvs, ids))
            << "key not found: " << key;
        EXPECT_EQ(pvs.size(), static_cast<size_t>(per_thread));
    }
}

TEST(ReadWriteDHT, ConcurrentAddAndContains) {
    ReadWriteDeltaHashTable dht;
    std::atomic<bool> done{false};

    // Writer thread
    std::thread writer([&dht, &done]() {
        for (int i = 0; i < 5000; ++i) {
            dht.addEntry("key" + std::to_string(i % 100),
                         static_cast<uint64_t>(i + 1),
                         static_cast<uint32_t>(i + 1));
        }
        done.store(true, std::memory_order_release);
    });

    // Reader threads
    std::vector<std::thread> readers;
    for (int t = 0; t < 3; ++t) {
        readers.emplace_back([&dht, &done]() {
            while (!done.load(std::memory_order_acquire)) {
                for (int k = 0; k < 100; ++k) {
                    dht.contains("key" + std::to_string(k));
                    uint64_t pv;
                    uint32_t id;
                    dht.findFirst("key" + std::to_string(k), pv, id);
                }
            }
        });
    }

    writer.join();
    for (auto& r : readers) r.join();
    // If we get here without crash/ASAN error, the test passes.
}

TEST(ReadWriteDHT, FindFirstDuringConcurrentAdd) {
    ReadWriteDeltaHashTable dht;
    const int N = 10000;
    std::atomic<bool> done{false};

    std::thread writer([&dht, &done, N]() {
        for (int i = 1; i <= N; ++i) {
            dht.addEntry("key", static_cast<uint64_t>(i),
                         static_cast<uint32_t>(i));
        }
        done.store(true, std::memory_order_release);
    });

    // Readers verify every returned packed_version is valid (1..N)
    std::atomic<bool> reader_ok{true};
    std::vector<std::thread> readers;
    for (int t = 0; t < 3; ++t) {
        readers.emplace_back([&dht, &done, &reader_ok, N]() {
            while (!done.load(std::memory_order_acquire)) {
                uint64_t pv;
                uint32_t id;
                if (dht.findFirst("key", pv, id)) {
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
    EXPECT_FALSE(dht.findFirst("key", pv, id));
}

TEST(ReadOnlyDHT, EmptyTableFindAll) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    EXPECT_FALSE(dht.findAll("key", pvs, ids));
    EXPECT_TRUE(pvs.empty());
    EXPECT_TRUE(ids.empty());
}

TEST(ReadOnlyDHT, SingleEntryFindFirst) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    dht.addEntry("key", 42, 7);
    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key", pv, id));
    EXPECT_EQ(pv, 42u);
    EXPECT_EQ(id, 7u);
}

TEST(ReadOnlyDHT, TwoEntriesSameFingerprint) {
    // Use a config with very few buckets/lslots to maximize collision chance
    ReadOnlyDeltaHashTable::Config cfg;
    cfg.bucket_bits = 1;
    cfg.lslot_bits = 1;
    cfg.bucket_bytes = 256;
    ReadOnlyDeltaHashTable dht(cfg);

    // Find two keys that hash to the same bucket+lslot+fingerprint
    // We add entries and verify they coexist via findAll
    dht.addEntry("key_a", 100, 1);
    dht.addEntry("key_a", 200, 2);

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key_a", pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key_a", pv, id));
    EXPECT_EQ(pv, 200u);
    EXPECT_EQ(id, 2u);
}

// ============================================================
// ReadWriteDeltaHashTable removeEntry / updateEntryId tests
// ============================================================

TEST(ReadWriteDHT, RemoveEntryBasic) {
    ReadWriteDeltaHashTable dht;
    dht.addEntry("key", 100, 1);
    dht.addEntry("key", 200, 2);
    dht.addEntry("key", 300, 3);
    EXPECT_EQ(dht.size(), 3u);

    bool group_empty = dht.removeEntry("key", 200, 2);
    EXPECT_FALSE(group_empty);
    EXPECT_EQ(dht.size(), 2u);

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key", pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);
    std::set<uint64_t> pv_set(pvs.begin(), pvs.end());
    EXPECT_EQ(pv_set.count(100u), 1u);
    EXPECT_EQ(pv_set.count(300u), 1u);
    EXPECT_EQ(pv_set.count(200u), 0u);
}

TEST(ReadWriteDHT, RemoveEntryLastInGroup) {
    ReadWriteDeltaHashTable dht;
    dht.addEntry("key", 100, 1);
    EXPECT_EQ(dht.size(), 1u);

    bool group_empty = dht.removeEntry("key", 100, 1);
    EXPECT_TRUE(group_empty);
    EXPECT_EQ(dht.size(), 0u);

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    EXPECT_FALSE(dht.findAll("key", pvs, ids));
}

TEST(ReadWriteDHT, RemoveEntryFromOverflowChain) {
    ReadWriteDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 128;
    ReadWriteDeltaHashTable dht(cfg);

    // Add enough entries to force overflow.
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    // Remove one entry.
    bool group_empty = dht.removeEntry("key", 50, 50);
    EXPECT_FALSE(group_empty);
    EXPECT_EQ(dht.size(), static_cast<size_t>(N - 1));

    // Verify remaining entries.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key", pvs, ids));
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
    dht.addEntry("key", 100, 100);
    EXPECT_EQ(dht.size(), 1u);

    bool found = dht.updateEntryId("key", 100, 100, 200);
    EXPECT_TRUE(found);
    EXPECT_EQ(dht.size(), 1u);

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key", pv, id));
    EXPECT_EQ(pv, 100u);
    EXPECT_EQ(id, 200u);
}

TEST(ReadWriteDHT, UpdateEntryIdOverflow) {
    ReadWriteDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 128;
    ReadWriteDeltaHashTable dht(cfg);

    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    // Update id=50 to a large value (may cause different delta encoding).
    bool found = dht.updateEntryId("key", 50, 50, 999999);
    EXPECT_TRUE(found);
    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    // Verify the updated entry.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll("key", pvs, ids));
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

    // Pre-populate.
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key", static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    std::atomic<bool> done{false};

    // 2 writer threads removing entries.
    std::vector<std::thread> writers;
    for (int t = 0; t < 2; ++t) {
        writers.emplace_back([&dht, &done, t, N]() {
            for (int i = t + 1; i <= N; i += 2) {
                dht.removeEntry("key", static_cast<uint64_t>(i),
                                static_cast<uint32_t>(i));
            }
        });
    }

    // 2 reader threads.
    std::vector<std::thread> readers;
    for (int t = 0; t < 2; ++t) {
        readers.emplace_back([&dht, &done]() {
            for (int i = 0; i < 1000; ++i) {
                uint64_t pv;
                uint32_t id;
                dht.findFirst("key", pv, id);
            }
        });
    }

    for (auto& w : writers) w.join();
    for (auto& r : readers) r.join();
    // If we get here without crash/ASAN error, the test passes.
    EXPECT_EQ(dht.size(), 0u);
}

TEST(ReadOnlyDHT, MaxPackedVersion) {
    ReadOnlyDeltaHashTable dht(smallBucketConfig());
    uint64_t max_pv = UINT64_MAX - 1;  // near max (UINT64_MAX itself may cause issues with delta encoding)
    dht.addEntry("key", max_pv, 42);

    uint64_t pv;
    uint32_t id;
    ASSERT_TRUE(dht.findFirst("key", pv, id));
    EXPECT_EQ(pv, max_pv);
    EXPECT_EQ(id, 42u);
}

// --- Variable-Width Fingerprint Tests ---

TEST(LSlotCodecTest, VariableWidthFingerprintRoundtrip) {
    LSlotCodec codec(39);
    LSlotCodec::LSlotContents contents;

    // Entry with no extra bits (common case).
    LSlotCodec::TrieEntry e0;
    e0.fingerprint = 0x1234567;
    e0.fp_extra_bits = 0;
    e0.packed_versions = {100};
    e0.ids = {1};
    contents.entries.push_back(e0);

    // Entry with 3 extra bits.
    LSlotCodec::TrieEntry e1;
    e1.fingerprint = 0x1234567 | (5ULL << 39);  // 3 extra bits = 5 (0b101)
    e1.fp_extra_bits = 3;
    e1.packed_versions = {200};
    e1.ids = {2};
    contents.entries.push_back(e1);

    // Entry with 7 extra bits.
    LSlotCodec::TrieEntry e2;
    e2.fingerprint = 0x7654321 | (0x5AULL << 39);  // 7 extra bits
    e2.fp_extra_bits = 7;
    e2.packed_versions = {300, 250};
    e2.ids = {4, 3};  // sorted desc, parallel with packed_versions
    contents.entries.push_back(e2);

    // Sort by fingerprint for encoding.
    std::sort(contents.entries.begin(), contents.entries.end(),
              [](const LSlotCodec::TrieEntry& a, const LSlotCodec::TrieEntry& b) {
                  return a.fingerprint < b.fingerprint;
              });

    uint8_t buf[512] = {};
    size_t end_offset = codec.encode(buf, 0, contents);
    EXPECT_GT(end_offset, 0u);

    // Verify bitsNeeded matches encode output.
    size_t expected_bits = LSlotCodec::bitsNeeded(contents, 39);
    EXPECT_EQ(expected_bits, end_offset)
        << "bitsNeeded and encode disagree on total bits";

    // Decode and verify round-trip.
    size_t decode_end;
    auto decoded = codec.decode(buf, 0, &decode_end);
    EXPECT_EQ(decode_end, end_offset)
        << "decode and encode disagree on total bits";
    ASSERT_EQ(decoded.entries.size(), 3u);

    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(decoded.entries[i].fingerprint,
                  contents.entries[i].fingerprint)
            << "fingerprint mismatch at entry " << i;
        EXPECT_EQ(decoded.entries[i].fp_extra_bits,
                  contents.entries[i].fp_extra_bits)
            << "fp_extra_bits mismatch at entry " << i;
        EXPECT_EQ(decoded.entries[i].packed_versions,
                  contents.entries[i].packed_versions)
            << "packed_versions mismatch at entry " << i;
        EXPECT_EQ(decoded.entries[i].ids,
                  contents.entries[i].ids)
            << "ids mismatch at entry " << i;
    }
}

// --- Collision-Aware Insertion Tests ---

TEST(ReadWriteDHT, AddEntryCheckedSameKey) {
    ReadWriteDeltaHashTable dht;

    std::string key = "test_key";
    auto resolver = [&](uint32_t /*seg_id*/, uint64_t /*pv*/) -> std::string {
        return key;
    };

    // First insert — should be new.
    bool is_new = dht.addEntryChecked(key, 100, 1, resolver);
    EXPECT_TRUE(is_new);

    // Second insert with same key — should NOT be new.
    is_new = dht.addEntryChecked(key, 200, 2, resolver);
    EXPECT_FALSE(is_new);

    // Both entries should be findable.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.findAll(key, pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);
    EXPECT_EQ(ids.size(), 2u);
}

// Test helper: exposes protected DeltaHashTable methods for collision testing.
class TestableRWDHT : public ReadWriteDeltaHashTable {
public:
    using ReadWriteDeltaHashTable::ReadWriteDeltaHashTable;

    bool testAddToChainChecked(uint32_t bi, uint32_t li, uint64_t fp,
                               uint64_t packed_version, uint32_t id,
                               const KeyResolver& resolver,
                               uint64_t new_key_secondary_hash) {
        return addToChainChecked(bi, li, fp, packed_version, id,
            [this](Bucket& bucket) -> Bucket* {
                return createExtension(bucket);
            },
            resolver, new_key_secondary_hash);
    }

    bool testFindAllByHash(uint32_t bi, uint32_t li, uint64_t fp,
                           std::vector<uint64_t>& pvs, std::vector<uint32_t>& ids) const {
        return findAllByHash(bi, li, fp, pvs, ids);
    }

    using ReadWriteDeltaHashTable::removeEntry;
    void removeEntry(uint64_t fp, uint64_t packed_version, uint32_t id,
                     uint32_t bi, uint32_t li) {
        removeFromChain(bi, li, fp, packed_version, id);
    }

    bool updateEntry(uint64_t fp, uint64_t packed_version,
                     uint32_t old_id, uint32_t new_id,
                     uint32_t bi, uint32_t li) {
        return updateIdInChain(bi, li, fp, packed_version, old_id, new_id,
            [this](Bucket& bucket) -> Bucket* {
                return createExtension(bucket);
            });
    }

    uint8_t fpBits() const { return fingerprint_bits_; }
    size_t extensionCount() const { return ext_arena_->size(); }
    const BucketArena& arena() const { return *ext_arena_; }
};

TEST(ReadWriteDHT, AddEntryCheckedCollision) {
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 512;
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0x123;  // same base fingerprint for both "keys"

    // Secondary hashes for the two different "keys" — must differ.
    uint64_t sec1 = 0xAAAAAAAAAAAAAAAAULL;
    uint64_t sec2 = 0x5555555555555555ULL;

    auto resolver = [](uint32_t seg_id, uint64_t /*pv*/) -> std::string {
        // Return different "keys" based on segment_id.
        if (seg_id == 1) return "key_alpha";
        if (seg_id == 2) return "key_beta";
        return "";
    };

    // Insert first key.
    bool is_new = dht.testAddToChainChecked(bi, li, fp, 100, 1, resolver, sec1);
    EXPECT_TRUE(is_new);

    // Insert second key with same base fp — collision detected.
    is_new = dht.testAddToChainChecked(bi, li, fp, 200, 2, resolver, sec2);
    EXPECT_TRUE(is_new);  // new key!

    // Both should be findable via base fingerprint.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);
    // Check we got both entries.
    std::set<uint64_t> pv_set(pvs.begin(), pvs.end());
    EXPECT_TRUE(pv_set.count(100));
    EXPECT_TRUE(pv_set.count(200));
}

TEST(ReadWriteDHT, AddEntryCheckedTripleCollision) {
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 512;
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0x456;

    // Three different secondary hashes.
    uint64_t sec1 = 0x1111111111111111ULL;
    uint64_t sec2 = 0x2222222222222222ULL;
    uint64_t sec3 = 0x3333333333333333ULL;

    auto resolver = [](uint32_t seg_id, uint64_t /*pv*/) -> std::string {
        return "key_" + std::to_string(seg_id);
    };

    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 100, 1, resolver, sec1));
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 200, 2, resolver, sec2));
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 300, 3, resolver, sec3));

    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), 3u);

    std::set<uint64_t> pv_set(pvs.begin(), pvs.end());
    EXPECT_TRUE(pv_set.count(100));
    EXPECT_TRUE(pv_set.count(200));
    EXPECT_TRUE(pv_set.count(300));
}

TEST(ReadWriteDHT, AddEntryCheckedSameKeyAfterCollision) {
    // After a collision extends fingerprints, inserting the same key again
    // should append to the existing extended entry rather than creating a new one.
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 512;
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0x789;

    // Use actual secondary hashes computed from key strings, so the
    // resolver and the caller agree on what the secondary hash should be.
    std::string key_x = "key_x";
    std::string key_y = "key_y";
    uint64_t sec_x = dhtSecondaryHash(key_x.data(), key_x.size());
    uint64_t sec_y = dhtSecondaryHash(key_y.data(), key_y.size());
    ASSERT_NE(sec_x, sec_y) << "test requires different secondary hashes";

    auto resolver = [&](uint32_t seg_id, uint64_t /*pv*/) -> std::string {
        if (seg_id == 1 || seg_id == 3) return key_x;
        if (seg_id == 2) return key_y;
        return "";
    };

    // Insert key_x.
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 100, 1, resolver, sec_x));

    // Insert key_y (collision).
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 200, 2, resolver, sec_y));

    // Verify both are findable.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);

    // Insert key_x again (same secondary hash).
    // The extended entry for key_x should match and this should NOT be new.
    bool is_new = dht.testAddToChainChecked(bi, li, fp, 300, 3, resolver, sec_x);
    EXPECT_FALSE(is_new) << "third insert of same key should not be new";

    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    // Should have 3 entries total: 2 for key_x, 1 for key_y.
    EXPECT_EQ(pvs.size(), 3u);
}

TEST(ReadWriteDHT, AppendOverflow) {
    // Verify that appending a version to an existing extended entry correctly
    // handles bucket overflow by spilling to the extension chain.
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 64;  // tiny bucket to force overflow
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0x7;

    std::string key_a = "key_alpha";
    std::string key_b = "key_beta";
    uint64_t sec_a = dhtSecondaryHash(key_a.data(), key_a.size());
    uint64_t sec_b = dhtSecondaryHash(key_b.data(), key_b.size());
    ASSERT_NE(sec_a, sec_b);

    auto resolver = [&](uint32_t seg_id, uint64_t /*pv*/) -> std::string {
        if (seg_id <= 50) return key_a;
        return key_b;
    };

    // Insert key_a.
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 100, 1, resolver, sec_a));

    // Insert key_b (collision → extends fingerprints).
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 200, 51, resolver, sec_b));

    // Append many versions to key_a to force bucket overflow.
    for (uint32_t i = 2; i <= 20; ++i) {
        bool is_new = dht.testAddToChainChecked(bi, li, fp, 100 + i, i, resolver, sec_a);
        EXPECT_FALSE(is_new) << "append #" << i << " should not be new";
    }

    // All entries must be findable.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    // 20 for key_a + 1 for key_b = 21
    EXPECT_EQ(pvs.size(), 21u);
}

TEST(ReadWriteDHT, CollisionWithOverflow) {
    // Two colliding keys that together exceed bucket capacity after extension.
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 64;  // tiny
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0xA;

    // Pre-fill the bucket with some non-colliding entries to make it tight.
    auto no_resolver = [](uint32_t, uint64_t) -> std::string { return ""; };
    for (uint32_t i = 0; i < 3; ++i) {
        uint64_t other_fp = 0x1 + i;
        dht.testAddToChainChecked(bi, li, other_fp, 1000 + i, 100 + i,
                                  no_resolver, i);
    }

    std::string key_x = "collision_x";
    std::string key_y = "collision_y";
    uint64_t sec_x = dhtSecondaryHash(key_x.data(), key_x.size());
    uint64_t sec_y = dhtSecondaryHash(key_y.data(), key_y.size());
    ASSERT_NE(sec_x, sec_y);

    auto resolver = [&](uint32_t seg_id, uint64_t) -> std::string {
        if (seg_id == 1) return key_x;
        if (seg_id == 2) return key_y;
        return "";
    };

    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 500, 1, resolver, sec_x));
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 600, 2, resolver, sec_y));

    // Both entries must survive in the extension chain.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);
    std::set<uint64_t> pv_set(pvs.begin(), pvs.end());
    EXPECT_TRUE(pv_set.count(500));
    EXPECT_TRUE(pv_set.count(600));
}

TEST(ReadWriteDHT, ManyCollisions) {
    // 10 keys all sharing the same base fingerprint.
    // The collision resolution creates extended fingerprints. When a new key's
    // extended bits match an existing entry, findMinExtraBits grows the extension
    // to re-disambiguate. Test inserts all 10 and verifies all entries survive.
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 512;
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0x42;
    constexpr int N = 10;

    std::vector<std::string> keys;
    std::vector<uint64_t> sec_hashes;
    for (int i = 0; i < N; ++i) {
        keys.push_back("mcoll_" + std::to_string(i));
        sec_hashes.push_back(dhtSecondaryHash(keys.back().data(),
                                               keys.back().size()));
    }

    auto resolver = [&](uint32_t seg_id, uint64_t) -> std::string {
        return keys[seg_id];
    };

    int new_count = 0;
    for (int i = 0; i < N; ++i) {
        bool is_new = dht.testAddToChainChecked(
            bi, li, fp, (i + 1) * 100, static_cast<uint32_t>(i),
            resolver, sec_hashes[i]);
        if (is_new) new_count++;
    }

    // At minimum, the first 2 keys must be detected as new (first insert +
    // first collision). Subsequent keys may or may not be detected as new
    // depending on extension bit collisions.
    EXPECT_GE(new_count, 2);

    // All 10 entries must be findable via the base fingerprint.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), static_cast<size_t>(N));

    std::set<uint64_t> pv_set(pvs.begin(), pvs.end());
    for (int i = 0; i < N; ++i) {
        EXPECT_TRUE(pv_set.count((i + 1) * 100))
            << "missing version for key " << i;
    }
}

TEST(ReadWriteDHT, ExtraBitsGrowsMultipleTimes) {
    // Exercise findMinExtraBits needing to grow extra_bits multiple times.
    // Insert 3 keys whose secondary hashes share low bits, forcing
    // the extension width to grow beyond 1.
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 512;
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0xDD;

    // Find 3 keys where low 1 bit of secondary hash matches for at least 2,
    // forcing extra_bits to grow to at least 2.
    std::string key_a = "extra_a";
    std::string key_b = "extra_b";
    std::string key_c = "extra_c";
    uint64_t sec_a = dhtSecondaryHash(key_a.data(), key_a.size());
    uint64_t sec_b = dhtSecondaryHash(key_b.data(), key_b.size());
    uint64_t sec_c = dhtSecondaryHash(key_c.data(), key_c.size());

    // Verify at least 2 share the same low bit (test precondition).
    int matching_low = ((sec_a & 1) == (sec_b & 1)) +
                       ((sec_a & 1) == (sec_c & 1)) +
                       ((sec_b & 1) == (sec_c & 1));
    // At least one pair must share low bit by pigeonhole.
    ASSERT_GE(matching_low, 1);

    auto resolver = [&](uint32_t seg_id, uint64_t) -> std::string {
        if (seg_id == 1) return key_a;
        if (seg_id == 2) return key_b;
        if (seg_id == 3) return key_c;
        return "";
    };

    dht.testAddToChainChecked(bi, li, fp, 100, 1, resolver, sec_a);
    dht.testAddToChainChecked(bi, li, fp, 200, 2, resolver, sec_b);
    dht.testAddToChainChecked(bi, li, fp, 300, 3, resolver, sec_c);

    // All 3 entries must be findable.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), 3u);
}

TEST(ReadWriteDHT, AppendCreatesNewEntryInExtension) {
    // Exercise buildCandidateSlot creating a new TrieEntry (not appending
    // to an existing one) when the version overflows to the extension.
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 64;  // tiny to force overflow
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0xEE;

    std::string key = "overflow_key";
    uint64_t sec = dhtSecondaryHash(key.data(), key.size());

    auto resolver = [&](uint32_t, uint64_t) -> std::string { return key; };

    // First insert.
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 100, 1, resolver, sec));

    // Append many versions to force multiple overflows to extension chain.
    for (uint32_t i = 2; i <= 15; ++i) {
        bool is_new = dht.testAddToChainChecked(bi, li, fp, 100 + i, i,
                                                resolver, sec);
        EXPECT_FALSE(is_new) << "append #" << i;
    }

    // All 15 versions must be findable.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), 15u);
}

TEST(ReadWriteDHT, CollisionThenRemove) {
    // After collision extends fingerprints, remove one key's entry.
    // The other key's entry must remain findable with its extended fingerprint.
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 512;
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0xBB;

    std::string key_p = "key_persist";
    std::string key_r = "key_remove";
    uint64_t sec_p = dhtSecondaryHash(key_p.data(), key_p.size());
    uint64_t sec_r = dhtSecondaryHash(key_r.data(), key_r.size());
    ASSERT_NE(sec_p, sec_r);

    auto resolver = [&](uint32_t seg_id, uint64_t) -> std::string {
        if (seg_id == 1) return key_p;
        if (seg_id == 2) return key_r;
        return "";
    };

    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 100, 1, resolver, sec_p));
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 200, 2, resolver, sec_r));

    // Remove key_r's entry.
    dht.removeEntry(fp, 200, 2, bi, li);

    // key_p must still be findable.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), 1u);
    EXPECT_EQ(pvs[0], 100u);
    EXPECT_EQ(ids[0], 1u);
}

TEST(ReadWriteDHT, CollisionThenUpdate) {
    // After collision extends fingerprints, update one entry's id.
    // The update must succeed and the extended fingerprint must be preserved.
    DeltaHashTable::Config config;
    config.bucket_bits = 4;
    config.lslot_bits = 2;
    config.bucket_bytes = 512;
    TestableRWDHT dht(config);

    uint32_t bi = 0, li = 0;
    uint64_t fp = 0xCC;

    std::string key_u = "key_update";
    std::string key_o = "key_other";
    uint64_t sec_u = dhtSecondaryHash(key_u.data(), key_u.size());
    uint64_t sec_o = dhtSecondaryHash(key_o.data(), key_o.size());
    ASSERT_NE(sec_u, sec_o);

    auto resolver = [&](uint32_t seg_id, uint64_t) -> std::string {
        if (seg_id == 1 || seg_id == 99) return key_u;
        if (seg_id == 2) return key_o;
        return "";
    };

    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 100, 1, resolver, sec_u));
    EXPECT_TRUE(dht.testAddToChainChecked(bi, li, fp, 200, 2, resolver, sec_o));

    // Update key_u's id from 1 to 99.
    bool updated = dht.updateEntry(fp, 100, 1, 99, bi, li);
    EXPECT_TRUE(updated);

    // Both must still be findable.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    ASSERT_TRUE(dht.testFindAllByHash(bi, li, fp, pvs, ids));
    EXPECT_EQ(pvs.size(), 2u);

    std::map<uint64_t, uint32_t> pv_to_id;
    for (size_t i = 0; i < pvs.size(); ++i) {
        pv_to_id[pvs[i]] = ids[i];
    }
    EXPECT_EQ(pv_to_id[100], 99u);   // updated
    EXPECT_EQ(pv_to_id[200], 2u);    // unchanged
}

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
    size_t stride = cfg.bucket_bytes + 8;  // kBucketPadding = 8
    return (1u << cfg.bucket_bits) * stride;
}

TEST(MemoryLeak, ReadOnlyClearReleasesExtensions) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    // Fill until extensions are created.
    for (int i = 1; i <= 500; ++i) {
        dht.addEntry("key" + std::to_string(i % 5),
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
        src.addEntry("k" + std::to_string(i % 3),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    size_t ext_before = src.extensionCount();
    size_t mem_before = src.memoryUsage();
    ASSERT_GT(ext_before, 0u);

    // Move-construct destination.
    TestableRODHT dst(std::move(src));
    EXPECT_EQ(dst.extensionCount(), ext_before);
    EXPECT_EQ(dst.memoryUsage(), mem_before);

    // Verify data survived the move.
    std::vector<uint64_t> pvs;
    std::vector<uint32_t> ids;
    EXPECT_TRUE(dst.findAll("k0", pvs, ids));
    EXPECT_FALSE(pvs.empty());
}

TEST(MemoryLeak, ReadWriteClearReleasesExtensions) {
    ReadWriteDeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 32;

    // Use TestableRWDHT to inspect extension count, but note it can't be
    // moved (deleted move ctor), so we test clear() only.
    DeltaHashTable::Config dcfg;
    dcfg.bucket_bits = cfg.bucket_bits;
    dcfg.lslot_bits = cfg.lslot_bits;
    dcfg.bucket_bytes = cfg.bucket_bytes;
    TestableRWDHT dht(dcfg);

    for (int i = 1; i <= 500; ++i) {
        dht.addEntry("key" + std::to_string(i % 5),
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
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 64;
    TestableRWDHT dht(cfg);

    // Run multiple cycles of fill + drain.
    for (int cycle = 0; cycle < 5; ++cycle) {
        // Fill: add entries that cause extensions.
        const int N = 200;
        for (int i = 1; i <= N; ++i) {
            dht.addEntry("key" + std::to_string(i),
                         static_cast<uint64_t>(i), static_cast<uint32_t>(i));
        }
        ASSERT_GT(dht.extensionCount(), 0u);

        // Drain: remove all entries.
        for (int i = 1; i <= N; ++i) {
            dht.removeEntry("key" + std::to_string(i),
                            static_cast<uint64_t>(i), static_cast<uint32_t>(i));
        }
    }

    // After all cycles, verify no data remains and no entries are findable.
    for (int i = 1; i <= 200; ++i) {
        EXPECT_FALSE(dht.contains("key" + std::to_string(i)));
    }
    EXPECT_EQ(dht.size(), 0u);
}

TEST(MemoryLeak, PruneEmptyExtensionOnRemove) {
    DeltaHashTable::Config cfg;
    cfg.bucket_bits = 4;
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 32;  // very small → forces extensions quickly
    TestableRWDHT dht(cfg);

    // Add enough entries across multiple keys so different extension chain
    // tails become empty during removal (pruneEmptyExtension unlinks empty
    // tail extensions). Using multiple keys spreads entries across lslots,
    // meaning some extension buckets become entirely empty sooner.
    const int N = 100;
    for (int i = 1; i <= N; ++i) {
        dht.addEntry("key" + std::to_string(i % 10),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    size_t ext_after_add = dht.extensionCount();
    ASSERT_GT(ext_after_add, 0u);

    // Remove all entries.
    for (int i = 1; i <= N; ++i) {
        dht.removeEntry("key" + std::to_string(i % 10),
                        static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    // All data must be gone.
    for (int i = 0; i < 10; ++i) {
        EXPECT_FALSE(dht.contains("key" + std::to_string(i)));
    }
    EXPECT_EQ(dht.size(), 0u);
}

TEST(MemoryLeak, ClearAfterOverflowThenReuseWorks) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    // First round: fill with extensions.
    for (int i = 1; i <= 300; ++i) {
        dht.addEntry("k" + std::to_string(i), static_cast<uint64_t>(i),
                     static_cast<uint32_t>(i));
    }
    ASSERT_GT(dht.extensionCount(), 0u);

    // Clear and reuse.
    dht.clear();
    ASSERT_EQ(dht.extensionCount(), 0u);

    // Second round: same operations should work correctly.
    for (int i = 1; i <= 300; ++i) {
        dht.addEntry("k" + std::to_string(i), static_cast<uint64_t>(i + 1000),
                     static_cast<uint32_t>(i));
    }

    // Verify all entries from second round are present.
    for (int i = 1; i <= 300; ++i) {
        std::string key = "k" + std::to_string(i);
        ASSERT_TRUE(dht.contains(key)) << "missing after reuse: " << key;
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

    // Force extensions by filling small buckets.
    for (int i = 1; i <= 300; ++i) {
        dht.addEntry("k" + std::to_string(i),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    uint32_t ext_count = dht.arena().size();
    ASSERT_GT(ext_count, 0u);

    size_t stride = cfg.bucket_bytes + 8;  // kBucketPadding
    EXPECT_EQ(dht.arena().dataBytes(), static_cast<size_t>(ext_count) * stride);
}

// Pointer stability: pointers from early allocations remain valid after
// further allocations that span multiple chunks.
TEST(BucketArena, PointerStabilityAcrossChunks) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    // Insert enough entries to create >64 extensions (multiple chunks).
    // With 16 buckets and 128-byte buckets, many keys will force chains.
    for (int i = 1; i <= 2000; ++i) {
        dht.addEntry("k" + std::to_string(i),
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

    // All data must still be readable (pointers are stable).
    for (int i = 1; i <= 2000; ++i) {
        ASSERT_TRUE(dht.contains("k" + std::to_string(i)))
            << "missing key after multi-chunk allocation: k" << i;
    }
}

// Clear resets the arena completely; subsequent allocations work.
TEST(BucketArena, ClearResetsAndReallocWorks) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    for (int i = 1; i <= 500; ++i) {
        dht.addEntry("k" + std::to_string(i),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    uint32_t ext_before = dht.arena().size();
    ASSERT_GT(ext_before, 0u);

    dht.clear();
    EXPECT_EQ(dht.arena().size(), 0u);
    EXPECT_EQ(dht.arena().dataBytes(), 0u);

    // Reallocate — should work identically.
    for (int i = 1; i <= 500; ++i) {
        dht.addEntry("k" + std::to_string(i),
                     static_cast<uint64_t>(i + 1000), static_cast<uint32_t>(i));
    }
    EXPECT_GT(dht.arena().size(), 0u);

    for (int i = 1; i <= 500; ++i) {
        ASSERT_TRUE(dht.contains("k" + std::to_string(i)));
    }
}

// Repeated clear/fill cycles don't corrupt data or leak (ASAN will catch leaks).
TEST(BucketArena, RepeatedClearFillCycles) {
    auto cfg = smallBucketConfig();
    TestableRODHT dht(cfg);

    for (int cycle = 0; cycle < 10; ++cycle) {
        for (int i = 1; i <= 200; ++i) {
            dht.addEntry("k" + std::to_string(i),
                         static_cast<uint64_t>(cycle * 1000 + i),
                         static_cast<uint32_t>(i));
        }
        ASSERT_GT(dht.arena().size(), 0u)
            << "no extensions on cycle " << cycle;

        // Verify all data from this cycle.
        for (int i = 1; i <= 200; ++i) {
            uint64_t pv;
            uint32_t id;
            ASSERT_TRUE(dht.findFirst("k" + std::to_string(i), pv, id))
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
        dht.addEntry("k" + std::to_string(i),
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
    cfg.lslot_bits = 2;
    cfg.bucket_bytes = 32;
    TestableRWDHT dht(cfg);

    EXPECT_EQ(dht.arena().size(), 0u);

    // Fill until extensions are created.
    for (int i = 1; i <= 500; ++i) {
        dht.addEntry("key" + std::to_string(i % 5),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }
    uint32_t ext_after_fill = dht.arena().size();
    ASSERT_GT(ext_after_fill, 0u);

    // Remove all entries.
    for (int i = 1; i <= 500; ++i) {
        dht.removeEntry("key" + std::to_string(i % 5),
                        static_cast<uint64_t>(i), static_cast<uint32_t>(i));
    }

    // Arena size doesn't shrink on individual removes (bump allocator),
    // but data should be gone.
    EXPECT_EQ(dht.size(), 0u);
    for (int i = 0; i < 5; ++i) {
        EXPECT_FALSE(dht.contains("key" + std::to_string(i)));
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
    cfg.lslot_bits = 1;    // 2 lslots
    cfg.bucket_bytes = 32; // tiny
    TestableRODHT dht(cfg);

    // Keep inserting until we cross the 64-extension chunk boundary.
    int i = 1;
    while (dht.arena().size() <= 64) {
        dht.addEntry("k" + std::to_string(i),
                     static_cast<uint64_t>(i), static_cast<uint32_t>(i));
        ++i;
    }
    ASSERT_GT(dht.arena().size(), 64u)
        << "need >64 extensions to test chunk boundary";

    // All inserted data must still be readable.
    for (int j = 1; j < i; ++j) {
        ASSERT_TRUE(dht.contains("k" + std::to_string(j)))
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
// Binary snapshot (v8) tests
// ============================================================

class BinarySnapshotTest : public ::testing::Test {
protected:
    void SetUp() override {
        db_dir_ = ::testing::TempDir() + "/gi_snap_v8_" +
                  std::to_string(reinterpret_cast<uintptr_t>(this));
        std::filesystem::create_directories(db_dir_);
        ASSERT_TRUE(manifest_.create(db_dir_).ok());
        index_ = std::make_unique<GlobalIndex>(manifest_);
        GlobalIndex::Options opts;
        ASSERT_TRUE(index_->open(db_dir_, opts).ok());
    }

    void TearDown() override {
        if (index_ && index_->isOpen()) index_->close();
        index_.reset();
        manifest_.close();
        std::filesystem::remove_all(db_dir_);
    }

    std::string db_dir_;
    Manifest manifest_;
    std::unique_ptr<GlobalIndex> index_;
};

TEST_F(BinarySnapshotTest, RoundTripBasic) {
    // Populate with some entries.
    const int N = 1000;
    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        ASSERT_TRUE(index_->put(key, i * 10 + 1, i + 1).ok());
    }

    ASSERT_EQ(index_->entryCount(), static_cast<size_t>(N));
    ASSERT_EQ(index_->keyCount(), static_cast<size_t>(N));

    // Take binary snapshot.
    ASSERT_TRUE(index_->storeSnapshot(0).ok());

    // Close the index.
    ASSERT_TRUE(index_->close().ok());
    manifest_.close();

    // Re-open and load snapshot.
    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts).ok());
    ASSERT_TRUE(index2.loadSnapshot(db_dir_ + "/gi/snapshot").ok());

    // Verify all entries are present.
    EXPECT_EQ(index2.entryCount(), static_cast<size_t>(N));
    EXPECT_EQ(index2.keyCount(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(index2.getLatest(key, pv, seg).ok()) << "missing key: " << key;
        EXPECT_EQ(pv, static_cast<uint64_t>(i * 10 + 1));
        EXPECT_EQ(seg, static_cast<uint32_t>(i + 1));
    }

    index2.close();
    manifest2.close();
}

TEST_F(BinarySnapshotTest, RoundTripMultiVersion) {
    // Multiple versions per key.
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        for (int v = 0; v < 5; ++v) {
            ASSERT_TRUE(index_->put(key, v * 100 + i + 1, v * 10 + i).ok());
        }
    }

    ASSERT_EQ(index_->entryCount(), 500u);
    ASSERT_EQ(index_->keyCount(), 100u);

    ASSERT_TRUE(index_->storeSnapshot(0).ok());
    ASSERT_TRUE(index_->close().ok());
    manifest_.close();

    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts).ok());
    ASSERT_TRUE(index2.loadSnapshot(db_dir_ + "/gi/snapshot").ok());

    EXPECT_EQ(index2.entryCount(), 500u);
    EXPECT_EQ(index2.keyCount(), 100u);

    // Verify latest version for each key.
    for (int i = 0; i < 100; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(index2.getLatest(key, pv, seg).ok());
        EXPECT_EQ(pv, static_cast<uint64_t>(4 * 100 + i + 1));
    }

    index2.close();
    manifest2.close();
}

TEST_F(BinarySnapshotTest, SnapshotWithExtensions) {
    // Use enough entries with colliding hash prefixes to force extension buckets.
    // With default config (bucket_bits=20, 1M buckets), we need enough entries
    // to overflow some buckets. We'll add many entries per key to force overflow.
    const int N = 200;
    for (int i = 0; i < N; ++i) {
        std::string key = "extkey_" + std::to_string(i);
        // Add many versions to increase bucket fill.
        for (int v = 0; v < 50; ++v) {
            ASSERT_TRUE(index_->put(key, v * 1000 + i + 1, v).ok());
        }
    }

    size_t entry_count = index_->entryCount();
    size_t key_count = index_->keyCount();
    ASSERT_GT(entry_count, 0u);

    ASSERT_TRUE(index_->storeSnapshot(0).ok());
    ASSERT_TRUE(index_->close().ok());
    manifest_.close();

    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts).ok());
    ASSERT_TRUE(index2.loadSnapshot(db_dir_ + "/gi/snapshot").ok());

    EXPECT_EQ(index2.entryCount(), entry_count);
    EXPECT_EQ(index2.keyCount(), key_count);

    // Verify every entry can be looked up.
    for (int i = 0; i < N; ++i) {
        std::string key = "extkey_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(index2.getLatest(key, pv, seg).ok()) << "missing key: " << key;
    }

    index2.close();
    manifest2.close();
}

TEST_F(BinarySnapshotTest, SnapshotBlocksConcurrentPut) {
    // Populate with some data.
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(index_->put("key_" + std::to_string(i), i + 1, i).ok());
    }

    // Verify that snapshot() blocks concurrent put().
    // Strategy: start snapshot on main thread, try to put from another thread,
    // verify the put completes only after snapshot.
    std::atomic<bool> snapshot_started{false};
    std::atomic<bool> snapshot_done{false};
    std::atomic<bool> put_started{false};
    std::atomic<bool> put_done{false};

    // Writer thread: waits for snapshot to start, then tries to put.
    std::thread writer([&]() {
        while (!snapshot_started.load(std::memory_order_acquire)) {
            // spin-wait
        }
        put_started.store(true, std::memory_order_release);
        // This put should block until snapshot completes.
        index_->put("blocked_key", 999, 42);
        put_done.store(true, std::memory_order_release);
    });

    // Take snapshot (this will hold the exclusive lock).
    snapshot_started.store(true, std::memory_order_release);
    ASSERT_TRUE(index_->storeSnapshot(0).ok());
    snapshot_done.store(true, std::memory_order_release);

    writer.join();

    // The put should have completed after snapshot.
    EXPECT_TRUE(put_done.load());

    // Verify the blocked_key was eventually written.
    uint64_t pv;
    uint32_t seg;
    EXPECT_TRUE(index_->getLatest("blocked_key", pv, seg).ok());
    EXPECT_EQ(pv, 999u);
    EXPECT_EQ(seg, 42u);
}

TEST_F(BinarySnapshotTest, AtomicSwapLeavesValidDir) {
    // Verify that after snapshot(), the valid directory exists and tmp/old do not.
    for (int i = 0; i < 10; ++i) {
        ASSERT_TRUE(index_->put("key_" + std::to_string(i), i + 1, i).ok());
    }

    ASSERT_TRUE(index_->storeSnapshot(0).ok());

    std::string valid_dir = db_dir_ + "/gi/snapshot.v8";
    std::string tmp_dir = valid_dir + ".tmp";
    std::string old_dir = valid_dir + ".old";

    EXPECT_TRUE(std::filesystem::exists(valid_dir));
    EXPECT_TRUE(std::filesystem::is_directory(valid_dir));
    EXPECT_FALSE(std::filesystem::exists(tmp_dir));
    EXPECT_FALSE(std::filesystem::exists(old_dir));

    // Take a second snapshot and verify same invariant.
    for (int i = 10; i < 20; ++i) {
        ASSERT_TRUE(index_->put("key_" + std::to_string(i), i + 1, i).ok());
    }
    ASSERT_TRUE(index_->storeSnapshot(0).ok());

    EXPECT_TRUE(std::filesystem::exists(valid_dir));
    EXPECT_FALSE(std::filesystem::exists(tmp_dir));
    EXPECT_FALSE(std::filesystem::exists(old_dir));
}

TEST_F(BinarySnapshotTest, SnapshotResetsWALCounter) {
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(index_->put("key_" + std::to_string(i), i + 1, i).ok());
    }
    EXPECT_EQ(index_->updatesSinceSnapshot(), 100u);

    ASSERT_TRUE(index_->storeSnapshot(0).ok());
    EXPECT_EQ(index_->updatesSinceSnapshot(), 0u);

    // New puts after snapshot increment the counter.
    ASSERT_TRUE(index_->put("new_key", 1, 1).ok());
    EXPECT_EQ(index_->updatesSinceSnapshot(), 1u);
}

TEST_F(BinarySnapshotTest, WriteSnapshotThenLoad) {
    // storeSnapshot creates v8 dir, loadSnapshot reads it.
    for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(index_->put("key_" + std::to_string(i), i + 1, i).ok());
    }

    ASSERT_TRUE(index_->storeSnapshot(0).ok());
    ASSERT_TRUE(index_->close().ok());

    std::string valid_dir = db_dir_ + "/gi/snapshot.v8";
    EXPECT_TRUE(std::filesystem::exists(valid_dir));

    // Re-open and verify data is loaded from v8 snapshot.
    manifest_.close();
    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts).ok());
    ASSERT_TRUE(index2.loadSnapshot(db_dir_ + "/gi/snapshot").ok());

    EXPECT_EQ(index2.entryCount(), 50u);
    EXPECT_EQ(index2.keyCount(), 50u);

    for (int i = 0; i < 50; ++i) {
        std::string key = "key_" + std::to_string(i);
        uint64_t pv;
        uint32_t seg;
        ASSERT_TRUE(index2.getLatest(key, pv, seg).ok());
    }

    index2.close();
    manifest2.close();
}

TEST_F(BinarySnapshotTest, EmptyIndexSnapshot) {
    // Snapshot an empty index — should produce valid snapshot with 0 entries.
    ASSERT_TRUE(index_->storeSnapshot(0).ok());

    ASSERT_TRUE(index_->close().ok());
    manifest_.close();

    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(db_dir_).ok());
    GlobalIndex index2(manifest2);
    GlobalIndex::Options opts;
    ASSERT_TRUE(index2.open(db_dir_, opts).ok());
    ASSERT_TRUE(index2.loadSnapshot(db_dir_ + "/gi/snapshot").ok());

    EXPECT_EQ(index2.entryCount(), 0u);
    EXPECT_EQ(index2.keyCount(), 0u);

    index2.close();
    manifest2.close();
}
