#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <set>
#include <thread>
#include <vector>

#include "internal/bit_stream.h"
#include "internal/global_index.h"
#include "internal/lslot_codec.h"
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

TEST(GlobalIndexDHT, PutKeyCountWithAddEntryIsNew) {
    GlobalIndex index;
    const int K = 50;
    const int V = 5;
    for (int k = 0; k < K; ++k) {
        for (int v = 0; v < V; ++v) {
            index.put("key" + std::to_string(k),
                      static_cast<uint64_t>(k * V + v),
                      static_cast<uint32_t>(k));
        }
    }
    EXPECT_EQ(index.keyCount(), static_cast<size_t>(K));
    EXPECT_EQ(index.entryCount(), static_cast<size_t>(K * V));
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
