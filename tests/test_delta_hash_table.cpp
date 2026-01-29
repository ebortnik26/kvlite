#include <gtest/gtest.h>

#include <map>
#include <set>

#include "internal/delta_hash_table.h"
#include "internal/l1_index.h"

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

    auto [payload, inserted] = dht.insert("hello", 42);
    EXPECT_TRUE(inserted);
    EXPECT_EQ(payload, 42u);

    auto [found_payload, found] = dht.find("hello");
    EXPECT_TRUE(found);
    EXPECT_EQ(found_payload, 42u);

    EXPECT_EQ(dht.size(), 1u);
}

TEST(DeltaHashTable, FindNonExistent) {
    DeltaHashTable dht(testConfig());
    auto [payload, found] = dht.find("missing");
    EXPECT_FALSE(found);
    EXPECT_EQ(payload, 0u);
}

TEST(DeltaHashTable, InsertDuplicate) {
    DeltaHashTable dht(testConfig());

    auto [p1, ins1] = dht.insert("key1", 10);
    EXPECT_TRUE(ins1);

    auto [p2, ins2] = dht.insert("key1", 20);
    EXPECT_FALSE(ins2);
    EXPECT_EQ(p2, 10u);  // returns existing payload
    EXPECT_EQ(dht.size(), 1u);
}

TEST(DeltaHashTable, Remove) {
    DeltaHashTable dht(testConfig());

    dht.insert("key1", 10);
    dht.insert("key2", 20);
    EXPECT_EQ(dht.size(), 2u);

    EXPECT_TRUE(dht.remove("key1"));
    EXPECT_EQ(dht.size(), 1u);

    auto [p1, f1] = dht.find("key1");
    EXPECT_FALSE(f1);

    auto [p2, f2] = dht.find("key2");
    EXPECT_TRUE(f2);
    EXPECT_EQ(p2, 20u);
}

TEST(DeltaHashTable, RemoveNonExistent) {
    DeltaHashTable dht(testConfig());
    EXPECT_FALSE(dht.remove("missing"));
}

TEST(DeltaHashTable, Clear) {
    DeltaHashTable dht(testConfig());

    for (int i = 0; i < 10; ++i) {
        dht.insert("key" + std::to_string(i), static_cast<uint64_t>(i + 1));
    }
    EXPECT_EQ(dht.size(), 10u);

    dht.clear();
    EXPECT_EQ(dht.size(), 0u);

    for (int i = 0; i < 10; ++i) {
        auto [p, f] = dht.find("key" + std::to_string(i));
        EXPECT_FALSE(f);
    }
}

TEST(DeltaHashTable, ForEach) {
    DeltaHashTable dht(testConfig());

    dht.insert("a", 10);
    dht.insert("b", 20);
    dht.insert("c", 30);

    std::map<uint64_t, uint64_t> collected;
    dht.forEach([&](uint64_t hash, uint64_t payload) {
        collected[hash] = payload;
    });

    EXPECT_EQ(collected.size(), 3u);

    std::set<uint64_t> all_payloads;
    for (const auto& [hash, payload] : collected) {
        all_payloads.insert(payload);
    }
    EXPECT_EQ(all_payloads.count(10u), 1u);
    EXPECT_EQ(all_payloads.count(20u), 1u);
    EXPECT_EQ(all_payloads.count(30u), 1u);
}

TEST(DeltaHashTable, ManyKeys) {
    DeltaHashTable dht(testConfig());

    const int N = 200;
    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto [payload, inserted] = dht.insert(key, static_cast<uint64_t>(i * 10 + 1));
        EXPECT_TRUE(inserted);
    }

    EXPECT_EQ(dht.size(), static_cast<size_t>(N));

    for (int i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto [payload, found] = dht.find(key);
        ASSERT_TRUE(found) << "key not found: " << key;
        EXPECT_EQ(payload, static_cast<uint64_t>(i * 10 + 1));
    }
}

TEST(DeltaHashTable, InsertAfterRemove) {
    DeltaHashTable dht(testConfig());

    dht.insert("key1", 10);
    EXPECT_TRUE(dht.remove("key1"));

    auto [p, f] = dht.find("key1");
    EXPECT_FALSE(f);

    // Re-insert
    auto [payload, inserted] = dht.insert("key1", 20);
    EXPECT_TRUE(inserted);
    EXPECT_EQ(payload, 20u);
    EXPECT_EQ(dht.size(), 1u);
}

TEST(DeltaHashTable, MemoryUsage) {
    DeltaHashTable dht(testConfig());
    size_t empty_usage = dht.memoryUsage();
    EXPECT_GT(empty_usage, 0u);

    for (int i = 0; i < 50; ++i) {
        dht.insert("key_" + std::to_string(i), static_cast<uint64_t>(i + 1));
    }

    // No KeyRecords allocated (payloads are inline in buckets), so memory
    // may not increase much, but it should not decrease.
    EXPECT_GE(dht.memoryUsage(), empty_usage);
}

TEST(DeltaHashTable, EmptyKeyAndBinaryKey) {
    DeltaHashTable dht(testConfig());

    auto [p1, i1] = dht.insert("", 1);
    EXPECT_TRUE(i1);

    std::string binary_key("\x00\x01\x02\x03", 4);
    auto [p2, i2] = dht.insert(binary_key, 2);
    EXPECT_TRUE(i2);

    EXPECT_EQ(dht.size(), 2u);

    auto [fp1, ff1] = dht.find("");
    EXPECT_TRUE(ff1);
    EXPECT_EQ(fp1, 1u);

    auto [fp2, ff2] = dht.find(binary_key);
    EXPECT_TRUE(ff2);
    EXPECT_EQ(fp2, 2u);
}

// --- DHT UpdatePayload Tests ---

TEST(DeltaHashTable, UpdatePayload) {
    DeltaHashTable dht(testConfig());

    dht.insert("key1", 100);

    EXPECT_TRUE(dht.updatePayload("key1", 200));

    auto [payload, found] = dht.find("key1");
    EXPECT_TRUE(found);
    EXPECT_EQ(payload, 200u);
}

TEST(DeltaHashTable, UpdatePayloadNonExistent) {
    DeltaHashTable dht(testConfig());

    EXPECT_FALSE(dht.updatePayload("missing", 100));
}

TEST(DeltaHashTable, UpdatePayloadMultipleKeys) {
    DeltaHashTable dht(testConfig());

    dht.insert("key1", 10);
    dht.insert("key2", 20);
    dht.insert("key3", 30);

    EXPECT_TRUE(dht.updatePayload("key2", 999));

    auto [p1, f1] = dht.find("key1");
    EXPECT_TRUE(f1);
    EXPECT_EQ(p1, 10u);

    auto [p2, f2] = dht.find("key2");
    EXPECT_TRUE(f2);
    EXPECT_EQ(p2, 999u);

    auto [p3, f3] = dht.find("key3");
    EXPECT_TRUE(f3);
    EXPECT_EQ(p3, 30u);
}

// --- DHT IdVecPool Tests ---

TEST(DeltaHashTable, IdVecPoolAllocate) {
    DeltaHashTable dht(testConfig());
    auto& pool = dht.idVecPool();

    uint32_t h = pool.allocate();
    EXPECT_TRUE(pool.get(h).empty());

    pool.get(h).push_back(42);
    EXPECT_EQ(pool.get(h)[0], 42u);
}

TEST(DeltaHashTable, IdVecPoolAllocateMultiple) {
    DeltaHashTable dht(testConfig());
    auto& pool = dht.idVecPool();

    uint32_t h1 = pool.allocate();
    uint32_t h2 = pool.allocate();
    EXPECT_NE(h1, h2);

    pool.get(h1).push_back(1);
    pool.get(h2).push_back(2);
    EXPECT_EQ(pool.get(h1)[0], 1u);
    EXPECT_EQ(pool.get(h2)[0], 2u);
}

TEST(DeltaHashTable, IdVecPoolRelease) {
    DeltaHashTable dht(testConfig());
    auto& pool = dht.idVecPool();

    uint32_t h1 = pool.allocate();
    pool.get(h1) = {10, 20, 30};
    EXPECT_EQ(pool.activeCount(), 1u);

    pool.release(h1);
    EXPECT_EQ(pool.activeCount(), 0u);

    // Recycled handle
    uint32_t h2 = pool.allocate();
    EXPECT_EQ(h2, h1);
    EXPECT_TRUE(pool.get(h2).empty());
}

// --- Payload Encoding Tests ---

TEST(PayloadEncoding, Inline1) {
    using namespace Payload;

    uint64_t p = makeInline1(42);
    EXPECT_TRUE(isInline(p));
    EXPECT_FALSE(isOverflow(p));
    EXPECT_EQ(inlineCount(p), 1u);
    EXPECT_EQ(inlineFileId(p, 0), 42u);
}

TEST(PayloadEncoding, Inline2) {
    using namespace Payload;

    uint64_t p = makeInline2(100, 200);
    EXPECT_TRUE(isInline(p));
    EXPECT_EQ(inlineCount(p), 2u);
    EXPECT_EQ(inlineFileId(p, 0), 100u);
    EXPECT_EQ(inlineFileId(p, 1), 200u);
}

TEST(PayloadEncoding, Inline1MaxFileId) {
    using namespace Payload;

    uint64_t p = makeInline1(kMaxInlineFileId);
    EXPECT_TRUE(isInline(p));
    EXPECT_EQ(inlineFileId(p, 0), kMaxInlineFileId);
}

TEST(PayloadEncoding, Inline2MaxFileIds) {
    using namespace Payload;

    uint64_t p = makeInline2(kMaxInlineFileId, kMaxInlineFileId);
    EXPECT_TRUE(isInline(p));
    EXPECT_EQ(inlineCount(p), 2u);
    EXPECT_EQ(inlineFileId(p, 0), kMaxInlineFileId);
    EXPECT_EQ(inlineFileId(p, 1), kMaxInlineFileId);
}

TEST(PayloadEncoding, Inline1Zero) {
    using namespace Payload;

    uint64_t p = makeInline1(0);
    EXPECT_TRUE(isInline(p));
    EXPECT_EQ(inlineCount(p), 1u);
    EXPECT_EQ(inlineFileId(p, 0), 0u);
}

TEST(PayloadEncoding, Inline2Asymmetric) {
    using namespace Payload;

    uint64_t p = makeInline2(0, kMaxInlineFileId);
    EXPECT_TRUE(isInline(p));
    EXPECT_EQ(inlineFileId(p, 0), 0u);
    EXPECT_EQ(inlineFileId(p, 1), kMaxInlineFileId);
}

TEST(PayloadEncoding, OverflowRoundTrip) {
    using namespace Payload;

    // Pool handle round-trip: handle → payload → handle
    uint32_t handle = 42;
    uint64_t p = fromHandle(handle);
    EXPECT_FALSE(isInline(p));
    EXPECT_TRUE(isOverflow(p));
    EXPECT_NE(p, 0u);  // must be non-zero (DHT uses 0 as "not found")

    uint32_t recovered = toHandle(p);
    EXPECT_EQ(recovered, handle);
}

TEST(PayloadEncoding, OverflowHandleZero) {
    using namespace Payload;

    // Handle 0 must produce a non-zero, non-inline payload
    uint64_t p = fromHandle(0);
    EXPECT_FALSE(isInline(p));
    EXPECT_NE(p, 0u);
    EXPECT_EQ(toHandle(p), 0u);
}

TEST(PayloadEncoding, InlineBitsDontOverlap) {
    using namespace Payload;

    // Verify that fid0 and fid1 fields don't interfere with each other
    for (uint32_t fid0 : {0u, 1u, 1000u, kMaxInlineFileId}) {
        for (uint32_t fid1 : {0u, 1u, 1000u, kMaxInlineFileId}) {
            uint64_t p = makeInline2(fid0, fid1);
            EXPECT_EQ(inlineFileId(p, 0), fid0)
                << "fid0=" << fid0 << " fid1=" << fid1;
            EXPECT_EQ(inlineFileId(p, 1), fid1)
                << "fid0=" << fid0 << " fid1=" << fid1;
        }
    }
}

// --- DHT + Payload Integration Tests ---

TEST(DeltaHashTable, InsertAndFindInlinePayload) {
    using namespace Payload;
    DeltaHashTable dht(testConfig());

    uint64_t p1 = makeInline1(42);
    auto [payload, inserted] = dht.insert("key1", p1);
    EXPECT_TRUE(inserted);
    EXPECT_EQ(payload, p1);

    auto [found_payload, found] = dht.find("key1");
    EXPECT_TRUE(found);
    EXPECT_TRUE(isInline(found_payload));
    EXPECT_EQ(inlineFileId(found_payload, 0), 42u);
}

TEST(DeltaHashTable, UpdateInlineToInline2) {
    using namespace Payload;
    DeltaHashTable dht(testConfig());

    dht.insert("key1", makeInline1(100));
    EXPECT_TRUE(dht.updatePayload("key1", makeInline2(200, 100)));

    auto [payload, found] = dht.find("key1");
    EXPECT_TRUE(found);
    EXPECT_TRUE(isInline(payload));
    EXPECT_EQ(inlineCount(payload), 2u);
    EXPECT_EQ(inlineFileId(payload, 0), 200u);
    EXPECT_EQ(inlineFileId(payload, 1), 100u);
}

TEST(DeltaHashTable, UpdateInlineToOverflow) {
    using namespace Payload;
    DeltaHashTable dht(testConfig());
    auto& pool = dht.idVecPool();

    dht.insert("key1", makeInline2(200, 100));

    uint32_t h = pool.allocate();
    pool.get(h) = {300, 200, 100};
    EXPECT_TRUE(dht.updatePayload("key1", fromHandle(h)));

    auto [payload, found] = dht.find("key1");
    EXPECT_TRUE(found);
    EXPECT_TRUE(isOverflow(payload));

    auto& recovered = pool.get(toHandle(payload));
    EXPECT_EQ(recovered.size(), 3u);
    EXPECT_EQ(recovered[0], 300u);
}

TEST(DeltaHashTable, PayloadSurvivesMultipleUpdates) {
    using namespace Payload;
    DeltaHashTable dht(testConfig());

    dht.insert("key1", makeInline1(1));
    dht.updatePayload("key1", makeInline1(2));
    dht.updatePayload("key1", makeInline2(3, 2));
    dht.updatePayload("key1", makeInline1(4));

    auto [payload, found] = dht.find("key1");
    EXPECT_TRUE(found);
    EXPECT_EQ(inlineCount(payload), 1u);
    EXPECT_EQ(inlineFileId(payload, 0), 4u);
}

TEST(DeltaHashTable, InsertByHashWithPayload) {
    using namespace Payload;
    DeltaHashTable dht(testConfig());

    uint64_t hash = 0xDEADBEEF12345678ULL;
    uint64_t p = makeInline2(10, 20);
    auto [payload, inserted] = dht.insertByHash(hash, p);
    EXPECT_TRUE(inserted);
    EXPECT_EQ(payload, p);

    // Can't find by key (hash is pre-computed), but forEach should see it
    size_t count = 0;
    dht.forEach([&](uint64_t h, uint64_t pl) {
        ++count;
        EXPECT_EQ(pl, p);
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

TEST(L1IndexDHT, RemoveFileFromInline2) {
    L1Index index;
    index.put("key1", 100);
    index.put("key1", 200);

    // Remove second (fid1)
    index.removeFile("key1", 100);

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 1u);
    EXPECT_EQ(fids[0], 200u);
    EXPECT_EQ(index.entryCount(), 1u);

    // Remove first (fid0)
    index.removeFile("key1", 200);
    EXPECT_FALSE(index.contains("key1"));
    EXPECT_EQ(index.entryCount(), 0u);
}

TEST(L1IndexDHT, RemoveFileDemotesOverflowToInline) {
    L1Index index;

    // Build up to overflow (3 file_ids)
    index.put("key1", 100);
    index.put("key1", 200);
    index.put("key1", 300);  // overflow
    EXPECT_EQ(index.entryCount(), 3u);

    // Remove one → should demote to inline2
    index.removeFile("key1", 200);
    EXPECT_EQ(index.entryCount(), 2u);

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 2u);
    EXPECT_EQ(fids[0], 300u);
    EXPECT_EQ(fids[1], 100u);

    // Remove another → should demote to inline1
    index.removeFile("key1", 100);
    EXPECT_EQ(index.entryCount(), 1u);

    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 1u);
    EXPECT_EQ(fids[0], 300u);

    // Remove last → key should be gone
    index.removeFile("key1", 300);
    EXPECT_EQ(index.entryCount(), 0u);
    EXPECT_FALSE(index.contains("key1"));
}

TEST(L1IndexDHT, RemoveFilePoolRecycling) {
    L1Index index;

    // Promote to overflow, then demote back — pool slot should be recycled
    index.put("key1", 100);
    index.put("key1", 200);
    index.put("key1", 300);  // overflow allocated

    index.removeFile("key1", 300);  // demote to inline2, pool released
    index.removeFile("key1", 200);  // demote to inline1

    // A second key going to overflow should reuse the released pool slot
    index.put("key2", 400);
    index.put("key2", 500);
    index.put("key2", 600);  // overflow — should reuse recycled handle

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key2", fids));
    ASSERT_EQ(fids.size(), 3u);
    EXPECT_EQ(fids[0], 600u);
}

TEST(L1IndexDHT, InlineToOverflowPromotion) {
    L1Index index;

    // 1 file_id: inline1
    index.put("key1", 100);
    EXPECT_EQ(index.entryCount(), 1u);

    // 2 file_ids: inline2
    index.put("key1", 200);
    EXPECT_EQ(index.entryCount(), 2u);

    // 3 file_ids: overflow
    index.put("key1", 300);
    EXPECT_EQ(index.entryCount(), 3u);

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 3u);
    EXPECT_EQ(fids[0], 300u);
    EXPECT_EQ(fids[1], 200u);
    EXPECT_EQ(fids[2], 100u);

    // 4+ file_ids: stays overflow
    index.put("key1", 400);
    EXPECT_EQ(index.entryCount(), 4u);

    ASSERT_TRUE(index.getFileIds("key1", fids));
    ASSERT_EQ(fids.size(), 4u);
    EXPECT_EQ(fids[0], 400u);
}

TEST(L1IndexDHT, PutExistingInInline2ReordersWithoutDuplication) {
    L1Index index;

    index.put("key1", 100);
    index.put("key1", 200);
    EXPECT_EQ(index.entryCount(), 2u);

    // Put 100 again — it exists in the second slot, should reorder
    index.put("key1", 100);

    std::vector<uint32_t> fids;
    ASSERT_TRUE(index.getFileIds("key1", fids));
    // Should still be 2 entries, reordered to [100, 200]
    ASSERT_EQ(fids.size(), 2u);
    EXPECT_EQ(fids[0], 100u);
    EXPECT_EQ(fids[1], 200u);
    // entry count should remain 2 (no new entry added)
    EXPECT_EQ(index.entryCount(), 2u);
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
    // inline1
    index.put("a", 10);
    // inline2
    index.put("b", 20);
    index.put("b", 30);
    // overflow
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

TEST(L1IndexDHT, SnapshotWithOverflow) {
    std::string path = "/tmp/test_l1_snapshot_overflow.dat";

    {
        L1Index index;
        index.put("key1", 100);
        index.put("key1", 200);
        index.put("key1", 300);  // triggers overflow
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
            using namespace Payload;
            for (int i = 0; i < KEYS_PER_THREAD; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                auto [payload, inserted] = dht.insert(key, makeInline1(static_cast<uint32_t>(t)));
                ASSERT_TRUE(inserted);
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
                auto [payload, found] = dht.find(key);
                if (found) {
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

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&dht, t]() {
            using namespace Payload;
            for (int i = 0; i < NUM_KEYS; ++i) {
                std::string key = "shared_key_" + std::to_string(i);
                auto [payload, inserted] = dht.insert(key, makeInline1(static_cast<uint32_t>(t)));
                // Either inserted or found existing - both fine
            }
        });
    }
    for (auto& t : threads) t.join();

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
        using namespace Payload;
        dht.insert("key_" + std::to_string(i), makeInline1(static_cast<uint32_t>(i)));
    }

    std::thread remover([&dht]() {
        for (int i = 0; i < NUM_KEYS; i += 2) {
            dht.remove("key_" + std::to_string(i));
        }
    });

    std::thread inserter([&dht]() {
        using namespace Payload;
        for (int i = NUM_KEYS; i < NUM_KEYS * 2; ++i) {
            dht.insert("key_" + std::to_string(i), makeInline1(static_cast<uint32_t>(i)));
        }
    });

    remover.join();
    inserter.join();

    // Verify odd keys still exist
    for (int i = 1; i < NUM_KEYS; i += 2) {
        auto [p, f] = dht.find("key_" + std::to_string(i));
        EXPECT_TRUE(f) << "missing odd key: " << i;
    }

    // Verify new keys exist
    for (int i = NUM_KEYS; i < NUM_KEYS * 2; ++i) {
        auto [p, f] = dht.find("key_" + std::to_string(i));
        EXPECT_TRUE(f) << "missing new key: " << i;
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
