#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

#include "internal/delta_hash_table_base.h"
#include "internal/global_index.h"
#include "internal/log_file.h"
#include "internal/entry_stream.h"
#include "internal/segment.h"
#include "internal/segment_index.h"
#include "internal/write_buffer.h"

using namespace kvlite::internal;
using kvlite::Status;

// --- Test fixture ---

class EntryStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        base_ = "/tmp/es_test_" + std::to_string(getpid()) + "_";
    }
    void TearDown() override {
        for (auto& seg : segments_) {
            seg.close();
        }
        for (auto& path : paths_) {
            ::unlink(path.c_str());
        }
    }

    // Create a segment, write entries, seal it, and register in GlobalIndex.
    size_t createSegment(
        uint32_t segment_id,
        const std::vector<std::tuple<std::string, uint64_t, std::string, bool>>& entries) {
        std::string path = base_ + std::to_string(segment_id) + ".data";
        paths_.push_back(path);
        segments_.emplace_back();
        size_t idx = segments_.size() - 1;
        auto& seg = segments_[idx];

        EXPECT_TRUE(seg.create(path, segment_id).ok());
        for (const auto& [key, version, value, tombstone] : entries) {
            EXPECT_TRUE(seg.put(key, version, value, tombstone).ok());
            gi_.put(key, version, segment_id);
        }
        EXPECT_TRUE(seg.seal().ok());
        return idx;
    }

    GlobalIndex gi_;
    std::vector<Segment> segments_;
    std::string base_;
    std::vector<std::string> paths_;
};

// --- ScanStream Tests ---

TEST_F(EntryStreamTest, Scan_AllEntries) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
    });
    auto& seg = segments_[idx];

    auto s = stream::scan(seg.logFile(), seg.dataSize());
    size_t count = 0;
    while (s->valid()) {
        count++;
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(count, 2u);
}

// --- SnapshotVisibility (scanVisible) Tests ---

TEST_F(EntryStreamTest, ScanVisible_SingleEntry) {
    size_t idx = createSegment(1, {{"key1", 10, "val1", false}});
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 10);

    std::vector<uint64_t> snapshots = {10};
    auto s = stream::scanVisible(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_EQ(s->entry().value, "val1");
    EXPECT_EQ(s->entry().version, 10u);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

TEST_F(EntryStreamTest, ScanVisible_EmptyVisibleSet) {
    size_t idx = createSegment(1, {{"key1", 1, "val1", false}});
    createSegment(2, {{"key1", 2, "val2", false}});
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 1);

    // Only snapshot at v2 -> latest is v2(seg2), nothing visible in seg1.
    std::vector<uint64_t> snapshots = {2};
    auto s = stream::scanVisible(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    EXPECT_FALSE(s->valid());
}

TEST_F(EntryStreamTest, ScanVisible_MultipleKeys) {
    std::string keyA = "key1";
    std::string keyB = "key2";
    uint64_t hashA = dhtHashBytes(keyA.data(), keyA.size());
    uint64_t hashB = dhtHashBytes(keyB.data(), keyB.size());
    if (hashA > hashB) {
        std::swap(keyA, keyB);
    }

    size_t idx = createSegment(1, {
        {keyA, 1, "A_v1", false},
        {keyB, 2, "B_v2", false},
    });
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put(keyA, 0, 1);
    si.put(keyB, 100, 2);

    std::vector<uint64_t> snapshots = {2};
    auto s = stream::scanVisible(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, keyA);

    ASSERT_TRUE(s->next().ok());
    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, keyB);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

TEST_F(EntryStreamTest, ScanVisible_TombstonePassThrough) {
    size_t idx = createSegment(1, {{"key1", 1, "val1", true}});
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 1);

    std::vector<uint64_t> snapshots = {1};
    auto s = stream::scanVisible(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_TRUE(s->entry().tombstone);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

// --- ScanLatest Tests ---

TEST_F(EntryStreamTest, ScanLatest_SingleSegment) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
    });
    auto& seg = segments_[idx];

    auto s = stream::scanLatest(
        gi_, 1, /*snapshot_version=*/2,
        seg.logFile(), seg.dataSize());

    ASSERT_TRUE(s->valid());
    std::vector<std::string> keys;
    while (s->valid()) {
        keys.push_back(std::string(s->entry().key));
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(keys.size(), 2u);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "key1") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "key2") != keys.end());
}

TEST_F(EntryStreamTest, ScanLatest_SupersededEntry) {
    size_t idx = createSegment(1, {{"key1", 1, "old", false}});
    createSegment(2, {{"key1", 2, "new", false}});
    auto& seg = segments_[idx];

    auto s = stream::scanLatest(
        gi_, 1, /*snapshot_version=*/2,
        seg.logFile(), seg.dataSize());

    EXPECT_FALSE(s->valid());
}

TEST_F(EntryStreamTest, ScanLatest_MultiSegmentDedup) {
    createSegment(1, {{"key1", 1, "old", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "new", false}});
    auto& seg2 = segments_[idx2];

    auto s = stream::scanLatest(
        gi_, 2, /*snapshot_version=*/2,
        seg2.logFile(), seg2.dataSize());

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_EQ(s->entry().value, "new");
    EXPECT_EQ(s->entry().version, 2u);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

TEST_F(EntryStreamTest, ScanLatest_SnapshotBound) {
    size_t idx = createSegment(1, {
        {"key1", 1, "v1", false},
        {"key1", 3, "v3", false},
    });
    auto& seg = segments_[idx];

    auto s = stream::scanLatest(
        gi_, 1, /*snapshot_version=*/2,
        seg.logFile(), seg.dataSize());

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_EQ(s->entry().version, 1u);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

// --- ScanLatestConsistent Tests ---

TEST_F(EntryStreamTest, ScanLatestConsistent_SingleSegment) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
    });
    auto& seg = segments_[idx];

    auto s = stream::scanLatestConsistent(
        gi_, 1, /*snapshot_version=*/2,
        seg.logFile(), seg.dataSize());

    ASSERT_TRUE(s->valid());
    std::vector<std::string> keys;
    while (s->valid()) {
        keys.push_back(std::string(s->entry().key));
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(keys.size(), 2u);
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "key1") != keys.end());
    EXPECT_TRUE(std::find(keys.begin(), keys.end(), "key2") != keys.end());
}

TEST_F(EntryStreamTest, ScanLatestConsistent_SupersededEntry) {
    size_t idx = createSegment(1, {{"key1", 1, "old", false}});
    createSegment(2, {{"key1", 2, "new", false}});
    auto& seg = segments_[idx];

    auto s = stream::scanLatestConsistent(
        gi_, 1, /*snapshot_version=*/2,
        seg.logFile(), seg.dataSize());

    EXPECT_FALSE(s->valid());
}

TEST_F(EntryStreamTest, ScanLatestConsistent_SnapshotBound) {
    size_t idx = createSegment(1, {
        {"key1", 1, "v1", false},
        {"key1", 3, "v3", false},
    });
    auto& seg = segments_[idx];

    auto s = stream::scanLatestConsistent(
        gi_, 1, /*snapshot_version=*/2,
        seg.logFile(), seg.dataSize());

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_EQ(s->entry().version, 1u);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

TEST_F(EntryStreamTest, ScanLatestConsistent_ImmuneToMutation) {
    // Populate seg1 with 50 keys.
    std::vector<std::tuple<std::string, uint64_t, std::string, bool>> entries;
    for (int i = 0; i < 50; ++i) {
        entries.push_back({"key" + std::to_string(i),
                           static_cast<uint64_t>(i + 1),
                           "val" + std::to_string(i), false});
    }
    size_t idx = createSegment(1, entries);
    auto& seg = segments_[idx];

    // Create scanLatestConsistent — precomputes visibility set now.
    auto s = stream::scanLatestConsistent(
        gi_, 1, /*snapshot_version=*/50,
        seg.logFile(), seg.dataSize());

    // Mutate GlobalIndex: supersede all keys with newer versions in seg2.
    for (int i = 0; i < 50; ++i) {
        gi_.put("key" + std::to_string(i),
                static_cast<uint64_t>(100 + i), 2);
    }

    // Drain the stream — should still yield all 50, since visibility was
    // precomputed before the mutations.
    size_t count = 0;
    while (s->valid()) {
        count++;
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(count, 50u);
}

TEST_F(EntryStreamTest, ScanLatest_NonAtomic_SeesMutation) {
    // Populate seg1 with 50 keys at versions 1..50.
    std::vector<std::tuple<std::string, uint64_t, std::string, bool>> entries;
    for (int i = 0; i < 50; ++i) {
        entries.push_back({"key" + std::to_string(i),
                           static_cast<uint64_t>(i + 1),
                           "val" + std::to_string(i), false});
    }
    size_t idx = createSegment(1, entries);
    auto& seg = segments_[idx];

    // Use snapshot_version=200 so new versions in seg2 are within the bound.
    // Note: the FilterStream constructor eagerly advances to the first matching
    // entry, so at most 1 entry can pass before we mutate the GlobalIndex.
    auto s = stream::scanLatest(
        gi_, 1, /*snapshot_version=*/200,
        seg.logFile(), seg.dataSize());

    // Supersede ALL keys with versions within the snapshot bound but in seg2.
    for (int i = 0; i < 50; ++i) {
        gi_.put("key" + std::to_string(i),
                static_cast<uint64_t>(51 + i), 2);
    }

    // Drain — scanLatest re-checks GlobalIndex per entry, so most entries
    // should be filtered out. The first entry was already loaded at
    // construction time, so count may be 1 (not 0).
    size_t count = 0;
    while (s->valid()) {
        count++;
        ASSERT_TRUE(s->next().ok());
    }

    // Non-atomic: most entries filtered out by mutation. At most 1 entry
    // survived (the one preloaded by FilterStream's constructor).
    // Contrast with scanLatestConsistent which would return all 50.
    EXPECT_LE(count, 1u);
}

TEST_F(EntryStreamTest, ScanLatestConsistent_ConcurrentMutation) {
    // Populate seg1 with 100 keys.
    std::vector<std::tuple<std::string, uint64_t, std::string, bool>> entries;
    for (int i = 0; i < 100; ++i) {
        entries.push_back({"key" + std::to_string(i),
                           static_cast<uint64_t>(i + 1),
                           "val" + std::to_string(i), false});
    }
    size_t idx = createSegment(1, entries);
    auto& seg = segments_[idx];

    // Create scanLatestConsistent — precomputes visibility set.
    auto s = stream::scanLatestConsistent(
        gi_, 1, /*snapshot_version=*/100,
        seg.logFile(), seg.dataSize());

    // Writer thread: supersede keys concurrently.
    std::thread writer([&] {
        for (int i = 0; i < 100; ++i) {
            gi_.put("key" + std::to_string(i),
                    static_cast<uint64_t>(200 + i), 2);
        }
    });

    // Drain — should still see all 100, since set was precomputed.
    size_t count = 0;
    while (s->valid()) {
        count++;
        ASSERT_TRUE(s->next().ok());
    }

    writer.join();

    EXPECT_EQ(count, 100u);
}

// --- MergeStream Tests ---

TEST_F(EntryStreamTest, Merge_SingleStream) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
    });
    auto& seg = segments_[idx];

    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.push_back(stream::scanLatest(
        gi_, 1, /*snapshot_version=*/2,
        seg.logFile(), seg.dataSize()));

    auto merged = stream::merge(std::move(streams));

    size_t count = 0;
    while (merged->valid()) {
        count++;
        ASSERT_TRUE(merged->next().ok());
    }
    EXPECT_EQ(count, 2u);
}

TEST_F(EntryStreamTest, Merge_TwoStreamsDisjointKeys) {
    size_t idx1 = createSegment(1, {{"key1", 1, "val1", false}});
    size_t idx2 = createSegment(2, {{"key2", 2, "val2", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.push_back(stream::scanLatest(gi_, 1, 2, seg1.logFile(), seg1.dataSize()));
    streams.push_back(stream::scanLatest(gi_, 2, 2, seg2.logFile(), seg2.dataSize()));

    auto merged = stream::merge(std::move(streams));

    std::vector<std::string> keys;
    while (merged->valid()) {
        keys.push_back(std::string(merged->entry().key));
        ASSERT_TRUE(merged->next().ok());
    }
    EXPECT_EQ(keys.size(), 2u);
}

TEST_F(EntryStreamTest, Merge_TwoStreamsSameKey) {
    createSegment(1, {{"key1", 1, "old", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "new", false}});
    auto& seg1 = segments_[0];
    auto& seg2 = segments_[idx2];

    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.push_back(stream::scanLatest(gi_, 1, 2, seg1.logFile(), seg1.dataSize()));
    streams.push_back(stream::scanLatest(gi_, 2, 2, seg2.logFile(), seg2.dataSize()));

    auto merged = stream::merge(std::move(streams));

    std::vector<std::string> keys;
    while (merged->valid()) {
        keys.push_back(std::string(merged->entry().key));
        ASSERT_TRUE(merged->next().ok());
    }
    EXPECT_EQ(keys.size(), 1u);
    EXPECT_EQ(keys[0], "key1");
}

TEST_F(EntryStreamTest, Merge_Empty) {
    std::vector<std::unique_ptr<EntryStream>> streams;
    auto merged = stream::merge(std::move(streams));
    EXPECT_FALSE(merged->valid());
}

TEST_F(EntryStreamTest, Merge_OrderVerification) {
    std::string keyA = "alpha";
    std::string keyB = "beta";
    uint64_t hashA = dhtHashBytes(keyA.data(), keyA.size());
    uint64_t hashB = dhtHashBytes(keyB.data(), keyB.size());
    if (hashA > hashB) {
        std::swap(keyA, keyB);
        std::swap(hashA, hashB);
    }

    size_t idx1 = createSegment(1, {{keyB, 2, "B_val", false}});
    size_t idx2 = createSegment(2, {{keyA, 1, "A_val", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.push_back(stream::scanLatest(gi_, 1, 2, seg1.logFile(), seg1.dataSize()));
    streams.push_back(stream::scanLatest(gi_, 2, 2, seg2.logFile(), seg2.dataSize()));

    auto merged = stream::merge(std::move(streams));

    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->entry().key, keyA);
    uint64_t first_hash = merged->entry().hash;

    ASSERT_TRUE(merged->next().ok());
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->entry().key, keyB);
    uint64_t second_hash = merged->entry().hash;

    EXPECT_LT(first_hash, second_hash);

    ASSERT_TRUE(merged->next().ok());
    EXPECT_FALSE(merged->valid());
}

// --- FilterStream Tests ---

TEST_F(EntryStreamTest, Filter_Custom) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
        {"key3", 3, "val3", false},
    });
    auto& seg = segments_[idx];

    // Filter to only entries with version > 1
    auto s = stream::filter(
        stream::scan(seg.logFile(), seg.dataSize()),
        [](const EntryStream::Entry& e) {
            return e.version > 1;
        });

    size_t count = 0;
    while (s->valid()) {
        EXPECT_GT(s->entry().version, 1u);
        count++;
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(count, 2u);
}

// --- Concurrent iteration + mutation tests ---
//
// These tests verify that draining an EntryStream while another thread
// mutates the GlobalIndex does not crash or corrupt.

TEST_F(EntryStreamTest, ConcurrentIterate_PutsDuringDrain) {
    // Populate initial data: seg1 with 100 keys.
    std::vector<std::tuple<std::string, uint64_t, std::string, bool>> entries;
    for (int i = 0; i < 100; ++i) {
        entries.push_back({"key" + std::to_string(i),
                           static_cast<uint64_t>(i + 1),
                           "val" + std::to_string(i), false});
    }
    size_t idx = createSegment(1, entries);
    auto& seg = segments_[idx];

    // Create scanLatest stream at snapshot = 100.
    auto s = stream::scanLatest(
        gi_, 1, /*snapshot_version=*/100,
        seg.logFile(), seg.dataSize());

    // Writer thread: put new keys into GlobalIndex concurrently.
    std::atomic<bool> done{false};
    std::thread writer([&] {
        for (int i = 0; i < 500; ++i) {
            gi_.put("new_key" + std::to_string(i),
                    static_cast<uint64_t>(200 + i), 2);
        }
        done = true;
    });

    // Drain the stream — should not crash or hang.
    size_t count = 0;
    while (s->valid()) {
        count++;
        ASSERT_TRUE(s->next().ok());
    }

    writer.join();

    // All 100 original keys should have been yielded (scanLatest checks
    // GlobalIndex per entry, but segment_id=1 entries are still there).
    EXPECT_EQ(count, 100u);
}

TEST_F(EntryStreamTest, ConcurrentIterate_DeletesDuringDrain) {
    // Populate: seg1 with 50 keys.
    std::vector<std::tuple<std::string, uint64_t, std::string, bool>> entries1;
    for (int i = 0; i < 50; ++i) {
        entries1.push_back({"key" + std::to_string(i),
                            static_cast<uint64_t>(i + 1),
                            "old" + std::to_string(i), false});
    }
    size_t idx1 = createSegment(1, entries1);
    auto& seg1 = segments_[idx1];

    // Create scanLatest on seg1 at snapshot = 50.
    auto s = stream::scanLatest(
        gi_, 1, /*snapshot_version=*/50,
        seg1.logFile(), seg1.dataSize());

    // Writer thread: add superseding versions to GlobalIndex in seg2.
    std::atomic<bool> done{false};
    std::thread writer([&] {
        for (int i = 0; i < 50; ++i) {
            gi_.put("key" + std::to_string(i),
                    static_cast<uint64_t>(100 + i), 2);
        }
        done = true;
    });

    // Drain the stream. Some entries may or may not be yielded depending
    // on race with the writer — the important thing is no crash.
    size_t count = 0;
    while (s->valid()) {
        count++;
        ASSERT_TRUE(s->next().ok());
    }

    writer.join();

    // Count may be 0..50 depending on timing — just verify no crash.
    EXPECT_LE(count, 50u);
}

TEST_F(EntryStreamTest, ConcurrentIterate_MergeWithPuts) {
    // Two segments, merge them while another thread adds to GlobalIndex.
    size_t idx1 = createSegment(1, {
        {"alpha", 1, "A1", false},
        {"gamma", 3, "G3", false},
    });
    size_t idx2 = createSegment(2, {
        {"beta", 2, "B2", false},
        {"delta", 4, "D4", false},
    });
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.push_back(stream::scanLatest(gi_, 1, 4, seg1.logFile(), seg1.dataSize()));
    streams.push_back(stream::scanLatest(gi_, 2, 4, seg2.logFile(), seg2.dataSize()));
    auto merged = stream::merge(std::move(streams));

    // Writer thread: concurrently update GlobalIndex.
    std::thread writer([&] {
        for (int i = 0; i < 200; ++i) {
            gi_.put("extra" + std::to_string(i),
                    static_cast<uint64_t>(100 + i), 3);
        }
    });

    // Drain merged stream — should not crash or hang.
    size_t count = 0;
    while (merged->valid()) {
        count++;
        ASSERT_TRUE(merged->next().ok());
    }

    writer.join();

    // Some entries may be filtered out due to concurrent GlobalIndex mutation
    // changing which segment holds the "latest" version. The important thing
    // is no crash or hang.
    EXPECT_LE(count, 4u);
}

TEST_F(EntryStreamTest, ConcurrentIterate_ScanVisibleWithPuts) {
    // Create a segment and set up a scanVisible stream, then mutate GlobalIndex.
    size_t idx = createSegment(1, {
        {"key1", 1, "v1", false},
        {"key2", 2, "v2", false},
        {"key3", 3, "v3", false},
    });
    auto& seg = segments_[idx];

    SegmentIndex si;
    si.put("key1", 0, 1);
    si.put("key2", 100, 2);
    si.put("key3", 200, 3);

    std::vector<uint64_t> snapshots = {3};
    // scanVisible pre-computes visible set at creation time, so concurrent
    // mutations to GlobalIndex should not affect it.
    auto s = stream::scanVisible(
        gi_, si, 1, snapshots, seg.logFile(), seg.dataSize());

    // Writer thread: add newer versions to GlobalIndex.
    std::thread writer([&] {
        for (int i = 0; i < 100; ++i) {
            gi_.put("key" + std::to_string(i % 3 + 1),
                    static_cast<uint64_t>(100 + i), 2);
        }
    });

    size_t count = 0;
    while (s->valid()) {
        count++;
        ASSERT_TRUE(s->next().ok());
    }

    writer.join();

    // scanVisible captured the visible set at construction — all 3 entries visible.
    EXPECT_EQ(count, 3u);
}

// --- TagSource Tests ---

TEST_F(EntryStreamTest, TagSource_SetsExtSlot) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
    });
    auto& seg = segments_[idx];

    constexpr size_t kSlot = 0;
    auto s = stream::tagSource(
        stream::scan(seg.logFile(), seg.dataSize()), 42, kSlot);

    size_t count = 0;
    while (s->valid()) {
        EXPECT_EQ(s->entry().ext[kSlot], 42u);
        count++;
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(count, 2u);
}

TEST_F(EntryStreamTest, TagSource_PreservesEntryFields) {
    size_t idx = createSegment(1, {{"key1", 5, "val5", true}});
    auto& seg = segments_[idx];

    constexpr size_t kSlot = 2;
    auto s = stream::tagSource(
        stream::scan(seg.logFile(), seg.dataSize()), 99, kSlot);

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_EQ(s->entry().value, "val5");
    EXPECT_EQ(s->entry().version, 5u);
    EXPECT_TRUE(s->entry().tombstone);
    EXPECT_EQ(s->entry().ext[kSlot], 99u);
}

TEST_F(EntryStreamTest, TagSource_PropagatesThroughMerge) {
    size_t idx1 = createSegment(1, {{"key1", 1, "v1", false}});
    size_t idx2 = createSegment(2, {{"key2", 2, "v2", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    constexpr size_t kSlot = 0;
    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.push_back(stream::tagSource(
        stream::scan(seg1.logFile(), seg1.dataSize()), 1, kSlot));
    streams.push_back(stream::tagSource(
        stream::scan(seg2.logFile(), seg2.dataSize()), 2, kSlot));

    auto merged = stream::merge(std::move(streams));

    // Both entries should retain their tagged segment_id through merge.
    std::set<uint64_t> seen_ids;
    while (merged->valid()) {
        seen_ids.insert(merged->entry().ext[kSlot]);
        ASSERT_TRUE(merged->next().ok());
    }
    EXPECT_EQ(seen_ids.count(1u), 1u);
    EXPECT_EQ(seen_ids.count(2u), 1u);
}

// --- Classify Tests ---

TEST_F(EntryStreamTest, Classify_KeepAndEliminate) {
    // key1 v1 (seg1) + key1 v2 (seg2). Only v2 visible.
    size_t idx1 = createSegment(1, {{"key1", 1, "old", false}});
    size_t idx2 = createSegment(2, {{"key1", 2, "new", false}});
    auto& seg1 = segments_[idx1];
    auto& seg2 = segments_[idx2];

    constexpr size_t kSlotSeg = 0;
    constexpr size_t kSlotAction = 1;

    std::vector<std::unique_ptr<EntryStream>> streams;
    streams.push_back(stream::tagSource(
        stream::scan(seg1.logFile(), seg1.dataSize()), 1, kSlotSeg));
    streams.push_back(stream::tagSource(
        stream::scan(seg2.logFile(), seg2.dataSize()), 2, kSlotSeg));

    auto merged = stream::merge(std::move(streams));

    uint64_t hash = dhtHashBytes("key1", 4);
    std::unordered_map<uint64_t, std::set<uint32_t>> visible;
    visible[hash] = {2};

    auto classified = stream::classify(
        std::move(merged), std::move(visible), kSlotAction);

    // First entry: key1 v1 → kEliminate, segment_id=1 preserved
    ASSERT_TRUE(classified->valid());
    EXPECT_EQ(classified->entry().version, 1u);
    EXPECT_EQ(classified->entry().ext[kSlotAction],
              static_cast<uint64_t>(EntryAction::kEliminate));
    EXPECT_EQ(classified->entry().ext[kSlotSeg], 1u);

    ASSERT_TRUE(classified->next().ok());
    // Second entry: key1 v2 → kKeep, segment_id=2 preserved
    ASSERT_TRUE(classified->valid());
    EXPECT_EQ(classified->entry().version, 2u);
    EXPECT_EQ(classified->entry().ext[kSlotAction],
              static_cast<uint64_t>(EntryAction::kKeep));
    EXPECT_EQ(classified->entry().ext[kSlotSeg], 2u);

    ASSERT_TRUE(classified->next().ok());
    EXPECT_FALSE(classified->valid());
}

TEST_F(EntryStreamTest, Classify_AllKeep) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
        {"key2", 2, "val2", false},
    });
    auto& seg = segments_[idx];

    constexpr size_t kSlotAction = 0;
    auto tagged = stream::scan(seg.logFile(), seg.dataSize());

    uint64_t h1 = dhtHashBytes("key1", 4);
    uint64_t h2 = dhtHashBytes("key2", 4);
    std::unordered_map<uint64_t, std::set<uint32_t>> visible;
    visible[h1] = {1};
    visible[h2] = {2};

    auto classified = stream::classify(
        std::move(tagged), std::move(visible), kSlotAction);

    size_t keep_count = 0;
    while (classified->valid()) {
        EXPECT_EQ(classified->entry().ext[kSlotAction],
                  static_cast<uint64_t>(EntryAction::kKeep));
        keep_count++;
        ASSERT_TRUE(classified->next().ok());
    }
    EXPECT_EQ(keep_count, 2u);
}

TEST_F(EntryStreamTest, Classify_AllEliminate) {
    size_t idx = createSegment(1, {
        {"key1", 1, "val1", false},
    });
    auto& seg = segments_[idx];

    constexpr size_t kSlotAction = 0;

    // Empty visible set → everything eliminated.
    std::unordered_map<uint64_t, std::set<uint32_t>> visible;
    auto classified = stream::classify(
        stream::scan(seg.logFile(), seg.dataSize()),
        std::move(visible), kSlotAction);

    ASSERT_TRUE(classified->valid());
    EXPECT_EQ(classified->entry().ext[kSlotAction],
              static_cast<uint64_t>(EntryAction::kEliminate));

    ASSERT_TRUE(classified->next().ok());
    EXPECT_FALSE(classified->valid());
}

// --- ScanWriteBuffer Tests ---

TEST(ScanWriteBufferTest, Basic) {
    WriteBuffer wb;
    wb.put("key1", 1, "val1", false);
    wb.put("key2", 2, "val2", false);

    auto s = stream::scanWriteBuffer(wb, /*snapshot_version=*/2);

    std::vector<std::string> keys;
    while (s->valid()) {
        keys.push_back(std::string(s->entry().key));
        EXPECT_FALSE(s->entry().tombstone);
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(keys.size(), 2u);
}

TEST(ScanWriteBufferTest, SnapshotBound) {
    WriteBuffer wb;
    wb.put("key1", 1, "val1", false);
    wb.put("key2", 5, "val2", false);
    wb.put("key3", 10, "val3", false);

    // Only entries with version <= 5 should be included.
    auto s = stream::scanWriteBuffer(wb, /*snapshot_version=*/5);

    std::vector<std::string> keys;
    while (s->valid()) {
        EXPECT_LE(s->entry().version, 5u);
        keys.push_back(std::string(s->entry().key));
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(keys.size(), 2u);
}

TEST(ScanWriteBufferTest, LatestPerKey) {
    WriteBuffer wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key1", 3, "v3", false);
    wb.put("key1", 5, "v5", false);

    auto s = stream::scanWriteBuffer(wb, /*snapshot_version=*/5);

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_EQ(s->entry().value, "v5");
    EXPECT_EQ(s->entry().version, 5u);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

TEST(ScanWriteBufferTest, LatestPerKeyWithSnapshotBound) {
    WriteBuffer wb;
    wb.put("key1", 1, "v1", false);
    wb.put("key1", 3, "v3", false);
    wb.put("key1", 5, "v5", false);

    // Only versions <= 3 visible.
    auto s = stream::scanWriteBuffer(wb, /*snapshot_version=*/3);

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_EQ(s->entry().value, "v3");
    EXPECT_EQ(s->entry().version, 3u);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

TEST(ScanWriteBufferTest, Empty) {
    WriteBuffer wb;

    auto s = stream::scanWriteBuffer(wb, /*snapshot_version=*/100);
    EXPECT_FALSE(s->valid());
}

TEST(ScanWriteBufferTest, Tombstone) {
    WriteBuffer wb;
    wb.put("key1", 1, "val1", false);
    wb.put("key1", 2, "", true);  // tombstone

    auto s = stream::scanWriteBuffer(wb, /*snapshot_version=*/2);

    ASSERT_TRUE(s->valid());
    EXPECT_EQ(s->entry().key, "key1");
    EXPECT_TRUE(s->entry().tombstone);

    ASSERT_TRUE(s->next().ok());
    EXPECT_FALSE(s->valid());
}

TEST(ScanWriteBufferTest, SortedByHash) {
    WriteBuffer wb;
    // Insert multiple keys; stream should yield them in hash order.
    wb.put("alpha", 1, "A", false);
    wb.put("beta", 2, "B", false);
    wb.put("gamma", 3, "G", false);

    auto s = stream::scanWriteBuffer(wb, /*snapshot_version=*/3);

    uint64_t prev_hash = 0;
    size_t count = 0;
    while (s->valid()) {
        EXPECT_GE(s->entry().hash, prev_hash);
        prev_hash = s->entry().hash;
        count++;
        ASSERT_TRUE(s->next().ok());
    }
    EXPECT_EQ(count, 3u);
}

// --- WriteBuffer Pin/Unpin Tests ---

TEST(WriteBufferPinTest, PinUnpin) {
    WriteBuffer wb;
    wb.put("key1", 1, "val1", false);

    EXPECT_EQ(wb.pinCount(), 0u);
    wb.pin();
    EXPECT_EQ(wb.pinCount(), 1u);
    wb.pin();
    EXPECT_EQ(wb.pinCount(), 2u);
    wb.unpin();
    EXPECT_EQ(wb.pinCount(), 1u);
    wb.unpin();
    EXPECT_EQ(wb.pinCount(), 0u);
}
