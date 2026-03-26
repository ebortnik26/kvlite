#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "internal/delta_hash_table.h"
#include "internal/manifest.h"
#include "internal/segment.h"
#include "internal/segment_storage_manager.h"

using namespace kvlite::internal;
using kvlite::Status;

class SegmentStorageManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = "/tmp/sm_test_" + std::to_string(getpid());
        std::filesystem::remove_all(path_);
    }
    void TearDown() override {
        std::filesystem::remove_all(path_);
    }

    // Create manifest + SegmentStorageManager, open (which also recovers).
    Status openSM(bool create = true) {
        manifest_ = std::make_unique<Manifest>();
        Status s = create ? manifest_->create(path_) : manifest_->open(path_);
        if (!s.ok()) return s;
        sm_ = std::make_unique<SegmentStorageManager>(*manifest_);
        return sm_->open(path_);
    }

    void closeSM() {
        sm_->close();
        manifest_->close();
    }

    // Helper: create a sealed segment via SegmentStorageManager so it is tracked.
    uint32_t createTrackedSegment(const std::string& key,
                                  const std::string& value) {
        uint32_t id = sm_->allocateSegmentId();
        EXPECT_TRUE(sm_->createSegment(id).ok());

        Segment* seg = sm_->getSegment(id);
        EXPECT_NE(seg, nullptr);
        EXPECT_TRUE(seg->put(key, 1, value, false,
                            dhtHashBytes(key.data(), key.size())).ok());
        EXPECT_TRUE(seg->seal().ok());

        return id;
    }

    // Helper: create an orphan .data file not tracked by manifest.
    void createOrphanFile(uint32_t fake_id) {
        std::string seg_dir = path_ + "/segments";
        std::filesystem::create_directories(seg_dir);
        std::string orphan =
            seg_dir + "/segment_" + std::to_string(fake_id) + "_0.data";
        std::ofstream(orphan) << "orphan";
    }

    // Helper: delete all partition files for a segment.
    void deleteSegmentFiles(uint32_t id) {
        std::string base = sm_->segmentBasePath(id);
        for (int p = 0; p < 16; ++p) {
            std::filesystem::remove(Segment::partitionPath(base, p));
        }
    }

    std::string path_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<SegmentStorageManager> sm_;
};

// removeSegment() should delete the physical file from disk.
TEST_F(SegmentStorageManagerTest, RemoveSegmentDeletesFile) {
    ASSERT_TRUE(openSM().ok());

    uint32_t id = createTrackedSegment("k", "v");
    std::string seg_path = Segment::partitionPath(sm_->segmentBasePath(id), 0);

    // File exists before removal.
    ASSERT_TRUE(std::filesystem::exists(seg_path));

    ASSERT_TRUE(sm_->removeSegment(id).ok());

    // File must be gone after removal.
    EXPECT_FALSE(std::filesystem::exists(seg_path));
    EXPECT_EQ(sm_->getSegment(id), nullptr);

    closeSM();
}

// recover() should delete orphan segment files not tracked by the manifest.
TEST_F(SegmentStorageManagerTest, RecoverPurgesOrphanFiles) {
    // Phase 1: create a tracked segment and an orphan file.
    {
        ASSERT_TRUE(openSM().ok());
        createTrackedSegment("k1", "v1");
        closeSM();
    }

    // Plant an orphan file.
    createOrphanFile(999);
    ASSERT_TRUE(std::filesystem::exists(
        path_ + "/segments/segment_999_0.data"));

    // Phase 2: reopen — orphan should be purged.
    {
        ASSERT_TRUE(openSM(false).ok());

        // Orphan should be gone.
        EXPECT_FALSE(std::filesystem::exists(
            path_ + "/segments/segment_999_0.data"));

        // Tracked segment should still exist.
        EXPECT_EQ(sm_->segmentCount(), 1u);

        closeSM();
    }
}

// recover() should tolerate missing segment files within the [min, max] range.
// This happens when a segment is removed (e.g., by GC) but min/max aren't
// fully updated before a crash, leaving a gap.
TEST_F(SegmentStorageManagerTest, RecoverToleratesMissingFile) {
    {
        ASSERT_TRUE(openSM().ok());
        uint32_t id = createTrackedSegment("k1", "v1");
        closeSM();

        // Delete the segment files behind the manifest's back.
        deleteSegmentFiles(id);
    }

    // Reopen — recover should succeed, treating the gap as a removed segment.
    ASSERT_TRUE(openSM(false).ok());
    EXPECT_EQ(sm_->segmentCount(), 0u);
    closeSM();
}

// Non-contiguous segment IDs: gaps in [min, max] range are tolerated.
// Simulates GC removing middle segments without updating min/max.
TEST_F(SegmentStorageManagerTest, RecoverNonContiguousIds) {
    // Create segments 1, 2, 3.
    ASSERT_TRUE(openSM().ok());
    uint32_t id1 = createTrackedSegment("k1", "v1");
    uint32_t id2 = createTrackedSegment("k2", "v2");
    uint32_t id3 = createTrackedSegment("k3", "v3");
    closeSM();

    // Delete segment 2's files (simulate GC crash that removed the file
    // but didn't update the manifest).
    for (int p = 0; p < 16; ++p) {
        std::string base = path_ + "/segments/segment_" + std::to_string(id2);
        std::filesystem::remove(Segment::partitionPath(base, p));
    }

    // Reopen — should succeed with only segments 1 and 3.
    ASSERT_TRUE(openSM(false).ok());
    EXPECT_EQ(sm_->segmentCount(), 2u);
    EXPECT_NE(sm_->getSegment(id1), nullptr);
    EXPECT_EQ(sm_->getSegment(id2), nullptr);  // gap
    EXPECT_NE(sm_->getSegment(id3), nullptr);
    closeSM();
}

// Segment IDs remain monotonic across close/reopen cycles.
TEST_F(SegmentStorageManagerTest, MonotonicIdsAcrossReopen) {
    uint32_t last_id;

    // Cycle 1: create segments.
    {
        ASSERT_TRUE(openSM().ok());
        createTrackedSegment("k1", "v1");
        last_id = createTrackedSegment("k2", "v2");
        closeSM();
    }

    // Cycle 2: reopen and allocate more IDs.
    {
        ASSERT_TRUE(openSM(false).ok());
        uint32_t new_id = sm_->allocateSegmentId();
        EXPECT_GT(new_id, last_id);  // strictly monotonic
        last_id = createTrackedSegment("k3", "v3");
        closeSM();
    }

    // Cycle 3: reopen again.
    {
        ASSERT_TRUE(openSM(false).ok());
        uint32_t new_id = sm_->allocateSegmentId();
        EXPECT_GT(new_id, last_id);
        closeSM();
    }
}

// Orphan file outside the [min, max] range is purged on recovery.
TEST_F(SegmentStorageManagerTest, OrphanOutsideRangeIsPurged) {
    {
        ASSERT_TRUE(openSM().ok());
        createTrackedSegment("k1", "v1");
        closeSM();
    }

    // Plant orphan with ID far outside the tracked range.
    createOrphanFile(12345);
    ASSERT_TRUE(std::filesystem::exists(
        path_ + "/segments/segment_12345_0.data"));

    {
        ASSERT_TRUE(openSM(false).ok());

        EXPECT_FALSE(std::filesystem::exists(
            path_ + "/segments/segment_12345_0.data"));
        EXPECT_EQ(sm_->segmentCount(), 1u);
        closeSM();
    }
}

// Non-contiguous IDs with orphan outside range: orphans are purged,
// gaps within the [min, max] range are simply skipped.
TEST_F(SegmentStorageManagerTest, NonContiguousWithOrphanPurge) {
    // Create segments 1, 2, 3.
    ASSERT_TRUE(openSM().ok());
    uint32_t id1 = createTrackedSegment("k1", "v1");
    uint32_t id2 = createTrackedSegment("k2", "v2");
    uint32_t id3 = createTrackedSegment("k3", "v3");
    closeSM();

    // Delete segment 2's files (gap).
    for (int p = 0; p < 16; ++p) {
        std::string base = path_ + "/segments/segment_" + std::to_string(id2);
        std::filesystem::remove(Segment::partitionPath(base, p));
    }

    // Also plant an orphan outside the range.
    createOrphanFile(999);

    {
        ASSERT_TRUE(openSM(false).ok());

        // Gap at id2 is tolerated: 2 segments loaded.
        EXPECT_EQ(sm_->segmentCount(), 2u);
        EXPECT_NE(sm_->getSegment(id1), nullptr);
        EXPECT_EQ(sm_->getSegment(id2), nullptr);
        EXPECT_NE(sm_->getSegment(id3), nullptr);

        // Orphan outside range is purged.
        EXPECT_FALSE(std::filesystem::exists(
            path_ + "/segments/segment_999_0.data"));

        closeSM();
    }
}

// Partial partition crash: a complete segment (4 partitions) survives recovery,
// while an incomplete segment (only 2 of 4 partition files) is skipped and its
// orphan files are purged.
TEST_F(SegmentStorageManagerTest, PartialPartitionCrashRecovery) {
    uint32_t complete_id;
    uint32_t incomplete_id;

    // Phase 1: create a fully sealed 4-partition segment.
    {
        manifest_ = std::make_unique<Manifest>();
        ASSERT_TRUE(manifest_->create(path_).ok());
        sm_ = std::make_unique<SegmentStorageManager>(*manifest_, true, 4);
        ASSERT_TRUE(sm_->open(path_).ok());

        complete_id = sm_->allocateSegmentId();
        ASSERT_TRUE(sm_->createSegment(complete_id).ok());

        Segment* seg = sm_->getSegment(complete_id);
        ASSERT_NE(seg, nullptr);
        ASSERT_TRUE(seg->put("k1", 1, "v1", false,
                             dhtHashBytes("k1", 2)).ok());
        ASSERT_TRUE(seg->put("k2", 2, "v2", false,
                             dhtHashBytes("k2", 2)).ok());
        ASSERT_TRUE(seg->put("k3", 3, "v3", false,
                             dhtHashBytes("k3", 2)).ok());
        ASSERT_TRUE(seg->seal().ok());

        // Allocate a second ID so the manifest range covers it.
        incomplete_id = sm_->allocateSegmentId();
        // Register it so manifest max covers this ID.
        sm_->registerSegmentId(incomplete_id);

        closeSM();
    }

    // Phase 2: simulate a crash of the second segment mid-flush.
    // Create only 2 of the 4 expected partition files with invalid content.
    {
        std::string seg_dir = path_ + "/segments";
        std::string base = seg_dir + "/segment_" + std::to_string(incomplete_id);
        for (int p = 0; p < 2; ++p) {
            std::string ppath = Segment::partitionPath(base, p);
            std::ofstream(ppath) << "fake partial flush data";
        }

        // Verify the fake files exist.
        for (int p = 0; p < 2; ++p) {
            ASSERT_TRUE(std::filesystem::exists(
                Segment::partitionPath(base, p)));
        }
        // Partitions 2 and 3 should NOT exist.
        for (int p = 2; p < 4; ++p) {
            ASSERT_FALSE(std::filesystem::exists(
                Segment::partitionPath(base, p)));
        }
    }

    // Phase 3: reopen with 4 partitions — recovery should handle both cases.
    {
        manifest_ = std::make_unique<Manifest>();
        ASSERT_TRUE(manifest_->open(path_).ok());
        sm_ = std::make_unique<SegmentStorageManager>(*manifest_, true, 4);
        ASSERT_TRUE(sm_->open(path_).ok());

        // The complete segment should be present (4 valid sealed partitions).
        EXPECT_NE(sm_->getSegment(complete_id), nullptr);

        // The incomplete segment should NOT be loaded (open fails on fake data).
        EXPECT_EQ(sm_->getSegment(incomplete_id), nullptr);

        // Only the complete segment is counted.
        EXPECT_EQ(sm_->segmentCount(), 1u);

        // Orphan partition files from the incomplete segment should be purged.
        std::string base = path_ + "/segments/segment_" +
                           std::to_string(incomplete_id);
        for (int p = 0; p < 2; ++p) {
            EXPECT_FALSE(std::filesystem::exists(
                Segment::partitionPath(base, p)));
        }

        closeSM();
    }
}

// New DB (empty manifest) recovers successfully with no segments.
TEST_F(SegmentStorageManagerTest, RecoverEmptyDB) {
    ASSERT_TRUE(openSM().ok());
    EXPECT_EQ(sm_->segmentCount(), 0u);
    EXPECT_EQ(sm_->allocateSegmentId(), 1u);
    closeSM();

    // Reopen on the same dir — still empty.
    ASSERT_TRUE(openSM(false).ok());
    EXPECT_EQ(sm_->segmentCount(), 0u);
    closeSM();
}
