#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

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
        EXPECT_TRUE(seg->put(key, 1, value, false).ok());
        EXPECT_TRUE(seg->seal().ok());

        return id;
    }

    // Helper: create an orphan .data file not tracked by manifest.
    void createOrphanFile(uint32_t fake_id) {
        std::string seg_dir = path_ + "/segments";
        std::filesystem::create_directories(seg_dir);
        std::string orphan =
            seg_dir + "/segment_" + std::to_string(fake_id) + ".data";
        std::ofstream(orphan) << "orphan";
    }

    std::string path_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<SegmentStorageManager> sm_;
};

// removeSegment() should delete the physical file from disk.
TEST_F(SegmentStorageManagerTest, RemoveSegmentDeletesFile) {
    ASSERT_TRUE(openSM().ok());

    uint32_t id = createTrackedSegment("k", "v");
    std::string seg_path = sm_->segmentPath(id);

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
        path_ + "/segments/segment_999.data"));

    // Phase 2: reopen — orphan should be purged.
    {
        ASSERT_TRUE(openSM(false).ok());

        // Orphan should be gone.
        EXPECT_FALSE(std::filesystem::exists(
            path_ + "/segments/segment_999.data"));

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

        // Delete the segment file behind the manifest's back.
        std::filesystem::remove(path_ + "/segments/segment_" + std::to_string(id) + ".data");
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

    // Delete segment 2's file (simulate GC crash that removed the file
    // but didn't update the manifest).
    std::filesystem::remove(path_ + "/segments/segment_" + std::to_string(id2) + ".data");

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
        path_ + "/segments/segment_12345.data"));

    {
        ASSERT_TRUE(openSM(false).ok());

        EXPECT_FALSE(std::filesystem::exists(
            path_ + "/segments/segment_12345.data"));
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

    // Delete segment 2's file (gap).
    std::filesystem::remove(path_ + "/segments/segment_" + std::to_string(id2) + ".data");

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
            path_ + "/segments/segment_999.data"));

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
