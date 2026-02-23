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
    Status openSM(bool create = true,
                  SegmentStorageManager::Options opts = SegmentStorageManager::Options{}) {
        manifest_ = std::make_unique<Manifest>();
        Status s = create ? manifest_->create(path_) : manifest_->open(path_);
        if (!s.ok()) return s;
        sm_ = std::make_unique<SegmentStorageManager>(*manifest_);
        return sm_->open(path_, opts);
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
        std::string orphan =
            path_ + "/segment_" + std::to_string(fake_id) + ".data";
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

// recover() with purge_untracked_files=true should delete orphan segment files.
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
        path_ + "/segment_999.data"));

    // Phase 2: reopen with purge enabled.
    {
        SegmentStorageManager::Options opts;
        opts.purge_untracked_files = true;
        ASSERT_TRUE(openSM(false, opts).ok());

        // Orphan should be gone.
        EXPECT_FALSE(std::filesystem::exists(
            path_ + "/segment_999.data"));

        // Tracked segment should still exist.
        EXPECT_EQ(sm_->segmentCount(), 1u);

        closeSM();
    }
}

// recover() with purge_untracked_files=false (default) should keep orphan files.
TEST_F(SegmentStorageManagerTest, RecoverKeepsOrphansByDefault) {
    {
        ASSERT_TRUE(openSM().ok());
        createTrackedSegment("k1", "v1");
        closeSM();
    }

    createOrphanFile(999);

    {
        ASSERT_TRUE(openSM(false).ok());

        // Orphan should still be present.
        EXPECT_TRUE(std::filesystem::exists(
            path_ + "/segment_999.data"));

        closeSM();
    }
}

// recover() should fail gracefully when a manifest-tracked segment file
// is missing from disk.
TEST_F(SegmentStorageManagerTest, RecoverMissingManifestFile) {
    {
        ASSERT_TRUE(openSM().ok());
        uint32_t id = createTrackedSegment("k1", "v1");
        closeSM();

        // Delete the segment file behind the manifest's back.
        std::filesystem::remove(path_ + "/segment_" + std::to_string(id) + ".data");
    }

    // Reopen — recover should report an error for the missing file.
    Status s = openSM(false);
    EXPECT_FALSE(s.ok());
}
