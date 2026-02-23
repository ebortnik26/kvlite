#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

#include "internal/version_manager.h"
#include "internal/manifest.h"

using namespace kvlite::internal;
using kvlite::Status;

class VersionManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        path_ = "/tmp/vm_test_" + std::to_string(getpid());
        std::filesystem::remove_all(path_);
    }
    void TearDown() override {
        std::filesystem::remove_all(path_);
    }

    Status openVM(bool create = true,
                   VersionManager::Options opts = {}) {
        manifest_ = std::make_unique<Manifest>();
        Status s = create ? manifest_->create(path_) : manifest_->open(path_);
        if (!s.ok()) return s;
        vm_ = std::make_unique<VersionManager>(*manifest_);
        s = vm_->open(opts);
        if (!s.ok()) return s;
        return vm_->recover();
    }

    void closeVM() {
        vm_->close();
        manifest_->close();
    }

    std::string path_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<VersionManager> vm_;
};

TEST_F(VersionManagerTest, OpenClose) {
    ASSERT_TRUE(openVM().ok());
    ASSERT_TRUE(vm_->isOpen());
    ASSERT_TRUE(vm_->close().ok());
    ASSERT_FALSE(vm_->isOpen());
    manifest_->close();
}

TEST_F(VersionManagerTest, AllocateVersion) {
    ASSERT_TRUE(openVM().ok());

    EXPECT_EQ(vm_->latestVersion(), 0u);
    EXPECT_EQ(vm_->allocateVersion(), 1u);
    EXPECT_EQ(vm_->allocateVersion(), 2u);
    EXPECT_EQ(vm_->allocateVersion(), 3u);
    EXPECT_EQ(vm_->latestVersion(), 3u);

    closeVM();
}

TEST_F(VersionManagerTest, PersistAndRecover) {
    VersionManager::Options opts;
    opts.block_size = 4;

    {
        ASSERT_TRUE(openVM(true, opts).ok());
        for (int i = 0; i < 10; ++i) {
            vm_->allocateVersion();
        }
        closeVM();
    }

    {
        Manifest m2;
        ASSERT_TRUE(m2.open(path_).ok());  // reopen existing
        VersionManager vm2(m2);
        ASSERT_TRUE(vm2.open(opts).ok());
        ASSERT_TRUE(vm2.recover().ok());
        // After recovery, counter starts from persisted value.
        // Since close() persists final counter (10), we resume from there.
        EXPECT_EQ(vm2.latestVersion(), 10u);
        // Next allocated version should be > 10.
        uint64_t next = vm2.allocateVersion();
        EXPECT_EQ(next, 11u);
        ASSERT_TRUE(vm2.close().ok());
        m2.close();
    }
}

TEST_F(VersionManagerTest, BlockBoundaryPersistence) {
    VersionManager::Options opts;
    opts.block_size = 4;

    {
        ASSERT_TRUE(openVM(true, opts).ok());

        // Allocate version 1: exceeds persisted_counter_=0, should persist 4.
        EXPECT_EQ(vm_->allocateVersion(), 1u);

        // Check manifest: should contain "4".
        std::string val;
        ASSERT_TRUE(manifest_->get("next_version_id", val));
        EXPECT_EQ(val, "4");

        // Allocate versions 2, 3, 4: all <= 4, no new persist.
        EXPECT_EQ(vm_->allocateVersion(), 2u);
        EXPECT_EQ(vm_->allocateVersion(), 3u);
        EXPECT_EQ(vm_->allocateVersion(), 4u);
        ASSERT_TRUE(manifest_->get("next_version_id", val));
        EXPECT_EQ(val, "4");

        // Allocate version 5: exceeds 4, should persist 8.
        EXPECT_EQ(vm_->allocateVersion(), 5u);
        ASSERT_TRUE(manifest_->get("next_version_id", val));
        EXPECT_EQ(val, "8");

        // Simulate crash: do NOT call close() — just close the manifest.
        manifest_->close();
    }

    // Recover without close: should resume from persisted value (8).
    {
        Manifest m2;
        ASSERT_TRUE(m2.open(path_).ok());
        VersionManager vm2(m2);
        ASSERT_TRUE(vm2.open(opts).ok());
        ASSERT_TRUE(vm2.recover().ok());

        // Recovered from 8 (lost versions 6 and 7 — at most block_size-1 waste).
        EXPECT_EQ(vm2.latestVersion(), 8u);
        EXPECT_EQ(vm2.allocateVersion(), 9u);

        ASSERT_TRUE(vm2.close().ok());
        m2.close();
    }
}

TEST_F(VersionManagerTest, CreateAndReleaseSnapshot) {
    ASSERT_TRUE(openVM().ok());

    vm_->allocateVersion();  // v1
    vm_->allocateVersion();  // v2

    uint64_t snap = vm_->createSnapshot();
    EXPECT_EQ(snap, 2u);
    EXPECT_EQ(vm_->activeSnapshotCount(), 1u);
    EXPECT_EQ(vm_->oldestSnapshotVersion(), 2u);

    vm_->allocateVersion();  // v3
    // Oldest snapshot is still v2 even though current is v3.
    EXPECT_EQ(vm_->oldestSnapshotVersion(), 2u);

    vm_->releaseSnapshot(snap);
    EXPECT_EQ(vm_->activeSnapshotCount(), 0u);
    // No snapshots → oldest returns latestVersion.
    EXPECT_EQ(vm_->oldestSnapshotVersion(), 3u);

    closeVM();
}

TEST_F(VersionManagerTest, OldestSnapshotMultiple) {
    ASSERT_TRUE(openVM().ok());

    vm_->allocateVersion();  // v1
    uint64_t s1 = vm_->createSnapshot();  // snap at v1

    vm_->allocateVersion();  // v2
    uint64_t s2 = vm_->createSnapshot();  // snap at v2

    vm_->allocateVersion();  // v3

    EXPECT_EQ(vm_->activeSnapshotCount(), 2u);
    EXPECT_EQ(vm_->oldestSnapshotVersion(), s1);

    vm_->releaseSnapshot(s1);
    EXPECT_EQ(vm_->oldestSnapshotVersion(), s2);

    vm_->releaseSnapshot(s2);
    EXPECT_EQ(vm_->oldestSnapshotVersion(), vm_->latestVersion());

    closeVM();
}

TEST_F(VersionManagerTest, SnapshotVersionsEmpty) {
    ASSERT_TRUE(openVM().ok());

    // No snapshots, latestVersion = 0.
    auto sv = vm_->snapshotVersions();
    ASSERT_EQ(sv.size(), 1u);
    EXPECT_EQ(sv[0], 0u);  // just latestVersion

    closeVM();
}

TEST_F(VersionManagerTest, SnapshotVersionsWithSnapshots) {
    ASSERT_TRUE(openVM().ok());

    vm_->allocateVersion();  // v1
    uint64_t s1 = vm_->createSnapshot();

    vm_->allocateVersion();  // v2
    vm_->allocateVersion();  // v3
    uint64_t s2 = vm_->createSnapshot();

    vm_->allocateVersion();  // v4

    auto sv = vm_->snapshotVersions();
    // Should contain: s1, s2, latestVersion(4), sorted ascending.
    ASSERT_EQ(sv.size(), 3u);
    EXPECT_EQ(sv[0], s1);   // 1
    EXPECT_EQ(sv[1], s2);   // 3
    EXPECT_EQ(sv[2], 4u);   // latestVersion

    // Verify sorted ascending.
    EXPECT_TRUE(std::is_sorted(sv.begin(), sv.end()));

    vm_->releaseSnapshot(s1);
    vm_->releaseSnapshot(s2);
    closeVM();
}

TEST_F(VersionManagerTest, SnapshotVersionsLatestEqualsSnapshot) {
    ASSERT_TRUE(openVM().ok());

    vm_->allocateVersion();  // v1
    vm_->allocateVersion();  // v2
    uint64_t s = vm_->createSnapshot();  // snap at v2

    // latestVersion == snapshot version → no duplicate.
    auto sv = vm_->snapshotVersions();
    ASSERT_EQ(sv.size(), 1u);
    EXPECT_EQ(sv[0], s);  // 2

    vm_->releaseSnapshot(s);
    closeVM();
}

TEST_F(VersionManagerTest, DoubleOpenFails) {
    ASSERT_TRUE(openVM().ok());
    Status s = vm_->open({});
    EXPECT_FALSE(s.ok());
    closeVM();
}
