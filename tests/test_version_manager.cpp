#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

#include "internal/version_manager.h"

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
    std::string path_;
};

TEST_F(VersionManagerTest, OpenClose) {
    VersionManager vm;
    ASSERT_FALSE(vm.isOpen());
    ASSERT_TRUE(vm.open(path_, {}).ok());
    ASSERT_TRUE(vm.isOpen());
    ASSERT_TRUE(vm.close().ok());
    ASSERT_FALSE(vm.isOpen());
}

TEST_F(VersionManagerTest, AllocateVersion) {
    VersionManager vm;
    ASSERT_TRUE(vm.open(path_, {}).ok());

    EXPECT_EQ(vm.latestVersion(), 0u);
    EXPECT_EQ(vm.allocateVersion(), 1u);
    EXPECT_EQ(vm.allocateVersion(), 2u);
    EXPECT_EQ(vm.allocateVersion(), 3u);
    EXPECT_EQ(vm.latestVersion(), 3u);

    ASSERT_TRUE(vm.close().ok());
}

TEST_F(VersionManagerTest, PersistAndRecover) {
    {
        VersionManager vm;
        ASSERT_TRUE(vm.open(path_, {}).ok());
        for (int i = 0; i < 10; ++i) {
            vm.allocateVersion();
        }
        ASSERT_TRUE(vm.close().ok());
    }

    {
        VersionManager vm;
        ASSERT_TRUE(vm.open(path_, {}).ok());
        // After recovery, counter starts from persisted value.
        // Since close() persists final counter (10), we resume from there.
        EXPECT_GE(vm.latestVersion(), 10u);
        // Next allocated version should be > 10.
        uint64_t next = vm.allocateVersion();
        EXPECT_GT(next, 10u);
        ASSERT_TRUE(vm.close().ok());
    }
}

TEST_F(VersionManagerTest, CreateAndReleaseSnapshot) {
    VersionManager vm;
    ASSERT_TRUE(vm.open(path_, {}).ok());

    vm.allocateVersion();  // v1
    vm.allocateVersion();  // v2

    uint64_t snap = vm.createSnapshot();
    EXPECT_EQ(snap, 2u);
    EXPECT_EQ(vm.activeSnapshotCount(), 1u);
    EXPECT_EQ(vm.oldestSnapshotVersion(), 2u);

    vm.allocateVersion();  // v3
    // Oldest snapshot is still v2 even though current is v3.
    EXPECT_EQ(vm.oldestSnapshotVersion(), 2u);

    vm.releaseSnapshot(snap);
    EXPECT_EQ(vm.activeSnapshotCount(), 0u);
    // No snapshots → oldest returns latestVersion.
    EXPECT_EQ(vm.oldestSnapshotVersion(), 3u);

    ASSERT_TRUE(vm.close().ok());
}

TEST_F(VersionManagerTest, OldestSnapshotMultiple) {
    VersionManager vm;
    ASSERT_TRUE(vm.open(path_, {}).ok());

    vm.allocateVersion();  // v1
    uint64_t s1 = vm.createSnapshot();  // snap at v1

    vm.allocateVersion();  // v2
    uint64_t s2 = vm.createSnapshot();  // snap at v2

    vm.allocateVersion();  // v3

    EXPECT_EQ(vm.activeSnapshotCount(), 2u);
    EXPECT_EQ(vm.oldestSnapshotVersion(), s1);

    vm.releaseSnapshot(s1);
    EXPECT_EQ(vm.oldestSnapshotVersion(), s2);

    vm.releaseSnapshot(s2);
    EXPECT_EQ(vm.oldestSnapshotVersion(), vm.latestVersion());

    ASSERT_TRUE(vm.close().ok());
}

TEST_F(VersionManagerTest, SnapshotVersionsEmpty) {
    VersionManager vm;
    ASSERT_TRUE(vm.open(path_, {}).ok());

    // No snapshots, latestVersion = 0.
    auto sv = vm.snapshotVersions();
    ASSERT_EQ(sv.size(), 1u);
    EXPECT_EQ(sv[0], 0u);  // just latestVersion

    ASSERT_TRUE(vm.close().ok());
}

TEST_F(VersionManagerTest, SnapshotVersionsWithSnapshots) {
    VersionManager vm;
    ASSERT_TRUE(vm.open(path_, {}).ok());

    vm.allocateVersion();  // v1
    uint64_t s1 = vm.createSnapshot();

    vm.allocateVersion();  // v2
    vm.allocateVersion();  // v3
    uint64_t s2 = vm.createSnapshot();

    vm.allocateVersion();  // v4

    auto sv = vm.snapshotVersions();
    // Should contain: s1, s2, latestVersion(4), sorted ascending.
    ASSERT_EQ(sv.size(), 3u);
    EXPECT_EQ(sv[0], s1);   // 1
    EXPECT_EQ(sv[1], s2);   // 3
    EXPECT_EQ(sv[2], 4u);   // latestVersion

    // Verify sorted ascending.
    EXPECT_TRUE(std::is_sorted(sv.begin(), sv.end()));

    vm.releaseSnapshot(s1);
    vm.releaseSnapshot(s2);
    ASSERT_TRUE(vm.close().ok());
}

TEST_F(VersionManagerTest, SnapshotVersionsLatestEqualsSnapshot) {
    VersionManager vm;
    ASSERT_TRUE(vm.open(path_, {}).ok());

    vm.allocateVersion();  // v1
    vm.allocateVersion();  // v2
    uint64_t s = vm.createSnapshot();  // snap at v2

    // latestVersion == snapshot version → no duplicate.
    auto sv = vm.snapshotVersions();
    ASSERT_EQ(sv.size(), 1u);
    EXPECT_EQ(sv[0], s);  // 2

    vm.releaseSnapshot(s);
    ASSERT_TRUE(vm.close().ok());
}

TEST_F(VersionManagerTest, DoubleOpenFails) {
    VersionManager vm;
    ASSERT_TRUE(vm.open(path_, {}).ok());
    Status s = vm.open(path_, {});
    EXPECT_FALSE(s.ok());
    ASSERT_TRUE(vm.close().ok());
}
