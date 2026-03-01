// GC daemon integration tests
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include <filesystem>
#include <string>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

class GCDaemonTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "kvlite_test_gc_daemon";
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
    }

    void TearDown() override {
        if (db_.isOpen()) {
            db_.close();
        }
        fs::remove_all(test_dir_);
    }

    static kvlite::WriteOptions syncOpts() {
        kvlite::WriteOptions w;
        w.sync = true;
        return w;
    }

    fs::path test_dir_;
    kvlite::DB db_;
};

TEST_F(GCDaemonTest, AutoGCCompactsSegments) {
    kvlite::Options opts;
    opts.create_if_missing = true;
    opts.write_buffer_size = 256;   // tiny buffer -> many segments
    opts.gc_threshold = 0.1;        // very low threshold to trigger easily
    opts.gc_interval_sec = 1;       // fast wake-up
    opts.gc_max_segments = 10;
    ASSERT_TRUE(db_.open(test_dir_.string(), opts).ok());

    // Write multiple versions of the same keys to create dead entries.
    // Each sync write forces a flush -> new segment.
    const int kNumKeys = 20;
    const int kVersions = 3;
    for (int v = 0; v < kVersions; ++v) {
        for (int i = 0; i < kNumKeys; ++i) {
            std::string key = "key" + std::to_string(i);
            std::string val = "v" + std::to_string(v) + "_" + std::to_string(i);
            ASSERT_TRUE(db_.put(key, val, syncOpts()).ok());
        }
    }

    // Record initial segment count.
    kvlite::DBStats stats_before;
    ASSERT_TRUE(db_.getStats(stats_before).ok());
    ASSERT_GT(stats_before.num_log_files, 2u);

    // Wait for GC daemon to fire (up to 5 seconds).
    uint64_t segments_after = stats_before.num_log_files;
    for (int attempt = 0; attempt < 10; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        kvlite::DBStats stats;
        ASSERT_TRUE(db_.getStats(stats).ok());
        segments_after = stats.num_log_files;
        if (segments_after < stats_before.num_log_files) break;
    }

    EXPECT_LT(segments_after, stats_before.num_log_files)
        << "GC daemon should have reduced segment count";

    // Verify all latest values are still readable.
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string expected = "v" + std::to_string(kVersions - 1) + "_" +
                               std::to_string(i);
        std::string val;
        ASSERT_TRUE(db_.get(key, val).ok()) << "key=" << key;
        EXPECT_EQ(val, expected) << "key=" << key;
    }
}

TEST_F(GCDaemonTest, ManualPolicyNoAutoGC) {
    kvlite::Options opts;
    opts.create_if_missing = true;
    opts.write_buffer_size = 256;
    opts.gc_policy = kvlite::GCPolicy::MANUAL;
    opts.gc_threshold = 0.0;        // would always trigger if daemon ran
    opts.gc_interval_sec = 1;
    ASSERT_TRUE(db_.open(test_dir_.string(), opts).ok());

    // Create multi-version data across segments.
    for (int v = 0; v < 3; ++v) {
        for (int i = 0; i < 10; ++i) {
            ASSERT_TRUE(db_.put("k" + std::to_string(i),
                                "v" + std::to_string(v), syncOpts()).ok());
        }
    }

    kvlite::DBStats stats_before;
    ASSERT_TRUE(db_.getStats(stats_before).ok());

    // Wait long enough that the daemon would have fired if it were running.
    std::this_thread::sleep_for(std::chrono::seconds(3));

    kvlite::DBStats stats_after;
    ASSERT_TRUE(db_.getStats(stats_after).ok());
    EXPECT_EQ(stats_after.num_log_files, stats_before.num_log_files)
        << "MANUAL policy should prevent auto-GC";
}

TEST_F(GCDaemonTest, DisabledByZeroInterval) {
    kvlite::Options opts;
    opts.create_if_missing = true;
    opts.write_buffer_size = 256;
    opts.gc_threshold = 0.0;
    opts.gc_interval_sec = 0;       // explicitly disabled
    ASSERT_TRUE(db_.open(test_dir_.string(), opts).ok());

    for (int v = 0; v < 3; ++v) {
        for (int i = 0; i < 10; ++i) {
            ASSERT_TRUE(db_.put("k" + std::to_string(i),
                                "v" + std::to_string(v), syncOpts()).ok());
        }
    }

    kvlite::DBStats stats_before;
    ASSERT_TRUE(db_.getStats(stats_before).ok());

    std::this_thread::sleep_for(std::chrono::seconds(2));

    kvlite::DBStats stats_after;
    ASSERT_TRUE(db_.getStats(stats_after).ok());
    EXPECT_EQ(stats_after.num_log_files, stats_before.num_log_files)
        << "gc_interval_sec=0 should disable the GC daemon";
}
