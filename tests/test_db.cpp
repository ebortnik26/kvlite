// DB tests: buffer-first reads, auto-flush, sync writes, stats, snapshot reads,
//           remove with buffer, batch operations, snapshots, iterators.
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include "internal/manifest.h"
#include <filesystem>
#include <string>
#include <vector>
#include <map>
#include <set>

namespace fs = std::filesystem;

static kvlite::ReadOptions snapOpts(const kvlite::Snapshot& snap) {
    kvlite::ReadOptions opts;
    opts.snapshot = &snap;
    return opts;
}

class DBTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "kvlite_test_db";
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
    }

    void TearDown() override {
        if (db_.isOpen()) {
            db_.close();
        }
        fs::remove_all(test_dir_);
    }

    kvlite::Status openDB(kvlite::Options opts = {}) {
        opts.create_if_missing = true;
        return db_.open(test_dir_.string(), opts);
    }

    // Helper: sync write option forces flush after the write.
    static kvlite::WriteOptions syncOpts() {
        kvlite::WriteOptions w;
        w.sync = true;
        return w;
    }

    fs::path test_dir_;
    kvlite::DB db_;
};

// ── Buffer-first reads ──────────────────────────────────────────────────────

TEST_F(DBTest, UnflushedPutIsVisibleViaGet) {
    ASSERT_TRUE(openDB().ok());

    // put without sync -> stays in WriteBuffer
    ASSERT_TRUE(db_.put("k1", "v1").ok());

    // get should find it in the buffer
    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "v1");
}

TEST_F(DBTest, UnflushedRemoveIsVisibleViaGet) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    ASSERT_TRUE(db_.remove("k1").ok());

    std::string val;
    EXPECT_TRUE(db_.get("k1", val).isNotFound());
}

TEST_F(DBTest, UnflushedOverwriteReturnsLatest) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    ASSERT_TRUE(db_.put("k1", "v2").ok());

    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "v2");
}

TEST_F(DBTest, VersionFromBuffer) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::string key, val;
    uint64_t ver;
    ASSERT_TRUE(iter->next(key, val, ver).ok());
    EXPECT_EQ(key, "k1");
    EXPECT_EQ(val, "v1");
    EXPECT_GT(ver, 0u);
}

// ── Sync write forces flush ─────────────────────────────────────────────────

TEST_F(DBTest, SyncWriteMovesDataToSegment) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1", syncOpts()).ok());

    // Data should still be readable after flush (now from segment)
    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "v1");
}

TEST_F(DBTest, SyncWriteForcesFlush) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1", syncOpts()).ok());

    // After sync write, data should be in a segment
    kvlite::DBStats stats;
    ASSERT_TRUE(db_.getStats(stats).ok());
    EXPECT_GE(stats.num_log_files, 1u);
}

TEST_F(DBTest, FlushThenPutThenGet) {
    ASSERT_TRUE(openDB().ok());

    // First batch -> segment
    ASSERT_TRUE(db_.put("k1", "v1", syncOpts()).ok());

    // Second batch -> buffer
    ASSERT_TRUE(db_.put("k2", "v2").ok());

    // Both should be readable
    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "v1");

    ASSERT_TRUE(db_.get("k2", val).ok());
    EXPECT_EQ(val, "v2");
}

TEST_F(DBTest, OverwriteAcrossFlush) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "old", syncOpts()).ok());

    // Overwrite in new buffer
    ASSERT_TRUE(db_.put("k1", "new").ok());

    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "new");
}

// ── Auto-flush ──────────────────────────────────────────────────────────────

TEST_F(DBTest, AutoFlushTriggersOnLargeWrite) {
    kvlite::Options opts;
    opts.write_buffer_size = 256;  // tiny buffer -> auto-flush quickly
    ASSERT_TRUE(openDB(opts).ok());

    // Write enough data to exceed the tiny buffer
    std::string big_val(128, 'x');
    for (int i = 0; i < 10; ++i) {
        std::string key = "k" + std::to_string(i);
        ASSERT_TRUE(db_.put(key, big_val).ok());
    }

    // Verify stats show segments were created
    kvlite::DBStats stats;
    ASSERT_TRUE(db_.getStats(stats).ok());
    EXPECT_GT(stats.num_log_files, 0u);

    // All data should be readable
    for (int i = 0; i < 10; ++i) {
        std::string key = "k" + std::to_string(i);
        std::string val;
        ASSERT_TRUE(db_.get(key, val).ok());
        EXPECT_EQ(val, big_val);
    }
}

// ── Snapshot-based point-in-time reads ───────────────────────────────────────

TEST_F(DBTest, SnapshotReadFromBuffer) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    uint64_t ver1 = db_.getLatestVersion();
    kvlite::Snapshot snap1 = db_.createSnapshot();

    ASSERT_TRUE(db_.put("k1", "v2").ok());

    // Snapshot taken after v1 should see v1
    std::string val;
    ASSERT_TRUE(db_.get("k1", val, snapOpts(snap1)).ok());
    EXPECT_EQ(val, "v1");

    // Verify version via snapshot iterator
    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter, snapOpts(snap1)).ok());
    std::string key;
    uint64_t entry_ver;
    ASSERT_TRUE(iter->next(key, val, entry_ver).ok());
    EXPECT_EQ(entry_ver, ver1);

    db_.releaseSnapshot(snap1);
}

TEST_F(DBTest, SnapshotReadFromSegment) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    kvlite::Snapshot snap1 = db_.createSnapshot();
    ASSERT_TRUE(db_.put("k1", "v1_flush", syncOpts()).ok());

    ASSERT_TRUE(db_.put("k1", "v2", syncOpts()).ok());

    // Snapshot taken after v1 should still see v1
    std::string val;
    ASSERT_TRUE(db_.get("k1", val, snapOpts(snap1)).ok());
    EXPECT_EQ(val, "v1");

    db_.releaseSnapshot(snap1);
}

// ── Remove ──────────────────────────────────────────────────────────────────

TEST_F(DBTest, RemoveInBufferThenFlush) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    ASSERT_TRUE(db_.remove("k1", syncOpts()).ok());

    std::string val;
    EXPECT_TRUE(db_.get("k1", val).isNotFound());
}

TEST_F(DBTest, RemoveAfterFlush) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1", syncOpts()).ok());

    ASSERT_TRUE(db_.remove("k1").ok());

    std::string val;
    EXPECT_TRUE(db_.get("k1", val).isNotFound());
}

// ── Batch operations ────────────────────────────────────────────────────────

TEST_F(DBTest, WriteBatchInBuffer) {
    ASSERT_TRUE(openDB().ok());

    kvlite::WriteBatch batch;
    batch.put("k1", "v1");
    batch.put("k2", "v2");

    ASSERT_TRUE(db_.write(batch).ok());

    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "v1");
    ASSERT_TRUE(db_.get("k2", val).ok());
    EXPECT_EQ(val, "v2");
}

TEST_F(DBTest, WriteBatchThenFlush) {
    ASSERT_TRUE(openDB().ok());

    kvlite::WriteBatch batch;
    batch.put("k1", "v1");
    batch.put("k2", "v2");
    ASSERT_TRUE(db_.write(batch, syncOpts()).ok());

    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "v1");
    ASSERT_TRUE(db_.get("k2", val).ok());
    EXPECT_EQ(val, "v2");
}

TEST_F(DBTest, ReadBatch) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    ASSERT_TRUE(db_.put("k2", "v2", syncOpts()).ok());
    ASSERT_TRUE(db_.put("k3", "v3").ok());  // in buffer

    kvlite::ReadBatch rbatch;
    rbatch.get("k1");
    rbatch.get("k2");
    rbatch.get("k3");
    rbatch.get("missing");
    ASSERT_TRUE(db_.read(rbatch).ok());

    const auto& results = rbatch.results();
    ASSERT_EQ(results.size(), 4u);
    EXPECT_TRUE(results[0].status.ok());
    EXPECT_EQ(results[0].value, "v1");
    EXPECT_TRUE(results[1].status.ok());
    EXPECT_EQ(results[1].value, "v2");
    EXPECT_TRUE(results[2].status.ok());
    EXPECT_EQ(results[2].value, "v3");
    EXPECT_TRUE(results[3].status.isNotFound());
}

// ── Stats ───────────────────────────────────────────────────────────────────

TEST_F(DBTest, StatsAfterFlush) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1", syncOpts()).ok());

    kvlite::DBStats stats;
    ASSERT_TRUE(db_.getStats(stats).ok());
    EXPECT_EQ(stats.num_log_files, 1u);
    EXPECT_GT(stats.total_log_size, 0u);
    EXPECT_GT(stats.current_version, 0u);
}

TEST_F(DBTest, StatsMultipleSegments) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1", syncOpts()).ok());
    ASSERT_TRUE(db_.put("k2", "v2", syncOpts()).ok());

    kvlite::DBStats stats;
    ASSERT_TRUE(db_.getStats(stats).ok());
    EXPECT_EQ(stats.num_log_files, 2u);
    EXPECT_GT(stats.total_log_size, 0u);
}

// ── Exists ──────────────────────────────────────────────────────────────────

TEST_F(DBTest, ExistsInBuffer) {
    ASSERT_TRUE(openDB().ok());

    bool e;
    ASSERT_TRUE(db_.exists("k1", e).ok());
    EXPECT_FALSE(e);

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    ASSERT_TRUE(db_.exists("k1", e).ok());
    EXPECT_TRUE(e);
}

TEST_F(DBTest, ExistsAfterRemove) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    ASSERT_TRUE(db_.remove("k1").ok());

    bool e;
    ASSERT_TRUE(db_.exists("k1", e).ok());
    EXPECT_FALSE(e);
}

// ── Close flushes buffer ────────────────────────────────────────────────────

TEST_F(DBTest, CloseFlushesBuffer) {
    ASSERT_TRUE(openDB().ok());
    ASSERT_TRUE(db_.put("k1", "v1").ok());

    // Verify data is readable before close (from buffer)
    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "v1");

    // Close should flush buffer to segment (no crash)
    ASSERT_TRUE(db_.close().ok());
    EXPECT_FALSE(db_.isOpen());
}

// ── Snapshots ───────────────────────────────────────────────────────────────

TEST_F(DBTest, SnapshotReadsExistingData) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1", syncOpts()).ok());

    kvlite::Snapshot snap = db_.createSnapshot();
    EXPECT_GT(snap.version(), 0u);

    // Snapshot should see existing data
    std::string val;
    ASSERT_TRUE(db_.get("k1", val, snapOpts(snap)).ok());
    EXPECT_EQ(val, "v1");

    db_.releaseSnapshot(snap);
}

TEST_F(DBTest, SnapshotReadsMultipleKeys) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("k1", "v1").ok());
    ASSERT_TRUE(db_.put("k2", "v2", syncOpts()).ok());

    kvlite::Snapshot snap = db_.createSnapshot();

    // Snapshot should see all flushed data
    std::string val;
    ASSERT_TRUE(db_.get("k1", val, snapOpts(snap)).ok());
    EXPECT_EQ(val, "v1");
    ASSERT_TRUE(db_.get("k2", val, snapOpts(snap)).ok());
    EXPECT_EQ(val, "v2");

    // Non-existent key
    EXPECT_TRUE(db_.get("missing", val, snapOpts(snap)).isNotFound());

    db_.releaseSnapshot(snap);
}

// ── Iterator ────────────────────────────────────────────────────────────────

TEST_F(DBTest, IteratorOverFlushedData) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("a", "1").ok());
    ASSERT_TRUE(db_.put("b", "2").ok());
    ASSERT_TRUE(db_.put("c", "3", syncOpts()).ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::set<std::string> keys;
    std::string key, val;
    while (iter->next(key, val).ok()) {
        keys.insert(key);
    }

    EXPECT_EQ(keys.size(), 3u);
    EXPECT_TRUE(keys.count("a"));
    EXPECT_TRUE(keys.count("b"));
    EXPECT_TRUE(keys.count("c"));
}

TEST_F(DBTest, MultiSegmentGetWorks) {
    ASSERT_TRUE(openDB().ok());

    // Put to segment 1
    ASSERT_TRUE(db_.put("a", "1", syncOpts()).ok());

    // Put to segment 2
    ASSERT_TRUE(db_.put("b", "2", syncOpts()).ok());

    // Put to segment 3
    ASSERT_TRUE(db_.put("c", "3", syncOpts()).ok());

    kvlite::DBStats stats;
    ASSERT_TRUE(db_.getStats(stats).ok());
    EXPECT_EQ(stats.num_log_files, 3u);

    // All keys should be readable across segments
    std::string val;
    ASSERT_TRUE(db_.get("a", val).ok());
    EXPECT_EQ(val, "1");
    ASSERT_TRUE(db_.get("b", val).ok());
    EXPECT_EQ(val, "2");
    ASSERT_TRUE(db_.get("c", val).ok());
    EXPECT_EQ(val, "3");
}

TEST_F(DBTest, IteratorSnapshotVersion) {
    ASSERT_TRUE(openDB().ok());

    ASSERT_TRUE(db_.put("a", "1").ok());
    ASSERT_TRUE(db_.put("b", "2", syncOpts()).ok());

    std::unique_ptr<kvlite::DB::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    // Iterator should have a valid snapshot
    EXPECT_GT(iter->snapshot().version(), 0u);
}

// ── Error paths ─────────────────────────────────────────────────────────────

TEST_F(DBTest, OperationsOnClosedDBFail) {
    ASSERT_TRUE(openDB().ok());
    ASSERT_TRUE(db_.close().ok());

    std::string val;
    EXPECT_FALSE(db_.put("k", "v").ok());
    EXPECT_FALSE(db_.get("k", val).ok());
    EXPECT_FALSE(db_.remove("k").ok());
}

TEST_F(DBTest, DoubleOpenFails) {
    ASSERT_TRUE(openDB().ok());
    kvlite::Options opts;
    opts.create_if_missing = true;
    EXPECT_FALSE(db_.open(test_dir_.string(), opts).ok());
}

// ── Recovery ────────────────────────────────────────────────────────────────

TEST_F(DBTest, RecoverAfterFlush) {
    // Open, put with sync (flush), close.
    ASSERT_TRUE(openDB().ok());
    ASSERT_TRUE(db_.put("k1", "v1").ok());
    ASSERT_TRUE(db_.put("k2", "v2", syncOpts()).ok());
    ASSERT_TRUE(db_.put("k3", "v3").ok());
    // close() flushes the write buffer, so k3 will be in a segment too.
    ASSERT_TRUE(db_.close().ok());

    // Reopen and verify data survives.
    ASSERT_TRUE(openDB().ok());
    std::string val;
    ASSERT_TRUE(db_.get("k1", val).ok());
    EXPECT_EQ(val, "v1");
    ASSERT_TRUE(db_.get("k2", val).ok());
    EXPECT_EQ(val, "v2");
    ASSERT_TRUE(db_.get("k3", val).ok());
    EXPECT_EQ(val, "v3");
}

TEST_F(DBTest, RecoverMultipleFlushes) {
    ASSERT_TRUE(openDB().ok());
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(db_.put("key" + std::to_string(i),
                            "val" + std::to_string(i), syncOpts()).ok());
    }
    ASSERT_TRUE(db_.close().ok());

    // Reopen and verify all data.
    ASSERT_TRUE(openDB().ok());
    for (int i = 0; i < 5; ++i) {
        std::string val;
        ASSERT_TRUE(db_.get("key" + std::to_string(i), val).ok());
        EXPECT_EQ(val, "val" + std::to_string(i));
    }
}

// ── Clean close flag ────────────────────────────────────────────────────────

TEST_F(DBTest, CleanCloseFlag) {
    ASSERT_TRUE(openDB().ok());

    // Before any mutation, clean_close should be absent or "1" (from prior close).
    {
        kvlite::internal::Manifest m;
        ASSERT_TRUE(m.open(test_dir_.string()).ok());
        std::string val;
        // Fresh DB: flag absent is fine; if present it should be "1".
        if (m.get("clean_close", val)) {
            EXPECT_EQ(val, "1");
        }
        m.close();
    }

    // First mutation should set clean_close=0.
    ASSERT_TRUE(db_.put("k1", "v1", syncOpts()).ok());

    {
        kvlite::internal::Manifest m;
        ASSERT_TRUE(m.open(test_dir_.string()).ok());
        std::string val;
        ASSERT_TRUE(m.get("clean_close", val));
        EXPECT_EQ(val, "0");
        m.close();
    }

    // Close should set clean_close=1.
    ASSERT_TRUE(db_.close().ok());

    {
        kvlite::internal::Manifest m;
        ASSERT_TRUE(m.open(test_dir_.string()).ok());
        std::string val;
        ASSERT_TRUE(m.get("clean_close", val));
        EXPECT_EQ(val, "1");
        m.close();
    }
}
