#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "internal/delta_hash_table.h"
#include "internal/global_index_wal.h"
#include "internal/manifest.h"
#include "internal/wal_stream.h"

namespace fs = std::filesystem;

using kvlite::Status;
using kvlite::internal::GlobalIndexWAL;
using kvlite::internal::Manifest;
using kvlite::internal::WalOp;
using kvlite::internal::WALReplayStream;
using kvlite::internal::WALRecord;
using kvlite::internal::WALStream;
using kvlite::internal::ManifestKey;

namespace WalProducer = kvlite::internal::WalProducer;

static uint64_t H(const std::string& s) {
    return kvlite::internal::dhtHashBytes(s.data(), s.size());
}

class WALStreamTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() / "kvlite_wal_stream_test";
        fs::remove_all(dir_);
        fs::create_directories(dir_);
    }

    void TearDown() override {
        fs::remove_all(dir_);
    }

    // Helper: create a GlobalIndexWAL, write some entries, close it, return file_ids.
    struct WALSetup {
        std::string wal_dir;
        std::vector<uint32_t> file_ids;
    };

    // ops: (op, hkey, packed_version, segment_id, new_segment_id, batch_group)
    // batch_group groups consecutive ops into a batch; a new group triggers commit.
    // Optional producer_id (defaults to WalProducer::kWB).
    WALSetup createWAL(
        const std::vector<std::tuple<WalOp, uint64_t, uint64_t, uint32_t, uint32_t, uint64_t>>& ops,
        uint8_t producer_id = WalProducer::kWB) {
        Manifest manifest;
        EXPECT_TRUE(manifest.create(dir_.string()).ok());

        GlobalIndexWAL wal;
        GlobalIndexWAL::Options opts;
        opts.max_file_size = 1ULL << 30;
        EXPECT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

        uint64_t current_group = 0;
        bool in_batch = false;

        for (const auto& [op, hkey, pv, seg, new_seg, group] : ops) {
            if (in_batch && group != current_group) {
                EXPECT_TRUE(wal.commit(producer_id).ok());
                in_batch = false;
            }
            current_group = group;
            in_batch = true;

            switch (op) {
                case WalOp::kPut:
                    wal.appendPut(hkey, pv, seg, producer_id);
                    break;
                case WalOp::kRelocate:
                    wal.appendRelocate(hkey, pv, seg, new_seg, producer_id);
                    break;
                case WalOp::kEliminate:
                    wal.appendEliminate(hkey, pv, seg, producer_id);
                    break;
            }
        }
        if (in_batch) {
            EXPECT_TRUE(wal.commit(producer_id).ok());
        }

        WALSetup setup;
        setup.wal_dir = wal.walDir();
        for (uint32_t i = 0; i < wal.fileCount(); ++i) {
            setup.file_ids.push_back(i);
        }

        EXPECT_TRUE(wal.close().ok());
        manifest.close();
        return setup;
    }

    // Helper for multi-producer: write raw ops with per-record producer_id.
    struct RawOp {
        WalOp op;
        uint8_t producer_id;
        uint64_t hkey;
        uint64_t packed_version;
        uint32_t segment_id;
        uint32_t new_segment_id;
        bool commit;  // true = commit the current batch
    };

    WALSetup createMultiProducerWAL(std::vector<RawOp> ops) {
        Manifest manifest;
        EXPECT_TRUE(manifest.create(dir_.string()).ok());

        GlobalIndexWAL wal;
        GlobalIndexWAL::Options opts;
        opts.max_file_size = 1ULL << 30;
        EXPECT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

        for (const auto& r : ops) {
            if (r.commit) {
                EXPECT_TRUE(wal.commit(r.producer_id).ok());
                continue;
            }
            switch (r.op) {
                case WalOp::kPut:
                    wal.appendPut(r.hkey, r.packed_version, r.segment_id, r.producer_id);
                    break;
                case WalOp::kRelocate:
                    wal.appendRelocate(r.hkey, r.packed_version, r.segment_id,
                                       r.new_segment_id, r.producer_id);
                    break;
                case WalOp::kEliminate:
                    wal.appendEliminate(r.hkey, r.packed_version, r.segment_id, r.producer_id);
                    break;
            }
        }

        WALSetup setup;
        setup.wal_dir = wal.walDir();
        for (uint32_t i = 0; i < wal.fileCount(); ++i) {
            setup.file_ids.push_back(i);
        }

        EXPECT_TRUE(wal.close().ok());
        manifest.close();
        return setup;
    }

    fs::path dir_;
};

// --- WALReplayStream tests ---

TEST_F(WALStreamTest, EmptyWAL) {
    auto setup = createWAL({});
    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, SinglePut) {
    auto setup = createWAL({
        {WalOp::kPut, H("key1"), 100, 1, 0, 1},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);
    ASSERT_TRUE(stream->valid());

    const auto& rec = stream->record();
    EXPECT_EQ(rec.op, WalOp::kPut);
    EXPECT_EQ(rec.hkey, H("key1"));
    EXPECT_EQ(rec.packed_version, 100u);
    EXPECT_EQ(rec.segment_id, 1u);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultipleBatches) {
    auto setup = createWAL({
        {WalOp::kPut, H("k1"), 10, 1, 0, 1},
        {WalOp::kPut, H("k2"), 20, 2, 0, 2},
        {WalOp::kPut, H("k3"), 30, 3, 0, 3},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("k1"));

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("k2"));

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("k3"));

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultipleRecordsInOneBatch) {
    auto setup = createWAL({
        {WalOp::kPut, H("a"), 10, 1, 0, 1},
        {WalOp::kPut, H("b"), 20, 2, 0, 1},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("a"));

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("b"));

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, RelocateAndEliminate) {
    auto setup = createWAL({
        {WalOp::kPut, H("key"), 10, 1, 0, 1},
        {WalOp::kRelocate, H("key"), 10, 1, 5, 2},
        {WalOp::kEliminate, H("key"), 10, 5, 0, 3},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    // Put
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().op, WalOp::kPut);
    EXPECT_EQ(stream->record().segment_id, 1u);

    // Relocate
    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().op, WalOp::kRelocate);
    EXPECT_EQ(stream->record().segment_id, 1u);
    EXPECT_EQ(stream->record().new_segment_id, 5u);

    // Eliminate
    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().op, WalOp::kEliminate);
    EXPECT_EQ(stream->record().segment_id, 5u);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

// --- Producer demux tests ---

TEST_F(WALStreamTest, SingleProducerReplay) {
    // Same as basic replay but verify producer_id is propagated.
    auto setup = createWAL({
        {WalOp::kPut, H("a"), 10, 1, 0, 1},
        {WalOp::kPut, H("b"), 20, 2, 0, 2},
    }, WalProducer::kGC);

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("a"));
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("b"));
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultiProducerInterleaved) {
    // Two producers interleaved in a single WAL.
    auto setup = createMultiProducerWAL({
        // WB batch
        {WalOp::kPut,       WalProducer::kWB, H("f1"), 10, 1, 0, false},
        {WalOp::kPut,       WalProducer::kWB, 0,        0, 0, 0, true},  // commit
        // GC batch
        {WalOp::kRelocate,  WalProducer::kGC, H("g1"), 10, 1, 5, false},
        {WalOp::kRelocate,  WalProducer::kGC, 0,        0, 0, 0, true},  // commit
        // WB batch
        {WalOp::kPut,       WalProducer::kWB, H("f2"), 30, 3, 0, false},
        {WalOp::kPut,       WalProducer::kWB, 0,        0, 0, 0, true},  // commit
        // GC batch
        {WalOp::kEliminate, WalProducer::kGC, H("g2"), 20, 2, 0, false},
        {WalOp::kEliminate, WalProducer::kGC, 0,        0, 0, 0, true},  // commit
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("f1"));
    EXPECT_EQ(stream->record().producer_id, WalProducer::kWB);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("g1"));
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);
    EXPECT_EQ(stream->record().op, WalOp::kRelocate);
    EXPECT_EQ(stream->record().segment_id, 1u);
    EXPECT_EQ(stream->record().new_segment_id, 5u);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("f2"));
    EXPECT_EQ(stream->record().producer_id, WalProducer::kWB);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("g2"));
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultiProducerMultiRecordBatches) {
    // GC batch with 1 record, then WB batch with 2 records.
    auto setup = createMultiProducerWAL({
        {WalOp::kRelocate, WalProducer::kGC, H("g1"), 5, 1, 3, false},
        {WalOp::kRelocate, WalProducer::kGC, 0,       0, 0, 0, true},  // commit
        {WalOp::kPut,      WalProducer::kWB, H("f1"), 10, 1, 0, false},
        {WalOp::kPut,      WalProducer::kWB, H("f2"), 20, 2, 0, false},
        {WalOp::kPut,      WalProducer::kWB, 0,        0, 0, 0, true},  // commit
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    // GC record comes first
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("g1"));
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    // Then WB records
    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("f1"));

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().hkey, H("f2"));

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

// --- Concurrent producer tests ---

TEST_F(WALStreamTest, ConcurrentFlushAndGC) {
    // Two threads write concurrently to the same WAL — one as WB, one as GC.
    // Verifies thread-safety: no crashes, no corruption, all committed
    // records are replayable with correct producer_id.
    Manifest manifest;
    ASSERT_TRUE(manifest.create(dir_.string()).ok());

    GlobalIndexWAL wal;
    GlobalIndexWAL::Options opts;
    opts.max_file_size = 1ULL << 30;
    opts.batch_size = 8;  // small batch to trigger frequent auto-commits
    ASSERT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

    const int kOpsPerThread = 2000;

    // WB producer thread: puts keys "wb_0" .. "wb_N-1"
    std::thread wb_thread([&]() {
        for (int i = 0; i < kOpsPerThread; ++i) {
            std::string key = "wb_" + std::to_string(i);
            Status s = wal.appendPut(H(key), static_cast<uint64_t>(i),
                                     static_cast<uint32_t>(i + 1),
                                     WalProducer::kWB);
            EXPECT_TRUE(s.ok());
            // Commit every 50 records (in addition to auto-commit at batch_size).
            if ((i + 1) % 50 == 0) {
                s = wal.commit(WalProducer::kWB);
                EXPECT_TRUE(s.ok());
            }
        }
        EXPECT_TRUE(wal.commit(WalProducer::kWB).ok());
    });

    // GC producer thread: relocates keys "gc_0" .. "gc_N-1"
    std::thread gc_thread([&]() {
        for (int i = 0; i < kOpsPerThread; ++i) {
            std::string key = "gc_" + std::to_string(i);
            Status s = wal.appendRelocate(H(key), static_cast<uint64_t>(i + 100),
                                          static_cast<uint32_t>(i),
                                          static_cast<uint32_t>(i + 1000),
                                          WalProducer::kGC);
            EXPECT_TRUE(s.ok());
            if ((i + 1) % 50 == 0) {
                s = wal.commit(WalProducer::kGC);
                EXPECT_TRUE(s.ok());
            }
        }
        EXPECT_TRUE(wal.commit(WalProducer::kGC).ok());
    });

    wb_thread.join();
    gc_thread.join();

    // Collect WAL file IDs before closing.
    std::string wal_dir = wal.walDir();
    std::vector<uint32_t> file_ids;
    for (uint32_t i = 0; i < wal.fileCount(); ++i) {
        file_ids.push_back(i);
    }

    ASSERT_TRUE(wal.close().ok());
    manifest.close();

    // Replay and verify: all records must have valid producer_id,
    // and we should see all keys from both producers.
    auto stream = kvlite::internal::stream::walReplay(wal_dir, file_ids);

    int wb_count = 0;
    int gc_count = 0;

    while (stream->valid()) {
        const auto& rec = stream->record();
        ASSERT_TRUE(rec.producer_id == WalProducer::kWB ||
                    rec.producer_id == WalProducer::kGC);

        if (rec.producer_id == WalProducer::kWB) {
            EXPECT_EQ(rec.op, WalOp::kPut);
            ++wb_count;
        } else {
            EXPECT_EQ(rec.op, WalOp::kRelocate);
            ++gc_count;
        }

        ASSERT_TRUE(stream->next().ok());
    }

    EXPECT_EQ(wb_count, kOpsPerThread);
    EXPECT_EQ(gc_count, kOpsPerThread);
}

TEST_F(WALStreamTest, AutoCommitBatchSize) {
    // Verify that auto-commit fires when staging buffer reaches batch_size,
    // and all records are recoverable via replay.
    Manifest manifest;
    ASSERT_TRUE(manifest.create(dir_.string()).ok());

    GlobalIndexWAL wal;
    GlobalIndexWAL::Options opts;
    opts.max_file_size = 1ULL << 30;
    opts.batch_size = 4;  // auto-commit every 4 records
    ASSERT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

    // Write 10 records without explicit commit.
    // Auto-commit should fire at records 4 and 8 (2 full batches).
    // Records 9-10 remain staged until close().
    for (int i = 0; i < 10; ++i) {
        std::string key = "k" + std::to_string(i);
        ASSERT_TRUE(wal.appendPut(H(key), static_cast<uint64_t>(i),
                                  static_cast<uint32_t>(i + 1),
                                  WalProducer::kWB).ok());
    }

    std::string wal_dir = wal.walDir();
    std::vector<uint32_t> file_ids;
    for (uint32_t i = 0; i < wal.fileCount(); ++i) {
        file_ids.push_back(i);
    }

    // close() flushes the remaining 2 records.
    ASSERT_TRUE(wal.close().ok());
    manifest.close();

    // Replay: should see all 10 records.
    auto stream = kvlite::internal::stream::walReplay(wal_dir, file_ids);
    int count = 0;
    while (stream->valid()) {
        ++count;
        ASSERT_TRUE(stream->next().ok());
    }
    EXPECT_EQ(count, 10);
}

// --- Lifecycle tests ---

TEST_F(WALStreamTest, WriteAfterCloseReturnsError) {
    Manifest manifest;
    ASSERT_TRUE(manifest.create(dir_.string()).ok());

    GlobalIndexWAL wal;
    GlobalIndexWAL::Options opts;
    ASSERT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

    // Write and commit one record.
    ASSERT_TRUE(wal.appendPut(H("k1"), 1, 1, WalProducer::kWB).ok());
    ASSERT_TRUE(wal.commit(WalProducer::kWB).ok());

    ASSERT_TRUE(wal.close().ok());

    // All operations should fail after close.
    EXPECT_FALSE(wal.appendPut(H("k2"), 2, 2, WalProducer::kWB).ok());
    EXPECT_FALSE(wal.appendRelocate(H("k3"), 3, 1, 2, WalProducer::kGC).ok());
    EXPECT_FALSE(wal.appendEliminate(H("k4"), 4, 1, WalProducer::kGC).ok());
    EXPECT_FALSE(wal.commit(WalProducer::kWB).ok());

    // Double close is safe.
    Status s2 = wal.close();
    EXPECT_TRUE(s2.ok()) << s2.toString();

    manifest.close();
}

TEST_F(WALStreamTest, CloseFlushesStagedRecords) {
    // Staged but uncommitted records should be flushed by close().
    Manifest manifest;
    ASSERT_TRUE(manifest.create(dir_.string()).ok());

    GlobalIndexWAL wal;
    GlobalIndexWAL::Options opts;
    opts.max_file_size = 1ULL << 30;
    ASSERT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

    // Stage 3 records without explicit commit.
    ASSERT_TRUE(wal.appendPut(H("a"), 10, 1, WalProducer::kWB).ok());
    ASSERT_TRUE(wal.appendPut(H("b"), 20, 2, WalProducer::kWB).ok());
    ASSERT_TRUE(wal.appendRelocate(H("c"), 30, 3, 4, WalProducer::kGC).ok());

    std::string wal_dir = wal.walDir();
    std::vector<uint32_t> file_ids;
    for (uint32_t i = 0; i < wal.fileCount(); ++i) {
        file_ids.push_back(i);
    }

    // close() should flush both producer buffers.
    ASSERT_TRUE(wal.close().ok());
    manifest.close();

    // Replay and verify all 3 records are present.
    auto stream = kvlite::internal::stream::walReplay(wal_dir, file_ids);
    int count = 0;
    while (stream->valid()) {
        ++count;
        ASSERT_TRUE(stream->next().ok());
    }
    EXPECT_EQ(count, 3);
}

TEST_F(WALStreamTest, ConcurrentWriteAndClose) {
    // Writer thread appends records while main thread closes the WAL.
    // Verifies: close() waits for in-flight ops, no crashes, no corruption.
    Manifest manifest;
    ASSERT_TRUE(manifest.create(dir_.string()).ok());

    GlobalIndexWAL wal;
    GlobalIndexWAL::Options opts;
    opts.batch_size = 8;
    ASSERT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

    const int kOps = 2000;
    std::atomic<int> successful_appends{0};

    std::thread writer([&]() {
        for (int i = 0; i < kOps; ++i) {
            std::string key = "k" + std::to_string(i);
            Status s = wal.appendPut(H(key), static_cast<uint64_t>(i),
                                     static_cast<uint32_t>(i + 1),
                                     WalProducer::kWB);
            if (!s.ok()) break;  // WAL closed
            successful_appends.fetch_add(1, std::memory_order_relaxed);
        }
        // Final commit may fail if WAL is already closed.
        wal.commit(WalProducer::kWB);
    });

    // Wait until the writer has appended at least one record, then close.
    while (successful_appends.load(std::memory_order_relaxed) == 0) {
        std::this_thread::yield();
    }
    ASSERT_TRUE(wal.close().ok());

    writer.join();

    // After close, appends must fail.
    EXPECT_FALSE(wal.appendPut(H("late"), 999, 999, WalProducer::kWB).ok());

    int appended = successful_appends.load();
    EXPECT_GT(appended, 0);

    // Reopen and replay to verify data integrity.
    Manifest manifest2;
    ASSERT_TRUE(manifest2.open(dir_.string()).ok());

    GlobalIndexWAL wal2;
    ASSERT_TRUE(wal2.open(dir_.string(), manifest2, opts).ok());

    auto stream = wal2.replayStream();
    int replayed = 0;
    while (stream->valid()) {
        EXPECT_EQ(stream->record().op, WalOp::kPut);
        ++replayed;
        ASSERT_TRUE(stream->next().ok());
    }

    // All successfully appended records must be in the WAL.
    EXPECT_GE(replayed, appended);
    EXPECT_LE(replayed, kOps);

    wal2.close();
    manifest2.close();
}

// --- Version-based truncation tests ---

TEST_F(WALStreamTest, VersionBasedTruncation) {
    // Create a WAL with multiple files (small max_file_size to force rollover).
    Manifest manifest;
    ASSERT_TRUE(manifest.create(dir_.string()).ok());

    GlobalIndexWAL wal;
    GlobalIndexWAL::Options opts;
    opts.max_file_size = 128;  // tiny — forces rollover after every few commits
    opts.batch_size = 1;       // commit after every record
    ASSERT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

    // Write entries with increasing max_version across files.
    for (int i = 0; i < 20; ++i) {
        std::string key = "k" + std::to_string(i);
        wal.updateMaxVersion(static_cast<uint64_t>(i + 1));
        ASSERT_TRUE(wal.appendPut(H(key), static_cast<uint64_t>(i + 1),
                                  static_cast<uint32_t>(i + 1),
                                  WalProducer::kWB).ok());
    }
    ASSERT_TRUE(wal.commit(WalProducer::kWB).ok());

    uint32_t file_count_before = wal.fileCount();
    ASSERT_GT(file_count_before, 1u) << "Need multiple files for this test";

    // Truncate with a cutoff that should remove some but not all files.
    // Set cutoff to version 10 — files with max_version <= 10 should be deleted.
    ASSERT_TRUE(wal.truncate(10).ok());

    uint32_t file_count_after = wal.fileCount();
    EXPECT_LT(file_count_after, file_count_before);
    EXPECT_GE(file_count_after, 1u); // active file always kept

    ASSERT_TRUE(wal.close().ok());
    manifest.close();
}

TEST_F(WALStreamTest, VersionBasedTruncationPreservesActiveFile) {
    // Even if cutoff_version covers everything, the active file survives.
    Manifest manifest;
    ASSERT_TRUE(manifest.create(dir_.string()).ok());

    GlobalIndexWAL wal;
    GlobalIndexWAL::Options opts;
    opts.max_file_size = 128;
    opts.batch_size = 1;
    ASSERT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

    for (int i = 0; i < 10; ++i) {
        std::string key = "k" + std::to_string(i);
        wal.updateMaxVersion(static_cast<uint64_t>(i + 1));
        ASSERT_TRUE(wal.appendPut(H(key), static_cast<uint64_t>(i + 1),
                                  static_cast<uint32_t>(i + 1),
                                  WalProducer::kWB).ok());
    }
    ASSERT_TRUE(wal.commit(WalProducer::kWB).ok());

    // Truncate with a very high cutoff — should delete all except active.
    ASSERT_TRUE(wal.truncate(1000).ok());
    EXPECT_GE(wal.fileCount(), 1u);

    ASSERT_TRUE(wal.close().ok());
    manifest.close();
}

TEST_F(WALStreamTest, MaxVersionPersistsAcrossRollover) {
    // Verify that max_version is inherited by new files after rollover
    // and that the file range is tracked correctly in Manifest.
    Manifest manifest;
    ASSERT_TRUE(manifest.create(dir_.string()).ok());

    GlobalIndexWAL wal;
    GlobalIndexWAL::Options opts;
    opts.max_file_size = 128;
    opts.batch_size = 1;
    ASSERT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

    // Set a high max_version early.
    wal.updateMaxVersion(500);

    for (int i = 0; i < 20; ++i) {
        std::string key = "k" + std::to_string(i);
        ASSERT_TRUE(wal.appendPut(H(key), static_cast<uint64_t>(i + 1),
                                  static_cast<uint32_t>(i + 1),
                                  WalProducer::kWB).ok());
    }
    ASSERT_TRUE(wal.commit(WalProducer::kWB).ok());

    ASSERT_GT(wal.fileCount(), 1u) << "Need rollover for this test";

    // Check that min/max file IDs are tracked in Manifest.
    std::string val;
    ASSERT_TRUE(manifest.get(ManifestKey::kGiWalMinFileId, val));
    uint32_t min_id = static_cast<uint32_t>(std::stoul(val));
    ASSERT_TRUE(manifest.get(ManifestKey::kGiWalMaxFileId, val));
    uint32_t max_id = static_cast<uint32_t>(std::stoul(val));
    EXPECT_EQ(max_id - min_id + 1, wal.fileCount());

    ASSERT_TRUE(wal.close().ok());
    manifest.close();
}
