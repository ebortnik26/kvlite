#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

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

namespace WalProducer = kvlite::internal::WalProducer;

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

    // ops: (op, key, packed_version, segment_id, new_segment_id, batch_group)
    // batch_group groups consecutive ops into a batch; a new group triggers commit.
    // Optional producer_id (defaults to WalProducer::kWB).
    WALSetup createWAL(
        const std::vector<std::tuple<WalOp, std::string, uint64_t, uint32_t, uint32_t, uint64_t>>& ops,
        uint8_t producer_id = WalProducer::kWB) {
        Manifest manifest;
        EXPECT_TRUE(manifest.create(dir_.string()).ok());

        GlobalIndexWAL wal;
        GlobalIndexWAL::Options opts;
        opts.max_file_size = 1ULL << 30;
        EXPECT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

        uint64_t current_group = 0;
        bool in_batch = false;

        for (const auto& [op, key, pv, seg, new_seg, group] : ops) {
            if (in_batch && group != current_group) {
                EXPECT_TRUE(wal.commit(producer_id).ok());
                in_batch = false;
            }
            current_group = group;
            in_batch = true;

            switch (op) {
                case WalOp::kPut:
                    wal.appendPut(key, pv, seg, producer_id);
                    break;
                case WalOp::kRelocate:
                    wal.appendRelocate(key, pv, seg, new_seg, producer_id);
                    break;
                case WalOp::kEliminate:
                    wal.appendEliminate(key, pv, seg, producer_id);
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
        std::string key;
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
                    wal.appendPut(r.key, r.packed_version, r.segment_id, r.producer_id);
                    break;
                case WalOp::kRelocate:
                    wal.appendRelocate(r.key, r.packed_version, r.segment_id,
                                       r.new_segment_id, r.producer_id);
                    break;
                case WalOp::kEliminate:
                    wal.appendEliminate(r.key, r.packed_version, r.segment_id, r.producer_id);
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
        {WalOp::kPut, "key1", 100, 1, 0, 1},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);
    ASSERT_TRUE(stream->valid());

    const auto& rec = stream->record();
    EXPECT_EQ(rec.op, WalOp::kPut);
    EXPECT_EQ(rec.key, "key1");
    EXPECT_EQ(rec.packed_version, 100u);
    EXPECT_EQ(rec.segment_id, 1u);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultipleBatches) {
    auto setup = createWAL({
        {WalOp::kPut, "k1", 10, 1, 0, 1},
        {WalOp::kPut, "k2", 20, 2, 0, 2},
        {WalOp::kPut, "k3", 30, 3, 0, 3},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "k1");

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "k2");

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "k3");

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultipleRecordsInOneBatch) {
    auto setup = createWAL({
        {WalOp::kPut, "a", 10, 1, 0, 1},
        {WalOp::kPut, "b", 20, 2, 0, 1},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "a");

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "b");

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, RelocateAndEliminate) {
    auto setup = createWAL({
        {WalOp::kPut, "key", 10, 1, 0, 1},
        {WalOp::kRelocate, "key", 10, 1, 5, 2},
        {WalOp::kEliminate, "key", 10, 5, 0, 3},
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
        {WalOp::kPut, "a", 10, 1, 0, 1},
        {WalOp::kPut, "b", 20, 2, 0, 2},
    }, WalProducer::kGC);

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "a");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "b");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultiProducerInterleaved) {
    // Two producers interleaved in a single WAL.
    auto setup = createMultiProducerWAL({
        // WB batch
        {WalOp::kPut,       WalProducer::kWB, "f1", 10, 1, 0, false},
        {WalOp::kPut,       WalProducer::kWB, "",    0, 0, 0, true},  // commit
        // GC batch
        {WalOp::kRelocate,  WalProducer::kGC, "g1", 10, 1, 5, false},
        {WalOp::kRelocate,  WalProducer::kGC, "",    0, 0, 0, true},  // commit
        // WB batch
        {WalOp::kPut,       WalProducer::kWB, "f2", 30, 3, 0, false},
        {WalOp::kPut,       WalProducer::kWB, "",    0, 0, 0, true},  // commit
        // GC batch
        {WalOp::kEliminate, WalProducer::kGC, "g2", 20, 2, 0, false},
        {WalOp::kEliminate, WalProducer::kGC, "",    0, 0, 0, true},  // commit
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "f1");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kWB);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "g1");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);
    EXPECT_EQ(stream->record().op, WalOp::kRelocate);
    EXPECT_EQ(stream->record().segment_id, 1u);
    EXPECT_EQ(stream->record().new_segment_id, 5u);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "f2");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kWB);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "g2");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultiProducerMultiRecordBatches) {
    // GC batch with 1 record, then WB batch with 2 records.
    auto setup = createMultiProducerWAL({
        {WalOp::kRelocate, WalProducer::kGC, "g1", 5, 1, 3, false},
        {WalOp::kRelocate, WalProducer::kGC, "",   0, 0, 0, true},  // commit
        {WalOp::kPut,      WalProducer::kWB, "f1", 10, 1, 0, false},
        {WalOp::kPut,      WalProducer::kWB, "f2", 20, 2, 0, false},
        {WalOp::kPut,      WalProducer::kWB, "",    0, 0, 0, true},  // commit
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    // GC record comes first
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "g1");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    // Then WB records
    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "f1");

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "f2");

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
            Status s = wal.appendPut(key, static_cast<uint64_t>(i),
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
            Status s = wal.appendRelocate(key, static_cast<uint64_t>(i + 100),
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
        ASSERT_TRUE(wal.appendPut(key, static_cast<uint64_t>(i),
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
    ASSERT_TRUE(wal.appendPut("k1", 1, 1, WalProducer::kWB).ok());
    ASSERT_TRUE(wal.commit(WalProducer::kWB).ok());

    ASSERT_TRUE(wal.close().ok());

    // All operations should fail after close.
    EXPECT_FALSE(wal.appendPut("k2", 2, 2, WalProducer::kWB).ok());
    EXPECT_FALSE(wal.appendRelocate("k3", 3, 1, 2, WalProducer::kGC).ok());
    EXPECT_FALSE(wal.appendEliminate("k4", 4, 1, WalProducer::kGC).ok());
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
    ASSERT_TRUE(wal.appendPut("a", 10, 1, WalProducer::kWB).ok());
    ASSERT_TRUE(wal.appendPut("b", 20, 2, WalProducer::kWB).ok());
    ASSERT_TRUE(wal.appendRelocate("c", 30, 3, 4, WalProducer::kGC).ok());

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
            Status s = wal.appendPut(key, static_cast<uint64_t>(i),
                                     static_cast<uint32_t>(i + 1),
                                     WalProducer::kWB);
            if (!s.ok()) break;  // WAL closed
            successful_appends.fetch_add(1, std::memory_order_relaxed);
        }
        // Final commit may fail if WAL is already closed.
        wal.commit(WalProducer::kWB);
    });

    // Give writer a head start, then close.
    std::this_thread::sleep_for(std::chrono::microseconds(50));
    ASSERT_TRUE(wal.close().ok());

    writer.join();

    // After close, appends must fail.
    EXPECT_FALSE(wal.appendPut("late", 999, 999, WalProducer::kWB).ok());

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
