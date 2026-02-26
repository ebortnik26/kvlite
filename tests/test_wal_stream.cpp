#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
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

    // ops: (op, key, packed_version, segment_id, new_segment_id, commit_version)
    // Optional producer_id (defaults to WalProducer::kWB).
    WALSetup createWAL(
        const std::vector<std::tuple<WalOp, std::string, uint64_t, uint32_t, uint32_t, uint64_t>>& ops,
        uint8_t producer_id = WalProducer::kWB) {
        // Group consecutive ops with same commit_version into a batch.
        Manifest manifest;
        EXPECT_TRUE(manifest.create(dir_.string()).ok());

        GlobalIndexWAL wal;
        GlobalIndexWAL::Options opts;
        opts.max_file_size = 1ULL << 30;
        EXPECT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

        uint64_t current_cv = 0;
        bool in_batch = false;

        for (const auto& [op, key, pv, seg, new_seg, cv] : ops) {
            if (in_batch && cv != current_cv) {
                // Commit the previous batch.
                EXPECT_TRUE(wal.commit(current_cv, producer_id).ok());
                in_batch = false;
            }
            current_cv = cv;
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
                default:
                    break;
            }
        }
        if (in_batch) {
            EXPECT_TRUE(wal.commit(current_cv, producer_id).ok());
        }

        WALSetup setup;
        setup.wal_dir = wal.walDir();
        // Collect file IDs before closing (fileCount() gives us the count).
        // We know file IDs start at 0 and increment.
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
        uint64_t commit_version;  // 0 for data records, non-zero triggers commit
    };

    WALSetup createMultiProducerWAL(std::vector<RawOp> ops) {
        Manifest manifest;
        EXPECT_TRUE(manifest.create(dir_.string()).ok());

        GlobalIndexWAL wal;
        GlobalIndexWAL::Options opts;
        opts.max_file_size = 1ULL << 30;
        EXPECT_TRUE(wal.open(dir_.string(), manifest, opts).ok());

        for (const auto& r : ops) {
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
                case WalOp::kCommit:
                    EXPECT_TRUE(wal.commit(r.commit_version, r.producer_id).ok());
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
        {WalOp::kPut, "key1", 100, 1, 0, 50},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);
    ASSERT_TRUE(stream->valid());

    const auto& rec = stream->record();
    EXPECT_EQ(rec.op, WalOp::kPut);
    EXPECT_EQ(rec.key, "key1");
    EXPECT_EQ(rec.packed_version, 100u);
    EXPECT_EQ(rec.segment_id, 1u);
    EXPECT_EQ(rec.commit_version, 50u);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultipleBatches) {
    auto setup = createWAL({
        {WalOp::kPut, "k1", 10, 1, 0, 100},
        {WalOp::kPut, "k2", 20, 2, 0, 200},
        {WalOp::kPut, "k3", 30, 3, 0, 300},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    // Record 1
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "k1");
    EXPECT_EQ(stream->record().commit_version, 100u);

    // Record 2
    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "k2");
    EXPECT_EQ(stream->record().commit_version, 200u);

    // Record 3
    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "k3");
    EXPECT_EQ(stream->record().commit_version, 300u);

    // End
    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultipleRecordsInOneBatch) {
    auto setup = createWAL({
        {WalOp::kPut, "a", 10, 1, 0, 500},
        {WalOp::kPut, "b", 20, 2, 0, 500},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "a");
    EXPECT_EQ(stream->record().commit_version, 500u);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "b");
    EXPECT_EQ(stream->record().commit_version, 500u);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, RelocateAndEliminate) {
    auto setup = createWAL({
        {WalOp::kPut, "key", 10, 1, 0, 100},
        {WalOp::kRelocate, "key", 10, 1, 5, 200},
        {WalOp::kEliminate, "key", 10, 5, 0, 300},
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
        {WalOp::kPut, "a", 10, 1, 0, 100},
        {WalOp::kPut, "b", 20, 2, 0, 200},
    }, WalProducer::kGC);

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "a");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);
    EXPECT_EQ(stream->record().commit_version, 100u);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "b");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);
    EXPECT_EQ(stream->record().commit_version, 200u);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultiProducerInterleaved) {
    // Two producers interleaved in a single WAL, batches ordered by commit_version.
    // WB commits at 100, 300; GC commits at 200, 400.
    auto setup = createMultiProducerWAL({
        // WB batch (cv=100)
        {WalOp::kPut,    WalProducer::kWB, "f1", 10, 1, 0, 0},
        {WalOp::kCommit, WalProducer::kWB, "",    0, 0, 0, 100},
        // GC batch (cv=200)
        {WalOp::kRelocate, WalProducer::kGC, "g1", 10, 1, 5, 0},
        {WalOp::kCommit,   WalProducer::kGC, "",    0, 0, 0, 200},
        // WB batch (cv=300)
        {WalOp::kPut,    WalProducer::kWB, "f2", 30, 3, 0, 0},
        {WalOp::kCommit, WalProducer::kWB, "",    0, 0, 0, 300},
        // GC batch (cv=400)
        {WalOp::kEliminate, WalProducer::kGC, "g2", 20, 2, 0, 0},
        {WalOp::kCommit,    WalProducer::kGC, "",    0, 0, 0, 400},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    // Expected order by commit_version: 100, 200, 300, 400
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "f1");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kWB);
    EXPECT_EQ(stream->record().commit_version, 100u);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "g1");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);
    EXPECT_EQ(stream->record().commit_version, 200u);
    EXPECT_EQ(stream->record().op, WalOp::kRelocate);
    EXPECT_EQ(stream->record().segment_id, 1u);
    EXPECT_EQ(stream->record().new_segment_id, 5u);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "f2");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kWB);
    EXPECT_EQ(stream->record().commit_version, 300u);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "g2");
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);
    EXPECT_EQ(stream->record().commit_version, 400u);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, MultiProducerMultiRecordBatches) {
    // GC batch (cv=50) with 1 record, then WB batch (cv=100) with 2 records.
    auto setup = createMultiProducerWAL({
        {WalOp::kRelocate, WalProducer::kGC, "g1", 5, 1, 3, 0},
        {WalOp::kCommit,   WalProducer::kGC, "",   0, 0, 0, 50},
        {WalOp::kPut,    WalProducer::kWB, "f1", 10, 1, 0, 0},
        {WalOp::kPut,    WalProducer::kWB, "f2", 20, 2, 0, 0},
        {WalOp::kCommit, WalProducer::kWB, "",    0, 0, 0, 100},
    });

    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);

    // GC record (cv=50) comes first
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "g1");
    EXPECT_EQ(stream->record().commit_version, 50u);
    EXPECT_EQ(stream->record().producer_id, WalProducer::kGC);

    // Then WB records (cv=100)
    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "f1");
    EXPECT_EQ(stream->record().commit_version, 100u);

    ASSERT_TRUE(stream->next().ok());
    ASSERT_TRUE(stream->valid());
    EXPECT_EQ(stream->record().key, "f2");
    EXPECT_EQ(stream->record().commit_version, 100u);

    ASSERT_TRUE(stream->next().ok());
    EXPECT_FALSE(stream->valid());
}
