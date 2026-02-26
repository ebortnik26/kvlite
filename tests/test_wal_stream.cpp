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
using kvlite::internal::WALMergeStream;
using kvlite::internal::WALReplayStream;
using kvlite::internal::WALRecord;
using kvlite::internal::WALStream;

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

    WALSetup createWAL(const std::string& name,
                       const std::vector<std::tuple<WalOp, std::string, uint64_t, uint32_t, uint32_t, uint64_t>>& ops) {
        // ops: (op, key, packed_version, segment_id, new_segment_id, commit_version)
        // Group consecutive ops with same commit_version into a batch.
        Manifest manifest;
        EXPECT_TRUE(manifest.create(dir_.string()).ok());

        GlobalIndexWAL wal;
        GlobalIndexWAL::Options opts;
        opts.max_file_size = 1ULL << 30;
        EXPECT_TRUE(wal.open(dir_.string(), manifest, opts, name).ok());

        uint64_t current_cv = 0;
        bool in_batch = false;

        for (const auto& [op, key, pv, seg, new_seg, cv] : ops) {
            if (in_batch && cv != current_cv) {
                // Commit the previous batch.
                EXPECT_TRUE(wal.commit(current_cv).ok());
                in_batch = false;
            }
            current_cv = cv;
            in_batch = true;

            switch (op) {
                case WalOp::kPut:
                    wal.appendPut(key, pv, seg);
                    break;
                case WalOp::kRelocate:
                    wal.appendRelocate(key, pv, seg, new_seg);
                    break;
                case WalOp::kEliminate:
                    wal.appendEliminate(key, pv, seg);
                    break;
                default:
                    break;
            }
        }
        if (in_batch) {
            EXPECT_TRUE(wal.commit(current_cv).ok());
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

    fs::path dir_;
};

// --- WALReplayStream tests ---

TEST_F(WALStreamTest, EmptyWAL) {
    auto setup = createWAL("empty", {});
    auto stream = kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids);
    EXPECT_FALSE(stream->valid());
}

TEST_F(WALStreamTest, SinglePut) {
    auto setup = createWAL("single", {
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
    auto setup = createWAL("multi", {
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
    auto setup = createWAL("batch", {
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
    auto setup = createWAL("reloc", {
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

// --- WALMergeStream tests ---

TEST_F(WALStreamTest, MergeEmpty) {
    std::vector<std::unique_ptr<WALStream>> inputs;
    auto merged = kvlite::internal::stream::walMerge(std::move(inputs));
    EXPECT_FALSE(merged->valid());
}

TEST_F(WALStreamTest, MergeSingleStream) {
    auto setup = createWAL("single_merge", {
        {WalOp::kPut, "a", 10, 1, 0, 100},
        {WalOp::kPut, "b", 20, 2, 0, 200},
    });

    std::vector<std::unique_ptr<WALStream>> inputs;
    inputs.push_back(kvlite::internal::stream::walReplay(setup.wal_dir, setup.file_ids));

    auto merged = kvlite::internal::stream::walMerge(std::move(inputs));

    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "a");
    EXPECT_EQ(merged->record().commit_version, 100u);

    ASSERT_TRUE(merged->next().ok());
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "b");
    EXPECT_EQ(merged->record().commit_version, 200u);

    ASSERT_TRUE(merged->next().ok());
    EXPECT_FALSE(merged->valid());
}

TEST_F(WALStreamTest, MergeTwoStreamsInterleaved) {
    // Flush WAL: commits at version 100, 300
    auto flush = createWAL("flush", {
        {WalOp::kPut, "f1", 10, 1, 0, 100},
        {WalOp::kPut, "f2", 30, 3, 0, 300},
    });

    // GC WAL: commits at version 200, 400
    auto gc = createWAL("gc", {
        {WalOp::kRelocate, "g1", 10, 1, 5, 200},
        {WalOp::kEliminate, "g2", 20, 2, 0, 400},
    });

    std::vector<std::unique_ptr<WALStream>> inputs;
    inputs.push_back(kvlite::internal::stream::walReplay(flush.wal_dir, flush.file_ids));
    inputs.push_back(kvlite::internal::stream::walReplay(gc.wal_dir, gc.file_ids));

    auto merged = kvlite::internal::stream::walMerge(std::move(inputs));

    // Expected order by commit_version: 100, 200, 300, 400
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "f1");
    EXPECT_EQ(merged->record().commit_version, 100u);

    ASSERT_TRUE(merged->next().ok());
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "g1");
    EXPECT_EQ(merged->record().commit_version, 200u);

    ASSERT_TRUE(merged->next().ok());
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "f2");
    EXPECT_EQ(merged->record().commit_version, 300u);

    ASSERT_TRUE(merged->next().ok());
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "g2");
    EXPECT_EQ(merged->record().commit_version, 400u);

    ASSERT_TRUE(merged->next().ok());
    EXPECT_FALSE(merged->valid());
}

TEST_F(WALStreamTest, MergeSameCommitVersion) {
    // Both WALs commit at the same version — both records should appear.
    auto wal_a = createWAL("a", {
        {WalOp::kPut, "a1", 10, 1, 0, 100},
    });
    auto wal_b = createWAL("b", {
        {WalOp::kPut, "b1", 20, 2, 0, 100},
    });

    std::vector<std::unique_ptr<WALStream>> inputs;
    inputs.push_back(kvlite::internal::stream::walReplay(wal_a.wal_dir, wal_a.file_ids));
    inputs.push_back(kvlite::internal::stream::walReplay(wal_b.wal_dir, wal_b.file_ids));

    auto merged = kvlite::internal::stream::walMerge(std::move(inputs));

    ASSERT_TRUE(merged->valid());
    auto key1 = std::string(merged->record().key);
    EXPECT_EQ(merged->record().commit_version, 100u);

    ASSERT_TRUE(merged->next().ok());
    ASSERT_TRUE(merged->valid());
    auto key2 = std::string(merged->record().key);
    EXPECT_EQ(merged->record().commit_version, 100u);

    ASSERT_TRUE(merged->next().ok());
    EXPECT_FALSE(merged->valid());

    // Both keys should appear (order within same version is unspecified).
    EXPECT_TRUE((key1 == "a1" && key2 == "b1") || (key1 == "b1" && key2 == "a1"));
}

TEST_F(WALStreamTest, MergeWithOneEmptyStream) {
    auto flush = createWAL("flush_data", {
        {WalOp::kPut, "x", 10, 1, 0, 100},
    });
    auto gc = createWAL("gc_empty", {});

    std::vector<std::unique_ptr<WALStream>> inputs;
    inputs.push_back(kvlite::internal::stream::walReplay(flush.wal_dir, flush.file_ids));
    inputs.push_back(kvlite::internal::stream::walReplay(gc.wal_dir, gc.file_ids));

    auto merged = kvlite::internal::stream::walMerge(std::move(inputs));

    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "x");

    ASSERT_TRUE(merged->next().ok());
    EXPECT_FALSE(merged->valid());
}

TEST_F(WALStreamTest, MergeMultiRecordBatches) {
    // Flush WAL: one batch with 2 records at commit_version 100
    auto flush = createWAL("flush_multi", {
        {WalOp::kPut, "f1", 10, 1, 0, 100},
        {WalOp::kPut, "f2", 20, 2, 0, 100},
    });

    // GC WAL: one batch with 1 record at commit_version 50 (should come first)
    auto gc = createWAL("gc_multi", {
        {WalOp::kRelocate, "g1", 5, 1, 3, 50},
    });

    std::vector<std::unique_ptr<WALStream>> inputs;
    inputs.push_back(kvlite::internal::stream::walReplay(flush.wal_dir, flush.file_ids));
    inputs.push_back(kvlite::internal::stream::walReplay(gc.wal_dir, gc.file_ids));

    auto merged = kvlite::internal::stream::walMerge(std::move(inputs));

    // GC record (cv=50) comes first
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "g1");
    EXPECT_EQ(merged->record().commit_version, 50u);

    // Then flush records (cv=100)
    ASSERT_TRUE(merged->next().ok());
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "f1");
    EXPECT_EQ(merged->record().commit_version, 100u);

    ASSERT_TRUE(merged->next().ok());
    ASSERT_TRUE(merged->valid());
    EXPECT_EQ(merged->record().key, "f2");
    EXPECT_EQ(merged->record().commit_version, 100u);

    ASSERT_TRUE(merged->next().ok());
    EXPECT_FALSE(merged->valid());
}
