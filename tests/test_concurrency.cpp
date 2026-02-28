// Multi-threaded concurrency and atomicity tests for kvlite
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include <filesystem>
#include <map>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <set>
#include <mutex>

namespace fs = std::filesystem;

static kvlite::ReadOptions snapOpts(const kvlite::Snapshot& snap) {
    kvlite::ReadOptions opts;
    opts.snapshot = &snap;
    return opts;
}

class ConcurrencyTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / "kvlite_test_concurrency";
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);

        kvlite::Options opts;
        opts.create_if_missing = true;
        ASSERT_TRUE(db_.open(test_dir_.string(), opts).ok());
    }

    void TearDown() override {
        if (db_.isOpen()) {
            db_.close();
        }
        fs::remove_all(test_dir_);
    }

    fs::path test_dir_;
    kvlite::DB db_;
};

// --- Concurrent Write Tests ---

TEST_F(ConcurrencyTest, ConcurrentPuts) {
    const int num_threads = 8;
    const int ops_per_thread = 1000;
    std::atomic<int> success_count{0};

    auto worker = [&](int thread_id) {
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = "key_" + std::to_string(thread_id) + "_" + std::to_string(i);
            std::string value = "value_" + std::to_string(thread_id) + "_" + std::to_string(i);
            if (db_.put(key, value).ok()) {
                success_count++;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), num_threads * ops_per_thread);

    // Verify all keys are readable
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = "key_" + std::to_string(t) + "_" + std::to_string(i);
            std::string expected = "value_" + std::to_string(t) + "_" + std::to_string(i);
            std::string value;
            ASSERT_TRUE(db_.get(key, value).ok()) << "Missing key: " << key;
            EXPECT_EQ(value, expected);
        }
    }
}

TEST_F(ConcurrencyTest, ConcurrentPutsSameKey) {
    const int num_threads = 8;
    const int ops_per_thread = 100;

    auto worker = [&](int thread_id) {
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string value = "value_" + std::to_string(thread_id) + "_" + std::to_string(i);
            db_.put("shared_key", value);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    // Key should exist and have some value
    std::string value;
    ASSERT_TRUE(db_.get("shared_key", value).ok());
    EXPECT_FALSE(value.empty());
}

// --- Concurrent Read Tests ---

TEST_F(ConcurrencyTest, ConcurrentReads) {
    // Pre-populate
    for (int i = 0; i < 100; ++i) {
        ASSERT_TRUE(db_.put("key" + std::to_string(i), "value" + std::to_string(i)).ok());
    }

    const int num_threads = 8;
    const int reads_per_thread = 1000;
    std::atomic<int> success_count{0};

    auto reader = [&](int thread_id) {
        std::mt19937 rng(thread_id);
        std::uniform_int_distribution<int> dist(0, 99);

        for (int i = 0; i < reads_per_thread; ++i) {
            int key_num = dist(rng);
            std::string key = "key" + std::to_string(key_num);
            std::string expected = "value" + std::to_string(key_num);
            std::string value;

            if (db_.get(key, value).ok() && value == expected) {
                success_count++;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(reader, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), num_threads * reads_per_thread);
}

// --- Concurrent Read/Write Tests ---

TEST_F(ConcurrencyTest, ConcurrentReadWrite) {
    const int num_writers = 4;
    const int num_readers = 4;
    const int ops_per_thread = 500;
    std::atomic<bool> stop{false};
    std::atomic<int> write_count{0};
    std::atomic<int> read_count{0};

    auto writer = [&](int thread_id) {
        for (int i = 0; i < ops_per_thread && !stop; ++i) {
            std::string key = "key" + std::to_string(i % 100);
            std::string value = "value_" + std::to_string(thread_id) + "_" + std::to_string(i);
            if (db_.put(key, value).ok()) {
                write_count++;
            }
        }
    };

    auto reader = [&](int thread_id) {
        std::mt19937 rng(thread_id + 100);
        std::uniform_int_distribution<int> dist(0, 99);

        for (int i = 0; i < ops_per_thread && !stop; ++i) {
            std::string key = "key" + std::to_string(dist(rng));
            std::string value;
            // Reads may return NotFound or OK, both are valid
            kvlite::Status s = db_.get(key, value);
            if (s.ok() || s.isNotFound()) {
                read_count++;
            }
        }
    };

    std::vector<std::thread> threads;

    for (int i = 0; i < num_writers; ++i) {
        threads.emplace_back(writer, i);
    }
    for (int i = 0; i < num_readers; ++i) {
        threads.emplace_back(reader, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(write_count.load(), num_writers * ops_per_thread);
    EXPECT_EQ(read_count.load(), num_readers * ops_per_thread);
}

// --- WriteBatch Atomicity Tests ---

TEST_F(ConcurrencyTest, WriteBatchAtomicity) {
    // Test that WriteBatch operations are atomic - all keys get same version
    const int num_threads = 4;
    const int batches_per_thread = 100;

    auto writer = [&](int thread_id) {
        for (int i = 0; i < batches_per_thread; ++i) {
            kvlite::WriteBatch batch;
            std::string prefix = "t" + std::to_string(thread_id) + "_b" + std::to_string(i) + "_";

            batch.put(prefix + "k1", "v1");
            batch.put(prefix + "k2", "v2");
            batch.put(prefix + "k3", "v3");

            EXPECT_TRUE(db_.write(batch).ok());
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(writer, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    // Verify atomicity: all keys in a batch should have same version.
    // Collect all key→version pairs via iterator.
    std::unique_ptr<kvlite::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::map<std::string, uint64_t> key_versions;
    {
        std::string key, value;
        uint64_t version;
        while (iter->next(key, value, version).ok()) {
            key_versions[key] = version;
        }
    }

    for (int t = 0; t < num_threads; ++t) {
        for (int b = 0; b < batches_per_thread; ++b) {
            std::string prefix = "t" + std::to_string(t) + "_b" + std::to_string(b) + "_";

            auto it1 = key_versions.find(prefix + "k1");
            auto it2 = key_versions.find(prefix + "k2");
            auto it3 = key_versions.find(prefix + "k3");
            ASSERT_NE(it1, key_versions.end()) << prefix + "k1 missing";
            ASSERT_NE(it2, key_versions.end()) << prefix + "k2 missing";
            ASSERT_NE(it3, key_versions.end()) << prefix + "k3 missing";

            EXPECT_EQ(it1->second, it2->second) << "Batch " << prefix << " not atomic";
            EXPECT_EQ(it2->second, it3->second) << "Batch " << prefix << " not atomic";
        }
    }
}

// --- Snapshot Isolation Tests ---

TEST_F(ConcurrencyTest, SnapshotIsolation) {
    ASSERT_TRUE(db_.put("key", "initial").ok());

    kvlite::Snapshot snapshot = db_.createSnapshot();

    const int num_writers = 4;
    const int writes_per_thread = 100;

    auto writer = [&](int thread_id) {
        for (int i = 0; i < writes_per_thread; ++i) {
            std::string value = "v_" + std::to_string(thread_id) + "_" + std::to_string(i);
            db_.put("key", value);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_writers; ++i) {
        threads.emplace_back(writer, i);
    }

    // While writes are happening, snapshot should always see "initial"
    std::atomic<bool> snapshot_consistent{true};
    std::thread reader([&]() {
        for (int i = 0; i < 1000; ++i) {
            std::string value;
            if (db_.get("key", value, snapOpts(snapshot)).ok()) {
                if (value != "initial") {
                    snapshot_consistent = false;
                    break;
                }
            }
        }
    });

    for (auto& t : threads) {
        t.join();
    }
    reader.join();

    EXPECT_TRUE(snapshot_consistent.load());

    db_.releaseSnapshot(snapshot);
}

TEST_F(ConcurrencyTest, ConcurrentSnapshots) {
    ASSERT_TRUE(db_.put("counter", "0").ok());

    const int num_threads = 4;
    std::vector<kvlite::Snapshot> snapshots;
    std::vector<std::string> expected_values(num_threads);

    // Create snapshots interleaved with writes
    for (int i = 0; i < num_threads; ++i) {
        expected_values[i] = std::to_string(i);
        ASSERT_TRUE(db_.put("counter", expected_values[i]).ok());
        snapshots.push_back(db_.createSnapshot());
    }

    // Write more
    ASSERT_TRUE(db_.put("counter", "final").ok());

    // Each snapshot should see its expected value
    for (int i = 0; i < num_threads; ++i) {
        std::string value;
        ASSERT_TRUE(db_.get("counter", value, snapOpts(snapshots[i])).ok());
        EXPECT_EQ(value, expected_values[i]);
    }

    for (auto& snap : snapshots) {
        db_.releaseSnapshot(snap);
    }
}

// --- Iterator Consistency Tests ---

TEST_F(ConcurrencyTest, IteratorConsistencyDuringWrites) {
    // Pre-populate
    std::set<std::string> initial_keys;
    for (int i = 0; i < 100; ++i) {
        std::string key = "key" + std::to_string(i);
        ASSERT_TRUE(db_.put(key, "value" + std::to_string(i)).ok());
        initial_keys.insert(key);
    }

    std::unique_ptr<kvlite::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    // Start concurrent writes
    std::atomic<bool> stop_writing{false};
    std::thread writer([&]() {
        int counter = 100;
        while (!stop_writing) {
            db_.put("new_key_" + std::to_string(counter++), "new_value");
            db_.put("key0", "modified");  // Modify existing
        }
    });

    // Iterator should see exactly the initial keys
    std::set<std::string> iterated_keys;
    std::string key, value;
    while (iter->next(key, value).ok()) {
        iterated_keys.insert(key);
    }

    stop_writing = true;
    writer.join();

    EXPECT_EQ(iterated_keys, initial_keys);
}

// --- Version Ordering Tests ---

TEST_F(ConcurrencyTest, VersionOrderingUnderConcurrency) {
    const int num_threads = 4;
    const int ops_per_thread = 100;

    std::vector<uint64_t> all_versions;

    auto writer = [&](int thread_id) {
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = "key_" + std::to_string(thread_id) + "_" + std::to_string(i);
            ASSERT_TRUE(db_.put(key, "value").ok());
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(writer, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    // Collect all versions via iterator.
    std::unique_ptr<kvlite::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::string key, value;
    uint64_t version;
    while (iter->next(key, value, version).ok()) {
        all_versions.push_back(version);
    }

    // All versions should be unique
    std::set<uint64_t> unique_versions(all_versions.begin(), all_versions.end());
    EXPECT_EQ(unique_versions.size(), all_versions.size());
}

// --- Stress Test ---

TEST_F(ConcurrencyTest, StressTest) {
    const int num_threads = 8;
    const int ops_per_thread = 1000;
    const int num_keys = 50;

    std::atomic<int> total_ops{0};
    std::atomic<int> errors{0};

    auto worker = [&](int thread_id) {
        std::mt19937 rng(thread_id);
        std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
        std::uniform_int_distribution<int> op_dist(0, 9);

        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = "key" + std::to_string(key_dist(rng));
            int op = op_dist(rng);

            kvlite::Status s;
            if (op < 5) {  // 50% writes
                s = db_.put(key, "value_" + std::to_string(thread_id) + "_" + std::to_string(i));
            } else if (op < 8) {  // 30% reads
                std::string value;
                s = db_.get(key, value);
                if (!s.ok() && !s.isNotFound()) {
                    errors++;
                }
                s = kvlite::Status::OK();  // NotFound is acceptable
            } else if (op < 9) {  // 10% removes
                s = db_.remove(key);
            } else {  // 10% exists
                bool exists;
                s = db_.exists(key, exists);
            }

            if (s.ok()) {
                total_ops++;
            } else {
                errors++;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(total_ops.load(), num_threads * ops_per_thread);
}

// --- ReadBatch Atomicity Test ---

TEST_F(ConcurrencyTest, ReadBatchAtomicity) {
    // Pre-populate with related keys
    ASSERT_TRUE(db_.put("account_balance", "1000").ok());
    ASSERT_TRUE(db_.put("account_pending", "0").ok());

    std::atomic<bool> stop{false};
    std::atomic<bool> inconsistency_found{false};

    // Writer thread modifies both keys atomically
    std::thread writer([&]() {
        int balance = 1000;
        int pending = 0;
        while (!stop) {
            // Transfer 10 from balance to pending
            balance -= 10;
            pending += 10;

            kvlite::WriteBatch batch;
            batch.put("account_balance", std::to_string(balance));
            batch.put("account_pending", std::to_string(pending));
            db_.write(batch);

            // Transfer back
            balance += 10;
            pending -= 10;

            kvlite::WriteBatch batch2;
            batch2.put("account_balance", std::to_string(balance));
            batch2.put("account_pending", std::to_string(pending));
            db_.write(batch2);
        }
    });

    // Reader thread uses ReadBatch to read both atomically
    std::thread reader([&]() {
        for (int i = 0; i < 1000 && !inconsistency_found; ++i) {
            kvlite::ReadBatch batch;
            batch.get("account_balance");
            batch.get("account_pending");
            db_.read(batch);

            const auto& results = batch.results();
            if (results.size() == 2 && results[0].status.ok() && results[1].status.ok()) {
                int balance = std::stoi(results[0].value);
                int pending = std::stoi(results[1].value);
                // Total should always be 1000
                if (balance + pending != 1000) {
                    inconsistency_found = true;
                }
            }
        }
    });

    reader.join();
    stop = true;
    writer.join();

    EXPECT_FALSE(inconsistency_found.load());
}

// --- WriteBatch All-or-Nothing Visibility ---

TEST_F(ConcurrencyTest, WriteBatchAllOrNothingViaReadBatch) {
    // A writer thread continuously writes 3-key batches.
    // A reader thread uses ReadBatch (snapshot-isolated) to atomically
    // read all 3 keys and verify they share the same version.
    const int kBatches = 500;
    std::atomic<bool> stop{false};
    std::atomic<bool> violation{false};

    std::thread writer([&]() {
        for (int i = 0; i < kBatches && !violation; ++i) {
            kvlite::WriteBatch batch;
            std::string val = std::to_string(i);
            batch.put("atom_k1", val);
            batch.put("atom_k2", val);
            batch.put("atom_k3", val);
            EXPECT_TRUE(db_.write(batch).ok());
        }
        stop = true;
    });

    std::thread reader([&]() {
        while (!stop && !violation) {
            kvlite::ReadBatch rbatch;
            rbatch.get("atom_k1");
            rbatch.get("atom_k2");
            rbatch.get("atom_k3");
            auto s = db_.read(rbatch);
            if (!s.ok()) continue;

            const auto& results = rbatch.results();
            if (results.size() != 3) continue;

            // Before the first batch lands, all may be NotFound.
            if (results[0].status.isNotFound() && results[1].status.isNotFound() &&
                results[2].status.isNotFound()) continue;

            // All-or-nothing: if any key is found, all must be found.
            if (results[0].status.isNotFound() || results[1].status.isNotFound() ||
                results[2].status.isNotFound()) {
                violation = true;
                break;
            }

            // All found — values must match (same batch wrote them).
            if (results[0].value != results[1].value ||
                results[1].value != results[2].value) {
                violation = true;
                break;
            }
        }
    });

    writer.join();
    reader.join();

    EXPECT_FALSE(violation.load()) << "Partial batch visibility detected";
}

TEST_F(ConcurrencyTest, SnapshotDoesNotSeeUncommittedVersion) {
    // Verify that a snapshot taken during concurrent puts never sees
    // a version whose WB insertion hasn't completed yet.
    //
    // Strategy: N writer threads continuously put unique keys. A reader
    // thread repeatedly creates a snapshot, reads every key that should
    // exist at that snapshot version, and verifies the value is present
    // (not missing due to a half-committed version).
    const int kWriters = 4;
    const int kOpsPerWriter = 500;
    std::atomic<bool> writers_done{false};
    std::atomic<bool> violation{false};

    // Each writer writes keys "wT_I" with value "vT_I".
    auto writer = [&](int tid) {
        for (int i = 0; i < kOpsPerWriter && !violation; ++i) {
            std::string key = "w" + std::to_string(tid) + "_" + std::to_string(i);
            std::string val = "v" + std::to_string(tid) + "_" + std::to_string(i);
            EXPECT_TRUE(db_.put(key, val).ok());
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kWriters; ++t) {
        threads.emplace_back(writer, t);
    }

    // Reader: take snapshots and verify every key readable at that snapshot
    // actually returns a value (not NotFound due to uncommitted version).
    std::thread reader([&]() {
        while (!writers_done && !violation) {
            kvlite::Snapshot snap = db_.createSnapshot();
            uint64_t snap_ver = snap.version();

            // Read a few keys that *should* exist if their version <= snap_ver.
            // We can't know exactly which keys exist, but any key returned by
            // get-without-snapshot whose version <= snap_ver must also be
            // returned by get-with-snapshot.
            //
            // Simpler check: every key we successfully read without a snapshot
            // and whose version <= snap_ver must be readable with the snapshot.
            for (int tid = 0; tid < kWriters; ++tid) {
                for (int i = 0; i < 10; ++i) {
                    std::string key = "w" + std::to_string(tid) + "_" + std::to_string(i);
                    std::string val_current;
                    uint64_t ver;
                    auto s = db_.get(key, val_current, ver);
                    if (!s.ok()) continue;  // not yet written

                    if (ver <= snap_ver) {
                        // Must be visible through the snapshot.
                        std::string val_snap;
                        auto s2 = db_.get(key, val_snap, snapOpts(snap));
                        if (s2.isNotFound()) {
                            violation = true;
                            break;
                        }
                    }
                }
                if (violation) break;
            }
            db_.releaseSnapshot(snap);
        }
    });

    for (auto& t : threads) {
        t.join();
    }
    writers_done = true;
    reader.join();

    EXPECT_FALSE(violation.load())
        << "Snapshot missed a committed version (uncommitted version leaked)";
}

TEST_F(ConcurrencyTest, SnapshotGetWaitsForInFlightPuts) {
    // Verify that snapshot-based reads correctly wait for all versions
    // up to the snapshot version to be committed.
    //
    // Writer threads do rapid puts. A reader takes a snapshot immediately
    // after a put, then reads back through the snapshot. The snapshot
    // version may include in-flight puts from other threads, but the
    // get() must wait for them before returning.
    const int kWriters = 4;
    const int kOpsPerWriter = 500;
    std::atomic<bool> done{false};
    std::atomic<bool> violation{false};

    auto writer = [&](int tid) {
        for (int i = 0; i < kOpsPerWriter && !violation; ++i) {
            std::string key = "snap_wait_" + std::to_string(tid) + "_" + std::to_string(i);
            EXPECT_TRUE(db_.put(key, "val").ok());
        }
    };

    // Reader: continuously create snapshots and verify keys are visible.
    std::thread reader([&]() {
        int checks = 0;
        while (!done && !violation && checks < 2000) {
            kvlite::Snapshot snap = db_.createSnapshot();
            // Any key whose version <= snap.version() must be readable.
            // We just wrote keys — try reading them through the snapshot.
            // The snapshot version may be ahead of committed_version_,
            // so get() must wait. If it doesn't wait, we'd get NotFound
            // for a key that exists.
            for (int tid = 0; tid < kWriters && !violation; ++tid) {
                std::string key = "snap_wait_" + std::to_string(tid) + "_0";
                std::string val;
                uint64_t ver;
                auto s = db_.get(key, val, ver);
                if (!s.ok()) continue;

                if (ver <= snap.version()) {
                    auto s2 = db_.get(key, val, snapOpts(snap));
                    if (s2.isNotFound()) {
                        violation = true;
                    }
                }
            }
            db_.releaseSnapshot(snap);
            ++checks;
        }
    });

    std::vector<std::thread> threads;
    for (int t = 0; t < kWriters; ++t) {
        threads.emplace_back(writer, t);
    }
    for (auto& t : threads) {
        t.join();
    }
    done = true;
    reader.join();

    EXPECT_FALSE(violation.load())
        << "Snapshot get returned NotFound for a committed key";
}

TEST_F(ConcurrencyTest, SnapshotExistsWaitsForInFlightPuts) {
    // Same as above but using exists() instead of get().
    const int kWriters = 4;
    const int kOpsPerWriter = 500;
    std::atomic<bool> done{false};
    std::atomic<bool> violation{false};

    auto writer = [&](int tid) {
        for (int i = 0; i < kOpsPerWriter && !violation; ++i) {
            std::string key = "exists_wait_" + std::to_string(tid) + "_" + std::to_string(i);
            EXPECT_TRUE(db_.put(key, "val").ok());
        }
    };

    std::thread reader([&]() {
        int checks = 0;
        while (!done && !violation && checks < 2000) {
            kvlite::Snapshot snap = db_.createSnapshot();
            for (int tid = 0; tid < kWriters && !violation; ++tid) {
                std::string key = "exists_wait_" + std::to_string(tid) + "_0";
                std::string val;
                uint64_t ver;
                auto s = db_.get(key, val, ver);
                if (!s.ok()) continue;

                if (ver <= snap.version()) {
                    bool found = false;
                    auto s2 = db_.exists(key, found, snapOpts(snap));
                    if (s2.ok() && !found) {
                        violation = true;
                    }
                }
            }
            db_.releaseSnapshot(snap);
            ++checks;
        }
    });

    std::vector<std::thread> threads;
    for (int t = 0; t < kWriters; ++t) {
        threads.emplace_back(writer, t);
    }
    for (auto& t : threads) {
        t.join();
    }
    done = true;
    reader.join();

    EXPECT_FALSE(violation.load())
        << "Snapshot exists() missed a committed key";
}

TEST_F(ConcurrencyTest, WriteBatchSameVersionUnderConcurrency) {
    // Multiple threads write batches concurrently.
    // After all threads finish, verify every batch's keys share one version.
    const int kThreads = 4;
    const int kBatchesPerThread = 200;
    const int kKeysPerBatch = 5;

    auto writer = [&](int tid) {
        for (int b = 0; b < kBatchesPerThread; ++b) {
            kvlite::WriteBatch batch;
            std::string prefix = "t" + std::to_string(tid) +
                                 "_b" + std::to_string(b) + "_";
            for (int k = 0; k < kKeysPerBatch; ++k) {
                batch.put(prefix + "k" + std::to_string(k),
                          "v" + std::to_string(k));
            }
            EXPECT_TRUE(db_.write(batch).ok());
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back(writer, t);
    }
    for (auto& t : threads) {
        t.join();
    }

    // Collect all key→version pairs via iterator.
    std::unique_ptr<kvlite::Iterator> iter;
    ASSERT_TRUE(db_.createIterator(iter).ok());

    std::map<std::string, uint64_t> key_versions;
    {
        std::string key, value;
        uint64_t version;
        while (iter->next(key, value, version).ok()) {
            key_versions[key] = version;
        }
    }

    // Verify: every batch's keys share one version.
    for (int tid = 0; tid < kThreads; ++tid) {
        for (int b = 0; b < kBatchesPerThread; ++b) {
            std::string prefix = "t" + std::to_string(tid) +
                                 "_b" + std::to_string(b) + "_";

            std::string first_key = prefix + "k0";
            auto it0 = key_versions.find(first_key);
            ASSERT_NE(it0, key_versions.end()) << "Missing: " << first_key;
            uint64_t expected_ver = it0->second;

            for (int k = 1; k < kKeysPerBatch; ++k) {
                std::string key = prefix + "k" + std::to_string(k);
                auto it = key_versions.find(key);
                ASSERT_NE(it, key_versions.end()) << "Missing: " << key;
                EXPECT_EQ(it->second, expected_ver)
                    << "Version mismatch in batch " << prefix
                    << " key " << k;
            }
        }
    }
}
