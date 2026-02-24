// Multi-threaded concurrency and atomicity tests for kvlite
#include <gtest/gtest.h>
#include <kvlite/kvlite.h>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <set>
#include <mutex>

namespace fs = std::filesystem;

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

    // Verify atomicity: all keys in a batch should have same version
    for (int t = 0; t < num_threads; ++t) {
        for (int b = 0; b < batches_per_thread; ++b) {
            std::string prefix = "t" + std::to_string(t) + "_b" + std::to_string(b) + "_";

            std::string v1, v2, v3;
            uint64_t ver1, ver2, ver3;

            ASSERT_TRUE(db_.get(prefix + "k1", v1, ver1).ok());
            ASSERT_TRUE(db_.get(prefix + "k2", v2, ver2).ok());
            ASSERT_TRUE(db_.get(prefix + "k3", v3, ver3).ok());

            EXPECT_EQ(ver1, ver2) << "Batch " << prefix << " not atomic";
            EXPECT_EQ(ver2, ver3) << "Batch " << prefix << " not atomic";
        }
    }
}

// --- Snapshot Isolation Tests ---

TEST_F(ConcurrencyTest, SnapshotIsolation) {
    ASSERT_TRUE(db_.put("key", "initial").ok());

    std::unique_ptr<kvlite::DB::Snapshot> snapshot;
    ASSERT_TRUE(db_.createSnapshot(snapshot).ok());

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
            if (snapshot->get("key", value).ok()) {
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

    db_.releaseSnapshot(std::move(snapshot));
}

TEST_F(ConcurrencyTest, ConcurrentSnapshots) {
    ASSERT_TRUE(db_.put("counter", "0").ok());

    const int num_threads = 4;
    std::vector<std::unique_ptr<kvlite::DB::Snapshot>> snapshots(num_threads);
    std::vector<std::string> expected_values(num_threads);

    // Create snapshots interleaved with writes
    for (int i = 0; i < num_threads; ++i) {
        expected_values[i] = std::to_string(i);
        ASSERT_TRUE(db_.put("counter", expected_values[i]).ok());
        ASSERT_TRUE(db_.createSnapshot(snapshots[i]).ok());
    }

    // Write more
    ASSERT_TRUE(db_.put("counter", "final").ok());

    // Each snapshot should see its expected value
    for (int i = 0; i < num_threads; ++i) {
        std::string value;
        ASSERT_TRUE(snapshots[i]->get("counter", value).ok());
        EXPECT_EQ(value, expected_values[i]);
    }

    for (auto& snap : snapshots) {
        db_.releaseSnapshot(std::move(snap));
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

    std::unique_ptr<kvlite::DB::Iterator> iter;
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

    std::mutex versions_mutex;
    std::vector<uint64_t> all_versions;

    auto writer = [&](int thread_id) {
        for (int i = 0; i < ops_per_thread; ++i) {
            std::string key = "key_" + std::to_string(thread_id) + "_" + std::to_string(i);
            ASSERT_TRUE(db_.put(key, "value").ok());

            std::string value;
            uint64_t version;
            ASSERT_TRUE(db_.get(key, value, version).ok());

            std::lock_guard<std::mutex> lock(versions_mutex);
            all_versions.push_back(version);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(writer, i);
    }

    for (auto& t : threads) {
        t.join();
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

    // Verify: every batch's keys share one version.
    for (int tid = 0; tid < kThreads; ++tid) {
        for (int b = 0; b < kBatchesPerThread; ++b) {
            std::string prefix = "t" + std::to_string(tid) +
                                 "_b" + std::to_string(b) + "_";

            uint64_t expected_ver = 0;
            for (int k = 0; k < kKeysPerBatch; ++k) {
                std::string key = prefix + "k" + std::to_string(k);
                std::string val;
                uint64_t ver;
                ASSERT_TRUE(db_.get(key, val, ver).ok()) << "Missing: " << key;
                if (k == 0) {
                    expected_ver = ver;
                } else {
                    EXPECT_EQ(ver, expected_ver)
                        << "Version mismatch in batch " << prefix
                        << " key " << k;
                }
            }
        }
    }
}
