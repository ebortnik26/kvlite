#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "kvlite/db.h"
#include "kvlite/options.h"
#include "kvlite/status.h"
#include "zipfian_generator.h"

#include <kll_sketch.hpp>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct Config {
    int threads = 1;
    uint64_t num_ops = 1'000'000;
    uint64_t num_keys = 1'000'000;
    int put_pct = 50;
    int get_pct = 50;
    int key_size = 16;
    int value_size = 100;
    bool preload = true;
    int report_interval = 5;
    size_t memtable_size = 64 * 1024 * 1024;
    bool buffered_writes = true;
    std::string key_dist = "uniform";
    bool extended = false;
    std::string flame_output;  // empty = disabled
    std::string db_path;
};

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

static void printUsage(const char* prog) {
    std::fprintf(stderr,
        "Usage: %s [OPTIONS] <db_path>\n"
        "\n"
        "  --threads N            Worker threads (default: 1)\n"
        "  --num-ops N            Total operations in timed phase (default: 1000000)\n"
        "  --num-keys N           Key space size (default: 1000000)\n"
        "  --put-pct N            Put percentage 0-100 (default: 50)\n"
        "  --get-pct N            Get percentage 0-100 (default: 50)\n"
        "  --key-size N           Key size in bytes (default: 16)\n"
        "  --value-size N         Value size in bytes (default: 100)\n"
        "  --no-preload           Skip preload phase\n"
        "  --report-interval N    Seconds between reports (default: 5)\n"
        "  --memtable-size N      Memtable capacity in bytes (default: 67108864)\n"
        "  --key-dist NAME        Key distribution: uniform or zipf [YCSB] (default: uniform)\n"
        "  --no-buffered-writes   Disable LogFile write buffering\n"
        "  -x, --extended         Print extended report (DB stats, DHT codec stats)\n"
        "  -f, --flame [PATH]     Generate flame graph SVG (default: flamegraph.svg)\n"
        "  --help                 Show this message\n",
        prog);
}

static bool parseArgs(int argc, char** argv, Config& cfg) {
    for (int i = 1; i < argc; ++i) {
        auto arg = [&](const char* name) {
            return std::strcmp(argv[i], name) == 0;
        };
        auto nextInt = [&]() -> int64_t {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "Missing value for %s\n", argv[i]);
                std::exit(1);
            }
            return std::atoll(argv[++i]);
        };

        if (arg("--help") || arg("-h")) { printUsage(argv[0]); std::exit(0); }
        else if (arg("--threads"))           cfg.threads = static_cast<int>(nextInt());
        else if (arg("--num-ops"))           cfg.num_ops = static_cast<uint64_t>(nextInt());
        else if (arg("--num-keys"))          cfg.num_keys = static_cast<uint64_t>(nextInt());
        else if (arg("--put-pct"))           cfg.put_pct = static_cast<int>(nextInt());
        else if (arg("--get-pct"))           cfg.get_pct = static_cast<int>(nextInt());
        else if (arg("--key-size"))          cfg.key_size = static_cast<int>(nextInt());
        else if (arg("--value-size"))        cfg.value_size = static_cast<int>(nextInt());
        else if (arg("--no-preload"))        cfg.preload = false;
        else if (arg("--no-buffered-writes")) cfg.buffered_writes = false;
        else if (arg("--report-interval"))   cfg.report_interval = static_cast<int>(nextInt());
        else if (arg("--memtable-size")) cfg.memtable_size = static_cast<size_t>(nextInt());
        else if (arg("--key-dist")) {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "Missing value for %s\n", argv[i]);
                std::exit(1);
            }
            cfg.key_dist = argv[++i];
        }
        else if (arg("-x") || arg("--extended")) cfg.extended = true;
        else if (arg("-f") || arg("--flame")) {
            // Optional path argument: next arg that doesn't start with '-'
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                cfg.flame_output = argv[++i];
            } else {
                cfg.flame_output = "flamegraph.svg";
            }
        }
        else if (argv[i][0] == '-') {
            std::fprintf(stderr, "Unknown flag: %s\n", argv[i]);
            return false;
        } else {
            cfg.db_path = argv[i];
        }
    }
    return true;
}

static bool validateConfig(const Config& cfg) {
    if (cfg.db_path.empty()) {
        std::fprintf(stderr, "Error: db_path is required\n");
        return false;
    }
    if (cfg.put_pct + cfg.get_pct != 100) {
        std::fprintf(stderr,
            "Error: --put-pct (%d) + --get-pct (%d) = %d, must equal 100\n",
            cfg.put_pct, cfg.get_pct, cfg.put_pct + cfg.get_pct);
        return false;
    }
    if (cfg.threads < 1) {
        std::fprintf(stderr, "Error: --threads must be >= 1\n");
        return false;
    }
    if (cfg.key_size < 1) {
        std::fprintf(stderr, "Error: --key-size must be >= 1\n");
        return false;
    }
    if (cfg.key_dist != "uniform" && cfg.key_dist != "zipf") {
        std::fprintf(stderr,
            "Error: --key-dist must be \"uniform\" or \"zipf\", got \"%s\"\n",
            cfg.key_dist.c_str());
        return false;
    }
    // Check that key_size can represent num_keys distinct keys.
    // With zero-padded decimal, key_size digits can represent 10^key_size values.
    // But we cap the check at 18 digits (uint64_t max is ~1.8e19).
    if (cfg.key_size < 18) {
        uint64_t max_distinct = 1;
        for (int i = 0; i < cfg.key_size; ++i) {
            max_distinct *= 10;
            if (max_distinct >= cfg.num_keys) break;
        }
        if (max_distinct < cfg.num_keys) {
            std::fprintf(stderr,
                "Error: --key-size %d can represent at most %llu distinct keys, "
                "but --num-keys is %llu\n",
                cfg.key_size,
                static_cast<unsigned long long>(max_distinct),
                static_cast<unsigned long long>(cfg.num_keys));
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------
// Key / Value generation
// ---------------------------------------------------------------------------

static std::string makeKey(uint64_t id, int key_size) {
    // Zero-padded decimal string, e.g. "0000000000000042" for key_size=16
    std::string key(static_cast<size_t>(key_size), '0');
    int pos = key_size - 1;
    while (id > 0 && pos >= 0) {
        key[static_cast<size_t>(pos--)] = '0' + static_cast<char>(id % 10);
        id /= 10;
    }
    return key;
}

static std::string makeValue(uint64_t id, int value_size) {
    std::string value(static_cast<size_t>(value_size), '\0');
    // Deterministic fill via multiplicative hash
    uint64_t h = id * 0x9E3779B97F4A7C15ULL;
    for (int i = 0; i < value_size; ++i) {
        value[static_cast<size_t>(i)] = static_cast<char>(
            'A' + static_cast<char>(h % 26));
        h = h * 6364136223846793005ULL + 1442695040888963407ULL;
    }
    return value;
}

// ---------------------------------------------------------------------------
// Op types
// ---------------------------------------------------------------------------

enum class OpType { kPut = 0, kGet = 1, kCount = 2 };

// ---------------------------------------------------------------------------
// Per-thread state
// ---------------------------------------------------------------------------

static constexpr int kNumOpTypes = static_cast<int>(OpType::kCount);

struct ThreadState {
    std::mutex mu;  // protects sketches during reporter merge

    // Latency sketches (microseconds)
    datasketches::kll_sketch<double> interval_sketch[kNumOpTypes];
    datasketches::kll_sketch<double> cumulative_sketch[kNumOpTypes];

    // Lock-free counters
    std::atomic<uint64_t> put_count{0};
    std::atomic<uint64_t> get_found{0};
    std::atomic<uint64_t> get_not_found{0};
    std::atomic<uint64_t> errors{0};

    // Reporter-owned snapshots for delta computation
    uint64_t prev_put_count = 0;
    uint64_t prev_get_found = 0;
    uint64_t prev_get_not_found = 0;

    ThreadState()
        : interval_sketch{datasketches::kll_sketch<double>(200),
                          datasketches::kll_sketch<double>(200)},
          cumulative_sketch{datasketches::kll_sketch<double>(200),
                            datasketches::kll_sketch<double>(200)} {}
};

// ---------------------------------------------------------------------------
// Timing helper
// ---------------------------------------------------------------------------

using Clock = std::chrono::steady_clock;

static double elapsedUs(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double, std::micro>(end - start).count();
}

static double elapsedSec(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration<double>(end - start).count();
}

// ---------------------------------------------------------------------------
// Preload
// ---------------------------------------------------------------------------

static bool preload(kvlite::DB& db, const Config& cfg) {
    std::printf("Preloading %llu keys...\n",
                static_cast<unsigned long long>(cfg.num_keys));
    auto t0 = Clock::now();

    for (uint64_t i = 0; i < cfg.num_keys; ++i) {
        auto s = db.put(makeKey(i, cfg.key_size),
                        makeValue(i, cfg.value_size));
        if (!s.ok()) {
            std::fprintf(stderr, "Preload error at key %llu: %s\n",
                         static_cast<unsigned long long>(i),
                         s.toString().c_str());
            return false;
        }
        if ((i + 1) % 100'000 == 0) {
            std::printf("  preloaded %llu / %llu\n",
                        static_cast<unsigned long long>(i + 1),
                        static_cast<unsigned long long>(cfg.num_keys));
        }
    }

    double dur = elapsedSec(t0, Clock::now());
    std::printf("Preload complete: %llu keys in %.1fs (%.0f keys/sec)\n\n",
                static_cast<unsigned long long>(cfg.num_keys), dur,
                static_cast<double>(cfg.num_keys) / dur);
    return true;
}

// ---------------------------------------------------------------------------
// Worker thread
// ---------------------------------------------------------------------------

static void workerThread(kvlite::DB& db, const Config& cfg,
                         ThreadState& state,
                         std::atomic<uint64_t>& global_ops_done,
                         uint64_t total_ops, int thread_id) {
    std::mt19937_64 rng(static_cast<uint64_t>(thread_id) * 7919 + 42);
    std::uniform_int_distribution<uint64_t> uniform_key_dist(0, cfg.num_keys - 1);
    std::uniform_int_distribution<int> op_dist(0, 99);

    // Optional Zipfian generator (constructed only when needed).
    // ZipfianGenerator precomputes zeta_n which is O(num_keys), so we build
    // it once per thread rather than per-operation.
    std::unique_ptr<ZipfianGenerator> zipf_gen;
    if (cfg.key_dist == "zipf") {
        zipf_gen = std::make_unique<ZipfianGenerator>(cfg.num_keys);
    }

    auto nextKey = [&]() -> uint64_t {
        if (zipf_gen) return (*zipf_gen)(rng);
        return uniform_key_dist(rng);
    };

    while (true) {
        uint64_t my_op = global_ops_done.fetch_add(1, std::memory_order_relaxed);
        if (my_op >= total_ops) break;

        uint64_t key_id = nextKey();
        int roll = op_dist(rng);

        OpType op;
        if (roll < cfg.put_pct) {
            op = OpType::kPut;
        } else {
            op = OpType::kGet;
        }

        auto t0 = Clock::now();

        if (op == OpType::kPut) {
            auto s = db.put(makeKey(key_id, cfg.key_size),
                            makeValue(key_id, cfg.value_size));
            auto t1 = Clock::now();
            double lat = elapsedUs(t0, t1);
            {
                std::lock_guard<std::mutex> lk(state.mu);
                state.interval_sketch[0].update(lat);
                state.cumulative_sketch[0].update(lat);
            }
            state.put_count.fetch_add(1, std::memory_order_relaxed);
            if (!s.ok()) {
                state.errors.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            std::string value;
            auto s = db.get(makeKey(key_id, cfg.key_size), value);
            auto t1 = Clock::now();
            double lat = elapsedUs(t0, t1);
            {
                std::lock_guard<std::mutex> lk(state.mu);
                state.interval_sketch[1].update(lat);
                state.cumulative_sketch[1].update(lat);
            }
            if (s.ok()) {
                state.get_found.fetch_add(1, std::memory_order_relaxed);
            } else if (s.isNotFound()) {
                state.get_not_found.fetch_add(1, std::memory_order_relaxed);
            } else {
                state.errors.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Report printing
// ---------------------------------------------------------------------------

static void printReportHeader() {
    std::printf("%-5s %10s %10s %10s %10s %10s %10s %10s\n",
                "Op", "Count", "Found", "Miss",
                "p50(us)", "p99(us)", "p99.9", "p99.99");
}

static void printReportRow(const char* name,
                           uint64_t count, uint64_t found, uint64_t miss,
                           datasketches::kll_sketch<double>& sketch) {
    if (count == 0) {
        std::printf("%-5s %10llu %10s %10s %10s %10s %10s %10s\n",
                    name,
                    static_cast<unsigned long long>(count),
                    "-", "-", "-", "-", "-", "-");
        return;
    }

    double p50    = sketch.get_quantile(0.5);
    double p99    = sketch.get_quantile(0.99);
    double p999   = sketch.get_quantile(0.999);
    double p9999  = sketch.get_quantile(0.9999);

    auto fmtFound = [&]() -> std::string {
        if (found == static_cast<uint64_t>(-1)) return "-";
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%llu",
                      static_cast<unsigned long long>(found));
        return buf;
    };
    auto fmtMiss = [&]() -> std::string {
        if (miss == static_cast<uint64_t>(-1)) return "-";
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%llu",
                      static_cast<unsigned long long>(miss));
        return buf;
    };

    std::printf("%-5s %10llu %10s %10s %10.1f %10.1f %10.1f %10.1f\n",
                name,
                static_cast<unsigned long long>(count),
                fmtFound().c_str(),
                fmtMiss().c_str(),
                p50, p99, p999, p9999);
}

// ---------------------------------------------------------------------------
// Reporter thread
// ---------------------------------------------------------------------------

static void reporterThread(std::vector<ThreadState>& states,
                           std::atomic<bool>& done,
                           int interval_sec,
                           kvlite::DB& db) {
    using namespace std::chrono;
    auto next_report = Clock::now() + seconds(interval_sec);

    uint64_t prev_stall_count = 0;
    uint64_t prev_stall_us = 0;

    while (!done.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_until(next_report);
        if (done.load(std::memory_order_relaxed)) break;
        next_report += seconds(interval_sec);

        // Merge interval sketches from all threads
        datasketches::kll_sketch<double> merged[kNumOpTypes] = {
            datasketches::kll_sketch<double>(200),
            datasketches::kll_sketch<double>(200)
        };
        uint64_t total_puts = 0, total_found = 0, total_miss = 0;

        for (auto& st : states) {
            uint64_t pc = st.put_count.load(std::memory_order_relaxed);
            uint64_t gf = st.get_found.load(std::memory_order_relaxed);
            uint64_t gm = st.get_not_found.load(std::memory_order_relaxed);

            total_puts += (pc - st.prev_put_count);
            total_found += (gf - st.prev_get_found);
            total_miss += (gm - st.prev_get_not_found);

            st.prev_put_count = pc;
            st.prev_get_found = gf;
            st.prev_get_not_found = gm;

            std::lock_guard<std::mutex> lk(st.mu);
            for (int op = 0; op < kNumOpTypes; ++op) {
                if (!st.interval_sketch[op].is_empty()) {
                    merged[op].merge(st.interval_sketch[op]);
                    // Reset interval sketch
                    st.interval_sketch[op] = datasketches::kll_sketch<double>(200);
                }
            }
        }

        // Stall deltas
        kvlite::DBStats stats;
        uint64_t d_stall_count = 0;
        double d_stall_ms = 0.0;
        if (db.getStats(stats).ok()) {
            d_stall_count = stats.stall_count - prev_stall_count;
            d_stall_ms = static_cast<double>(stats.stall_total_us - prev_stall_us) / 1000.0;
            prev_stall_count = stats.stall_count;
            prev_stall_us = stats.stall_total_us;
        }

        uint64_t total_get = total_found + total_miss;
        uint64_t total_ops = total_puts + total_get;
        double rate = static_cast<double>(total_ops) / interval_sec;

        std::printf("\n--- Interval (%ds) ---\n", interval_sec);
        printReportHeader();
        printReportRow("put", total_puts,
                       static_cast<uint64_t>(-1),
                       static_cast<uint64_t>(-1),
                       merged[0]);
        printReportRow("get", total_get, total_found, total_miss, merged[1]);
        std::printf("Total: %llu ops  (%.0f ops/sec)",
                    static_cast<unsigned long long>(total_ops), rate);
        if (d_stall_count > 0) {
            std::printf("  |  stalls: %llu (%.1f ms)",
                        static_cast<unsigned long long>(d_stall_count),
                        d_stall_ms);
        }
        std::printf("\n");
        std::fflush(stdout);
    }
}

// ---------------------------------------------------------------------------
// Final report
// ---------------------------------------------------------------------------

static void printFinalReport(std::vector<ThreadState>& states,
                             double elapsed_sec,
                             kvlite::DB& db,
                             bool extended = false) {
    // Merge cumulative sketches
    datasketches::kll_sketch<double> merged[kNumOpTypes] = {
        datasketches::kll_sketch<double>(200),
        datasketches::kll_sketch<double>(200)
    };
    uint64_t total_puts = 0, total_found = 0, total_miss = 0, total_errors = 0;

    for (auto& st : states) {
        total_puts += st.put_count.load(std::memory_order_relaxed);
        total_found += st.get_found.load(std::memory_order_relaxed);
        total_miss += st.get_not_found.load(std::memory_order_relaxed);
        total_errors += st.errors.load(std::memory_order_relaxed);

        std::lock_guard<std::mutex> lk(st.mu);
        for (int op = 0; op < kNumOpTypes; ++op) {
            if (!st.cumulative_sketch[op].is_empty()) {
                merged[op].merge(st.cumulative_sketch[op]);
            }
        }
    }

    uint64_t total_get = total_found + total_miss;
    uint64_t total_ops = total_puts + total_get;

    std::printf("\n========== FINAL REPORT ==========\n");
    std::printf("Elapsed: %.2f s\n", elapsed_sec);
    std::printf("Total ops: %llu  (%.0f ops/sec)\n",
                static_cast<unsigned long long>(total_ops),
                static_cast<double>(total_ops) / elapsed_sec);
    if (total_errors > 0) {
        std::printf("Errors: %llu\n",
                    static_cast<unsigned long long>(total_errors));
    }
    std::printf("\n");
    printReportHeader();
    printReportRow("put", total_puts,
                   static_cast<uint64_t>(-1),
                   static_cast<uint64_t>(-1),
                   merged[0]);
    printReportRow("get", total_get, total_found, total_miss, merged[1]);

    if (extended) {
        kvlite::DBStats stats;
        auto s = db.getStats(stats);
        if (s.ok()) {
            std::printf("\n--- DB Stats ---\n");
            std::printf("Log files:    %llu\n",
                        static_cast<unsigned long long>(stats.num_log_files));
            std::printf("Total size:   %llu bytes (%.1f MB)\n",
                        static_cast<unsigned long long>(stats.total_log_size),
                        static_cast<double>(stats.total_log_size) / (1024.0 * 1024.0));
            std::printf("Live entries: %llu\n",
                        static_cast<unsigned long long>(stats.num_live_entries));
            std::printf("Version:      %llu\n",
                        static_cast<unsigned long long>(stats.current_version));

            std::printf("\n--- Background Stats ---\n");
            std::printf("%-7s %8s %12s %12s\n",
                        "Task", "Count", "Total(ms)", "Avg(ms)");
            auto bgRow = [](const char* name, uint64_t count, uint64_t total_us) {
                double total_ms = static_cast<double>(total_us) / 1000.0;
                double avg_ms = count > 0 ? total_ms / static_cast<double>(count) : 0.0;
                std::printf("%-7s %8llu %12.1f %12.1f\n",
                            name,
                            static_cast<unsigned long long>(count),
                            total_ms, avg_ms);
            };
            bgRow("flush", stats.flush_count, stats.flush_total_us);
            bgRow("gc", stats.gc_count, stats.gc_total_us);
            bgRow("savept", stats.savepoint_count, stats.savepoint_total_us);
            bgRow("stall", stats.stall_count, stats.stall_total_us);

            std::printf("\n--- DHT Codec Stats ---\n");
            std::printf("%-7s %12s %12s %12s\n",
                        "Op", "Count", "Total(ms)", "Avg(us)");
            auto codecRow = [](const char* name, uint64_t count, uint64_t total_ns) {
                double total_ms = static_cast<double>(total_ns) / 1e6;
                double avg_us = count > 0
                    ? static_cast<double>(total_ns) / (static_cast<double>(count) * 1e3)
                    : 0.0;
                std::printf("%-7s %12llu %12.1f %12.3f\n",
                            name,
                            static_cast<unsigned long long>(count),
                            total_ms, avg_us);
            };
            codecRow("encode", stats.dht_encode_count, stats.dht_encode_total_ns);
            codecRow("decode", stats.dht_decode_count, stats.dht_decode_total_ns);
            double ext_ratio = stats.dht_num_buckets > 0
                ? static_cast<double>(stats.dht_ext_count) / stats.dht_num_buckets
                : 0.0;
            std::printf("Ext/bucket ratio: %u / %u (%.4f)\n",
                        stats.dht_ext_count, stats.dht_num_buckets, ext_ratio);
        }
    }

    std::printf("==================================\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    Config cfg;
    if (!parseArgs(argc, argv, cfg) || !validateConfig(cfg)) {
        printUsage(argv[0]);
        return 1;
    }

    // --- Flame graph: re-exec under perf, then post-process ---
    if (!cfg.flame_output.empty()) {
        // Check for perf
        if (std::system("command -v perf >/dev/null 2>&1") != 0) {
            std::fprintf(stderr,
                "Error: 'perf' not found. Install linux-tools-generic or "
                "the perf binary for your kernel.\n");
            return 1;
        }
        // Check for FlameGraph scripts
        const char* fg_dir = std::getenv("FLAMEGRAPH_DIR");
        std::string stackcollapse = fg_dir
            ? std::string(fg_dir) + "/stackcollapse-perf.pl"
            : "stackcollapse-perf.pl";
        std::string flamegraph_pl = fg_dir
            ? std::string(fg_dir) + "/flamegraph.pl"
            : "flamegraph.pl";

        std::string check_cmd = "command -v " + stackcollapse + " >/dev/null 2>&1";
        if (std::system(check_cmd.c_str()) != 0) {
            std::fprintf(stderr,
                "Error: '%s' not found. Clone FlameGraph repo and add to PATH "
                "or set FLAMEGRAPH_DIR.\n", stackcollapse.c_str());
            return 1;
        }

        std::string perf_data = "/tmp/kvbench_perf_" + std::to_string(getpid()) + ".data";
        std::string output_svg = cfg.flame_output;

        // Build child argv: same args minus -f/--flame (and its optional path)
        std::vector<std::string> child_args;
        child_args.push_back(argv[0]);
        for (int i = 1; i < argc; ++i) {
            if (std::strcmp(argv[i], "-f") == 0 || std::strcmp(argv[i], "--flame") == 0) {
                // Skip the flag and its optional path argument
                if (i + 1 < argc && argv[i + 1][0] != '-') ++i;
                continue;
            }
            child_args.push_back(argv[i]);
        }

        // Build perf record command
        std::vector<std::string> perf_args;
        perf_args.push_back("perf");
        perf_args.push_back("record");
        perf_args.push_back("-g");
        perf_args.push_back("--call-graph");
        perf_args.push_back("dwarf");
        perf_args.push_back("-o");
        perf_args.push_back(perf_data);
        perf_args.push_back("--");
        for (auto& a : child_args) perf_args.push_back(a);

        // Convert to char*[]
        std::vector<char*> exec_argv;
        for (auto& s : perf_args) exec_argv.push_back(&s[0]);
        exec_argv.push_back(nullptr);

        std::printf("Flame graph: recording with perf...\n");

        pid_t pid = fork();
        if (pid < 0) {
            std::perror("fork");
            return 1;
        }
        if (pid == 0) {
            // Child: exec perf record
            execvp(exec_argv[0], exec_argv.data());
            std::perror("execvp perf");
            _exit(127);
        }

        // Parent: wait for child
        int wstatus = 0;
        waitpid(pid, &wstatus, 0);
        if (!WIFEXITED(wstatus) || WEXITSTATUS(wstatus) != 0) {
            std::fprintf(stderr, "Warning: perf record exited with status %d\n",
                         WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : -1);
        }

        // Post-process: perf script | stackcollapse | flamegraph > svg
        std::string post_cmd =
            "perf script -i " + perf_data +
            " | " + stackcollapse +
            " | " + flamegraph_pl +
            " > " + output_svg;
        std::printf("Flame graph: generating %s...\n", output_svg.c_str());
        int rc = std::system(post_cmd.c_str());
        if (rc != 0) {
            std::fprintf(stderr, "Warning: flame graph generation failed (exit %d)\n", rc);
        } else {
            std::printf("Flame graph written to %s\n", output_svg.c_str());
        }

        // Cleanup
        std::remove(perf_data.c_str());
        return (rc == 0) ? 0 : 1;
    }

    std::printf("kvbench — kvlite benchmark tool\n");
    std::printf("  db_path:           %s\n", cfg.db_path.c_str());
    std::printf("  threads:           %d\n", cfg.threads);
    std::printf("  num_ops:           %llu\n",
                static_cast<unsigned long long>(cfg.num_ops));
    std::printf("  num_keys:          %llu\n",
                static_cast<unsigned long long>(cfg.num_keys));
    std::printf("  put_pct:           %d%%\n", cfg.put_pct);
    std::printf("  get_pct:           %d%%\n", cfg.get_pct);
    std::printf("  key_size:          %d\n", cfg.key_size);
    std::printf("  value_size:        %d\n", cfg.value_size);
    std::printf("  key_dist:          %s\n", cfg.key_dist.c_str());
    std::printf("  preload:           %s\n", cfg.preload ? "yes" : "no");
    std::printf("  report_interval:   %ds\n", cfg.report_interval);
    std::printf("  memtable_size: %llu\n",
                static_cast<unsigned long long>(cfg.memtable_size));
    std::printf("  buffered_writes:   %s\n", cfg.buffered_writes ? "yes" : "no");
    std::printf("\n");

    // Open DB
    kvlite::Options opts;
    opts.create_if_missing = true;
    opts.gc_interval_sec = 0;
    opts.verify_checksums = false;
    opts.memtable_size = cfg.memtable_size;
    opts.buffered_writes = cfg.buffered_writes;

    kvlite::DB db;
    auto s = db.open(cfg.db_path, opts);
    if (!s.ok()) {
        std::fprintf(stderr, "Failed to open DB: %s\n", s.toString().c_str());
        return 1;
    }

    // Preload
    if (cfg.preload) {
        if (!preload(db, cfg)) {
            db.close();
            return 1;
        }
    }

    // Prepare thread states
    std::vector<ThreadState> states(static_cast<size_t>(cfg.threads));
    std::atomic<uint64_t> global_ops_done{0};
    std::atomic<bool> reporter_done{false};

    // Start reporter
    std::thread reporter(reporterThread,
                         std::ref(states),
                         std::ref(reporter_done),
                         cfg.report_interval,
                         std::ref(db));

    // Start workers
    auto t0 = Clock::now();
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(cfg.threads));
    for (int i = 0; i < cfg.threads; ++i) {
        workers.emplace_back(workerThread,
                             std::ref(db), std::cref(cfg),
                             std::ref(states[static_cast<size_t>(i)]),
                             std::ref(global_ops_done),
                             cfg.num_ops, i);
    }

    // Wait for workers
    for (auto& w : workers) w.join();
    auto t1 = Clock::now();

    // Stop reporter
    reporter_done.store(true, std::memory_order_relaxed);
    reporter.join();

    double elapsed = elapsedSec(t0, t1);
    printFinalReport(states, elapsed, db, cfg.extended);

    s = db.close();
    if (!s.ok()) {
        std::fprintf(stderr, "Warning: DB close: %s\n", s.toString().c_str());
    }

    return 0;
}
