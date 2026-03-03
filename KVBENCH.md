# kvbench — kvlite Benchmark Tool

A YCSB/db_bench-style benchmark tool for kvlite. Drives the public API with
configurable workload parameters and reports throughput and latency percentiles.

## Building

```bash
cmake -B build -DKVLITE_BUILD_TOOLS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --target kvbench
```

## Quick Start

```bash
# Default 50/50 read-write mix, 1M ops, 1 thread
./build/src/tools/kvbench /tmp/mydb

# Multi-threaded
./build/src/tools/kvbench --threads 4 /tmp/mydb
```

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--threads N` | 1 | Worker threads |
| `--num-ops N` | 1000000 | Total operations in timed phase |
| `--num-keys N` | 1000000 | Key space size |
| `--put-pct N` | 50 | Put percentage (0-100) |
| `--get-pct N` | 50 | Get percentage (0-100) |
| `--key-size N` | 16 | Key size in bytes |
| `--value-size N` | 100 | Value size in bytes |
| `--no-preload` | (preload on) | Skip preload phase |
| `--report-interval N` | 5 | Seconds between interval reports |
| `--write-buffer-size N` | 67108864 | Write buffer size in bytes (64 MB) |
| `--help` | | Show usage |

All `--*-pct` flags must sum to exactly 100.

## Workload Examples

```bash
# Read-heavy (95% get, 5% put)
./build/src/tools/kvbench --put-pct 5 --get-pct 95 /tmp/bench_read

# Write-heavy (95% put, 5% get)
./build/src/tools/kvbench --put-pct 95 --get-pct 5 /tmp/bench_write

# Write-only (no preload needed)
./build/src/tools/kvbench --put-pct 100 --get-pct 0 --no-preload /tmp/bench_wo

# Read-only (preload first, then 100% gets)
./build/src/tools/kvbench --put-pct 0 --get-pct 100 /tmp/bench_ro

# Large values
./build/src/tools/kvbench --value-size 4096 --num-ops 100000 /tmp/bench_large

# Small key space (high contention)
./build/src/tools/kvbench --num-keys 1000 --threads 4 /tmp/bench_hot
```

## Output Format

### Interval Report (every N seconds)

```
--- Interval (5s) ---
Op        Count      Found       Miss   p50(us)   p99(us)    p99.9   p99.99
put      125432          -          -       2.3      45.2    312.5   1024.7
get      124891     118200       6691       1.8      32.1    287.3    987.2
Total: 250323 ops  (50065 ops/sec)
```

### Final Report

Same format with cumulative stats, elapsed time, and DB statistics (log files,
total size, live entries, version).

### Column Descriptions

| Column | Description |
|--------|-------------|
| Op | Operation type (put, get) |
| Count | Number of operations |
| Found | Get operations that returned a value |
| Miss | Get operations that returned NotFound |
| p50 | Median latency in microseconds |
| p99 | 99th percentile latency |
| p99.9 | 99.9th percentile latency |
| p99.99 | 99.99th percentile latency |

## Tips

- **Drop caches** before read benchmarks: `echo 3 | sudo tee /proc/sys/vm/drop_caches`
- **Pin to cores** with `taskset -c 0-3 ./build/src/tools/kvbench --threads 4 ...`
- **Write-only** workloads should use `--no-preload` to avoid wasted work
- **Fresh DB** — delete the db_path directory between runs for clean measurements
- GC and savepoint daemons are disabled automatically during benchmarks
- CRC verification is disabled for maximum throughput
