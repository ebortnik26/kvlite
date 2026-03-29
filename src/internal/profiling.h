#ifndef KVLITE_INTERNAL_PROFILING_H
#define KVLITE_INTERNAL_PROFILING_H

#include <atomic>
#include <chrono>
#include <cstdint>

namespace kvlite {
namespace internal {

// Record elapsed time since t0 into (count, total) atomics.
// Duration selects the time unit (nanoseconds, microseconds, etc.).
template<typename Duration = std::chrono::nanoseconds>
inline void trackTime(std::atomic<uint64_t>& count,
                      std::atomic<uint64_t>& total,
                      std::chrono::steady_clock::time_point t0) {
    auto d = std::chrono::duration_cast<Duration>(
        std::chrono::steady_clock::now() - t0).count();
    count.fetch_add(1, std::memory_order_relaxed);
    total.fetch_add(static_cast<uint64_t>(d), std::memory_order_relaxed);
}

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_PROFILING_H
