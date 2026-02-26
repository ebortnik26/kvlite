#ifndef KVLITE_INTERNAL_SPINLOCK_H
#define KVLITE_INTERNAL_SPINLOCK_H

#include <atomic>
#include <cstdint>

namespace kvlite {
namespace internal {

// Lightweight spinlock using test-and-test-and-set with pause hint.
// Methods are const-qualified so the lock can be used with const references
// (the atomic member is mutable).
struct Spinlock {
    mutable std::atomic<uint8_t> locked{0};

    void lock() const {
        while (locked.exchange(1, std::memory_order_acquire) != 0) {
            while (locked.load(std::memory_order_relaxed) != 0) {
#if defined(__x86_64__) || defined(_M_X64)
                __builtin_ia32_pause();
#endif
            }
        }
    }

    void unlock() const {
        locked.store(0, std::memory_order_release);
    }
};

struct SpinlockGuard {
    const Spinlock& lock_;
    explicit SpinlockGuard(const Spinlock& l) : lock_(l) { lock_.lock(); }
    ~SpinlockGuard() { lock_.unlock(); }
    SpinlockGuard(const SpinlockGuard&) = delete;
    SpinlockGuard& operator=(const SpinlockGuard&) = delete;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_SPINLOCK_H
