#ifndef KVLITE_INTERNAL_PERIODIC_DAEMON_H
#define KVLITE_INTERNAL_PERIODIC_DAEMON_H

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>

namespace kvlite {
namespace internal {

class PeriodicDaemon {
public:
    PeriodicDaemon() = default;
    ~PeriodicDaemon() { stop(); }

    PeriodicDaemon(const PeriodicDaemon&) = delete;
    PeriodicDaemon& operator=(const PeriodicDaemon&) = delete;

    void start(uint32_t interval_sec, std::function<void()> fn) {
        fn_ = std::move(fn);
        interval_sec_ = interval_sec;
        stop_ = false;
        thread_ = std::thread([this] {
            std::unique_lock<std::mutex> lock(mu_);
            while (!stop_) {
                mu_cv_.wait_for(lock, std::chrono::seconds(interval_sec_),
                                [this] { return stop_; });
                if (stop_) break;
                lock.unlock();
                fn_();
                lock.lock();
            }
        });
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            stop_ = true;
        }
        mu_cv_.notify_one();
        if (thread_.joinable()) thread_.join();
    }

private:
    std::thread thread_;
    std::mutex mu_;
    std::condition_variable mu_cv_;
    bool stop_ = false;
    std::function<void()> fn_;
    uint32_t interval_sec_ = 0;
};

}  // namespace internal
}  // namespace kvlite

#endif  // KVLITE_INTERNAL_PERIODIC_DAEMON_H
