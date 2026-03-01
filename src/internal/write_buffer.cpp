#include "internal/write_buffer.h"

#include <cassert>

namespace kvlite {
namespace internal {

WriteBuffer::WriteBuffer(const Options& opts, FlushFn flush_fn)
    : opts_(opts),
      flush_fn_(std::move(flush_fn)),
      active_(std::make_unique<Memtable>()) {
    flush_thread_ = std::thread(&WriteBuffer::flushLoop, this);
}

WriteBuffer::~WriteBuffer() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        stop_ = true;
    }
    flush_cv_.notify_one();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
}

// --- Write operations ----------------------------------------------------

void WriteBuffer::put(const std::string& key, uint64_t version,
                      const std::string& value, bool tombstone) {
    std::unique_lock<std::mutex> lock(mu_);

    // Stall if all flush_depth slots are occupied.
    stall_cv_.wait(lock, [this] {
        return immutables_.size() < opts_.flush_depth - 1 || stop_;
    });

    active_->put(key, version, value, tombstone);
    maybeSeal();
}

void WriteBuffer::putBatch(const std::vector<Memtable::BatchOp>& ops,
                           uint64_t version) {
    if (ops.empty()) return;

    std::unique_lock<std::mutex> lock(mu_);

    stall_cv_.wait(lock, [this] {
        return immutables_.size() < opts_.flush_depth - 1 || stop_;
    });

    active_->putBatch(ops, version);
    maybeSeal();
}

// --- Read operations -----------------------------------------------------

bool WriteBuffer::getByVersion(const std::string& key, uint64_t upper_bound,
                               std::string& value, uint64_t& version,
                               bool& tombstone) const {
    std::lock_guard<std::mutex> lock(mu_);

    // Check active first (newest data).
    if (active_->getByVersion(key, upper_bound, value, version, tombstone)) {
        return true;
    }

    // Scan immutables back-to-front (newest to oldest).
    for (auto it = immutables_.rbegin(); it != immutables_.rend(); ++it) {
        if ((*it)->getByVersion(key, upper_bound, value, version, tombstone)) {
            return true;
        }
    }

    // Check the Memtable currently being flushed (oldest, still readable).
    if (flushing_ &&
        flushing_->getByVersion(key, upper_bound, value, version, tombstone)) {
        return true;
    }

    return false;
}

// --- Lifecycle -----------------------------------------------------------

void WriteBuffer::maybeSeal() {
    // Caller must hold mu_.
    if (active_->memoryUsage() >= opts_.memtable_size) {
        sealActive();
    }
}

void WriteBuffer::sealActive() {
    // Caller must hold mu_.
    if (active_->empty()) return;
    immutables_.push_back(std::move(active_));
    active_ = std::make_unique<Memtable>();
    flush_cv_.notify_one();
}

void WriteBuffer::drainFlush() {
    std::unique_lock<std::mutex> lock(mu_);

    // Seal active if non-empty.
    if (!active_->empty()) {
        sealActive();
    }

    // Wait until all immutables are flushed and no flush in progress.
    stall_cv_.wait(lock, [this] {
        return immutables_.empty() && flushing_ == nullptr;
    });
}

size_t WriteBuffer::memoryUsage() const {
    std::lock_guard<std::mutex> lock(mu_);
    return active_->memoryUsage();
}

bool WriteBuffer::empty() const {
    std::lock_guard<std::mutex> lock(mu_);
    return active_->empty() && immutables_.empty() && flushing_ == nullptr;
}

// --- Iterator support ----------------------------------------------------

WriteBuffer::PinnedSnapshot WriteBuffer::pinAll() {
    std::lock_guard<std::mutex> lock(mu_);
    PinnedSnapshot snap;

    // Active first.
    active_->pin();
    snap.memtables.push_back(active_.get());

    // Then immutables (front = oldest, back = newest).
    for (auto& mt : immutables_) {
        mt->pin();
        snap.memtables.push_back(mt.get());
    }

    // Include the Memtable currently being flushed, if any.
    if (flushing_) {
        flushing_->pin();
        snap.memtables.push_back(flushing_);
    }

    return snap;
}

void WriteBuffer::unpinAll(const PinnedSnapshot& snap) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto* mt : snap.memtables) {
        mt->unpin();
    }

    // Clean up retired Memtables that are no longer pinned.
    retired_.erase(
        std::remove_if(retired_.begin(), retired_.end(),
                       [](const std::unique_ptr<Memtable>& mt) {
                           return mt->pinCount() == 0;
                       }),
        retired_.end());
}

// --- Flush daemon --------------------------------------------------------

void WriteBuffer::flushLoop() {
    std::unique_lock<std::mutex> lock(mu_);
    while (true) {
        flush_cv_.wait(lock, [this] {
            return !immutables_.empty() || stop_;
        });

        if (stop_ && immutables_.empty()) break;

        // Take ownership of the oldest immutable.
        auto mt = std::move(immutables_.front());
        immutables_.pop_front();

        // Set flushing_ so reads can still find this Memtable's data.
        flushing_ = mt.get();

        // Release mu_ while flushing — the Memtable is immutable,
        // no locking needed inside it.
        lock.unlock();

        Status s = flush_fn_(*mt);
        (void)s;

        lock.lock();

        // Clear flushing_ — data is now in GI/segments.
        flushing_ = nullptr;

        // Retire or discard the flushed Memtable.
        if (mt->pinCount() > 0) {
            retired_.push_back(std::move(mt));
        }
        // else: mt is destroyed when unique_ptr goes out of scope.

        // Wake any stalled writers and drainFlush waiters.
        stall_cv_.notify_all();
    }
}

} // namespace internal
} // namespace kvlite
