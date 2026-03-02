#ifndef KVLITE_STATUS_H
#define KVLITE_STATUS_H

#include <string>

namespace kvlite {

class Status {
public:
    // Create a success status
    Status() : code_(kOk) {}

    // Copy and move
    Status(const Status&) = default;
    Status& operator=(const Status&) = default;
    Status(Status&&) = default;
    Status& operator=(Status&&) = default;

    // Status factory methods
    static Status OK() { return Status(); }
    static Status NotFound(const std::string& msg = "") { return Status(kNotFound, msg); }
    static Status Corruption(const std::string& msg = "") { return Status(kCorruption, msg); }
    static Status NotSupported(const std::string& msg = "") { return Status(kNotSupported, msg); }
    static Status InvalidArgument(const std::string& msg = "") { return Status(kInvalidArgument, msg); }
    static Status IOError(const std::string& msg = "") { return Status(kIOError, msg); }
    static Status Busy(const std::string& msg = "") { return Status(kBusy, msg); }
    static Status GCInProgress(const std::string& msg = "") { return Status(kGCInProgress, msg); }
    static Status ChecksumMismatch(const std::string& msg = "") { return Status(kChecksumMismatch, msg); }

    // Returns true if the status indicates success
    bool ok() const { return code_ == kOk; }

    // Status type checks
    bool isNotFound() const { return code_ == kNotFound; }
    bool isCorruption() const { return code_ == kCorruption; }
    bool isIOError() const { return code_ == kIOError; }
    bool isBusy() const { return code_ == kBusy; }
    bool isGCInProgress() const { return code_ == kGCInProgress; }
    bool isInvalidArgument() const { return code_ == kInvalidArgument; }
    bool isChecksumMismatch() const { return code_ == kChecksumMismatch; }

    // Return a string representation of this status
    std::string toString() const;

    // Return the error message
    const std::string& message() const { return message_; }

private:
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
        kBusy = 6,
        kGCInProgress = 7,
        kChecksumMismatch = 8
    };

    Status(Code code, const std::string& msg) : code_(code), message_(msg) {}

    Code code_;
    std::string message_;
};

} // namespace kvlite

#endif // KVLITE_STATUS_H
