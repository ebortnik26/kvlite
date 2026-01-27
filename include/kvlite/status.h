#ifndef KVLITE_STATUS_H
#define KVLITE_STATUS_H

#include <string>

namespace kvlite {

class Status {
public:
    // Create a success status
    Status() : code_(kOk) {}

    // Status factory methods
    static Status OK() { return Status(); }
    static Status NotFound(const std::string& msg = "") { return Status(kNotFound, msg); }
    static Status Corruption(const std::string& msg = "") { return Status(kCorruption, msg); }
    static Status NotSupported(const std::string& msg = "") { return Status(kNotSupported, msg); }
    static Status InvalidArgument(const std::string& msg = "") { return Status(kInvalidArgument, msg); }
    static Status IOError(const std::string& msg = "") { return Status(kIOError, msg); }

    // Returns true if the status indicates success
    bool ok() const { return code_ == kOk; }

    // Returns true if the status indicates a NotFound error
    bool isNotFound() const { return code_ == kNotFound; }

    // Returns true if the status indicates a Corruption error
    bool isCorruption() const { return code_ == kCorruption; }

    // Returns true if the status indicates an IOError
    bool isIOError() const { return code_ == kIOError; }

    // Return a string representation of this status
    std::string toString() const;

private:
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5
    };

    Status(Code code, const std::string& msg) : code_(code), message_(msg) {}

    Code code_;
    std::string message_;
};

} // namespace kvlite

#endif // KVLITE_STATUS_H
