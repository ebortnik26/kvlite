#include "kvlite/status.h"

namespace kvlite {

std::string Status::toString() const {
    static const char* names[] = {
        "OK", "NotFound", "Corruption", "NotSupported",
        "InvalidArgument", "IOError", "Busy", "GCInProgress",
        "ChecksumMismatch"
    };

    std::string result = names[static_cast<int>(code_)];
    if (!message_.empty()) {
        result += ": ";
        result += message_;
    }
    return result;
}

}  // namespace kvlite
