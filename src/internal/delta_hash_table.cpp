#include "internal/delta_hash_table.h"

#include <algorithm>
#include <cstring>
#include <unordered_map>

#include "internal/bit_stream.h"

namespace kvlite {
namespace internal {

// --- Construction / Move ---

DeltaHashTable::DeltaHashTable(const Config& config)
    : config_(config),
      fingerprint_bits_(64 - config.bucket_bits - config.lslot_bits),
      lslot_codec_(fingerprint_bits_),
      ext_arena_(nullptr) {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t stride = bucketStride();
    arena_ = std::make_unique<uint8_t[]>(
        static_cast<size_t>(num_buckets) * stride);
    std::memset(arena_.get(), 0,
                static_cast<size_t>(num_buckets) * stride);
    buckets_.resize(num_buckets);
    for (uint32_t i = 0; i < num_buckets; ++i) {
        buckets_[i].data = arena_.get() + static_cast<size_t>(i) * stride;
    }
    // Init buckets directly — ext_arena_ is not set yet (derived class owns it).
    for (auto& bucket : buckets_) {
        initBucket(bucket);
    }
}

DeltaHashTable::~DeltaHashTable() = default;
DeltaHashTable::DeltaHashTable(DeltaHashTable&&) noexcept = default;
DeltaHashTable& DeltaHashTable::operator=(DeltaHashTable&&) noexcept = default;

// --- Hash decomposition ---

uint64_t DeltaHashTable::hashKey(std::string_view key) const {
    return dhtHashBytes(key.data(), key.size());
}

uint32_t DeltaHashTable::bucketIndex(uint64_t hash) const {
    return static_cast<uint32_t>(hash >> (64 - config_.bucket_bits));
}

uint32_t DeltaHashTable::lslotIndex(uint64_t hash) const {
    return static_cast<uint32_t>(
        (hash >> (64 - config_.bucket_bits - config_.lslot_bits))
        & ((1u << config_.lslot_bits) - 1));
}

uint64_t DeltaHashTable::fingerprint(uint64_t hash) const {
    return hash & ((1ULL << fingerprint_bits_) - 1);
}

// --- Bucket data ---

uint32_t DeltaHashTable::bucketStride() const {
    return config_.bucket_bytes + kBucketPadding;
}

uint64_t DeltaHashTable::getExtensionPtr(const Bucket& bucket) const {
    uint64_t ptr = 0;
    std::memcpy(&ptr, bucket.data + config_.bucket_bytes - 8, 8);
    return ptr;
}

void DeltaHashTable::setExtensionPtr(Bucket& bucket, uint64_t ptr) const {
    std::memcpy(bucket.data + config_.bucket_bytes - 8, &ptr, 8);
}

size_t DeltaHashTable::bucketDataBits() const {
    return (config_.bucket_bytes - 8) * 8;
}

uint32_t DeltaHashTable::numLSlots() const {
    return 1u << config_.lslot_bits;
}

// --- Decode / Encode ---

DeltaHashTable::LSlotContents DeltaHashTable::decodeLSlot(
    const Bucket& bucket, uint32_t lslot_idx) const {
    size_t bit_off = lslot_codec_.bitOffset(bucket.data, lslot_idx);
    return lslot_codec_.decode(bucket.data, bit_off);
}

std::vector<DeltaHashTable::LSlotContents> DeltaHashTable::decodeAllLSlots(
    const Bucket& bucket) const {
    uint32_t n = numLSlots();
    std::vector<LSlotContents> slots(n);
    size_t offset = 0;
    for (uint32_t s = 0; s < n; ++s) {
        slots[s] = lslot_codec_.decode(bucket.data, offset, &offset);
    }
    return slots;
}

void DeltaHashTable::reencodeAllLSlots(
    Bucket& bucket, const std::vector<LSlotContents>& all_slots) {
    size_t data_bytes = config_.bucket_bytes - 8;
    uint64_t ext_ptr = getExtensionPtr(bucket);
    std::memset(bucket.data, 0, data_bytes);
    setExtensionPtr(bucket, ext_ptr);

    size_t write_offset = 0;
    for (uint32_t s = 0; s < all_slots.size(); ++s) {
        write_offset = lslot_codec_.encode(bucket.data, write_offset,
                                           all_slots[s]);
    }
}

size_t DeltaHashTable::totalBitsNeeded(
    const std::vector<LSlotContents>& all_slots) const {
    size_t bits = 0;
    for (const auto& slot : all_slots) {
        bits += LSlotCodec::bitsNeeded(slot, fingerprint_bits_);
    }
    return bits;
}

// --- Extension chain ---

const Bucket* DeltaHashTable::nextBucket(
    const Bucket& bucket) const {
    uint64_t ext = getExtensionPtr(bucket);
    return ext ? ext_arena_->get(static_cast<uint32_t>(ext)) : nullptr;
}

Bucket* DeltaHashTable::nextBucketMut(Bucket& bucket) {
    uint64_t ext = getExtensionPtr(bucket);
    return ext ? ext_arena_->get(static_cast<uint32_t>(ext)) : nullptr;
}

Bucket* DeltaHashTable::createExtension(Bucket& bucket) {
    uint32_t ext_ptr = ext_arena_->allocate();
    setExtensionPtr(bucket, ext_ptr);
    Bucket* ext_bucket = ext_arena_->get(ext_ptr);
    initBucket(*ext_bucket);
    return ext_bucket;
}

// --- Hash helpers ---

uint64_t DeltaHashTable::secondaryHash(std::string_view key) const {
    return dhtSecondaryHash(key.data(), key.size());
}

// Forward declaration — defined in the addToChainChecked helpers section below.
static DeltaHashTable::LSlotContents buildCandidateSlot(
    const DeltaHashTable::LSlotContents& slot,
    uint64_t full_fp, uint8_t fp_extra_bits,
    uint64_t packed_version, uint32_t id);

// --- addToChain ---

bool DeltaHashTable::addToChain(uint32_t bi, uint32_t li, uint64_t fp,
                                 uint64_t packed_version, uint32_t id,
                                 const std::function<Bucket*(Bucket&)>& createExtFn) {
    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;
    Bucket* bucket = &buckets_[bi];
    bool is_new = true;

    while (true) {
        auto all_slots = decodeAllLSlots(*bucket);

        if (is_new) {
            for (const auto& entry : all_slots[li].entries) {
                if ((entry.fingerprint & base_mask) == fp) {
                    is_new = false;
                    break;
                }
            }
        }

        auto candidate = buildCandidateSlot(all_slots[li], fp, 0,
                                            packed_version, id);
        if (tryCommitSlot(bucket, li, all_slots, std::move(candidate))) {
            return is_new;
        }

        Bucket* ext = nextBucketMut(*bucket);
        if (!ext) ext = createExtFn(*bucket);
        bucket = ext;
    }
}

// --- addToChainChecked helpers ---
//
// Collision resolution uses "variable-length fingerprints": when two keys
// share the same base fingerprint (hash collision), we extend their
// fingerprints with bits from a secondary hash to disambiguate them.
// The extension width (fp_extra_bits) is the minimum number of secondary
// hash bits needed to make all entries in the group pairwise unique.
//
// Helper call graph:
//   addToChainChecked
//   ├── findSameKeyMatch       — check if new key matches any existing entry
//   ├── appendToEntry          — add version to existing key's entry
//   │   ├── buildCandidateSlot — build modified slot offline (no mutation)
//   │   └── tryCommitSlot      — check fit and reencode if OK
//   └── resolveCollision       — extend fingerprints for all colliding keys
//       ├── findMinExtraBits   — find min bits to disambiguate all keys
//       └── extendAndInsert    — apply extension and add new entry

// Find minimum extra_bits such that new_sec and all entries in sec_hashes
// produce unique values when masked to extra_bits width. Tries widths
// starting at start_bits, incrementing until all N+1 values are distinct.
static uint8_t findMinExtraBits(
    const std::vector<uint64_t>& sec_hashes,
    uint64_t new_sec,
    uint8_t start_bits = 1) {
    uint8_t extra_bits = start_bits;
    while (extra_bits < 64) {
        uint64_t ext_mask = (1ULL << extra_bits) - 1;
        uint64_t new_ext = new_sec & ext_mask;
        bool all_unique = true;
        for (uint64_t sh : sec_hashes) {
            if ((sh & ext_mask) == new_ext) {
                all_unique = false;
                break;
            }
        }
        if (all_unique) {
            bool existing_unique = true;
            for (size_t a = 0; a < sec_hashes.size() && existing_unique; ++a) {
                for (size_t b = a + 1; b < sec_hashes.size(); ++b) {
                    if ((sec_hashes[a] & ext_mask) ==
                        (sec_hashes[b] & ext_mask)) {
                        existing_unique = false;
                        break;
                    }
                }
            }
            if (existing_unique) return extra_bits;
        }
        extra_bits++;
    }
    return extra_bits;
}

// Extend all matched entries' fingerprints with secondary hash bits and insert
// the new entry. Modifies all_slots in place. Returns the full (extended)
// fingerprint assigned to the new entry. Caller checks fit and handles overflow.
static uint64_t extendAndInsert(
    std::vector<DeltaHashTable::LSlotContents>& all_slots, uint32_t li,
    const std::vector<size_t>& match_indices,
    const std::vector<uint64_t>& sec_hashes,
    uint64_t fp, uint64_t new_key_secondary_hash,
    uint64_t packed_version, uint32_t id,
    uint8_t fingerprint_bits, uint8_t start_bits) {

    uint8_t extra_bits = findMinExtraBits(sec_hashes, new_key_secondary_hash,
                                          start_bits);
    uint64_t ext_mask = (1ULL << extra_bits) - 1;
    auto& entries = all_slots[li].entries;

    // Update existing entries with extended fingerprints.
    for (size_t i = 0; i < match_indices.size(); ++i) {
        entries[match_indices[i]].fp_extra_bits = extra_bits;
        entries[match_indices[i]].fingerprint =
            fp | ((sec_hashes[i] & ext_mask) << fingerprint_bits);
    }

    // Create and insert new entry.
    DeltaHashTable::TrieEntry new_entry;
    new_entry.fingerprint = fp | ((new_key_secondary_hash & ext_mask) << fingerprint_bits);
    new_entry.fp_extra_bits = extra_bits;
    new_entry.packed_versions.push_back(packed_version);
    new_entry.ids.push_back(id);
    uint64_t new_fp = new_entry.fingerprint;
    entries.push_back(std::move(new_entry));

    std::sort(entries.begin(), entries.end(),
              [](const DeltaHashTable::TrieEntry& a,
                 const DeltaHashTable::TrieEntry& b) {
                  return a.fingerprint < b.fingerprint;
              });

    return new_fp;
}

// Build a candidate lslot offline (pure function — no bucket mutation).
// Returns a copy of the slot with (packed_version, id) inserted into the
// entry matching full_fp, or with a new TrieEntry if no match exists.
static DeltaHashTable::LSlotContents buildCandidateSlot(
    const DeltaHashTable::LSlotContents& slot,
    uint64_t full_fp, uint8_t fp_extra_bits,
    uint64_t packed_version, uint32_t id) {
    DeltaHashTable::LSlotContents candidate = slot;
    bool found = false;
    for (auto& e : candidate.entries) {
        if (e.fingerprint == full_fp) {
            auto it = std::lower_bound(e.packed_versions.begin(),
                                       e.packed_versions.end(),
                                       packed_version,
                                       std::greater<uint64_t>());
            size_t pos = it - e.packed_versions.begin();
            e.packed_versions.insert(it, packed_version);
            e.ids.insert(e.ids.begin() + pos, id);
            found = true;
            break;
        }
    }
    if (!found) {
        DeltaHashTable::TrieEntry new_entry;
        new_entry.fingerprint = full_fp;
        new_entry.fp_extra_bits = fp_extra_bits;
        new_entry.packed_versions.push_back(packed_version);
        new_entry.ids.push_back(id);
        candidate.entries.push_back(std::move(new_entry));
        std::sort(candidate.entries.begin(), candidate.entries.end(),
                  [](const DeltaHashTable::TrieEntry& a,
                     const DeltaHashTable::TrieEntry& b) {
                      return a.fingerprint < b.fingerprint;
                  });
    }
    return candidate;
}

// Try replacing the lslot in a bucket with a candidate. Computes the total
// bit cost with the candidate substituted in; if it fits in bucket data area,
// commits (reencodes) and returns true. Otherwise returns false with no mutation.
bool DeltaHashTable::tryCommitSlot(
    Bucket* bucket, uint32_t li,
    std::vector<LSlotContents>& all_slots,
    LSlotContents&& candidate) {
    size_t old_bits = LSlotCodec::bitsNeeded(all_slots[li], fingerprint_bits_);
    size_t new_bits = LSlotCodec::bitsNeeded(candidate, fingerprint_bits_);
    size_t total = totalBitsNeeded(all_slots) - old_bits + new_bits;
    if (total <= bucketDataBits()) {
        all_slots[li] = std::move(candidate);
        reencodeAllLSlots(*bucket, all_slots);
        return true;
    }
    return false;
}

// Append a new (packed_version, id) to an existing key's TrieEntry.
// Builds a candidate offline, attempts to commit to the current bucket.
// On overflow, walks the extension chain to find a bucket with space.
void DeltaHashTable::appendToEntry(
    TrieEntry& entry,
    std::vector<LSlotContents>& all_slots,
    Bucket* bucket, uint32_t li,
    uint64_t packed_version, uint32_t id,
    const std::function<Bucket*(Bucket&)>& createExtFn) {

    auto candidate = buildCandidateSlot(all_slots[li], entry.fingerprint,
                                        entry.fp_extra_bits,
                                        packed_version, id);
    if (tryCommitSlot(bucket, li, all_slots, std::move(candidate))) {
        return;
    }

    // Doesn't fit — add the version to extension chain.
    uint64_t full_fp = entry.fingerprint;
    uint8_t extra_bits = entry.fp_extra_bits;
    Bucket* target = nextBucketMut(*bucket);
    if (!target) {
        target = createExtFn(*bucket);
    }
    while (true) {
        auto ext_slots = decodeAllLSlots(*target);
        auto ext_candidate = buildCandidateSlot(ext_slots[li], full_fp,
                                                extra_bits,
                                                packed_version, id);
        if (tryCommitSlot(target, li, ext_slots, std::move(ext_candidate))) {
            return;
        }
        Bucket* next = nextBucketMut(*target);
        if (!next) {
            next = createExtFn(*target);
        }
        target = next;
    }
}

// --- addToChainChecked ---

// For entries with extended fingerprints, compare extension bits directly.
// For entries with fp_extra_bits==0 (never extended), resolve the actual key
// via the resolver and compare secondary hashes.
// Returns the entry index if found, -1 otherwise.
int DeltaHashTable::findSameKeyMatch(
    const std::vector<LSlotContents>& all_slots, uint32_t li,
    const std::vector<size_t>& match_indices,
    const KeyResolver& resolver,
    uint64_t new_key_secondary_hash) const {
    auto& entries = all_slots[li].entries;
    for (size_t ei : match_indices) {
        auto& existing = entries[ei];
        if (existing.fp_extra_bits > 0) {
            uint64_t ext_existing = existing.fingerprint >> fingerprint_bits_;
            uint64_t ext_new = new_key_secondary_hash &
                ((1ULL << existing.fp_extra_bits) - 1);
            if (ext_existing == ext_new) return static_cast<int>(ei);
            continue;
        }
        std::string key = resolver(existing.ids[0], existing.packed_versions[0]);
        uint64_t sec = dhtSecondaryHash(key.data(), key.size());
        if (sec == new_key_secondary_hash) return static_cast<int>(ei);
    }
    return -1;
}

// Handle a fingerprint collision: all matched entries belong to different keys.
// Resolves each key to get its secondary hash, finds the minimum extension width
// that disambiguates all keys, updates existing entries' fingerprints, creates
// the new entry, and commits. On overflow, spills the new entry to extension.
void DeltaHashTable::resolveCollision(
    std::vector<LSlotContents>& all_slots,
    Bucket* bucket, uint32_t li, uint64_t fp,
    const std::vector<size_t>& match_indices,
    const KeyResolver& resolver,
    uint64_t new_key_secondary_hash,
    uint64_t packed_version, uint32_t id,
    const std::function<Bucket*(Bucket&)>& createExtFn) {

    auto& entries = all_slots[li].entries;
    std::vector<uint64_t> sec_hashes;
    uint8_t max_extra = 0;
    for (size_t mi : match_indices) {
        std::string k = resolver(entries[mi].ids[0],
                                 entries[mi].packed_versions[0]);
        sec_hashes.push_back(dhtSecondaryHash(k.data(), k.size()));
        if (entries[mi].fp_extra_bits > max_extra)
            max_extra = entries[mi].fp_extra_bits;
    }
    uint8_t start_bits = max_extra > 0 ? max_extra : 1;

    uint64_t new_fp = extendAndInsert(
        all_slots, li, match_indices, sec_hashes,
        fp, new_key_secondary_hash, packed_version, id,
        fingerprint_bits_, start_bits);

    if (totalBitsNeeded(all_slots) <= bucketDataBits()) {
        reencodeAllLSlots(*bucket, all_slots);
        return;
    }

    // Remove the new entry before reencoding, then spill it to extension.
    auto& ents = all_slots[li].entries;
    TrieEntry spilled;
    for (auto eit = ents.begin(); eit != ents.end(); ++eit) {
        if (eit->fingerprint == new_fp) {
            spilled = std::move(*eit);
            ents.erase(eit);
            break;
        }
    }
    reencodeAllLSlots(*bucket, all_slots);

    Bucket* ext = nextBucketMut(*bucket);
    if (!ext) ext = createExtFn(*bucket);
    auto ext_slots = decodeAllLSlots(*ext);
    ext_slots[li].entries.push_back(std::move(spilled));
    std::sort(ext_slots[li].entries.begin(), ext_slots[li].entries.end(),
              [](const TrieEntry& a, const TrieEntry& b) {
                  return a.fingerprint < b.fingerprint;
              });
    reencodeAllLSlots(*ext, ext_slots);
}

// Collision-aware insertion. Walks the bucket chain looking for entries with
// matching base fingerprint. Three outcomes per bucket:
//   1. No matches → walk to next bucket.
//   2. Same key found → append version (appendToEntry).
//   3. All matches are different keys → collision (resolveCollision).
// If no bucket has matches, falls through to addToChain (new key, no collision).
bool DeltaHashTable::addToChainChecked(
    uint32_t bi, uint32_t li, uint64_t fp,
    uint64_t packed_version, uint32_t id,
    const std::function<Bucket*(Bucket&)>& createExtFn,
    const KeyResolver& resolver,
    uint64_t new_key_secondary_hash) {

    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;

    Bucket* bucket = &buckets_[bi];
    while (bucket) {
        auto all_slots = decodeAllLSlots(*bucket);
        auto& entries = all_slots[li].entries;

        std::vector<size_t> match_indices;
        for (size_t ei = 0; ei < entries.size(); ++ei) {
            if ((entries[ei].fingerprint & base_mask) == fp)
                match_indices.push_back(ei);
        }

        if (match_indices.empty()) {
            bucket = nextBucketMut(*bucket);
            continue;
        }

        int same_key = findSameKeyMatch(all_slots, li, match_indices,
                                            resolver, new_key_secondary_hash);
        if (same_key >= 0) {
            appendToEntry(entries[same_key], all_slots, bucket, li,
                          packed_version, id, createExtFn);
            return false;
        }

        resolveCollision(all_slots, bucket, li, fp, match_indices,
                         resolver, new_key_secondary_hash,
                         packed_version, id, createExtFn);
        return true;
    }

    return addToChain(bi, li, fp, packed_version, id, createExtFn);
}

// --- Chain-walk helpers (static) ---

// Remove (packed_version, id) from the first entry whose base fingerprint
// matches fp. Erases the TrieEntry if it becomes empty.
// Returns true if the pair was found and removed.
static bool removeVersionFromSlot(
    DeltaHashTable::LSlotContents& slot,
    uint64_t base_mask, uint64_t fp,
    uint64_t packed_version, uint32_t id) {
    auto& entries = slot.entries;
    for (auto it = entries.begin(); it != entries.end(); ++it) {
        if ((it->fingerprint & base_mask) != fp) continue;
        for (size_t j = 0; j < it->packed_versions.size(); ++j) {
            if (it->packed_versions[j] == packed_version && it->ids[j] == id) {
                it->packed_versions.erase(it->packed_versions.begin() + j);
                it->ids.erase(it->ids.begin() + j);
                if (it->packed_versions.empty()) {
                    entries.erase(it);
                }
                return true;
            }
        }
        return false;  // right entry, pair not found
    }
    return false;
}

// Find (packed_version, old_id) in the first entry whose base fingerprint
// matches fp and replace old_id with new_id.
// Returns true if the pair was found and updated.
static bool updateIdInSlot(
    DeltaHashTable::LSlotContents& slot,
    uint64_t base_mask, uint64_t fp,
    uint64_t packed_version, uint32_t old_id, uint32_t new_id) {
    for (auto& entry : slot.entries) {
        if ((entry.fingerprint & base_mask) != fp) continue;
        for (size_t j = 0; j < entry.packed_versions.size(); ++j) {
            if (entry.packed_versions[j] == packed_version &&
                entry.ids[j] == old_id) {
                entry.ids[j] = new_id;
                return true;
            }
        }
        return false;  // right entry, pair not found
    }
    return false;
}

// --- Chain-walk member helpers ---

void DeltaHashTable::pruneEmptyExtension(Bucket* bucket) {
    Bucket* ext = nextBucketMut(*bucket);
    if (!ext) return;
    auto ext_slots = decodeAllLSlots(*ext);
    bool ext_empty = true;
    for (const auto& slot : ext_slots) {
        if (!slot.entries.empty()) { ext_empty = false; break; }
    }
    if (ext_empty && !nextBucket(*ext)) {
        setExtensionPtr(*bucket, 0);
    }
}

bool DeltaHashTable::isGroupEmpty(uint32_t bi, uint32_t li, uint64_t fp) const {
    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;
    const Bucket* b = &buckets_[bi];
    while (b) {
        LSlotContents contents = decodeLSlot(*b, li);
        for (const auto& entry : contents.entries) {
            if ((entry.fingerprint & base_mask) == fp) return false;
        }
        b = nextBucket(*b);
    }
    return true;
}

void DeltaHashTable::spillEntryToExtension(
    Bucket* bucket, uint32_t li, uint64_t base_mask, uint64_t fp,
    const std::function<Bucket*(Bucket&)>& createExtFn) {
    auto all_slots = decodeAllLSlots(*bucket);
    auto& entries = all_slots[li].entries;
    TrieEntry spilled;
    for (auto it = entries.begin(); it != entries.end(); ++it) {
        if ((it->fingerprint & base_mask) == fp) {
            spilled = std::move(*it);
            entries.erase(it);
            break;
        }
    }
    reencodeAllLSlots(*bucket, all_slots);

    Bucket* ext = nextBucketMut(*bucket);
    if (!ext) ext = createExtFn(*bucket);
    auto ext_slots = decodeAllLSlots(*ext);
    ext_slots[li].entries.push_back(std::move(spilled));
    std::sort(ext_slots[li].entries.begin(), ext_slots[li].entries.end(),
              [](const TrieEntry& a, const TrieEntry& b) {
                  return a.fingerprint < b.fingerprint;
              });
    reencodeAllLSlots(*ext, ext_slots);
}

// --- removeFromChain ---

bool DeltaHashTable::removeFromChain(uint32_t bi, uint32_t li, uint64_t fp,
                                      uint64_t packed_version, uint32_t id) {
    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;
    Bucket* bucket = &buckets_[bi];

    while (bucket) {
        auto all_slots = decodeAllLSlots(*bucket);
        if (removeVersionFromSlot(all_slots[li], base_mask, fp,
                                  packed_version, id)) {
            reencodeAllLSlots(*bucket, all_slots);
        }
        pruneEmptyExtension(bucket);
        bucket = nextBucketMut(*bucket);
    }

    return isGroupEmpty(bi, li, fp);
}

// --- updateIdInChain ---

bool DeltaHashTable::updateIdInChain(uint32_t bi, uint32_t li, uint64_t fp,
                                      uint64_t packed_version, uint32_t old_id,
                                      uint32_t new_id,
                                      const std::function<Bucket*(Bucket&)>& createExtFn) {
    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;
    Bucket* bucket = &buckets_[bi];

    while (bucket) {
        auto all_slots = decodeAllLSlots(*bucket);
        if (!updateIdInSlot(all_slots[li], base_mask, fp,
                            packed_version, old_id, new_id)) {
            bucket = nextBucketMut(*bucket);
            continue;
        }

        if (totalBitsNeeded(all_slots) <= bucketDataBits()) {
            reencodeAllLSlots(*bucket, all_slots);
            return true;
        }

        spillEntryToExtension(bucket, li, base_mask, fp, createExtFn);
        return true;
    }

    return false;
}

// --- Public read API ---

bool DeltaHashTable::findAllByHash(uint32_t bi, uint32_t li, uint64_t fp,
                                    std::vector<uint64_t>& packed_versions,
                                    std::vector<uint32_t>& ids) const {
    packed_versions.clear();
    ids.clear();

    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        LSlotContents contents = decodeLSlot(*bucket, li);
        for (const auto& entry : contents.entries) {
            if ((entry.fingerprint & base_mask) == fp) {
                packed_versions.insert(packed_versions.end(),
                                       entry.packed_versions.begin(),
                                       entry.packed_versions.end());
                ids.insert(ids.end(), entry.ids.begin(), entry.ids.end());
            }
        }
        bucket = nextBucket(*bucket);
    }
    return !packed_versions.empty();
}

bool DeltaHashTable::findFirstByHash(uint32_t bi, uint32_t li, uint64_t fp,
                                      uint64_t& packed_version, uint32_t& id) const {
    bool found = false;
    uint64_t best_pv = 0;
    uint32_t best_id = 0;

    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;

    const Bucket* bucket = &buckets_[bi];
    while (bucket) {
        LSlotContents contents = decodeLSlot(*bucket, li);

        for (const auto& entry : contents.entries) {
            if ((entry.fingerprint & base_mask) == fp && !entry.packed_versions.empty()) {
                uint64_t pv = entry.packed_versions[0];
                if (!found || pv > best_pv) {
                    best_pv = pv;
                    best_id = entry.ids[0];
                    found = true;
                }
            }
        }

        bucket = nextBucket(*bucket);
    }

    if (found) {
        packed_version = best_pv;
        id = best_id;
    }
    return found;
}

bool DeltaHashTable::findAll(std::string_view key,
                              std::vector<uint64_t>& packed_versions,
                              std::vector<uint32_t>& ids) const {
    uint64_t h = hashKey(key);
    return findAllByHash(bucketIndex(h), lslotIndex(h), fingerprint(h),
                         packed_versions, ids);
}

bool DeltaHashTable::findFirst(std::string_view key,
                                uint64_t& packed_version, uint32_t& id) const {
    uint64_t h = hashKey(key);
    return findFirstByHash(bucketIndex(h), lslotIndex(h), fingerprint(h),
                           packed_version, id);
}

bool DeltaHashTable::contains(std::string_view key) const {
    uint64_t pv;
    uint32_t id;
    return findFirst(key, pv, id);
}

void DeltaHashTable::forEach(
    const std::function<void(uint64_t hash, uint64_t packed_version,
                             uint32_t id)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t n_lslots = numLSlots();
    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        const Bucket* b = &buckets_[bi];
        while (b) {
            size_t offset = 0;
            for (uint32_t s = 0; s < n_lslots; ++s) {
                LSlotContents contents =
                    lslot_codec_.decode(b->data, offset, &offset);
                for (const auto& entry : contents.entries) {
                    uint64_t hash =
                        (static_cast<uint64_t>(bi) << (64 - config_.bucket_bits)) |
                        (static_cast<uint64_t>(s) << fingerprint_bits_) |
                        (entry.fingerprint & base_mask);
                    for (size_t i = 0; i < entry.packed_versions.size(); ++i) {
                        fn(hash, entry.packed_versions[i], entry.ids[i]);
                    }
                }
            }
            b = nextBucket(*b);
        }
    }
}

void DeltaHashTable::forEachGroup(
    const std::function<void(uint64_t hash,
                             const std::vector<uint64_t>& packed_versions,
                             const std::vector<uint32_t>& ids)>& fn) const {
    uint32_t num_buckets = 1u << config_.bucket_bits;
    uint32_t n_lslots = numLSlots();
    uint64_t base_mask = (1ULL << fingerprint_bits_) - 1;

    for (uint32_t bi = 0; bi < num_buckets; ++bi) {
        struct Group {
            uint32_t lslot;
            uint64_t fp;
            std::vector<uint64_t> packed_versions;
            std::vector<uint32_t> ids;
        };
        // O(1) lookup by (lslot << fp_bits) | base_fingerprint
        std::unordered_map<uint64_t, Group> group_map;

        const Bucket* b = &buckets_[bi];
        while (b) {
            size_t offset = 0;
            for (uint32_t s = 0; s < n_lslots; ++s) {
                LSlotContents contents =
                    lslot_codec_.decode(b->data, offset, &offset);
                for (const auto& entry : contents.entries) {
                    uint64_t base_fp = entry.fingerprint & base_mask;
                    uint64_t map_key = (static_cast<uint64_t>(s) << fingerprint_bits_)
                                       | base_fp;
                    auto it = group_map.find(map_key);
                    if (it != group_map.end()) {
                        it->second.packed_versions.insert(
                            it->second.packed_versions.end(),
                            entry.packed_versions.begin(),
                            entry.packed_versions.end());
                        it->second.ids.insert(
                            it->second.ids.end(),
                            entry.ids.begin(),
                            entry.ids.end());
                    } else {
                        group_map.emplace(map_key,
                            Group{s, base_fp,
                                  entry.packed_versions,
                                  entry.ids});
                    }
                }
            }
            b = nextBucket(*b);
        }

        for (const auto& [map_key, g] : group_map) {
            uint64_t hash =
                (static_cast<uint64_t>(bi) << (64 - config_.bucket_bits)) |
                (static_cast<uint64_t>(g.lslot) << fingerprint_bits_) |
                g.fp;  // g.fp is already the base fingerprint
            fn(hash, g.packed_versions, g.ids);
        }
    }
}

// --- Stats ---

size_t DeltaHashTable::memoryUsage() const {
    size_t stride = config_.bucket_bytes + kBucketPadding;
    return buckets_.size() * stride + ext_arena_->dataBytes();
}

void DeltaHashTable::clearBuckets() {
    for (auto& bucket : buckets_) {
        setExtensionPtr(bucket, 0);
        initBucket(bucket);
    }
    ext_arena_->clear();
}

void DeltaHashTable::initBucket(Bucket& bucket) {
    size_t data_bytes = config_.bucket_bytes - 8;
    uint64_t ext_ptr = getExtensionPtr(bucket);
    std::memset(bucket.data, 0, data_bytes);
    setExtensionPtr(bucket, ext_ptr);

    BitWriter writer(bucket.data, 0);
    for (uint32_t s = 0; s < numLSlots(); ++s) {
        writer.writeUnary(0);
    }
}

}  // namespace internal
}  // namespace kvlite
