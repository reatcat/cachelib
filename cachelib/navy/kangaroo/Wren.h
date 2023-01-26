#pragma once
#include <chrono>
#include <mutex>
#include <shared_mutex>

#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/kangaroo/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
class FairyWREN  {
 public:
  class EuIterator {
    // Iterator, does not hold any locks
    // need to guarantee no writes to EU during GC process
    public: 
      bool done() { return done_; }
      KangarooBucketId getBucket() { return kbid_; }

    private:
      friend FairyWREN;

      explicit EuIterator(KangarooBucketId kbid, KangarooBucketId next_kbid): 
            kbid_{kbid}, next_kbid_{next_kbid} {}
      bool done_ = false;
      KangarooBucketId kbid_;
      KangarooBucketId next_kbid_; // next bucket in EU
  };

  // Throw std::invalid_argument on bad config
  explicit FairyWREN(Device* device, uint64_t numBuckets, uint32_t bucketSize);
  ~FairyWREN();

  FairyWREN(const FairyWREN&) = delete;
  FairyWREN& operator=(const FairyWREN&) = delete;

  Buffer read(KangarooBucketId kbid);
  bool write(KangarooBucketId kbid, Buffer buffer);

  bool shouldClean();
  EuIterator getEuIterator();
  EuIterator getNext(EuIterator); // of next zone to erase
  bool erase(); // will throw error if not all buckets are rewritten

 private:
  Device* device_{nullptr};

  // Erase Unit Id
  class EuId {
   public:
    explicit EuId(uint32_t idx) : idx_{idx} {}

    bool operator==(const EuId& rhs) const noexcept {
      return idx_ == rhs.idx_;
    }
    bool operator!=(const EuId& rhs) const noexcept {
      return !(*this == rhs);
    }

    uint32_t index() const noexcept { return idx_; }

   private:
    uint64_t idx_;
  };

  struct EuIdentifier {
    EuId euid_; // location on device
    KangarooBucketId nextKbid_; // next bucket in zone

    EuIdentifier() : euid_{EuId((uint32_t) -1)}, 
      nextKbid_{KangarooBucketId((uint32_t) -1)} {}
  };
  struct EuMetadata {
    KangarooBucketId firstKbid_;

    EuMetadata() : firstKbid_{KangarooBucketId((uint32_t) -1)} {}
    EuMetadata(KangarooBucketId kbid) : firstKbid_{kbid} {}
  };

  EuId calcEuId(uint32_t erase_unit, uint32_t offset);
  EuId findEuId(KangarooBucketId kbid);
  uint32_t calcZone(EuId zbid);

  // page to zone and offset mapping
  EuIdentifier* kbidToEuid_;
  EuMetadata* euMetadata_;

  // only implementing FIFO eviction
  folly::SharedMutex writeMutex_;
  uint64_t writeEraseUnit_{0};
  uint64_t eraseEraseUnit_{0};
  uint32_t writeOffset_{0};
  uint64_t cleaningThreshold_{2};
  uint64_t numEus_;
  uint64_t euCap_; // actual erase unit usable capacity
  uint64_t euSize_; // max erase unit capacity (how eus are aligned)
  uint64_t numBuckets_;
  uint32_t bucketSize_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
