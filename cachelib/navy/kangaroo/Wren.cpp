#include <mutex>
#include <shared_mutex>

#include "cachelib/navy/kangaroo/Wren.h"

namespace facebook {
namespace cachelib {
namespace navy {

Wren::EuIterator Wren::getEuIterator() {
  KangarooBucketId first_kbid = euMetadata_[eraseEraseUnit_].firstKbid_;
  if (first_kbid.index() > numBuckets_) {
    auto ret = EuIterator(first_kbid, first_kbid);
    ret.done_ = true;
    return ret;
  }
  return EuIterator(first_kbid, kbidToEuid_[first_kbid.index()].nextKbid_);
}

Wren::EuIterator Wren::getNext(EuIterator euit) {
  if (euit.next_kbid_.index() > numBuckets_) {
    auto ret = EuIterator(euit.next_kbid_, euit.next_kbid_);
    ret.done_ = true;
    return ret;
  }
  return EuIterator(euit.next_kbid_, kbidToEuid_[euit.next_kbid_.index()].nextKbid_);
}

Wren::Wren(Device& device, uint64_t numBuckets, uint64_t bucketSize, uint64_t totalSize, uint64_t setOffset)
          : device_{device}, 
          numEus_{totalSize / euCap_},
          eraseEraseUnit_{numEus_ - 1},
          euCap_{device_.getIOZoneCapSize()},
          numBuckets_{numBuckets}, 
          bucketSize_{bucketSize},
          setOffset_{setOffset} {
  euMetadata_ = new EuMetadata[numEus_];
  kbidToEuid_ = new EuIdentifier[numBuckets_];
  XLOGF(INFO, "Num WREN zones {} from size {}, starting at writeEraseUnit_ {}", 
      numEus_, totalSize, writeEraseUnit_);
}

Wren::~Wren() {
  delete euMetadata_;
  delete kbidToEuid_;
}

Wren::EuId Wren::calcEuId(uint32_t erase_unit, uint32_t offset) {
  uint64_t euOffset = erase_unit * (euCap_ / bucketSize_);
  return EuId(euOffset + offset);
}

Wren::EuId Wren::findEuId(KangarooBucketId kbid) {
  XDCHECK(kbid.index() < numBuckets_);
  return kbidToEuid_[kbid.index()].euid_;
}

uint64_t Wren::getEuIdLoc(uint32_t erase_unit, uint32_t offset) {
  return getEuIdLoc(calcEuId(erase_unit, offset));
}

uint64_t Wren::getEuIdLoc(EuId euid) {
  uint64_t zone_offset = euid.index() % (euCap_ / bucketSize_);
  uint64_t zone = euid.index() / (euCap_ / bucketSize_);
  uint64_t offset = setOffset_ + zone_offset * bucketSize_ 
    + zone * device_.getIOZoneSize();
}

Buffer Wren::read(KangarooBucketId kbid) {
  EuId euid = findEuId(kbid);
  if (euid.index() >= numEus_ * euCap_) {
    // kbid has not yet been defined
    return device_.makeIOBuffer(bucketSize_);
  }
  uint64_t loc = getEuIdLoc(euid);

  auto buffer = device_.makeIOBuffer(bucketSize_);
  XDCHECK(!buffer.isNull());

  const bool res = device_.read(loc, buffer.size(), buffer.data());
  if (!res) {
    return {};
  }

  return buffer;
}

bool Wren::write(KangarooBucketId kbid, Buffer buffer) {
  {
    // TODO: deserialize
    std::unique_lock<folly::SharedMutex> lock{writeMutex_};
    if (writeEraseUnit_ == eraseEraseUnit_) {
        XLOG(INFO, "Writing caught up to erasing");
        return false;
    }
  
    if (writeOffset_ == 0) {
      XLOGF(INFO, "WREN Write: reseting zone {}", 
          getEuIdLoc(writeEraseUnit_, 0)/device_.getIOZoneSize());
      device_.reset(getEuIdLoc(writeEraseUnit_, 0), device_.getIOZoneSize());
    }

    // TODO: need to update chain before changing and deal with synchronization
    EuId euid = calcEuId(writeEraseUnit_, writeOffset_);
    uint64_t loc = getEuIdLoc(euid);
    /*XLOGF(INFO, "Writing {} bucket to zone {}, offset {}, location {}", 
        kbid.index(), loc / device_.getIOZoneSize(), 
        (loc % device_.getIOZoneSize()) / bucketSize_, loc);*/
    XDCHECK(euid.index() < numEus_ * euCap_);
    kbidToEuid_[kbid.index()].euid_ = euid;
    kbidToEuid_[kbid.index()].nextKbid_  = euMetadata_[writeEraseUnit_].firstKbid_;
    euMetadata_[writeEraseUnit_].firstKbid_ = kbid;

    bool ret = device_.write(loc, std::move(buffer));
    if (!ret) {
      XLOGF(INFO, "tried to write at {} euid, {}.{} calculated zone + offset, write zone {}, loc {}", 
          euid.index(), euid.index() / (euCap_/ bucketSize_), euid.index() % (euCap_/bucketSize_),
          writeEraseUnit_, loc);
    }
    
    writeOffset_++;
    if (writeOffset_ >= euCap_/bucketSize_) {
      device_.finish(getEuIdLoc(writeEraseUnit_, 0), device_.getIOZoneSize());
      XLOGF(INFO, "WREN Write: finishing zone {} old eu {} / {}", 
          getEuIdLoc(writeEraseUnit_, 0)/device_.getIOZoneSize(), writeEraseUnit_, numEus_);
      writeEraseUnit_ = (writeEraseUnit_ + 1) % numEus_;
      XLOGF(INFO, "WREN Write: new zone {} new eu {} / {}", 
          getEuIdLoc(writeEraseUnit_, 0)/device_.getIOZoneSize(), writeEraseUnit_, numEus_);
      writeOffset_ = 0;
    }

    return ret;

  }
}

bool Wren::shouldClean(double cleaningThreshold) {
  uint32_t freeEus = 0;
  uint32_t writeEu = writeEraseUnit_;
  if (eraseEraseUnit_ >= writeEu) {
    freeEus = eraseEraseUnit_ - writeEu;
  } else {
    freeEus = eraseEraseUnit_ + (numEus_ - writeEu);
  }
  return freeEus <= cleaningThreshold;
}

bool Wren::erase() {
  EuId euid = calcEuId(eraseEraseUnit_, 0);
  eraseEraseUnit_ = (eraseEraseUnit_ + 1) % numEus_;
  return device_.reset(euid.index(), euCap_);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
