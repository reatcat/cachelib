#include <mutex>
#include <shared_mutex>

#include "cachelib/navy/kangaroo/FairyWREN.h"

namespace facebook {
namespace cachelib {
namespace navy {

FairyWREN::EuIterator FairyWREN::getEuIterator() {
    KangarooBucketId first_kbid = euMetadata_[eraseEraseUnit_].firstKbid_;
    if (first_kbid.index() > numBuckets_) {
        auto ret = EuIterator(first_kbid, first_kbid);
        ret.done_ = true;
        return ret;
    }
    return EuIterator(first_kbid, kbidToEuid_[first_kbid.index()].nextKbid_);
}

FairyWREN::EuIterator FairyWREN::getNext(EuIterator euit) {
    if (euit.next_kbid_.index() > numBuckets_) {
        auto ret = EuIterator(euit.next_kbid_, euit.next_kbid_);
        ret.done_ = true;
        return ret;
    }
    return EuIterator(euit.next_kbid_, kbidToEuid_[euit.next_kbid_.index()].nextKbid_);
}

FairyWREN::FairyWREN(Device* device, uint64_t numBuckets, uint32_t bucketSize)
            : device_{device}, 
            numEus_{device_->getIONrOfZones()},
            eraseEraseUnit_{numEus_ - 1},
            euCap_{device_->getIOZoneCapSize()},
            numBuckets_{numBuckets}, 
            bucketSize_{bucketSize} {
    euMetadata_ = new EuMetadata[numEus_];
    kbidToEuid_ = new EuIdentifier[numBuckets_];
}

FairyWREN::~FairyWREN() {
    delete euMetadata_;
    delete kbidToEuid_;
}

FairyWREN::EuId FairyWREN::calcEuId(uint32_t erase_unit, uint32_t offset) {
    uint64_t euOffset = erase_unit * device_->getIOZoneSize();
    return EuId(euOffset + offset);
}

FairyWREN::EuId FairyWREN::findEuId(KangarooBucketId kbid) {
    XDCHECK(kbid.index() < numBuckets_);
    return kbidToEuid_[kbid.index()].euid_;
}

Buffer FairyWREN::read(KangarooBucketId kbid) {
    EuId euid = findEuId(kbid);

    auto buffer = device_->makeIOBuffer(bucketSize_);
    XDCHECK(!buffer.isNull());

    const bool res = device_->read(euid.index(), buffer.size(), buffer.data());
    if (!res) {
        return {};
    }

    return buffer;
}

bool FairyWREN::write(KangarooBucketId kbid, Buffer buffer) {
    {
        // TODO: deserialize
        std::unique_lock<folly::SharedMutex> lock{writeMutex_};
        if (writeEraseUnit_ == eraseEraseUnit_) {
            XLOG(INFO, "Writing caught up to erasing");
            return false;
        }

        // TODO: need to update chain before changing and deal with synchronization
        EuId euid = calcEuId(writeOffset_, writeEraseUnit_);
        kbidToEuid_[kbid.index()].euid_ = euid;
        kbidToEuid_[kbid.index()].nextKbid_  = euMetadata_[writeEraseUnit_].firstKbid_;
        euMetadata_[writeEraseUnit_].firstKbid_ = kbid;
        
        writeOffset_++;
        if (writeOffset_ > euCap_/bucketSize_) {
            writeEraseUnit_ = (writeEraseUnit_ + 1) % numEus_;
            writeOffset_ = 0;
        }

        return device_->write(euid.index(), std::move(buffer));
    }
}

bool FairyWREN::shouldClean(double cleaningThreshold) {
    uint32_t freeEus = 0;
    uint32_t writeEu = writeEraseUnit_;
    if (eraseEraseUnit_ >= writeEu) {
        freeEus = eraseEraseUnit_ - writeEu;
    } else {
        freeEus = eraseEraseUnit_ + (numEus_ - writeEu);
    }
    return freeEus <= cleaningThreshold;
}

bool FairyWREN::erase() {
    EuId euid = calcEuId(eraseEraseUnit_, 0);
    eraseEraseUnit_ = (eraseEraseUnit_ + 1) % numEus_;
    return device_->reset(euid.index(), euCap_);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
