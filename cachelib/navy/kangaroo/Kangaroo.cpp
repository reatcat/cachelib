#include <chrono>
#include <mutex>
#include <shared_mutex>

#include <folly/Format.h>
#include <folly/Random.h>

#include "cachelib/navy/kangaroo/Kangaroo.h"
#include "cachelib/navy/kangaroo/RripBucket.h"
#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
constexpr uint64_t kMinSizeDistribution = 64;
constexpr uint64_t kMinThresholdSizeDistribution = 8;
constexpr double kSizeDistributionGranularityFactor = 1.25;
} // namespace

constexpr uint32_t Kangaroo::kFormatVersion;

Kangaroo::Config& Kangaroo::Config::validate() {
  if (totalSetSize < bucketSize) {
    throw std::invalid_argument(
        folly::sformat("cache size: {} cannot be smaller than bucket size: {}",
                       totalSetSize,
                       bucketSize));
  }

  if (!folly::isPowTwo(bucketSize)) {
    throw std::invalid_argument(
        folly::sformat("invalid bucket size: {}", bucketSize));
  }

  if (totalSetSize > uint64_t{bucketSize} << 32) {
    throw std::invalid_argument(folly::sformat(
        "Can't address Kangaroo with 32 bits. Cache size: {}, bucket size: {}",
        totalSetSize,
        bucketSize));
  }

  if (cacheBaseOffset % bucketSize != 0 || totalSetSize % bucketSize != 0) {
    throw std::invalid_argument(folly::sformat(
        "cacheBaseOffset and totalSetSize need to be a multiple of bucketSize. "
        "cacheBaseOffset: {}, totalSetSize:{}, bucketSize: {}.",
        cacheBaseOffset,
        totalSetSize,
        bucketSize));
  }

  if (device == nullptr) {
    throw std::invalid_argument("device cannot be null");
  }

  if (rripBitVector == nullptr) {
    throw std::invalid_argument("need a RRIP bit vector");
  }

  if (bloomFilter && bloomFilter->numFilters() != numBuckets()) {
    throw std::invalid_argument(
        folly::sformat("bloom filter #filters mismatch #buckets: {} vs {}",
                       bloomFilter->numFilters(),
                       numBuckets()));
  }

  if (logConfig.logSize > 0 && avgSmallObjectSize == 0) {
    throw std::invalid_argument(
        folly::sformat("Need an avgSmallObjectSize for the log"));
  }

  if (hotBucketSize >= bucketSize) {
    throw std::invalid_argument(
        folly::sformat("Hot bucket size {} needs to be less than bucket size {}",
          hotBucketSize,
          bucketSize));
  }
  
  if (hotSetSize >= totalSetSize) {
    throw std::invalid_argument(
        folly::sformat("Hot cache size {} needs to be less than total set size {}",
          hotSetSize,
          totalSetSize));
  }
  return *this;
}

Kangaroo::Kangaroo(Config&& config)
    : Kangaroo{std::move(config.validate()), ValidConfigTag{}} {}

Kangaroo::Kangaroo(Config&& config, ValidConfigTag)
    : numCleaningThreads_{config.mergeThreads},
      enableHot_{config.hotColdSep},
      destructorCb_{[this, cb = std::move(config.destructorCb)](
                        HashedKey hk,
                        BufferView value,
                        DestructorEvent event) {
        sizeDist_.removeSize(hk.key().size() + value.size());
        if (cb) {
          cb(hk, value, event);
        }
      }},
      hotBucketSize_{config.hotBucketSize},
      bucketSize_{config.bucketSize},
      cacheBaseOffset_{config.cacheBaseOffset},
      hotCacheBaseOffset_{config.hotBaseOffset()},
      numBuckets_{config.numBuckets()},
      bloomFilter_{std::move(config.bloomFilter)},
      bitVector_{std::move(config.rripBitVector)},
      device_{*config.device},
      wrenDevice_{new Wren(device_, numBuckets_, bucketSize_ - hotBucketSize_, 
                config.totalSetSize - config.hotSetSize, cacheBaseOffset_)},
      wrenHotDevice_{new Wren(device_, numBuckets_, hotBucketSize_, 
                config.hotSetSize, hotCacheBaseOffset_)},
      sizeDist_{kMinSizeDistribution, bucketSize_,
                kSizeDistributionGranularityFactor},
      thresholdSizeDist_{10, bucketSize_, 10},
      thresholdNumDist_{1, 25, 1} {
  XLOGF(INFO,
        "Kangaroo created: buckets: {}, bucket size: {}, base offset: {}, hot base offset {}",
        numBuckets_,
        bucketSize_,
        cacheBaseOffset_,
        hotCacheBaseOffset_);
  XLOGF(INFO,
        "Kangaroo created: base offset zone: {}, hot base offset zone {}",
        cacheBaseOffset_ / (double) device_.getIOZoneSize(),
        hotCacheBaseOffset_ / (double) device_.getIOZoneSize());
  if (config.logConfig.logSize) {
    SetNumberCallback cb = [&](uint64_t hk) {return getKangarooBucketIdFromHash(hk);};
    config.logConfig.setNumberCallback = cb;
    config.logConfig.logIndexPartitions = config.logIndexPartitionsPerPhysical * config.logConfig.logPhysicalPartitions;
    uint64_t bytesPerIndex = config.logConfig.logSize / config.logConfig.logIndexPartitions;
    config.logConfig.device = config.device;
    fwLog_ = std::make_unique<FwLog>(std::move(config.logConfig));
  }

  reset();
  
  cleaningThreads_.reserve(numCleaningThreads_);
	for (uint64_t i = 0; i < numCleaningThreads_; i++) {
    if (!i) {
      cleaningThreads_.push_back(std::thread(&Kangaroo::cleanSegmentsLoop, this));
    } else {
      cleaningThreads_.push_back(std::thread(&Kangaroo::cleanSegmentsWaitLoop, this));
    }
	}

}

void Kangaroo::reset() {
  XLOG(INFO, "Reset Kangaroo");
  generationTime_ = getSteadyClock();

  if (bloomFilter_) {
    bloomFilter_->reset();
  }

  itemCount_.set(0);
  insertCount_.set(0);
  succInsertCount_.set(0);
  lookupCount_.set(0);
  succLookupCount_.set(0);
  removeCount_.set(0);
  succRemoveCount_.set(0);
  evictionCount_.set(0);
  logicalWrittenCount_.set(0);
  physicalWrittenCount_.set(0);
  ioErrorCount_.set(0);
  bfFalsePositiveCount_.set(0);
  bfProbeCount_.set(0);
  checksumErrorCount_.set(0);
  sizeDist_.reset();
}

double Kangaroo::bfFalsePositivePct() const {
  const auto probes = bfProbeCount_.get();
  if (bloomFilter_ && probes > 0) {
    return 100.0 * bfFalsePositiveCount_.get() / probes;
  } else {
    return 0;
  }
}

void Kangaroo::redivideBucket(RripBucket* hotBucket, RripBucket* coldBucket) {
  std::vector<std::unique_ptr<ObjectInfo>> movedKeys; // using hits as rrip value
  RedivideCallback moverCb = [&](HashedKey hk, BufferView value, uint8_t rrip_value) {
    auto ptr = std::make_unique<ObjectInfo>(hk, value, rrip_value, LogPageId(), 0);
    movedKeys.push_back(std::move(ptr));
    return;
  };

  RripBucket::Iterator it = coldBucket->getFirst();
  while (!it.done()) {

    // lower rrip values (ie highest pri objects) are at the end
    RripBucket::Iterator nextIt = coldBucket->getNext(it);
    while (!nextIt.done()) {
      nextIt = coldBucket->getNext(nextIt);
    }

    bool space = hotBucket->isSpaceRrip(it.hashedKey(), it.value(), it.rrip());
    if (!space) {
      break;
    }

    hotBucket->makeSpace(it.hashedKey(), it.value(), moverCb);
    hotBucket->insertRrip(it.hashedKey(), it.value(), it.rrip(), nullptr);
    coldBucket->remove(it.hashedKey(), nullptr);
    it = coldBucket->getFirst(); // just removed the last object
  }

  // move objects into cold bucket (if there's any overflow evict lower pri items) 
  for (auto& oi: movedKeys) {
    coldBucket->insertRrip(oi->key, oi->value.view(), oi->hits, nullptr);
  }
}

void Kangaroo::moveBucket(KangarooBucketId kbid, bool logFlush, int gcMode) {
  insertCount_.inc();
  multiInsertCalls_.inc();

  uint64_t insertCount = 0;
  uint64_t evictCount = 0;
  uint64_t removedCount = 0;

  uint64_t passedItemSize = 0;
  uint64_t passedCount = 0;

	std::vector<std::unique_ptr<ObjectInfo>> ois;
  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(kbid)};

    if (logFlush || fwOptimizations_) {
      ois = fwLog_->getObjectsToMove(kbid, logFlush);
    }
    if (!ois.size() && logFlush) {
      if (kbid.index() % 10000 == 5) {
        XLOGF(INFO, "No objects to move for {}", kbid.index());
      }
      return; // still need to move bucket if gc caused
    }
    
    auto coldBuffer = readBucket(kbid, false);
    if (coldBuffer.isNull()) {
      if (kbid.index() % 10000 == 5) {
        XLOGF(INFO, "cold bucket read error for {}", kbid.index());
      }
      ioErrorCount_.inc();
      return; 
    }
    auto* coldBucket = reinterpret_cast<RripBucket*>(coldBuffer.data());
    if (kbid.index() % 100000 == 5) {
      XLOGF(INFO, "Cold bucket ({}) has {} objects", kbid.index(), coldBucket->size());
    }
    
    auto hotBuffer = readBucket(kbid, true);
    if (hotBuffer.isNull()) {
      ioErrorCount_.inc();
      return; 
    }
    auto* hotBucket = reinterpret_cast<RripBucket*>(hotBuffer.data());
    uint32_t hotCount = hotBucket->size();
    if (kbid.index() % 100000 == 5) {
      XLOGF(INFO, "Hot bucket ({}) has {} objects", kbid.index(), hotBucket->size());
    }

    coldBucket->reorder([&](uint32_t keyIdx) {return bvGetHit(kbid, hotCount + keyIdx);});

    uint32_t random = folly::Random::rand32(0, hotRebuildFreq_);
    if (!random) {
      // need to rebuild hot-cold seperation
      /*if (kbid.index() % 2000 == 10) {
        XLOGF(INFO, "Cold bucket ({}) has {} objects before divide", kbid.index(), coldBucket->size());
        XLOGF(INFO, "Hot bucket ({}) has {} objects before divide", kbid.index(), hotBucket->size());
        XLOGF(INFO, "Redividing objects ({})", kbid.index());
      }*/

      hotBucket->reorder([&](uint32_t keyIdx) {return bvGetHit(kbid, keyIdx);});
      redivideBucket(hotBucket, coldBucket);

      /*if (kbid.index() % 2000 == 10) {
        XLOGF(INFO, "Cold bucket ({}) has {} objects after divide", kbid.index(), coldBucket->size());
        XLOGF(INFO, "Hot bucket ({}) has {} objects after divide", kbid.index(), hotBucket->size());
      }*/

    }
    bitVector_->clear(kbid.index());
      
    if (!random || gcMode == 2) {
      // rewrite hot bucket if either gc caused by hot sets or redivide
      const auto res = writeBucket(kbid, std::move(hotBuffer), true);
      if (bloomFilter_) {
        bloomFilter_->clear(kbid.index());
      }
      ioErrorCount_.inc();
    }

    // only insert new objects into cold set
    for (auto& oi: ois) {
      passedItemSize += oi->key.key().size() + oi->value.size();
      passedCount++;

      if (coldBucket->isSpace(oi->key, oi->value.view(), oi->hits)) {
        removedCount += coldBucket->remove(oi->key, nullptr);
        evictCount += coldBucket->insert(oi->key, oi->value.view(), oi->hits, nullptr);
        sizeDist_.addSize(oi->key.key().size() + oi->value.size());
        insertCount++;
      } else {
        //XLOGF(INFO, "Readmitting {}", kbid.index());
        fwLog_->readmit(oi);
        readmitInsertCount_.inc();
      }
    }
    /*if (kbid.index() % 20000 == 10) {
      XLOGF(INFO, "Cold bucket ({}) has {} objects after move bucket", kbid.index(), coldBucket->size());
    }*/

    //XLOGF(INFO, "Write bucket {}", kbid.index());
    const auto res = writeBucket(kbid, std::move(coldBuffer), false);
    if (!res) {
      if (bloomFilter_) {
        bloomFilter_->clear(kbid.index());
      }
      ioErrorCount_.inc();
      return;
    }

    if (bloomFilter_) {
      bfRebuild(kbid, hotBucket);
      bfBuild(kbid, coldBucket);
    }
  }

  thresholdSizeDist_.addSize(passedItemSize);
  thresholdNumDist_.addSize(passedCount * 2);

  setInsertCount_.add(insertCount);
  itemCount_.sub(evictCount + removedCount);
  logItemCount_.sub(insertCount);
  setItemCount_.sub(evictCount + removedCount);
  setItemCount_.add(insertCount);
  evictionCount_.add(evictCount);
  succInsertCount_.add(insertCount);
        
  physicalWrittenCount_.add(bucketSize_);
  //XLOGF(INFO, "Finish bucket {}", kbid.index());
  return;
}

uint64_t Kangaroo::getMaxItemSize() const {
  // does not include per item overhead
  return bucketSize_ - sizeof(RripBucket);
}

void Kangaroo::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bh_items", itemCount_.get());
  visitor("navy_bh_inserts", insertCount_.get());
  visitor("navy_bh_succ_inserts", succInsertCount_.get());
  visitor("navy_bh_lookups", lookupCount_.get());
  visitor("navy_bh_succ_lookups", succLookupCount_.get());
  visitor("navy_bh_removes", removeCount_.get());
  visitor("navy_bh_succ_removes", succRemoveCount_.get());
  visitor("navy_bh_evictions", evictionCount_.get());
  visitor("navy_bh_logical_written", logicalWrittenCount_.get());
  uint64_t logBytesWritten = (fwLog_) ? fwLog_->getBytesWritten() : 0;
  visitor("navy_bh_physical_written", physicalWrittenCount_.get() + logBytesWritten);
  visitor("navy_bh_io_errors", ioErrorCount_.get());
  visitor("navy_bh_bf_false_positive_pct", bfFalsePositivePct());
  visitor("navy_bh_checksum_errors", checksumErrorCount_.get());
	visitor("navy_fwlog_false_positive_pct", fwLog_->falsePositivePct());
	visitor("navy_fwlog_fragmentation_pct", fwLog_->fragmentationPct());
	visitor("navy_fwlog_extra_reads_pct", fwLog_->extraReadsPct());
  auto snapshot = sizeDist_.getSnapshot();
  for (auto& kv : snapshot) {
    auto statName = folly::sformat("navy_bh_approx_bytes_in_size_{}", kv.first);
    visitor(statName.c_str(), kv.second);
  }
}

void Kangaroo::persist(RecordWriter& rw) {
  XLOG(INFO, "Starting kangaroo persist");
  serialization::BigHashPersistentData pd;
  *pd.version() = kFormatVersion;
  *pd.generationTime() = generationTime_.count();
  *pd.itemCount() = itemCount_.get();
  *pd.bucketSize() = bucketSize_;
  *pd.cacheBaseOffset() = cacheBaseOffset_;
  *pd.numBuckets() = numBuckets_;
  *pd.sizeDist() = sizeDist_.getSnapshot();
  serializeProto(pd, rw);

  if (bloomFilter_) {
    bloomFilter_->persist<ProtoSerializer>(rw);
    XLOG(INFO, "bloom filter persist done");
  }

  XLOG(INFO, "Finished kangaroo persist");
}

bool Kangaroo::recover(RecordReader& rr) {
  XLOG(INFO, "Starting kangaroo recovery");
  try {
    auto pd = deserializeProto<serialization::BigHashPersistentData>(rr);
    if (*pd.version() != kFormatVersion) {
      throw std::logic_error{
          folly::sformat("invalid format version {}, expected {}",
                         *pd.version(),
                         kFormatVersion)};
    }

    auto configEquals =
        static_cast<uint64_t>(*pd.bucketSize()) == bucketSize_ &&
        static_cast<uint64_t>(*pd.cacheBaseOffset()) == cacheBaseOffset_ &&
        static_cast<uint64_t>(*pd.numBuckets()) == numBuckets_;
    if (!configEquals) {
      auto configStr = serializeToJson(pd);
      XLOGF(ERR, "Recovery config: {}", configStr.c_str());
      throw std::logic_error{"config mismatch"};
    }

    generationTime_ = std::chrono::nanoseconds{*pd.generationTime()};
    itemCount_.set(*pd.itemCount());
    sizeDist_ = SizeDistribution{*pd.sizeDist_ref()};
    if (bloomFilter_) {
      bloomFilter_->recover<ProtoSerializer>(rr);
      XLOG(INFO, "Recovered bloom filter");
    }
  } catch (const std::exception& e) {
    XLOGF(ERR, "Exception: {}", e.what());
    XLOG(ERR, "Failed to recover kangaroo. Resetting cache.");

    reset();
    return false;
  }
  XLOG(INFO, "Finished kangaroo recovery");
  return true;
}

Status Kangaroo::insert(HashedKey hk,
                       BufferView value) {
  const auto bid = getKangarooBucketId(hk);
  insertCount_.inc();

  Status ret = fwLog_->insert(hk, value);
  if (ret == Status::Ok) {
    sizeDist_.addSize(hk.key().size() + value.size());
    succInsertCount_.inc();
  }
  logicalWrittenCount_.add(hk.key().size() + value.size());
  logInsertCount_.inc();
  itemCount_.inc();
  logItemCount_.inc();
  return ret;
}

Status Kangaroo::lookup(HashedKey hk, Buffer& value) {
  const auto bid = getKangarooBucketId(hk);
  lookupCount_.inc();

  // first check log if it exists
  if (fwLog_) {
    Status ret = fwLog_->lookup(hk, value);
    if (ret == Status::Ok) {
      succLookupCount_.inc();
      logHits_.inc();
      return ret;
    }
  }

  RripBucket* bucket{nullptr};
  Buffer buffer;
  BufferView valueView;
  // scope of the lock is only needed until we read and mutate state for the
  // bucket. Once the bucket is read, the buffer is local and we can find
  // without holding the lock.
  {
    std::shared_lock<folly::SharedMutex> lock{getMutex(bid)};

    if (bfReject(bid, hk.keyHash())) {
      return Status::NotFound;
    }

    buffer = readBucket(bid, true); 
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }

    bucket = reinterpret_cast<RripBucket*>(buffer.data());

    /* TODO: moving this inside lock could cause performance problem */
    valueView = bucket->find(hk, [&](uint32_t keyIdx) {bvSetHit(bid, keyIdx);});
    
    uint64_t hotItems = 0;
    if (valueView.isNull()) {
      uint64_t hotItems = bucket->size();
      buffer = readBucket(bid, false);
      if (buffer.isNull()) {
        ioErrorCount_.inc();
        return Status::DeviceError;
      }

      bucket = reinterpret_cast<RripBucket*>(buffer.data());

      /* TODO: moving this inside lock could cause performance problem */
      valueView = bucket->find(hk, [&](uint32_t keyIdx) {bvSetHit(bid, keyIdx + hotItems);});
    } else {
        hotSetHits_.inc();
    }
  }
  
  if (valueView.isNull()) {
    bfFalsePositiveCount_.inc();
    if (bid.index() % 20000 == 10) {
      XLOGF(INFO, "valueView is null bid {}, key {}", bid.index(), hk.keyHash());
    }
    return Status::NotFound;
  }
  if (bid.index() % 20000 == 10) {
    XLOGF(INFO, "Found a set hit bucket {}, key {}", bid.index(), hk.keyHash());
  }
  value = Buffer{valueView};
  succLookupCount_.inc();
  setHits_.inc();
  return Status::Ok;
}

Status Kangaroo::remove(HashedKey hk) {
  const auto bid = getKangarooBucketId(hk);
  removeCount_.inc();

  if (fwLog_) {
    Status ret = fwLog_->remove(hk);
    if (ret == Status::Ok) {
      succRemoveCount_.inc();
      itemCount_.dec();
      logItemCount_.dec();
      return ret;
    }
  }

  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(bid)};
    if (bfReject(bid, hk.keyHash())) {
      return Status::NotFound;
    }

    // Get hot bucket first
    auto buffer = readBucket(bid, true);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }
    auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());
    bucket->reorder([&](uint32_t keyIdx) {return bvGetHit(bid, keyIdx);});

    // Get cold bucket
    uint64_t hotItems = bucket->size();
    auto coldBuffer = readBucket(bid, false);
    if (coldBuffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }
    auto* coldBucket = reinterpret_cast<RripBucket*>(coldBuffer.data());
    coldBucket->reorder([&](uint32_t keyIdx) {return bvGetHit(bid, keyIdx + hotItems);});

    if (!bucket->remove(hk, destructorCb_)) {
      if (!coldBucket->remove(hk, destructorCb_)) {
        bfFalsePositiveCount_.inc();
        return Status::NotFound;
      }
    }

    bool res;
    if (coldBucket) {
      res = writeBucket(bid, std::move(coldBuffer), false);
    } else {
      res = writeBucket(bid, std::move(buffer), true);
    }

    if (!res) {
      if (bloomFilter_) {
        bloomFilter_->clear(bid.index());
      }
      ioErrorCount_.inc();
      return Status::DeviceError;
    }

    if (bloomFilter_) {
      bfRebuild(bid, bucket);
      bfBuild(bid, coldBucket);
    }
    bitVector_->clear(bid.index());
  }

  itemCount_.dec();
  setItemCount_.dec();

  // We do not bump logicalWrittenCount_ because logically a
  // remove operation does not write, but for Kangaroo, it does
  // incur physical writes.
  physicalWrittenCount_.add(bucketSize_);
  succRemoveCount_.inc();
  return Status::Ok;
}

bool Kangaroo::couldExist(HashedKey hk) {
  const auto bid = getKangarooBucketId(hk);
  bool canExist = false;

  if (fwLog_) {
    canExist = fwLog_->couldExist(hk);
  }

  if (!canExist) {
    std::shared_lock<folly::SharedMutex> lock{getMutex(bid)};
    canExist = !bfReject(bid, hk.keyHash());
  }

  // the caller is not likely to issue a subsequent lookup when we return
  // false. hence tag this as a lookup. If we return the key can exist, the
  // caller will perform a lookupAsync and will be counted within lookup api.
  if (!canExist) {
    lookupCount_.inc();
  }
  
  if (lookupCount_.get() % 5000000 == 0) {
    XLOGF(INFO, "Lookup count {}, insert count {} log {} sets, set hits {} hot set hits {} log hits {}", 
        lookupCount_.get(), logInsertCount_.get(), setInsertCount_.get(),
        setHits_.get(), hotSetHits_.get(), logHits_.get());
  }

  return canExist;
}

bool Kangaroo::bfReject(KangarooBucketId bid, uint64_t keyHash) const {
  if (bloomFilter_) {
    bfProbeCount_.inc();
    if (!bloomFilter_->couldExist(bid.index(), keyHash)) {
      bfRejectCount_.inc();
      return true;
    }
  }
  return false;
}

bool Kangaroo::bvGetHit(KangarooBucketId bid, uint32_t keyIdx) const {
  if (bitVector_) {
    return bitVector_->get(bid.index(), keyIdx);
  }
  return false;
}

void Kangaroo::bvSetHit(KangarooBucketId bid, uint32_t keyIdx) const {
  if (bitVector_) {
    bitVector_->set(bid.index(), keyIdx);
  }
}

void Kangaroo::bfRebuild(KangarooBucketId bid, const RripBucket* bucket) {
  XDCHECK(bloomFilter_);
  XDCHECK(bucket);
  bloomFilter_->clear(bid.index());
  auto itr = bucket->getFirst();
  uint32_t i = 0;
  uint32_t total = bucket->size();
  while (!itr.done() && i < total) {
    if (i >= total) {
      XLOGF(INFO, "Bucket {}: has only {} items, iterating through {}, not done {}",
          bid.index(), total, i, itr.done());
    }
    bloomFilter_->set(bid.index(), itr.keyHash());
    itr = bucket->getNext(itr);
    i++;
  }
}

void Kangaroo::bfBuild(KangarooBucketId bid, const RripBucket* bucket) {
  XDCHECK(bloomFilter_);
  auto itr = bucket->getFirst();
  while (!itr.done()) {
    bloomFilter_->set(bid.index(), itr.keyHash());
    itr = bucket->getNext(itr);
  }
}

void Kangaroo::flush() {
  XLOG(INFO, "Flush big hash");
  device_.flush();
}

Buffer Kangaroo::readBucket(KangarooBucketId bid, bool hot) {
  Buffer buffer;
  bool newBuffer = false;
  if (hot) {
    buffer = wrenHotDevice_->read(bid, newBuffer);
  } else {
    buffer = wrenDevice_->read(bid, newBuffer);
  }

  if (bid.index() % 100000 == 5) {
    XLOGF(INFO, "Read {} hot {}, newBuffer {}, buffer is Null {}", 
        bid.index(), hot, newBuffer, buffer.isNull());
  }

  if (buffer.isNull()) {
    return {};
  }

  auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());

  const auto checksumSuccess =
      RripBucket::computeChecksum(buffer.view()) == bucket->getChecksum();
  // We can only know for certain this is a valid checksum error if bloom filter
  // is already initialized. Otherwise, it could very well be because we're
  // reading the bucket for the first time.
  if (!checksumSuccess && bloomFilter_) {
    checksumErrorCount_.inc();
  }

  if (!checksumSuccess || newBuffer || static_cast<uint64_t>(generationTime_.count()) !=
                              bucket->generationTime()) {
    if (bid.index() % 1000000 == 5) {
      XLOGF(INFO, "Reinit ({}) for hot? ({}), checksum suc? {}, newBuffer {}", 
          bid.index(), hot, !checksumSuccess, newBuffer);
    }
    RripBucket::initNew(buffer.mutableView(), generationTime_.count());
  }
  return buffer;
}

bool Kangaroo::writeBucket(KangarooBucketId bid, Buffer buffer, bool hot) {
  auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());
  bucket->setChecksum(RripBucket::computeChecksum(buffer.view()));
  if (hot) {
    return wrenHotDevice_->write(bid, std::move(buffer));
  }
  return wrenDevice_->write(bid, std::move(buffer));
}

bool Kangaroo::shouldLogFlush() {
	return fwLog_->shouldClean(flushingThreshold_);
}

bool Kangaroo::shouldUpperGC(bool hot) {
  if (hot) {
	  return wrenHotDevice_->shouldClean(gcUpperThreshold_);
  } else {
	  return wrenDevice_->shouldClean(gcUpperThreshold_);
  }
}

bool Kangaroo::shouldLowerGC(bool hot) {
  if (hot) {
	  return wrenHotDevice_->shouldClean(gcLowerThreshold_);
  } else {
	  return wrenDevice_->shouldClean(gcLowerThreshold_);
  }
}

void Kangaroo::performLogFlush() {
	{
    std::unique_lock<std::mutex> lock{cleaningSync_};
		cleaningSyncThreads_++;
	}

	while (true) {
		KangarooBucketId kbid = KangarooBucketId(0);
		{
			std::unique_lock<std::mutex> lock{cleaningSync_};
			if (performingLogFlush_ && !fwLog_->cleaningDone()) {
				kbid = fwLog_->getNextCleaningBucket();
			} else {
				break;
			}
		}
    if (kbid.index() % 100000 == 5) {
      XLOGF(INFO, "Moving kbid {}", kbid.index());
    }
		moveBucket(kbid, true, 0);
	}
	      
  performingLogFlush_ = false;
	{
    std::unique_lock<std::mutex> lock{cleaningSync_};
		cleaningSyncThreads_--;
		if (cleaningSyncThreads_ == 0) {
			cleaningSyncCond_.notify_all();
		} 	
	}
}

void Kangaroo::performGC() {
	{
    std::unique_lock<std::mutex> lock{cleaningSync_};
		cleaningSyncThreads_++;
	}

	bool done = false;
	while (!done) {
		KangarooBucketId kbid = KangarooBucketId(0);
    int gcMode = 0;
		{
			std::unique_lock<std::mutex> lock{cleaningSync_};
			if (performingGC_ && !euIterator_.done()) {
				kbid = euIterator_.getBucket();

        if (gcMode == 2) {
          euIterator_ = wrenHotDevice_->getNext(euIterator_);
        } else {
				  euIterator_ = wrenDevice_->getNext(euIterator_);
        }
        gcMode = performingGC_;
			} else {
				break;
			}
		}
    //XLOGF(INFO, "Moving kbid {}", kbid.index());
		moveBucket(kbid, false, gcMode);
	}

	performingGC_ = 0;
	{
    std::unique_lock<std::mutex> lock{cleaningSync_};
		cleaningSyncThreads_--;
		if (cleaningSyncThreads_ == 0) {
			cleaningSyncCond_.notify_all();
		} 	
	}
}

void Kangaroo::gcSetupTeardown(bool hot) {
  if (hot) {
    //XLOG(INFO, "Starting to clean HOT wren device");
    euIterator_ = wrenHotDevice_->getEuIterator();
    performingGC_ = 2;
  } else {
    //XLOG(INFO, "Starting to clean cold wren device");
    euIterator_ = wrenDevice_->getEuIterator();
    performingGC_ = 1;
  }
  cleaningSyncCond_.notify_all();
  performGC();

  {
    std::unique_lock<std::mutex> lock{cleaningSync_};
    while (cleaningSyncThreads_ != 0) {
      cleaningSyncCond_.wait(lock);
    }
  }
  if (hot) {
    wrenHotDevice_->erase();
  } else {
    wrenDevice_->erase();
  }
}

void Kangaroo::cleanSegmentsLoop() {
  //XLOG(INFO) << "Starting cleanSegmentsLooop";
  while (true) {
    if (killThread_) {
      break;
    }

    if (shouldLowerGC(false)) {
      gcSetupTeardown(false);
    } else if (shouldLowerGC(true)) {
      // sometimes need to clean hot sets
      gcSetupTeardown(true);
    } else if (shouldLogFlush()) {
      //XLOG(INFO) << "Starting Log Flush";
			// TODO: update locking mechanism
			fwLog_->startClean();
			performingLogFlush_ = true;
			cleaningSyncCond_.notify_all();

      //XLOG(INFO) << "After notify_all";
      performLogFlush();

			{
				std::unique_lock<std::mutex> lock{cleaningSync_};
				while (cleaningSyncThreads_ != 0) {
					cleaningSyncCond_.wait(lock);
				}
			}
			fwLog_->finishClean();
    } else if (shouldUpperGC(false)) {
      gcSetupTeardown(false);
    }
  }
	cleaningSyncCond_.notify_all();
}

void Kangaroo::cleanSegmentsWaitLoop() {
	// TODO: figure out locking
  while (true) {
    if (killThread_) {
      break;
    }

    if (performingGC_) {
        performGC();
    } else if (performingLogFlush_) {
        performLogFlush();
    }

		{
			std::unique_lock<std::mutex> lock{cleaningSync_};
			cleaningSyncCond_.wait(lock);
		}
  }
  exit(0);
}

Kangaroo::~Kangaroo() {
	killThread_ = true;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
