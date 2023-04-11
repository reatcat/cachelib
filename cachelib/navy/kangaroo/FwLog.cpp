#include <chrono>
#include <mutex>
#include <shared_mutex>

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include "cachelib/navy/kangaroo/FwLog.h"

namespace facebook {
namespace cachelib {
namespace navy {

Buffer FwLog::readLogPage(LogPageId lpid) {
  auto buffer = device_.makeIOBuffer(pageSize_);
  XDCHECK(!buffer.isNull());

  const bool res = 
    device_.read(getLogPageOffset(lpid), buffer.size(), buffer.data());
  if (!res) {
    return {};
  }
  // TODO: checksumming & generations
  return buffer;
}

Buffer FwLog::readLogSegment(LogSegmentId lsid) {
  auto buffer = device_.makeIOBuffer(segmentSize_);
  XDCHECK(!buffer.isNull());

  uint64_t offset = getLogSegmentOffset(lsid);
  const bool res = 
    device_.read(offset, buffer.size(), buffer.data());

  if (!res) {
    XLOG(INFO, "Read log segment failed");
    return {};
  }
  //XLOGF(INFO, "Read: reading zone {} offset {}, actual zone # {}, loc {}", 
  //    lsid.zone(), lsid.offset(), offset / device_.getIOZoneSize(), offset);
  // TODO: checksumming & generations
  return buffer;
}

bool FwLog::writeLogSegment(LogSegmentId lsid, Buffer buffer) {
  // TODO: set checksums 
  uint64_t offset = getLogSegmentOffset(lsid);
  if (lsid.offset() == 0) {
    XLOGF(INFO, "Write: reseting zone {}", offset / (double) device_.getIOZoneSize());
    device_.reset(offset, device_.getIOZoneSize());
  }
  logSegmentsWrittenCount_.inc();
  XLOGF(INFO, "Write: writing to zone {} offset {}, actual zone # {}, loc {}", 
      lsid.zone(), lsid.offset(), offset / device_.getIOZoneSize(), offset);
  bool ret =  device_.write(getLogSegmentOffset(lsid), std::move(buffer));
  if (!ret) {
    XLOGF(INFO, "FwLog Write Failed: writing to zone {} offset {}, actual zone # {}, loc {}", 
        lsid.zone(), lsid.offset(), offset / device_.getIOZoneSize(), offset);
  }
  if (getNextLsid(lsid).offset() == 0) {
    uint64_t zone_offset = getLogSegmentOffset(LogSegmentId(0, lsid.zone()));
    //XLOGF(INFO, "Write: finishing zone {}", zone_offset / (double) device_.getIOZoneSize());
    device_.finish(offset, device_.getIOZoneSize());
  }

  return ret;
}

bool FwLog::eraseSegments(LogSegmentId startLsid) {
	return device_.reset(getLogSegmentOffset(startLsid), flushGranularity_);
}

// only flushes buffer to flash if needed
bool FwLog::flushLogSegment(uint32_t partition, bool wait) {
  //XLOGF(INFO, "Flushing Log Segment because of partition {}", partition);
	if (!bufferMetadataMutex_.try_lock()) {
    if (wait) {
      sleep(.001);
    }
		return false;
	} else {
		uint32_t oldOffset = bufferedSegmentOffset_;
		uint32_t updatedOffset = (bufferedSegmentOffset_ + 1) % numBufferedLogSegments_;

		{
			std::shared_lock<folly::SharedMutex> nextLock{logSegmentMutexs_[updatedOffset]};
			if (currentLogSegments_[updatedOffset]->getFullness(partition) < overflowLimit_) {
				bufferMetadataMutex_.unlock();
        flushLogCv_.notify_all();
				return true;
			}
		}
    
		LogSegmentId oldLsid = currentLogSegments_[oldOffset]->getLogSegmentId();
    {
			std::unique_lock<folly::SharedMutex> lock{cleaningMutex_};
      while (oldLsid.zone() == nextLsidToClean_.zone() && flushLogOnce_) {
        XLOGF(INFO, "waiting for cleaning {}, bufferedSegmentOffset {}", oldLsid.zone(), bufferedSegmentOffset_);
        cleaningCv_.wait(lock);
        XLOGF(INFO, "First writing {}, bufferedSegmentOffset_ {}", oldLsid.zone(), bufferedSegmentOffset_);
      }
    }

    // refresh pointers if had to wait
		oldOffset = bufferedSegmentOffset_;
		updatedOffset = (bufferedSegmentOffset_ + 1) % numBufferedLogSegments_;
		bufferedSegmentOffset_ = updatedOffset;
		oldLsid = currentLogSegments_[oldOffset]->getLogSegmentId();

		
    LogSegmentId nextLsid;
		{
			std::unique_lock<folly::SharedMutex> oldSegmentLock{getMutexFromSegment(oldLsid)};
			std::unique_lock<folly::SharedMutex> oldBufferLock{logSegmentMutexs_[oldOffset]};

      nextLsid = getNextLsid(oldLsid, numBufferedLogSegments_);
      //XLOGF(INFO, "{}, Old {}.{} new {}.{}", numBufferedLogSegments_, oldLsid.zone(), oldLsid.offset(),
      //    nextLsid.zone(), nextLsid.offset());

      //XLOGF(INFO, "Old Lsid {} LBA, segmentSize {}", 
      //   getLogSegmentOffset(oldLsid) / 4096, segmentSize_);
			writeLogSegment(oldLsid, 
          std::move(Buffer(logSegmentBuffers_[oldOffset].view(), pageSize_)));
      currentLogSegments_[oldOffset]->clear(nextLsid);
		}
    
    if (nextLsid.zone() == numLogZones_ - numBufferedLogSegments_) {
      flushLogOnce_ = true;
    }
	
	}

  bufferMetadataMutex_.unlock();
  flushLogCv_.notify_all();

  return true;
}

LogSegmentId FwLog::getNextLsid(LogSegmentId lsid, uint32_t increment) {
	// partitions within segment so partition # doesn't matter
  XDCHECK(increment < numSegmentsPerZone_); // not correct otherwise
  if (lsid.offset() + increment >= numSegmentsPerZone_) {
    return LogSegmentId((lsid.offset() + increment) % numSegmentsPerZone_, 
        (lsid.zone() + 1) % numLogZones_);
  }
  return LogSegmentId(lsid.offset() + increment, lsid.zone());
}

LogSegmentId FwLog::getNextLsid(LogSegmentId lsid) {
  return getNextLsid(lsid, 1);
}

FwLog::~FwLog() {
  for (uint64_t i = 0; i < logIndexPartitions_; i++) {
    delete index_[i];
  }
  delete index_;
	for (uint32_t i = 0; i < numBufferedLogSegments_; i++) {
		delete currentLogSegments_[i];
	}
}

FwLog::FwLog(Config&& config)
    : FwLog{std::move(config.validate()), ValidConfigTag{}} {}

FwLog::Config& FwLog::Config::validate() {
  if (logSize < readSize) {
    throw std::invalid_argument(
        folly::sformat("log size: {} cannot be smaller than read size: {}",
                       logSize,
                       readSize));
  }
  
  if (logSize < segmentSize) {
    throw std::invalid_argument(
        folly::sformat("log size: {} cannot be smaller than segment size: {}",
                       logSize,
                       readSize));
  }

  if (!folly::isPowTwo(readSize)) {
    throw std::invalid_argument(
        folly::sformat("invalid read size: {}", readSize));
  }
  
  if (logSize > uint64_t{readSize} << 32) {
    throw std::invalid_argument(folly::sformat(
        "Can't address kangaroo log with 32 bits. Log size: {}, read size: {}",
        logSize,
        readSize));
  }

  if (segmentSize % readSize != 0 || logSize % readSize != 0) {
    throw std::invalid_argument(folly::sformat(
        "logSize and segmentSize need to be a multiple of readSize. "
        "segmentSize: {}, logSize:{}, readSize: {}.",
        segmentSize,
        logSize,
        readSize));
  }
  
  if (logSize % segmentSize != 0) {
    throw std::invalid_argument(folly::sformat(
        "logSize must be a multiple of segmentSize. "
        "logSize:{}, segmentSize: {}.",
        logSize,
        segmentSize));
  }

  if (logPhysicalPartitions == 0) {
    throw std::invalid_argument(folly::sformat(
          "number physical partitions needs to be greater than 0"
          ));
  }

  if (logIndexPartitions % logPhysicalPartitions != 0) {
    throw std::invalid_argument(folly::sformat(
          "the number of index partitions must be a multiple of the physical partitions"
    ));
  }

  if (logSize / logPhysicalPartitions % readSize != 0) {
    throw std::invalid_argument(folly::sformat(
          "Phycial partition size must be a multiple of read size"
    ));
  }

  if (numTotalIndexBuckets % logIndexPartitions != 0) {
    throw std::invalid_argument(folly::sformat(
          "Index entries {} must be a multiple of index partitions {}",
          numTotalIndexBuckets, logIndexPartitions
    ));
  }

  if (device == nullptr) {
    throw std::invalid_argument("device cannot be null");
  }

  if (numTotalIndexBuckets == 0) {
      throw std::invalid_argument("need to have a number of index buckets");
  }
  
  if (flushGranularity % segmentSize != 0) {
      throw std::invalid_argument("flushGranularity should be a multiple of writeGranularity");
  }

  return *this;
}

FwLog::FwLog(Config&& config, ValidConfigTag)
    : pageSize_{config.readSize},
      segmentSize_{config.segmentSize},
      logBaseOffset_{config.logBaseOffset},
      logSize_{config.logSize},
      pagesPerSegment_{segmentSize_ / pageSize_},
      numSegments_{logSize_ / segmentSize_},
			flushGranularity_{config.flushGranularity},
      device_{*config.device},
      logIndexPartitions_{config.logIndexPartitions},
      index_{new ChainedLogIndex*[logIndexPartitions_]},
      numLogZones_{logSize_ / device_.getIOZoneCapSize()},
      numSegmentsPerZone_{numSegments_ / numLogZones_},
      pagesPerZone_{numSegmentsPerZone_ * pagesPerSegment_},
      logPhysicalPartitions_{config.logPhysicalPartitions},
      physicalPartitionSize_{logSize_ / logPhysicalPartitions_},
      pagesPerPartitionSegment_{pagesPerSegment_ / logPhysicalPartitions_},
      //nextLsidToClean_{std::make_unique<LogSegmentId>(0, 0)},
      logSegmentBuffers_{new Buffer[numBufferedLogSegments_]},
      currentLogSegments_{new FwLogSegment*[numBufferedLogSegments_]},
      setNumberCallback_{config.setNumberCallback},
      numIndexEntries_{config.numTotalIndexBuckets},
			nextLsidToClean_{LogSegmentId(0,0)},
      threshold_{config.threshold} {
  XLOGF(INFO,
        "FwLog created: size: {}, read size: {}, segment size: {}, base offset: {}, pages per partition segment {}",
        logSize_,
        pageSize_,
        segmentSize_,
        logBaseOffset_,
				pagesPerPartitionSegment_);
  XLOGF(INFO, "FwLog: num zones {}, segments per zone {}", numLogZones_, numSegmentsPerZone_);
  for (uint64_t i = 0; i < logIndexPartitions_; i++) {
    index_[i] = new ChainedLogIndex(numIndexEntries_ / logIndexPartitions_, 
            config.sizeAllocations, setNumberCallback_);
  }
  logSegmentMutexs_ = std::make_unique<folly::SharedMutex[]>(logPhysicalPartitions_);
  reset();
}

bool FwLog::shouldClean(double cleaningThreshold) {
	LogSegmentId currentLsid = LogSegmentId(0, 0);
	{
		std::shared_lock<folly::SharedMutex> nextLock{logSegmentMutexs_[bufferedSegmentOffset_]};
		currentLsid = currentLogSegments_[bufferedSegmentOffset_]->getLogSegmentId();
	}

  auto nextLsid = getNextLsid(currentLsid);
	uint64_t nextWriteLoc = nextLsid.offset() + nextLsid.zone() * numSegmentsPerZone_;
	uint64_t nextCleaningLoc = nextLsidToClean_.offset() + nextLsidToClean_.zone() * numSegmentsPerZone_;
  uint64_t freeSegments = 0;
  if (nextCleaningLoc >= nextWriteLoc) {
    freeSegments = nextCleaningLoc - nextWriteLoc;
  } else {
    freeSegments = nextCleaningLoc + (numSegments_ - nextWriteLoc);
  }
  return freeSegments <= (numSegments_ * cleaningThreshold);
}

std::vector<std::unique_ptr<ObjectInfo>> FwLog::getObjectsToMove(KangarooBucketId bid, bool checkThreshold) {
  //XLOGF(INFO, "Getting objects for {}", bid.index());
  uint64_t indexPartition = getIndexPartition(bid);
  uint64_t physicalPartition = getPhysicalPartition(bid);
  ChainedLogIndex::BucketIterator indexIt;
	
	// TODO: performance, stop early if threshold if not reached
  std::vector<std::unique_ptr<ObjectInfo>> objects; 
	BufferView value;
	HashedKey key = HashedKey("");
	uint8_t hits;
	LogPageId lpid;
	uint32_t tag;

  /* allow reinsertion to index if not enough objects to move */
  indexIt = index_[indexPartition]->getHashBucketIterator(bid);
  while (!indexIt.done()) {

    hits = indexIt.hits();
    tag = indexIt.tag();
    indexIt = index_[indexPartition]->getNext(indexIt);
    lpid = getLogPageId(index_[indexPartition]->find(bid, tag), physicalPartition);
    if (!lpid.isValid()) {
        continue;
    }

    // Find value, could be in in-memory buffer or on nvm
    Buffer buffer;
    //XLOGF(INFO, "lookupBufferedTag {}", bid.index());
    Status status = lookupBufferedTag(tag, key, buffer, lpid);
    //XLOGF(INFO, "lookupBufferedTag ended {}", bid.index());
    if (status != Status::Ok) {
      //XLOGF(INFO, "readLogPage {}", bid.index());
      buffer = readLogPage(lpid);
      if (buffer.isNull()) {
        ioErrorCount_.inc();
        continue;
      }
      LogBucket* page = reinterpret_cast<LogBucket*>(buffer.data());
      flushPageReads_.inc();
      value = page->findTag(tag, key);
    } else {
      value = buffer.view();
    }

    //XLOGF(INFO, "moving on {}", bid.index());
    if (value.isNull()) {
      index_[indexPartition]->remove(tag, bid, getPartitionOffset(lpid));
      continue;
    } else if (setNumberCallback_(key.keyHash()) != bid) {
      flushFalsePageReads_.inc();
      continue;
    }
    index_[indexPartition]->remove(tag, bid, getPartitionOffset(lpid));
    auto ptr = std::make_unique<ObjectInfo>(key, value, hits, lpid, tag);
    objects.push_back(std::move(ptr));
  }
  //XLOGF(INFO, "finished finding objects {}", bid.index());
  
	if (objects.size() < threshold_ && checkThreshold) {
	  thresholdNotHit_.inc();
    for (auto& item: objects) {
      readmit(item);
    }
		objects.resize(0);
  } else {
	  moveBucketCalls_.inc();
  }

	return objects;
}

void FwLog::readmit(std::unique_ptr<ObjectInfo>& oi) {
	/* reinsert items attempted to be moved into index unless in segment to flush */
	moveBucketSuccessfulRets_.inc();
	if (cleaningSegment_ && getSegmentId(oi->lpid) != cleaningSegment_->getLogSegmentId()) {
		uint64_t indexPartition = getIndexPartition(oi->key);
		KangarooBucketId bid = setNumberCallback_(oi->key.keyHash());
		index_[indexPartition]->insert(oi->tag, bid, getPartitionOffset(oi->lpid), oi->hits);
	} else if (oi->hits) {
		readmit(oi->key, oi->value.view());
	}
	return;
}

void FwLog::startClean() {
  {
    std::shared_lock<folly::SharedMutex> segmentLock{getMutexFromSegment(nextLsidToClean_)};
    cleaningBuffer_ = readLogSegment(nextLsidToClean_);
    if (cleaningBuffer_.isNull()) {
      ioErrorCount_.inc();
			cleaningSegment_ = nullptr;
      return;
    }
	}
	cleaningSegment_ = std::make_unique<FwLogSegment>(segmentSize_, pageSize_, 
			nextLsidToClean_, logPhysicalPartitions_, cleaningBuffer_.mutableView(), false);
  cleaningSegmentIt_ = cleaningSegment_->getFirst();
  /*XLOGF(INFO, "Starting to clean {}.{}, cleaning segment iterator done? {}",
      cleaningSegment_->getLogSegmentId().zone(), 
      cleaningSegment_->getLogSegmentId().offset(),
      cleaningSegmentIt_->done());*/
}

void FwLog::finishClean() {
  {
    std::unique_lock<folly::SharedMutex> lock{cleaningMutex_};
    nextLsidToClean_ = getNextLsid(nextLsidToClean_);
    cleaningCv_.notify_all();
  }
	flushLogSegmentsCount_.inc();
	delete cleaningSegmentIt_;
	cleaningSegment_ = nullptr;
  
  flushLogCv_.notify_all();

  //XLOG(INFO) << "FwLog finish clean";
}

bool FwLog::cleaningDone() {
	while (!cleaningSegmentIt_->done()) {
		uint64_t indexPartition = getIndexPartition(cleaningSegmentIt_->key());
		uint32_t hits = 0;
    auto po = index_[indexPartition]->lookup(cleaningSegmentIt_->key(), false, &hits);
		LogPageId lpid = getLogPageId(po,
				cleaningSegmentIt_->partition());
    /*if (lpid.isValid()) {
      XLOGF(INFO, "Lpid {}, segment {}.{} cleaning segment {}.{}, po {} partition {}", lpid.index(), 
          getSegmentId(lpid).zone(), getSegmentId(lpid).offset(),
          nextLsidToClean_.zone(), nextLsidToClean_.offset(), po.index(), cleaningSegmentIt_->partition());
    }*/
		if (!lpid.isValid() || nextLsidToClean_ != getSegmentId(lpid)) {
			if (lpid.isValid()) {
					indexSegmentMismatch_.inc();
			}
			cleaningSegmentIt_ = cleaningSegment_->getNext(cleaningSegmentIt_);
			notFoundInLogIndex_.inc();
		} else {
			foundInLogIndex_.inc();
			return false;
		}
	}
  if (cleaningSegment_->getLogSegmentId().offset() == 0) {
    XLOGF(INFO, "Cleaning done for zone {} offset {}", 
        cleaningSegment_->getLogSegmentId().zone(), cleaningSegment_->getLogSegmentId().offset());
  }
	return true;
}

KangarooBucketId FwLog::getNextCleaningBucket() {
	// need external lock
	KangarooBucketId kbid = setNumberCallback_(cleaningSegmentIt_->key().keyHash());
	cleaningSegmentIt_ = cleaningSegment_->getNext(cleaningSegmentIt_);
	return kbid;
}

double FwLog::falsePositivePct() const {
  XLOG(INFO, "FwLog false positive");
  return 100. * keyCollisionCount_.get() / (lookupCount_.get() + removeCount_.get());
}

double FwLog::extraReadsPct() const {
  return 100. * flushFalsePageReads_.get() / flushPageReads_.get();
}

double FwLog::fragmentationPct() const {
  auto found = foundInLogIndex_.get();
  return 100. * found / (notFoundInLogIndex_.get() + found);
}

uint64_t FwLog::getBytesWritten() const {
  return logSegmentsWrittenCount_.get() * segmentSize_;
}
  
Status FwLog::lookup(HashedKey hk, Buffer& value) {
  lookupCount_.inc();
  uint64_t indexPartition = getIndexPartition(hk);
  uint64_t physicalPartition = getPhysicalPartition(hk);
  LogPageId lpid = getLogPageId(index_[indexPartition]->lookup(hk, true, nullptr), physicalPartition);
  if (!lpid.isValid()) {
    return Status::NotFound;
  }

  Buffer buffer;
  BufferView valueView;
  LogBucket* page;
  {
    std::shared_lock<folly::SharedMutex> lock{getMutexFromPage(lpid)};

    // check if page is buffered in memory and read it
    Status ret = lookupBuffered(hk, value, lpid);
    if (ret != Status::Retry) {
      return ret;
    }

    buffer = readLogPage(lpid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }
  }

  page = reinterpret_cast<LogBucket*>(buffer.data());
  
  valueView = page->find(hk);
  if (valueView.isNull()) {
    keyCollisionCount_.inc();
    return Status::NotFound;
  }

  value = Buffer{valueView};
  succLookupCount_.inc();
  return Status::Ok;
} 

bool FwLog::couldExist(HashedKey hk) {
  uint64_t indexPartition = getIndexPartition(hk);
  uint64_t physicalPartition = getPhysicalPartition(hk);
  LogPageId lpid = getLogPageId(index_[indexPartition]->lookup(hk, true, nullptr), physicalPartition);
  if (!lpid.isValid()) {
    lookupCount_.inc();
    return false;
  }

  return true;
} 

Status FwLog::insert(HashedKey hk,
              BufferView value) {
  // TODO: update for fw log
  //
  LogPageId lpid;
  LogSegmentId lsid;
  uint64_t physicalPartition = getPhysicalPartition(hk);
  Status ret;
	
  while (!lpid.isValid()) {
    uint32_t buffer = numBufferedLogSegments_ + 1;
    bool flushSegment = false;
    for (uint32_t i = 0; i < numBufferedLogSegments_; i++) {
      // logSegment handles concurrent inserts
      // lock to prevent write out of segment
      uint32_t bufferNum = (i + bufferedSegmentOffset_) % numBufferedLogSegments_;
      std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[bufferNum]};

      lpid = getLogPageId(
          currentLogSegments_[bufferNum]->getLogSegmentId(),
          currentLogSegments_[bufferNum]->insert(hk, value, physicalPartition));
      if (lpid.isValid()) {
        buffer = bufferNum;
        break;
      } 
    }

    if (lpid.isValid()) {
      uint64_t indexPartition = getIndexPartition(hk);
      auto ret = index_[indexPartition]->insert(hk, getPartitionOffset(lpid));
      if (ret == Status::NotFound) {
        replaceIndexInsert_.inc();
        ret = Status::Ok;
      } 
      insertCount_.inc();
      if (ret == Status::Ok) {
        succInsertCount_.inc();
        bytesInserted_.add(hk.key().size() + value.size());
      }
    }
    
    if (buffer != bufferedSegmentOffset_) {
      // only wait if could not allocate lpid
      flushLogSegment(physicalPartition, !lpid.isValid());
    }
  }
  return ret;
  /*
	if (lpid.isValid()) {
		return ret;
	} else if (!lpid.isValid()) { 
    return insert(hk, value);
  } else {
    return Status::Rejected;
  }
  */
}

void FwLog::readmit(HashedKey hk,
              BufferView value) {
  LogPageId lpid;
  LogSegmentId lsid;
  uint64_t physicalPartition = getPhysicalPartition(hk);
  readmitRequests_.inc();
  
  {
    // logSegment handles concurrent inserts
    // lock to prevent write out of segment
    std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[bufferedSegmentOffset_]};
    lpid = getLogPageId(
        currentLogSegments_[bufferedSegmentOffset_]->getLogSegmentId(),
        currentLogSegments_[bufferedSegmentOffset_]->insert(hk, value, physicalPartition));
    if (!lpid.isValid()) {
        // no room in segment so will not insert
        readmitRequestsFailed_.inc();
        return;
    }
  }
 
  uint64_t indexPartition = getIndexPartition(hk);
  auto ret = index_[indexPartition]->insert(hk, getPartitionOffset(lpid));
  readmitBytes_.add(hk.key().size() + value.size());
}

Status FwLog::remove(HashedKey hk) {
  uint64_t indexPartition = getIndexPartition(hk);
  uint64_t physicalPartition = getPhysicalPartition(hk);
  removeCount_.inc();
  LogPageId lpid;

  lpid = getLogPageId(index_[indexPartition]->lookup(hk, false, nullptr), physicalPartition);
  if (!lpid.isValid()) {
    return Status::NotFound;
  }

  Buffer buffer;
  BufferView valueView;
  LogBucket* page;
  {
    std::shared_lock<folly::SharedMutex> lock{getMutexFromPage(lpid)};

    // check if page is buffered in memory and read it
    Status ret = lookupBuffered(hk, buffer, lpid);
    if (ret == Status::Ok) {
	    Status status = index_[indexPartition]->remove(hk, getPartitionOffset(lpid));
      if (status == Status::Ok) {
	      succRemoveCount_.inc();
      }
      return status;
    } else if (ret == Status::Retry) {
      buffer = readLogPage(lpid);
      if (buffer.isNull()) {
        ioErrorCount_.inc();
        return Status::DeviceError;
      }
    } else {
      return ret;
    }
  }

  page = reinterpret_cast<LogBucket*>(buffer.data());
  
  valueView = page->find(hk);
  if (valueView.isNull()) {
    keyCollisionCount_.inc();
    return Status::NotFound;
  }
  
  Status status = index_[indexPartition]->remove(hk, getPartitionOffset(lpid));
  if (status == Status::Ok) {
    succRemoveCount_.inc();
  }
  return status;
}

void FwLog::flush() {
  // TODO: should probably flush buffered part of log
  return;
} 

void FwLog::reset() {
  itemCount_.set(0);
  insertCount_.set(0);
  succInsertCount_.set(0);
  lookupCount_.set(0);
  succLookupCount_.set(0);
  removeCount_.set(0);
  logicalWrittenCount_.set(0);
  physicalWrittenCount_.set(0);
  ioErrorCount_.set(0);
  checksumErrorCount_.set(0);

	XLOG(INFO) << "FwLog has segment size: " << segmentSize_;
  for (uint64_t i = 0; i < numBufferedLogSegments_; i++) {
    logSegmentBuffers_[i] = device_.makeIOBuffer(segmentSize_);
    currentLogSegments_[i] = new FwLogSegment(
          segmentSize_, pageSize_, LogSegmentId(i, 0), logPhysicalPartitions_,
          logSegmentBuffers_[i].mutableView(), true);
	}
  nextLsidToClean_ = LogSegmentId(0, 0);
}

Status FwLog::lookupBuffered(HashedKey hk, 
    Buffer& value, LogPageId lpid) {
  uint64_t partition = getPhysicalPartition(hk);
	uint32_t buffer = numBufferedLogSegments_ + 1;
	LogSegmentId lsid = getSegmentId(lpid);
	for (uint32_t i = 0; i < numBufferedLogSegments_; i++) {
		if (lsid == currentLogSegments_[i]->getLogSegmentId()) {
			buffer = i;
			break;
		}
	}
	if (buffer >= numBufferedLogSegments_) {
		return Status::Retry;
	}

  BufferView view;
  {
    std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[buffer]};
    if (!isBuffered(lpid, buffer)) {
      return Status::Retry;
    }
    view = currentLogSegments_[buffer]->find(hk, lpid);
    if (view.isNull()) {
      keyCollisionCount_.inc();
      return Status::NotFound;
    }
    value = Buffer{view};
  }
  return Status::Ok;
}

Status FwLog::lookupBufferedTag(uint32_t tag, HashedKey& hk, 
    Buffer& value, LogPageId lpid) {
  uint64_t partition = getPhysicalPartition(lpid);
	uint32_t buffer = numBufferedLogSegments_ + 1;
	LogSegmentId lsid = getSegmentId(lpid);
	for (uint32_t i = 0; i < numBufferedLogSegments_; i++) {
		if (lsid == currentLogSegments_[i]->getLogSegmentId()) {
			buffer = i;
			break;
		}
	}
	if (buffer >= numBufferedLogSegments_) {
		return Status::Retry;
	}

  BufferView view;
  {
    std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[buffer]};
    if (!isBuffered(lpid, buffer)) {
      return Status::Retry;
    }
    view = currentLogSegments_[buffer]->findTag(tag, hk, lpid);
    if (view.isNull()) {
      keyCollisionCount_.inc();
      return Status::NotFound;
    }
    value = Buffer{view};
  }
  return Status::Ok;
}

bool FwLog::isBuffered(LogPageId lpid, uint32_t buffer) {
  return getSegmentId(lpid) == currentLogSegments_[buffer]->getLogSegmentId();
}

} // namespace navy
} // namespace cachelib
} // namespace facebook
