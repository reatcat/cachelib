#pragma once

#include <condition_variable>
#include <stdexcept>
#include <vector>

#include <folly/SharedMutex.h>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/kangaroo/ChainedLogIndex.h"
#include "cachelib/navy/kangaroo/FwLogSegment.h"

namespace facebook {
namespace cachelib {
namespace navy {
class FwLog  {
 public:
  struct Config {
    uint32_t readSize{4 * 1024};
    uint32_t segmentSize{256 * 1024};

    // The range of device that Log will access is guaranted to be
    // with in [logBaseOffset, logBaseOffset + logSize)
    uint64_t logBaseOffset{};
    uint64_t logSize{0};
    Device* device{nullptr};

    // log partitioning
    uint64_t logPhysicalPartitions{};

    // for index
    uint64_t logIndexPartitions{};
    uint16_t sizeAllocations{1024};
    uint64_t numTotalIndexBuckets{};
    SetNumberCallback setNumberCallback{};

    // for merging to sets
    uint32_t threshold;
    SetMultiInsertCallback setMultiInsertCallback{};
    uint64_t mergeThreads{16};

    uint64_t flushGranularity{};

    Config& validate();
  };

  // Throw std::invalid_argument on bad config
  explicit FwLog(Config&& config);

  ~FwLog();

  FwLog(const FwLog&) = delete;
  FwLog& operator=(const FwLog&) = delete;

  bool couldExist(HashedKey hk);

  // Look up a key in KangarooLog. On success, it will return Status::Ok and
  // populate "value" with the value found. User should pass in a null
  // Buffer as "value" as any existing storage will be freed. If not found,
  // it will return Status::NotFound. And of course, on error, it returns
  // DeviceError.
  Status lookup(HashedKey hk, Buffer& value);

  // Inserts key and value into KangarooLog. This will replace an existing
  // key if found. If it failed to write, it will return DeviceError.
  Status insert(HashedKey hk, BufferView value);

  // Removes an entry from Kangaroo if found. Ok on success, NotFound on miss,
  // and DeviceError on error.
  Status remove(HashedKey hk);

  void flush();

  void reset();
	void readmit(std::unique_ptr<ObjectInfo>& oi);
  
	bool shouldClean(double cleaningThreshold);
	bool cleaningDone();
	void startClean();
	void finishClean();
	KangarooBucketId getNextCleaningBucket();
	std::vector<std::unique_ptr<ObjectInfo>> getObjectsToMove(KangarooBucketId bid, bool checkThreshold);
  
  double falsePositivePct() const;
  double extraReadsPct() const;
  double fragmentationPct() const;
  uint64_t getBytesWritten() const;


  // TODO: persist and recover not implemented

 private:

  struct ValidConfigTag {};
  FwLog(Config&& config, ValidConfigTag);

  Buffer readLogPage(LogPageId lpid);
  Buffer readLogSegment(LogSegmentId lsid);
  bool writeLogSegment(LogSegmentId lsid, Buffer buffer);
  bool eraseSegments(LogSegmentId startLsid);

  bool isBuffered(LogPageId lpid, uint64_t physicalPartition); // does not grab logSegmentMutex mutex
  Status lookupBuffered(HashedKey hk, Buffer& value, LogPageId lpid);
  Status lookupBufferedTag(uint32_t tag, HashedKey& hk, Buffer& value, LogPageId lpid);

  uint64_t getPhysicalPartition(LogPageId lpid) const {
    return (lpid.index() % pagesPerSegment_) / pagesPerPartitionSegment_;
  }
  uint64_t getLogSegmentOffset(LogSegmentId lsid) const {
    return logBaseOffset_ + segmentSize_ * lsid.index() 
      + physicalPartitionSize_ * lsid.partition();
  }

  uint64_t getPhysicalPartition(HashedKey hk) const {
    return getIndexPartition(hk) % logPhysicalPartitions_;
  }
  uint64_t getPhysicalPartition(KangarooBucketId kbid) const {
    return getIndexPartition(kbid) % logPhysicalPartitions_;
  }
  uint64_t getIndexPartition(HashedKey hk) const {
    return getLogIndexEntry(hk) % logIndexPartitions_;
  }
  uint64_t getIndexPartition(KangarooBucketId kbid) const {
    return getLogIndexEntry(kbid) % logIndexPartitions_;
  }
  uint64_t getLogIndexEntry(HashedKey hk) const {
    return getLogIndexEntry(setNumberCallback_(hk.keyHash())); 
  }
  uint64_t getLogIndexEntry(KangarooBucketId kbid) const {
    return kbid.index() % numIndexEntries_; 
  }
  
  uint64_t getLogPageOffset(LogPageId lpid) const {
    return logBaseOffset_ + pageSize_ * lpid.index();
  }

  LogPageId getLogPageId(PartitionOffset po, uint32_t physicalPartition) {
		uint32_t offset = po.index() % pagesPerPartitionSegment_;
		uint32_t segment_num = po.index() / pagesPerPartitionSegment_;
		uint32_t segment_offset = offset + physicalPartition * pagesPerPartitionSegment_;
    return LogPageId(segment_offset + segment_num * pagesPerSegment_, po.isValid());
  }
  PartitionOffset getPartitionOffset(LogPageId lpid) {
		uint32_t segment_offset = (lpid.index() % pagesPerSegment_) % pagesPerPartitionSegment_;
		uint32_t segment_num = lpid.index() / pagesPerSegment_;
    return PartitionOffset(segment_num * pagesPerPartitionSegment_ + segment_offset, lpid.isValid());
  }
  LogSegmentId getSegmentId(LogPageId lpid) const {
    uint32_t index = lpid.index() / pagesPerSegment_;
    return LogSegmentId(index, 0);
  }

  LogSegmentId getNextLsid(LogSegmentId lsid);
  
  // locks based on partition number, concurrent read, single modify
  folly::SharedMutex& getMutexFromSegment(LogSegmentId lsid) const {
    return mutex_[(lsid.partition()) & (NumMutexes - 1)];
  }
  folly::SharedMutex& getMutexFromPage(LogPageId lpid) const {
    return getMutexFromSegment(getSegmentId(lpid));
  }
  
  double cleaningThreshold_ = .1;
	Buffer cleaningBuffer_;
	std::unique_ptr<FwLogSegment> cleaningSegment_ = nullptr;
	FwLogSegment::Iterator* cleaningSegmentIt_ = nullptr;
  void cleanSegment(LogSegmentId lsid);
  void cleanSegmentsLoop(uint64_t threadId);
  bool shouldWakeCompaction(uint64_t threadId);
  void moveBucket(HashedKey hk, uint64_t count, LogSegmentId lsidToFlush);
  void readmit(HashedKey hk, BufferView value);
	bool flushLogSegment(uint32_t partition, bool wait);

  // Use birthday paradox to estimate number of mutexes given number of parallel
  // queries and desired probability of lock collision.
  static constexpr size_t NumMutexes = 16 * 1024;

  // Serialization format version. Never 0. Versions < 10 reserved for testing.
  static constexpr uint32_t kFormatVersion = 10;

  const uint64_t pageSize_{};
  const uint64_t segmentSize_{};
  const uint64_t logBaseOffset_{};
  const uint64_t logSize_{};
  const uint64_t pagesPerSegment_{};
  const uint64_t numSegments_{};
	const uint64_t flushGranularity_{};

  Device& device_;
  std::unique_ptr<folly::SharedMutex[]> mutex_{
      new folly::SharedMutex[NumMutexes]};
  const uint64_t logIndexPartitions_{};
  ChainedLogIndex** index_;
  const SetNumberCallback setNumberCallback_{};

  const uint64_t logPhysicalPartitions_{};
  const uint64_t physicalPartitionSize_{};
  const uint64_t pagesPerPartitionSegment_{};
  const uint64_t numIndexEntries_{};
  FwLogSegment** currentLogSegments_;
	const uint32_t numBufferedLogSegments_ = 2;
	uint32_t bufferedSegmentOffset_ = 0;
	double overflowLimit_ = .5;
  /* prevent access to log segment while it's being switched out
   * to disk, one for each buffered log Segment */
  std::unique_ptr<folly::SharedMutex[]> logSegmentMutexs_;
	folly::SharedMutex bufferMetadataMutex_;
  std::condition_variable_any flushLogCv_;
  Buffer* logSegmentBuffers_;

  LogSegmentId nextLsidToClean_;
  uint32_t threshold_{0};

  mutable AtomicCounter itemCount_;
  mutable AtomicCounter insertCount_;
  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter lookupCount_;
  mutable AtomicCounter succLookupCount_;
  mutable AtomicCounter removeCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter evictionCount_;
  mutable AtomicCounter keyCollisionCount_;
  mutable AtomicCounter logicalWrittenCount_;
  mutable AtomicCounter physicalWrittenCount_;
  mutable AtomicCounter ioErrorCount_;
  mutable AtomicCounter checksumErrorCount_;
  mutable AtomicCounter flushPageReads_;
  mutable AtomicCounter flushFalsePageReads_;
  mutable AtomicCounter flushLogSegmentsCount_;
  mutable AtomicCounter moveBucketCalls_;
  mutable AtomicCounter notFoundInLogIndex_;
  mutable AtomicCounter foundInLogIndex_;
  mutable AtomicCounter indexSegmentMismatch_;
  mutable AtomicCounter replaceIndexInsert_;
  mutable AtomicCounter indexReplacementReinsertions_;
  mutable AtomicCounter indexReinsertions_;
  mutable AtomicCounter indexReinsertionFailed_;
  mutable AtomicCounter moveBucketSuccessfulRets_;
  mutable AtomicCounter thresholdNotHit_;
  mutable AtomicCounter sumCountCounter_;
  mutable AtomicCounter numCountCalls_;
  mutable AtomicCounter readmitBytes_;
  mutable AtomicCounter readmitRequests_;
  mutable AtomicCounter readmitRequestsFailed_;
  mutable AtomicCounter logSegmentsWrittenCount_;
  mutable AtomicCounter bytesInserted_;

};
} // namespace navy
} // namespace cachelib
} // namespace facebook
