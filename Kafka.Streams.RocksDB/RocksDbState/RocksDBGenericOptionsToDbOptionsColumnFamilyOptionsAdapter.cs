using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System;
using System.Collections.Generic;
/**
* The generic {@link DbOptions} class allows users to Set all configs on one object if only default column family
* is used. Because we use multiple column families, we need to use {@link DbOptions} and {@link ColumnFamilyOptions}
* that cover a part of all options each.
*
* This class do the translation between generic {@link DbOptions} into {@link DbOptions} and {@link ColumnFamilyOptions}.
*/
namespace Kafka.Streams.RocksDbState
{
    public class RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter : DbOptions
    {
        private readonly DbOptions dbOptions;
        private readonly ColumnFamilyOptions columnFamilyOptions;

        private readonly ILogger<RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter> logger;

        public RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(
            DbOptions dbOptions,
            ColumnFamilyOptions columnFamilyOptions)
        {
            this.dbOptions = dbOptions;
            this.columnFamilyOptions = columnFamilyOptions;
        }

        public DbOptions SetIncreaseParallelism(int totalThreads)
        {
            dbOptions.IncreaseParallelism(totalThreads);

            return this;
        }

        public new DbOptions SetCreateIfMissing(bool flag)
        {
            dbOptions.SetCreateIfMissing(flag);
            return this;
        }


        public DbOptions SetCreateMissingColumnFamilies(bool flag)
        {
            dbOptions.SetCreateMissingColumnFamilies(flag);
            return this;
        }


        public DbOptions SetEnv(Env env)
        {
            dbOptions.SetEnv(env.Handle);
            return this;
        }

        public DbOptions PrepareForBulkLoad()
        {
            /* From https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
             *
             * Q: What's the fastest way to load data into RocksDB?
             *
             * A: A fast way to direct insert data to the DB:
             *
             *  1. using single writer thread and insert in sorted order
             *  2. batch hundreds of keys into one write batch
             *  3. use vector memtable
             *  4. make sure options.max_background_flushes is at least 4
             *  5. before inserting the data,
             *       disable automatic compaction,
             *       Set options.level0_file_num_compaction_trigger,
             *           options.level0_slowdown_writes_trigger
             *           and options.level0_stop_writes_trigger to very large.
             *     After inserting all the data, issue a manual compaction.
             *
             * 3-5 will be automatically done if you call DbOptions::PrepareForBulkLoad() to your option
             */
            // (1) not in our control
            // (2) is done via bulk-loading API
            // (3) skipping because, not done in actual PrepareForBulkLoad() code in https://github.com/facebook/rocksdb/blob/master/options/options.cc
            //columnFamilyOptions.SetMemTableConfig(new VectorMemTableConfig());
            // (4-5) below:
            dbOptions.SetMaxBackgroundFlushes(4);
            columnFamilyOptions.SetDisableAutoCompactions(0);
            columnFamilyOptions.SetLevel0FileNumCompactionTrigger(1 << 30);
            columnFamilyOptions.SetLevel0SlowdownWritesTrigger(1 << 30);
            columnFamilyOptions.SetLevel0StopWritesTrigger(1 << 30);
            return this;
        }



        public DbOptions OptimizeForPointLookup(ulong blockCacheSizeMb)
        {
            columnFamilyOptions.OptimizeForPointLookup(blockCacheSizeMb);
            return this;
        }


        public DbOptions OptimizeLevelStyleCompaction()
        {
            columnFamilyOptions.OptimizeLevelStyleCompaction(512 * 1024 * 1024);
            return this;
        }


        public DbOptions OptimizeLevelStyleCompaction(ulong memtableMemoryBudget)
        {
            columnFamilyOptions.OptimizeLevelStyleCompaction(memtableMemoryBudget);
            return this;
        }


        public DbOptions OptimizeUniversalStyleCompaction()
        {
            columnFamilyOptions.OptimizeUniversalStyleCompaction(512 * 1024 * 1024);
            return this;
        }


        public DbOptions OptimizeUniversalStyleCompaction(ulong memtableMemoryBudget)
        {
            columnFamilyOptions.OptimizeUniversalStyleCompaction(memtableMemoryBudget);
            return this;
        }


        public DbOptions SetComparator(Comparator builtinComparator)
        {
            columnFamilyOptions.SetComparator(builtinComparator);
            return this;
        }

//        public DbOptions SetMergeOperatorName(MergeOperator mergeOperator)
//        {
//            columnFamilyOptions.SetMergeOperator(mergeOperator);
//            return this;
//        }
//
//        public DbOptions SetWriteBufferSize(uulong writeBufferSize)
//        {
//            columnFamilyOptions.SetWriteBufferSize(writeBufferSize);
//            return this;
//        }
//
//        public DbOptions SetMaxWriteBufferNumber(int maxWriteBufferNumber)
//        {
//            columnFamilyOptions.SetMaxWriteBufferNumber(maxWriteBufferNumber);
//            return this;
//        }
//
//        public DbOptions SetErrorIfExists(bool errorIfExists)
//        {
//            dbOptions.SetErrorIfExists(errorIfExists);
//            return this;
//        }
//
//        public DbOptions SetParanoidChecks(bool paranoidChecks)
//        {
//            dbOptions.SetParanoidChecks(paranoidChecks);
//            return this;
//        }
//
//        public DbOptions SetMaxFileOpeningThreads(int maxFileOpeningThreads)
//        {
//            dbOptions.SetMaxFileOpeningThreads(maxFileOpeningThreads);
//            return this;
//        }
//
//        public DbOptions SetMaxTotalWalSize(uulong maxTotalWalSize)
//        {
//            dbOptions.SetMaxTotalWalSize(maxTotalWalSize);
//            return this;
//        }
//
//        public DbOptions SetMaxOpenFiles(int maxOpenFiles)
//        {
//            dbOptions.SetMaxOpenFiles(maxOpenFiles);
//            return this;
//        }
//
//        public DbOptions SetUseFsync(int value)
//        {
//            dbOptions.SetUseFsync(value);
//            return this;
//        }
//
//
//        public DbOptions SetDbLogDir(string path)
//        {
//            dbOptions.SetDbLogDir(path);
//            return this;
//        }
//
//
//        public DbOptions SetWalDir(string walDir)
//        {
//            dbOptions.SetWalDir(walDir);
//            return this;
//        }
//
//        public DbOptions SetDeleteObsoleteFilesPeriodMicros(uulong micros)
//        {
//            dbOptions.SetDeleteObsoleteFilesPeriodMicros(micros);
//            return this;
//        }
//
//        public DbOptions EnableStatistics()
//        {
//            dbOptions.EnableStatistics();
//            return this;
//        }
//
//        public void SetBaseBackgroundCompactions(int baseBackgroundCompactions)
//        {
//            dbOptions.SetBaseBackgroundCompactions(baseBackgroundCompactions);
//        }
//
//
//        public DbOptions SetMaxBackgroundCompactions(int maxBackgroundCompactions)
//        {
//            dbOptions.SetMaxBackgroundCompactions(maxBackgroundCompactions);
//            return this;
//        }
//
//
//        public DbOptions SetMaxBackgroundFlushes(int maxBackgroundFlushes)
//        {
//            dbOptions.SetMaxBackgroundFlushes(maxBackgroundFlushes);
//            return this;
//        }
//
//        public DbOptions SetMaxLogFileSize(uulong maxLogFileSize)
//        {
//            dbOptions.SetMaxLogFileSize(maxLogFileSize);
//            return this;
//        }
//
//        public DbOptions SetLogFileTimeToRoll(uulong logFileTimeToRoll)
//        {
//            dbOptions.SetLogFileTimeToRoll(logFileTimeToRoll);
//            return this;
//        }
//
//        public DbOptions SetKeepLogFileNum(uulong keepLogFileNum)
//        {
//            dbOptions.SetKeepLogFileNum(keepLogFileNum);
//            return this;
//        }
//
//
//        public DbOptions SetRecycleLogFileNum(uulong recycleLogFileNum)
//        {
//            dbOptions.SetRecycleLogFileNum(recycleLogFileNum);
//            return this;
//        }
//
//        public DbOptions SetMaxManifestFileSize(uulong maxManifestFileSize)
//        {
//            dbOptions.SetMaxManifestFileSize(maxManifestFileSize);
//            return this;
//        }
//
//        public DbOptions SetTableCacheNumshardbits(int tableCacheNumshardbits)
//        {
//            dbOptions.SetTableCacheNumshardbits(tableCacheNumshardbits);
//            return this;
//        }
//
//
//        public ulong walTtlSeconds()
//        {
//            return dbOptions.walTtlSeconds();
//        }
//
//
//        public DbOptions SetWalTtlSeconds(ulong walTtlSeconds)
//        {
//            LOG.warn("option walTtlSeconds will be ignored: Streams does not expose RocksDB ttl functionality");
//            dbOptions.SetWalTtlSeconds(walTtlSeconds);
//            return this;
//        }
//
//
//        public ulong walSizeLimitMB()
//        {
//            return dbOptions.walSizeLimitMB();
//        }
//
//
//        public DbOptions SetWalSizeLimitMB(ulong sizeLimitMB)
//        {
//            dbOptions.SetWalSizeLimitMB(sizeLimitMB);
//            return this;
//        }
//
//
//        public ulong manifestPreallocationSize()
//        {
//            return dbOptions.manifestPreallocationSize();
//        }
//
//
//        public DbOptions SetManifestPreallocationSize(ulong size)
//        {
//            dbOptions.SetManifestPreallocationSize(size);
//            return this;
//        }
//
//
//        public DbOptions SetUseDirectReads(bool useDirectReads)
//        {
//            dbOptions.SetUseDirectReads(useDirectReads);
//            return this;
//        }
//
//
//        public bool useDirectReads()
//        {
//            return dbOptions.useDirectReads();
//        }
//
//
//        public DbOptions SetUseDirectIoForFlushAndCompaction(bool useDirectIoForFlushAndCompaction)
//        {
//            dbOptions.SetUseDirectIoForFlushAndCompaction(useDirectIoForFlushAndCompaction);
//            return this;
//        }
//
//
//        public bool useDirectIoForFlushAndCompaction()
//        {
//            return dbOptions.useDirectIoForFlushAndCompaction();
//        }
//
//
//        public DbOptions SetAllowFAllocate(bool allowFAllocate)
//        {
//            dbOptions.SetAllowFAllocate(allowFAllocate);
//            return this;
//        }
//
//
//        public bool allowFAllocate()
//        {
//            return dbOptions.allowFAllocate();
//        }
//
//
//        public bool allowMmapReads()
//        {
//            return dbOptions.allowMmapReads();
//        }
//
//
//        public DbOptions SetAllowMmapReads(bool allowMmapReads)
//        {
//            dbOptions.SetAllowMmapReads(allowMmapReads);
//            return this;
//        }
//
//
//        public bool allowMmapWrites()
//        {
//            return dbOptions.allowMmapWrites();
//        }
//
//
//        public DbOptions SetAllowMmapWrites(bool allowMmapWrites)
//        {
//            dbOptions.SetAllowMmapWrites(allowMmapWrites);
//            return this;
//        }
//
//
//        public bool isFdCloseOnExec()
//        {
//            return dbOptions.isFdCloseOnExec();
//        }
//
//
//        public DbOptions SetIsFdCloseOnExec(bool isFdCloseOnExec)
//        {
//            dbOptions.SetIsFdCloseOnExec(isFdCloseOnExec);
//            return this;
//        }
//
//
//        public int statsDumpPeriodSec()
//        {
//            return dbOptions.statsDumpPeriodSec();
//        }
//
//
//        public DbOptions SetStatsDumpPeriodSec(int statsDumpPeriodSec)
//        {
//            dbOptions.SetStatsDumpPeriodSec(statsDumpPeriodSec);
//            return this;
//        }
//
//
//        public bool adviseRandomOnOpen()
//        {
//            return dbOptions.adviseRandomOnOpen();
//        }
//
//
//        public DbOptions SetAdviseRandomOnOpen(bool adviseRandomOnOpen)
//        {
//            dbOptions.SetAdviseRandomOnOpen(adviseRandomOnOpen);
//            return this;
//        }
//
//
//        public DbOptions SetDbWriteBufferSize(ulong dbWriteBufferSize)
//        {
//            dbOptions.SetDbWriteBufferSize(dbWriteBufferSize);
//            return this;
//        }
//
//
//        public ulong dbWriteBufferSize()
//        {
//            return dbOptions.dbWriteBufferSize();
//        }
//
//
//        public DbOptions SetAccessHintOnCompactionStart(AccessHint accessHint)
//        {
//            dbOptions.SetAccessHintOnCompactionStart(accessHint);
//            return this;
//        }
//
//
//        public AccessHint accessHintOnCompactionStart()
//        {
//            return dbOptions.accessHintOnCompactionStart();
//        }
//
//
//        public DbOptions SetNewTableReaderForCompactionInputs(bool newTableReaderForCompactionInputs)
//        {
//            dbOptions.SetNewTableReaderForCompactionInputs(newTableReaderForCompactionInputs);
//            return this;
//        }
//
//
//        public bool newTableReaderForCompactionInputs()
//        {
//            return dbOptions.newTableReaderForCompactionInputs();
//        }
//
//
//        public DbOptions SetCompactionReadaheadSize(ulong compactionReadaheadSize)
//        {
//            dbOptions.SetCompactionReadaheadSize(compactionReadaheadSize);
//            return this;
//        }
//
//
//        public ulong compactionReadaheadSize()
//        {
//            return dbOptions.compactionReadaheadSize();
//        }
//
//
//        public DbOptions SetRandomAccessMaxBufferSize(ulong randomAccessMaxBufferSize)
//        {
//            dbOptions.SetRandomAccessMaxBufferSize(randomAccessMaxBufferSize);
//            return this;
//        }
//
//
//        public ulong randomAccessMaxBufferSize()
//        {
//            return dbOptions.randomAccessMaxBufferSize();
//        }
//
//
//        public DbOptions SetWritableFileMaxBufferSize(ulong writableFileMaxBufferSize)
//        {
//            dbOptions.SetWritableFileMaxBufferSize(writableFileMaxBufferSize);
//            return this;
//        }
//
//
//        public ulong writableFileMaxBufferSize()
//        {
//            return dbOptions.writableFileMaxBufferSize();
//        }
//
//
//        public bool useAdaptiveMutex()
//        {
//            return dbOptions.useAdaptiveMutex();
//        }
//
//
//        public DbOptions SetUseAdaptiveMutex(bool useAdaptiveMutex)
//        {
//            dbOptions.SetUseAdaptiveMutex(useAdaptiveMutex);
//            return this;
//        }
//
//
//        public ulong bytesPerSync()
//        {
//            return dbOptions.bytesPerSync();
//        }
//
//
//        public DbOptions SetBytesPerSync(ulong bytesPerSync)
//        {
//            dbOptions.SetBytesPerSync(bytesPerSync);
//            return this;
//        }
//
//
//        public DbOptions SetWalBytesPerSync(ulong walBytesPerSync)
//        {
//            dbOptions.SetWalBytesPerSync(walBytesPerSync);
//            return this;
//        }
//
//
//        public ulong walBytesPerSync()
//        {
//            return dbOptions.walBytesPerSync();
//        }
//
//
//        public DbOptions SetEnableThreadTracking(bool enableThreadTracking)
//        {
//            dbOptions.SetEnableThreadTracking(enableThreadTracking);
//            return this;
//        }
//
//
//        public bool enableThreadTracking()
//        {
//            return dbOptions.enableThreadTracking();
//        }
//
//
//        public DbOptions SetDelayedWriteRate(ulong delayedWriteRate)
//        {
//            dbOptions.SetDelayedWriteRate(delayedWriteRate);
//            return this;
//        }
//
//
//        public ulong delayedWriteRate()
//        {
//            return dbOptions.delayedWriteRate();
//        }
//
//
//        public DbOptions SetAllowConcurrentMemtableWrite(bool allowConcurrentMemtableWrite)
//        {
//            dbOptions.SetAllowConcurrentMemtableWrite(allowConcurrentMemtableWrite);
//            return this;
//        }
//
//
//        public bool allowConcurrentMemtableWrite()
//        {
//            return dbOptions.allowConcurrentMemtableWrite();
//        }
//
//
//        public DbOptions SetEnableWriteThreadAdaptiveYield(bool enableWriteThreadAdaptiveYield)
//        {
//            dbOptions.SetEnableWriteThreadAdaptiveYield(enableWriteThreadAdaptiveYield);
//            return this;
//        }
//
//
//        public bool enableWriteThreadAdaptiveYield()
//        {
//            return dbOptions.enableWriteThreadAdaptiveYield();
//        }
//
//
//        public DbOptions SetWriteThreadMaxYieldUsec(ulong writeThreadMaxYieldUsec)
//        {
//            dbOptions.SetWriteThreadMaxYieldUsec(writeThreadMaxYieldUsec);
//            return this;
//        }
//
//
//        public ulong writeThreadMaxYieldUsec()
//        {
//            return dbOptions.writeThreadMaxYieldUsec();
//        }
//
//
//        public DbOptions SetWriteThreadSlowYieldUsec(ulong writeThreadSlowYieldUsec)
//        {
//            dbOptions.SetWriteThreadSlowYieldUsec(writeThreadSlowYieldUsec);
//            return this;
//        }
//
//
//        public ulong writeThreadSlowYieldUsec()
//        {
//            return dbOptions.writeThreadSlowYieldUsec();
//        }
//
//
//        public DbOptions SetSkipStatsUpdateOnDbOpen(bool skipStatsUpdateOnDbOpen)
//        {
//            dbOptions.SetSkipStatsUpdateOnDbOpen(skipStatsUpdateOnDbOpen);
//            return this;
//        }
//
//
//        public bool skipStatsUpdateOnDbOpen()
//        {
//            return dbOptions.skipStatsUpdateOnDbOpen();
//        }
//
//
//        public DbOptions SetWalRecoveryMode(WALRecoveryMode walRecoveryMode)
//        {
//            dbOptions.SetWalRecoveryMode(walRecoveryMode);
//            return this;
//        }
//
//
//        public WALRecoveryMode walRecoveryMode()
//        {
//            return dbOptions.walRecoveryMode();
//        }
//
//
//        public DbOptions SetAllow2pc(bool allow2pc)
//        {
//            dbOptions.SetAllow2pc(allow2pc);
//            return this;
//        }
//
//
//        public bool allow2pc()
//        {
//            return dbOptions.allow2pc();
//        }
//
//
//        public DbOptions SetRowCache(Cache rowCache)
//        {
//            dbOptions.SetRowCache(rowCache);
//            return this;
//        }
//
//
//        public Cache rowCache()
//        {
//            return dbOptions.rowCache();
//        }
//
//
//        public DbOptions SetFailIfOptionsFileError(bool failIfOptionsFileError)
//        {
//            dbOptions.SetFailIfOptionsFileError(failIfOptionsFileError);
//            return this;
//        }
//
//
//        public bool failIfOptionsFileError()
//        {
//            return dbOptions.failIfOptionsFileError();
//        }
//
//
//        public DbOptions SetDumpMallocStats(bool dumpMallocStats)
//        {
//            dbOptions.SetDumpMallocStats(dumpMallocStats);
//            return this;
//        }

        public DbOptions SetInfoLogLevel(int infoLogLevel)
        {
            dbOptions.SetInfoLogLevel(infoLogLevel);
            return this;
        }


        public DbOptions SetCompressionPerLevel(Compression[] compressionLevels, ulong numLevels)
        {
            columnFamilyOptions.SetCompressionPerLevel(compressionLevels, numLevels);
            return this;
        }

        public DbOptions SetCompression(Compression compressionType)
        {
            columnFamilyOptions.SetCompression(compressionType);
            return this;
        }

        public DbOptions SetCompressionOptions(int p1, int p2, int p3, int p4)
        {
            columnFamilyOptions.SetCompressionOptions(p1, p2, p3, p4);
            return this;
        }

        public DbOptions SetCompactionStyle(Compaction compactionStyle)
        {
            columnFamilyOptions.SetCompactionStyle(compactionStyle);
            return this;
        }

        public DbOptions SetNumLevels(int numLevels)
        {
            columnFamilyOptions.SetNumLevels(numLevels);
            return this;
        }

        public DbOptions SetLevelZeroFileNumCompactionTrigger(int numFiles)
        {
            columnFamilyOptions.SetLevel0FileNumCompactionTrigger(numFiles);
            return this;
        }

        public DbOptions SetLevelZeroSlowdownWritesTrigger(int value)
        {
            columnFamilyOptions.SetLevel0SlowdownWritesTrigger(value);
            return this;
        }

        public DbOptions SetTargetFileSizeBase(ulong targetFileSizeBase)
        {
            columnFamilyOptions.SetTargetFileSizeBase(targetFileSizeBase);
            return this;
        }

        public new DbOptions SetTargetFileSizeMultiplier(int multiplier)
        {
            columnFamilyOptions.SetTargetFileSizeMultiplier(multiplier);
            return this;
        }

        public DbOptions SetMaxBytesForLevelBase(ulong maxBytesForLevelBase)
        {
            columnFamilyOptions.SetMaxBytesForLevelBase(maxBytesForLevelBase);
            return this;
        }

        public DbOptions SetLevelCompactionDynamicLevelBytes(bool enableLevelCompactionDynamicLevelBytes)
        {
            columnFamilyOptions.SetLevelCompactionDynamicLevelBytes(enableLevelCompactionDynamicLevelBytes);
            return this;
        }

        public DbOptions SetMaxBytesForLevelMultiplier(double multiplier)
        {
            columnFamilyOptions.SetMaxBytesForLevelMultiplier(multiplier);
            return this;
        }

        public DbOptions SetMaxCompactionBytes(ulong maxCompactionBytes)
        {
            columnFamilyOptions.SetMaxCompactionBytes(maxCompactionBytes);
            return this;
        }

        public DbOptions SetArenaBlockSize(ulong arenaBlockSize)
        {
            columnFamilyOptions.SetArenaBlockSize(arenaBlockSize);
            return this;
        }

        public DbOptions SetDisableAutoCompactions(int value)
        {
            columnFamilyOptions.SetDisableAutoCompactions(value);
            return this;
        }


        public DbOptions SetMaxSequentialSkipInIterations(ulong maxSequentialSkipInIterations)
        {
            columnFamilyOptions.SetMaxSequentialSkipInIterations(maxSequentialSkipInIterations);
            return this;
        }


        public DbOptions SetInplaceUpdateSupport(bool inplaceUpdateSupport)
        {
            columnFamilyOptions.SetInplaceUpdateSupport(inplaceUpdateSupport);
            return this;
        }



        public DbOptions SetInplaceUpdateNumLocks(ulong inplaceUpdateNumLocks)
        {
            columnFamilyOptions.SetInplaceUpdateNumLocks(inplaceUpdateNumLocks);
            return this;
        }



        public DbOptions SetMemtablePrefixBloomSizeRatio(double memtablePrefixBloomSizeRatio)
        {
            columnFamilyOptions.SetMemtablePrefixBloomSizeRatio(memtablePrefixBloomSizeRatio);
            return this;
        }

        public DbOptions SetBloomLocality(uint bloomLocality)
        {
            columnFamilyOptions.SetBloomLocality(bloomLocality);

            return this;
        }

        public DbOptions SetMaxSuccessiveMerges(ulong maxSuccessiveMerges)
        {
            columnFamilyOptions.SetMaxSuccessiveMerges(maxSuccessiveMerges);
            return this;
        }

        public DbOptions SetMinWriteBufferNumberToMerge(int minWriteBufferNumberToMerge)
        {
            columnFamilyOptions.SetMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge);
            return this;
        }

        public DbOptions SetOptimizeFiltersForHits(int value)
        {
            columnFamilyOptions.SetOptimizeFiltersForHits(value);
            return this;
        }


        public DbOptions SetMemtableHugePageSize(ulong memtableHugePageSize)
        {
            columnFamilyOptions.SetMemtableHugePageSize(memtableHugePageSize);
            return this;
        }



        public DbOptions SetSoftPendingCompactionBytesLimit(ulong softPendingCompactionBytesLimit)
        {
            columnFamilyOptions.SetSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
            return this;
        }

        public DbOptions SetHardPendingCompactionBytesLimit(ulong hardPendingCompactionBytesLimit)
        {
            columnFamilyOptions.SetHardPendingCompactionBytesLimit(hardPendingCompactionBytesLimit);
            return this;
        }


        public DbOptions SetLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger)
        {
            columnFamilyOptions.SetLevel0FileNumCompactionTrigger(level0FileNumCompactionTrigger);
            return this;
        }

        public DbOptions SetLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger)
        {
            columnFamilyOptions.SetLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger);
            return this;
        }


        public DbOptions SetLevel0StopWritesTrigger(int level0StopWritesTrigger)
        {
            columnFamilyOptions.SetLevel0StopWritesTrigger(level0StopWritesTrigger);
            return this;
        }

        public DbOptions SetMaxBytesForLevelMultiplierAdditional(int[] maxBytesForLevelMultiplierAdditional, ulong numLevels)
        {
            columnFamilyOptions.SetMaxBytesForLevelMultiplierAdditional(maxBytesForLevelMultiplierAdditional, numLevels);
            return this;
        }

        public DbOptions SetMaxWriteBufferNumberToMaintain(int maxWriteBufferNumberToMaintain)
        {
            columnFamilyOptions.SetMaxWriteBufferNumberToMaintain(maxWriteBufferNumberToMaintain);
            return this;
        }

        public DbOptions SetReportBgIoStats(bool reportBgIoStats)
        {
            columnFamilyOptions.SetReportBgIoStats(reportBgIoStats);
            return this;
        }

        public DbOptions SetCompactionOptionsUniversal(IntPtr compactionOptionsUniversal)
        {
            columnFamilyOptions.SetUniversalCompactionOptions(compactionOptionsUniversal);
            return this;
        }

        public DbOptions SetCompactionOptionsFIFO(IntPtr compactionOptionsFIFO)
        {
            columnFamilyOptions.SetFifoCompactionOptions(compactionOptionsFIFO);
            return this;
        }
    }
}
