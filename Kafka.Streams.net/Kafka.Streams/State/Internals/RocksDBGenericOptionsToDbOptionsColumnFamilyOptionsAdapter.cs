/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * The generic {@link Options} allows users to set all configs on one object if only default column family
     * is used. Because we use multiple column families, we need to use {@link DBOptions} and {@link ColumnFamilyOptions}
     * that cover a part of all options each.
     *
     * This do the translation between generic {@link Options} into {@link DBOptions} and {@link ColumnFamilyOptions}.
     */
    public class RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter : Options
    {
        private DBOptions dbOptions;
        private ColumnFamilyOptions columnFamilyOptions;

        private static org.slf4j.ILogger LOG = new LoggerFactory().CreateLogger < RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter);

        RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(DBOptions dbOptions,
                                                                   ColumnFamilyOptions columnFamilyOptions)
        {
            this.dbOptions = dbOptions;
            this.columnFamilyOptions = columnFamilyOptions;
        }

        public override Options setIncreaseParallelism(int totalThreads)
        {
            dbOptions.setIncreaseParallelism(totalThreads);
            return this;
        }

        public override Options setCreateIfMissing(bool flag)
        {
            dbOptions.setCreateIfMissing(flag);
            return this;
        }

        public override Options setCreateMissingColumnFamilies(bool flag)
        {
            dbOptions.setCreateMissingColumnFamilies(flag);
            return this;
        }

        public override Options setEnv(Env env)
        {
            dbOptions.setEnv(env);
            return this;
        }

        public override Env getEnv()
        {
            return dbOptions.getEnv();
        }

        public override Options prepareForBulkLoad()
        {
            /* From https://github.com/facebook/rocksdb/wiki/RocksDb-FAQ
             *
             * Q: What's the fastest way to load data into RocksDb?
             *
             * A: A fast way to direct insert data to the DB:
             *
             *  1. using single writer thread and insert in sorted order
             *  2. batch hundreds of keys into one write batch
             *  3. use vector memtable
             *  4. make sure options.max_background_flushes is at least 4
             *  5. before inserting the data,
             *       disable automatic compaction,
             *       set options.level0_file_num_compaction_trigger,
             *           options.level0_slowdown_writes_trigger
             *           and options.level0_stop_writes_trigger to very large.
             *     After inserting all the data, issue a manual compaction.
             *
             * 3-5 will be automatically done if you call Options::PrepareForBulkLoad() to your option
             */
            // (1) not in our control
            // (2) is done via bulk-loading API
            // (3) skipping because, not done in actual PrepareForBulkLoad() code in https://github.com/facebook/rocksdb/blob/master/options/options.cc
            //columnFamilyOptions.setMemTableConfig(new VectorMemTableConfig());
            // (4-5) below:
            dbOptions.setMaxBackgroundFlushes(4);
            columnFamilyOptions.setDisableAutoCompactions(true);
            columnFamilyOptions.setLevel0FileNumCompactionTrigger(1 << 30);
            columnFamilyOptions.setLevel0SlowdownWritesTrigger(1 << 30);
            columnFamilyOptions.setLevel0StopWritesTrigger(1 << 30);
            return this;
        }

        public override bool createIfMissing()
        {
            return dbOptions.createIfMissing();
        }

        public override bool createMissingColumnFamilies()
        {
            return dbOptions.createMissingColumnFamilies();
        }

        public override Options optimizeForSmallDb()
        {
            dbOptions.optimizeForSmallDb();
            columnFamilyOptions.optimizeForSmallDb();
            return this;
        }

        public override Options optimizeForPointLookup(long blockCacheSizeMb)
        {
            columnFamilyOptions.optimizeForPointLookup(blockCacheSizeMb);
            return this;
        }

        public override Options optimizeLevelStyleCompaction()
        {
            columnFamilyOptions.optimizeLevelStyleCompaction();
            return this;
        }

        public override Options optimizeLevelStyleCompaction(long memtableMemoryBudget)
        {
            columnFamilyOptions.optimizeLevelStyleCompaction(memtableMemoryBudget);
            return this;
        }

        public override Options optimizeUniversalStyleCompaction()
        {
            columnFamilyOptions.optimizeUniversalStyleCompaction();
            return this;
        }

        public override Options optimizeUniversalStyleCompaction(long memtableMemoryBudget)
        {
            columnFamilyOptions.optimizeUniversalStyleCompaction(memtableMemoryBudget);
            return this;
        }

        public override Options setComparator(BuiltinComparator builtinComparator)
        {
            columnFamilyOptions.setComparator(builtinComparator);
            return this;
        }

        public override Options setComparator(AbstractComparator<AbstractSlice<object>> comparator)
        {
            columnFamilyOptions.setComparator(comparator);
            return this;
        }

        public override Options setMergeOperatorName(string name)
        {
            columnFamilyOptions.setMergeOperatorName(name);
            return this;
        }

        public override Options setMergeOperator(MergeOperator mergeOperator)
        {
            columnFamilyOptions.setMergeOperator(mergeOperator);
            return this;
        }

        public override Options setWriteBufferSize(long writeBufferSize)
        {
            columnFamilyOptions.setWriteBufferSize(writeBufferSize);
            return this;
        }

        public override long writeBufferSize()
        {
            return columnFamilyOptions.writeBufferSize();
        }

        public override Options setMaxWriteBufferNumber(int maxWriteBufferNumber)
        {
            columnFamilyOptions.setMaxWriteBufferNumber(maxWriteBufferNumber);
            return this;
        }

        public override int maxWriteBufferNumber()
        {
            return columnFamilyOptions.maxWriteBufferNumber();
        }

        public override bool errorIfExists()
        {
            return dbOptions.errorIfExists();
        }

        public override Options setErrorIfExists(bool errorIfExists)
        {
            dbOptions.setErrorIfExists(errorIfExists);
            return this;
        }

        public override bool paranoidChecks()
        {
            bool columnFamilyParanoidFileChecks = columnFamilyOptions.paranoidFileChecks();
            bool dbOptionsParanoidChecks = dbOptions.paranoidChecks();

            if (columnFamilyParanoidFileChecks != dbOptionsParanoidChecks)
            {
                throw new InvalidOperationException("Config for paranoid checks for RockDB and ColumnFamilies should be the same.");
            }

            return dbOptionsParanoidChecks;
        }

        public override Options setParanoidChecks(bool paranoidChecks)
        {
            columnFamilyOptions.paranoidFileChecks();
            dbOptions.setParanoidChecks(paranoidChecks);
            return this;
        }

        public override int maxOpenFiles()
        {
            return dbOptions.maxOpenFiles();
        }

        public override Options setMaxFileOpeningThreads(int maxFileOpeningThreads)
        {
            dbOptions.setMaxFileOpeningThreads(maxFileOpeningThreads);
            return this;
        }

        public override int maxFileOpeningThreads()
        {
            return dbOptions.maxFileOpeningThreads();
        }

        public override Options setMaxTotalWalSize(long maxTotalWalSize)
        {
            dbOptions.setMaxTotalWalSize(maxTotalWalSize);
            return this;
        }

        public override long maxTotalWalSize()
        {
            return dbOptions.maxTotalWalSize();
        }

        public override Options setMaxOpenFiles(int maxOpenFiles)
        {
            dbOptions.setMaxOpenFiles(maxOpenFiles);
            return this;
        }

        public override bool useFsync()
        {
            return dbOptions.useFsync();
        }

        public override Options setUseFsync(bool useFsync)
        {
            dbOptions.setUseFsync(useFsync);
            return this;
        }

        public override Options setDbPaths(List<DbPath> dbPaths)
        {
            dbOptions.setDbPaths(dbPaths);
            return this;
        }

        public override List<DbPath> dbPaths()
        {
            return dbOptions.dbPaths();
        }

        public override string dbLogDir()
        {
            return dbOptions.dbLogDir();
        }

        public override Options setDbLogDir(string dbLogDir)
        {
            dbOptions.setDbLogDir(dbLogDir);
            return this;
        }

        public override string walDir()
        {
            return dbOptions.walDir();
        }

        public override Options setWalDir(string walDir)
        {
            dbOptions.setWalDir(walDir);
            return this;
        }

        public override long deleteObsoleteFilesPeriodMicros()
        {
            return dbOptions.deleteObsoleteFilesPeriodMicros();
        }

        public override Options setDeleteObsoleteFilesPeriodMicros(long micros)
        {
            dbOptions.setDeleteObsoleteFilesPeriodMicros(micros);
            return this;
        }

        public override int maxBackgroundCompactions()
        {
            return dbOptions.maxBackgroundCompactions();
        }

        public override Options setStatistics(Statistics statistics)
        {
            dbOptions.setStatistics(statistics);
            return this;
        }

        public override Statistics statistics()
        {
            return dbOptions.statistics();
        }

        public override void setBaseBackgroundCompactions(int baseBackgroundCompactions)
        {
            dbOptions.setBaseBackgroundCompactions(baseBackgroundCompactions);
        }

        public override int baseBackgroundCompactions()
        {
            return dbOptions.baseBackgroundCompactions();
        }

        public override Options setMaxBackgroundCompactions(int maxBackgroundCompactions)
        {
            dbOptions.setMaxBackgroundCompactions(maxBackgroundCompactions);
            return this;
        }

        public override void setMaxSubcompactions(int maxSubcompactions)
        {
            dbOptions.setMaxSubcompactions(maxSubcompactions);
        }

        public override int maxSubcompactions()
        {
            return dbOptions.maxSubcompactions();
        }

        public override int maxBackgroundFlushes()
        {
            return dbOptions.maxBackgroundFlushes();
        }

        public override Options setMaxBackgroundFlushes(int maxBackgroundFlushes)
        {
            dbOptions.setMaxBackgroundFlushes(maxBackgroundFlushes);
            return this;
        }

        public override int maxBackgroundJobs()
        {
            return dbOptions.maxBackgroundJobs();
        }

        public override Options setMaxBackgroundJobs(int maxBackgroundJobs)
        {
            dbOptions.setMaxBackgroundJobs(maxBackgroundJobs);
            return this;
        }

        public override long maxLogFileSize()
        {
            return dbOptions.maxLogFileSize();
        }

        public override Options setMaxLogFileSize(long maxLogFileSize)
        {
            dbOptions.setMaxLogFileSize(maxLogFileSize);
            return this;
        }

        public override long logFileTimeToRoll()
        {
            return dbOptions.logFileTimeToRoll();
        }

        public override Options setLogFileTimeToRoll(long logFileTimeToRoll)
        {
            dbOptions.setLogFileTimeToRoll(logFileTimeToRoll);
            return this;
        }

        public override long keepLogFileNum()
        {
            return dbOptions.keepLogFileNum();
        }

        public override Options setKeepLogFileNum(long keepLogFileNum)
        {
            dbOptions.setKeepLogFileNum(keepLogFileNum);
            return this;
        }

        public override Options setRecycleLogFileNum(long recycleLogFileNum)
        {
            dbOptions.setRecycleLogFileNum(recycleLogFileNum);
            return this;
        }

        public override long recycleLogFileNum()
        {
            return dbOptions.recycleLogFileNum();
        }

        public override long maxManifestFileSize()
        {
            return dbOptions.maxManifestFileSize();
        }

        public override Options setMaxManifestFileSize(long maxManifestFileSize)
        {
            dbOptions.setMaxManifestFileSize(maxManifestFileSize);
            return this;
        }

        public override Options setMaxTableFilesSizeFIFO(long maxTableFilesSize)
        {
            columnFamilyOptions.setMaxTableFilesSizeFIFO(maxTableFilesSize);
            return this;
        }

        public override long maxTableFilesSizeFIFO()
        {
            return columnFamilyOptions.maxTableFilesSizeFIFO();
        }

        public override int tableCacheNumshardbits()
        {
            return dbOptions.tableCacheNumshardbits();
        }

        public override Options setTableCacheNumshardbits(int tableCacheNumshardbits)
        {
            dbOptions.setTableCacheNumshardbits(tableCacheNumshardbits);
            return this;
        }

        public override long walTtlSeconds()
        {
            return dbOptions.walTtlSeconds();
        }

        public override Options setWalTtlSeconds(long walTtlSeconds)
        {
            LOG.LogWarning("option walTtlSeconds will be ignored: Streams does not expose RocksDb ttl functionality");
            dbOptions.setWalTtlSeconds(walTtlSeconds);
            return this;
        }

        public override long walSizeLimitMB()
        {
            return dbOptions.walSizeLimitMB();
        }

        public override Options setWalSizeLimitMB(long sizeLimitMB)
        {
            dbOptions.setWalSizeLimitMB(sizeLimitMB);
            return this;
        }

        public override long manifestPreallocationSize()
        {
            return dbOptions.manifestPreallocationSize();
        }

        public override Options setManifestPreallocationSize(long size)
        {
            dbOptions.setManifestPreallocationSize(size);
            return this;
        }

        public override Options setUseDirectReads(bool useDirectReads)
        {
            dbOptions.setUseDirectReads(useDirectReads);
            return this;
        }

        public override bool useDirectReads()
        {
            return dbOptions.useDirectReads();
        }

        public override Options setUseDirectIoForFlushAndCompaction(bool useDirectIoForFlushAndCompaction)
        {
            dbOptions.setUseDirectIoForFlushAndCompaction(useDirectIoForFlushAndCompaction);
            return this;
        }

        public override bool useDirectIoForFlushAndCompaction()
        {
            return dbOptions.useDirectIoForFlushAndCompaction();
        }

        public override Options setAllowFAllocate(bool allowFAllocate)
        {
            dbOptions.setAllowFAllocate(allowFAllocate);
            return this;
        }

        public override bool allowFAllocate()
        {
            return dbOptions.allowFAllocate();
        }

        public override bool allowMmapReads()
        {
            return dbOptions.allowMmapReads();
        }

        public override Options setAllowMmapReads(bool allowMmapReads)
        {
            dbOptions.setAllowMmapReads(allowMmapReads);
            return this;
        }

        public override bool allowMmapWrites()
        {
            return dbOptions.allowMmapWrites();
        }

        public override Options setAllowMmapWrites(bool allowMmapWrites)
        {
            dbOptions.setAllowMmapWrites(allowMmapWrites);
            return this;
        }

        public override bool isFdCloseOnExec()
        {
            return dbOptions.isFdCloseOnExec();
        }

        public override Options setIsFdCloseOnExec(bool isFdCloseOnExec)
        {
            dbOptions.setIsFdCloseOnExec(isFdCloseOnExec);
            return this;
        }

        public override int statsDumpPeriodSec()
        {
            return dbOptions.statsDumpPeriodSec();
        }

        public override Options setStatsDumpPeriodSec(int statsDumpPeriodSec)
        {
            dbOptions.setStatsDumpPeriodSec(statsDumpPeriodSec);
            return this;
        }

        public override bool adviseRandomOnOpen()
        {
            return dbOptions.adviseRandomOnOpen();
        }

        public override Options setAdviseRandomOnOpen(bool adviseRandomOnOpen)
        {
            dbOptions.setAdviseRandomOnOpen(adviseRandomOnOpen);
            return this;
        }

        public override Options setDbWriteBufferSize(long dbWriteBufferSize)
        {
            dbOptions.setDbWriteBufferSize(dbWriteBufferSize);
            return this;
        }

        public override long dbWriteBufferSize()
        {
            return dbOptions.dbWriteBufferSize();
        }

        public override Options setAccessHintOnCompactionStart(AccessHint accessHint)
        {
            dbOptions.setAccessHintOnCompactionStart(accessHint);
            return this;
        }

        public override AccessHint accessHintOnCompactionStart()
        {
            return dbOptions.accessHintOnCompactionStart();
        }

        public override Options setNewTableReaderForCompactionInputs(bool newTableReaderForCompactionInputs)
        {
            dbOptions.setNewTableReaderForCompactionInputs(newTableReaderForCompactionInputs);
            return this;
        }

        public override bool newTableReaderForCompactionInputs()
        {
            return dbOptions.newTableReaderForCompactionInputs();
        }

        public override Options setCompactionReadaheadSize(long compactionReadaheadSize)
        {
            dbOptions.setCompactionReadaheadSize(compactionReadaheadSize);
            return this;
        }

        public override long compactionReadaheadSize()
        {
            return dbOptions.compactionReadaheadSize();
        }

        public override Options setRandomAccessMaxBufferSize(long randomAccessMaxBufferSize)
        {
            dbOptions.setRandomAccessMaxBufferSize(randomAccessMaxBufferSize);
            return this;
        }

        public override long randomAccessMaxBufferSize()
        {
            return dbOptions.randomAccessMaxBufferSize();
        }

        public override Options setWritableFileMaxBufferSize(long writableFileMaxBufferSize)
        {
            dbOptions.setWritableFileMaxBufferSize(writableFileMaxBufferSize);
            return this;
        }

        public override long writableFileMaxBufferSize()
        {
            return dbOptions.writableFileMaxBufferSize();
        }

        public override bool useAdaptiveMutex()
        {
            return dbOptions.useAdaptiveMutex();
        }

        public override Options setUseAdaptiveMutex(bool useAdaptiveMutex)
        {
            dbOptions.setUseAdaptiveMutex(useAdaptiveMutex);
            return this;
        }

        public override long bytesPerSync()
        {
            return dbOptions.bytesPerSync();
        }

        public override Options setBytesPerSync(long bytesPerSync)
        {
            dbOptions.setBytesPerSync(bytesPerSync);
            return this;
        }

        public override Options setWalBytesPerSync(long walBytesPerSync)
        {
            dbOptions.setWalBytesPerSync(walBytesPerSync);
            return this;
        }

        public override long walBytesPerSync()
        {
            return dbOptions.walBytesPerSync();
        }

        public override Options setEnableThreadTracking(bool enableThreadTracking)
        {
            dbOptions.setEnableThreadTracking(enableThreadTracking);
            return this;
        }

        public override bool enableThreadTracking()
        {
            return dbOptions.enableThreadTracking();
        }

        public override Options setDelayedWriteRate(long delayedWriteRate)
        {
            dbOptions.setDelayedWriteRate(delayedWriteRate);
            return this;
        }

        public override long delayedWriteRate()
        {
            return dbOptions.delayedWriteRate();
        }

        public override Options setAllowConcurrentMemtableWrite(bool allowConcurrentMemtableWrite)
        {
            dbOptions.setAllowConcurrentMemtableWrite(allowConcurrentMemtableWrite);
            return this;
        }

        public override bool allowConcurrentMemtableWrite()
        {
            return dbOptions.allowConcurrentMemtableWrite();
        }

        public override Options setEnableWriteThreadAdaptiveYield(bool enableWriteThreadAdaptiveYield)
        {
            dbOptions.setEnableWriteThreadAdaptiveYield(enableWriteThreadAdaptiveYield);
            return this;
        }

        public override bool enableWriteThreadAdaptiveYield()
        {
            return dbOptions.enableWriteThreadAdaptiveYield();
        }

        public override Options setWriteThreadMaxYieldUsec(long writeThreadMaxYieldUsec)
        {
            dbOptions.setWriteThreadMaxYieldUsec(writeThreadMaxYieldUsec);
            return this;
        }

        public override long writeThreadMaxYieldUsec()
        {
            return dbOptions.writeThreadMaxYieldUsec();
        }

        public override Options setWriteThreadSlowYieldUsec(long writeThreadSlowYieldUsec)
        {
            dbOptions.setWriteThreadSlowYieldUsec(writeThreadSlowYieldUsec);
            return this;
        }

        public override long writeThreadSlowYieldUsec()
        {
            return dbOptions.writeThreadSlowYieldUsec();
        }

        public override Options setSkipStatsUpdateOnDbOpen(bool skipStatsUpdateOnDbOpen)
        {
            dbOptions.setSkipStatsUpdateOnDbOpen(skipStatsUpdateOnDbOpen);
            return this;
        }

        public override bool skipStatsUpdateOnDbOpen()
        {
            return dbOptions.skipStatsUpdateOnDbOpen();
        }

        public override Options setWalRecoveryMode(WALRecoveryMode walRecoveryMode)
        {
            dbOptions.setWalRecoveryMode(walRecoveryMode);
            return this;
        }

        public override WALRecoveryMode walRecoveryMode()
        {
            return dbOptions.walRecoveryMode();
        }

        public override Options setAllow2pc(bool allow2pc)
        {
            dbOptions.setAllow2pc(allow2pc);
            return this;
        }

        public override bool allow2pc()
        {
            return dbOptions.allow2pc();
        }

        public override Options setRowCache(Cache rowCache)
        {
            dbOptions.setRowCache(rowCache);
            return this;
        }

        public override Cache rowCache()
        {
            return dbOptions.rowCache();
        }

        public override Options setFailIfOptionsFileError(bool failIfOptionsFileError)
        {
            dbOptions.setFailIfOptionsFileError(failIfOptionsFileError);
            return this;
        }

        public override bool failIfOptionsFileError()
        {
            return dbOptions.failIfOptionsFileError();
        }

        public override Options setDumpMallocStats(bool dumpMallocStats)
        {
            dbOptions.setDumpMallocStats(dumpMallocStats);
            return this;
        }

        public override bool dumpMallocStats()
        {
            return dbOptions.dumpMallocStats();
        }

        public override Options setAvoidFlushDuringRecovery(bool avoidFlushDuringRecovery)
        {
            dbOptions.setAvoidFlushDuringRecovery(avoidFlushDuringRecovery);
            return this;
        }

        public override bool avoidFlushDuringRecovery()
        {
            return dbOptions.avoidFlushDuringRecovery();
        }

        public override Options setAvoidFlushDuringShutdown(bool avoidFlushDuringShutdown)
        {
            dbOptions.setAvoidFlushDuringShutdown(avoidFlushDuringShutdown);
            return this;
        }

        public override bool avoidFlushDuringShutdown()
        {
            return dbOptions.avoidFlushDuringShutdown();
        }

        public override MemTableConfig memTableConfig()
        {
            return columnFamilyOptions.memTableConfig();
        }

        public override Options setMemTableConfig(MemTableConfig config)
        {
            columnFamilyOptions.setMemTableConfig(config);
            return this;
        }

        public override Options setRateLimiter(RateLimiter rateLimiter)
        {
            dbOptions.setRateLimiter(rateLimiter);
            return this;
        }

        public override Options setSstFileManager(SstFileManager sstFileManager)
        {
            dbOptions.setSstFileManager(sstFileManager);
            return this;
        }

        public override Options setLogger(ILogger logger)
        {
            dbOptions.setLogger(logger);
            return this;
        }

        public override Options setInfoLogLevel(InfoLogLevel infoLogLevel)
        {
            dbOptions.setInfoLogLevel(infoLogLevel);
            return this;
        }

        public override InfoLogLevel infoLogLevel()
        {
            return dbOptions.infoLogLevel();
        }

        public override string memTableFactoryName()
        {
            return columnFamilyOptions.memTableFactoryName();
        }

        public override TableFormatConfig tableFormatConfig()
        {
            return columnFamilyOptions.tableFormatConfig();
        }

        public override Options setTableFormatConfig(TableFormatConfig config)
        {
            columnFamilyOptions.setTableFormatConfig(config);
            return this;
        }

        public override string tableFactoryName()
        {
            return columnFamilyOptions.tableFactoryName();
        }

        public override Options useFixedLengthPrefixExtractor(int n)
        {
            columnFamilyOptions.useFixedLengthPrefixExtractor(n);
            return this;
        }

        public override Options useCappedPrefixExtractor(int n)
        {
            columnFamilyOptions.useCappedPrefixExtractor(n);
            return this;
        }

        public override CompressionType compressionType()
        {
            return columnFamilyOptions.compressionType();
        }

        public override Options setCompressionPerLevel(List<CompressionType> compressionLevels)
        {
            columnFamilyOptions.setCompressionPerLevel(compressionLevels);
            return this;
        }

        public override List<CompressionType> compressionPerLevel()
        {
            return columnFamilyOptions.compressionPerLevel();
        }

        public override Options setCompressionType(CompressionType compressionType)
        {
            columnFamilyOptions.setCompressionType(compressionType);
            return this;
        }


        public override Options setBottommostCompressionType(CompressionType bottommostCompressionType)
        {
            columnFamilyOptions.setBottommostCompressionType(bottommostCompressionType);
            return this;
        }

        public override CompressionType bottommostCompressionType()
        {
            return columnFamilyOptions.bottommostCompressionType();
        }

        public override Options setCompressionOptions(CompressionOptions compressionOptions)
        {
            columnFamilyOptions.setCompressionOptions(compressionOptions);
            return this;
        }

        public override CompressionOptions compressionOptions()
        {
            return columnFamilyOptions.compressionOptions();
        }

        public override CompactionStyle compactionStyle()
        {
            return columnFamilyOptions.compactionStyle();
        }

        public override Options setCompactionStyle(CompactionStyle compactionStyle)
        {
            columnFamilyOptions.setCompactionStyle(compactionStyle);
            return this;
        }

        public override int numLevels()
        {
            return columnFamilyOptions.numLevels();
        }

        public override Options setNumLevels(int numLevels)
        {
            columnFamilyOptions.setNumLevels(numLevels);
            return this;
        }

        public override int levelZeroFileNumCompactionTrigger()
        {
            return columnFamilyOptions.levelZeroFileNumCompactionTrigger();
        }

        public override Options setLevelZeroFileNumCompactionTrigger(int numFiles)
        {
            columnFamilyOptions.setLevelZeroFileNumCompactionTrigger(numFiles);
            return this;
        }

        public override int levelZeroSlowdownWritesTrigger()
        {
            return columnFamilyOptions.levelZeroSlowdownWritesTrigger();
        }

        public override Options setLevelZeroSlowdownWritesTrigger(int numFiles)
        {
            columnFamilyOptions.setLevelZeroSlowdownWritesTrigger(numFiles);
            return this;
        }

        public override int levelZeroStopWritesTrigger()
        {
            return columnFamilyOptions.levelZeroStopWritesTrigger();
        }

        public override Options setLevelZeroStopWritesTrigger(int numFiles)
        {
            columnFamilyOptions.setLevelZeroStopWritesTrigger(numFiles);
            return this;
        }

        public override long targetFileSizeBase()
        {
            return columnFamilyOptions.targetFileSizeBase();
        }

        public override Options setTargetFileSizeBase(long targetFileSizeBase)
        {
            columnFamilyOptions.setTargetFileSizeBase(targetFileSizeBase);
            return this;
        }

        public override int targetFileSizeMultiplier()
        {
            return columnFamilyOptions.targetFileSizeMultiplier();
        }

        public override Options setTargetFileSizeMultiplier(int multiplier)
        {
            columnFamilyOptions.setTargetFileSizeMultiplier(multiplier);
            return this;
        }

        public override Options setMaxBytesForLevelBase(long maxBytesForLevelBase)
        {
            columnFamilyOptions.setMaxBytesForLevelBase(maxBytesForLevelBase);
            return this;
        }

        public override long maxBytesForLevelBase()
        {
            return columnFamilyOptions.maxBytesForLevelBase();
        }

        public override Options setLevelCompactionDynamicLevelBytes(bool enableLevelCompactionDynamicLevelBytes)
        {
            columnFamilyOptions.setLevelCompactionDynamicLevelBytes(enableLevelCompactionDynamicLevelBytes);
            return this;
        }

        public override bool levelCompactionDynamicLevelBytes()
        {
            return columnFamilyOptions.levelCompactionDynamicLevelBytes();
        }

        public override double maxBytesForLevelMultiplier()
        {
            return columnFamilyOptions.maxBytesForLevelMultiplier();
        }

        public override Options setMaxBytesForLevelMultiplier(double multiplier)
        {
            columnFamilyOptions.setMaxBytesForLevelMultiplier(multiplier);
            return this;
        }

        public override long maxCompactionBytes()
        {
            return columnFamilyOptions.maxCompactionBytes();
        }

        public override Options setMaxCompactionBytes(long maxCompactionBytes)
        {
            columnFamilyOptions.setMaxCompactionBytes(maxCompactionBytes);
            return this;
        }

        public override long arenaBlockSize()
        {
            return columnFamilyOptions.arenaBlockSize();
        }

        public override Options setArenaBlockSize(long arenaBlockSize)
        {
            columnFamilyOptions.setArenaBlockSize(arenaBlockSize);
            return this;
        }

        public override bool disableAutoCompactions()
        {
            return columnFamilyOptions.disableAutoCompactions();
        }

        public override Options setDisableAutoCompactions(bool disableAutoCompactions)
        {
            columnFamilyOptions.setDisableAutoCompactions(disableAutoCompactions);
            return this;
        }

        public override long maxSequentialSkipInIterations()
        {
            return columnFamilyOptions.maxSequentialSkipInIterations();
        }

        public override Options setMaxSequentialSkipInIterations(long maxSequentialSkipInIterations)
        {
            columnFamilyOptions.setMaxSequentialSkipInIterations(maxSequentialSkipInIterations);
            return this;
        }

        public override bool inplaceUpdateSupport()
        {
            return columnFamilyOptions.inplaceUpdateSupport();
        }

        public override Options setInplaceUpdateSupport(bool inplaceUpdateSupport)
        {
            columnFamilyOptions.setInplaceUpdateSupport(inplaceUpdateSupport);
            return this;
        }

        public override long inplaceUpdateNumLocks()
        {
            return columnFamilyOptions.inplaceUpdateNumLocks();
        }

        public override Options setInplaceUpdateNumLocks(long inplaceUpdateNumLocks)
        {
            columnFamilyOptions.setInplaceUpdateNumLocks(inplaceUpdateNumLocks);
            return this;
        }

        public override double memtablePrefixBloomSizeRatio()
        {
            return columnFamilyOptions.memtablePrefixBloomSizeRatio();
        }

        public override Options setMemtablePrefixBloomSizeRatio(double memtablePrefixBloomSizeRatio)
        {
            columnFamilyOptions.setMemtablePrefixBloomSizeRatio(memtablePrefixBloomSizeRatio);
            return this;
        }

        public override int bloomLocality()
        {
            return columnFamilyOptions.bloomLocality();
        }

        public override Options setBloomLocality(int bloomLocality)
        {
            columnFamilyOptions.setBloomLocality(bloomLocality);
            return this;
        }

        public override long maxSuccessiveMerges()
        {
            return columnFamilyOptions.maxSuccessiveMerges();
        }

        public override Options setMaxSuccessiveMerges(long maxSuccessiveMerges)
        {
            columnFamilyOptions.setMaxSuccessiveMerges(maxSuccessiveMerges);
            return this;
        }

        public override int minWriteBufferNumberToMerge()
        {
            return columnFamilyOptions.minWriteBufferNumberToMerge();
        }

        public override Options setMinWriteBufferNumberToMerge(int minWriteBufferNumberToMerge)
        {
            columnFamilyOptions.setMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge);
            return this;
        }

        public override Options setOptimizeFiltersForHits(bool optimizeFiltersForHits)
        {
            columnFamilyOptions.setOptimizeFiltersForHits(optimizeFiltersForHits);
            return this;
        }

        public override bool optimizeFiltersForHits()
        {
            return columnFamilyOptions.optimizeFiltersForHits();
        }

        public override Options setMemtableHugePageSize(long memtableHugePageSize)
        {
            columnFamilyOptions.setMemtableHugePageSize(memtableHugePageSize);
            return this;
        }

        public override long memtableHugePageSize()
        {
            return columnFamilyOptions.memtableHugePageSize();
        }

        public override Options setSoftPendingCompactionBytesLimit(long softPendingCompactionBytesLimit)
        {
            columnFamilyOptions.setSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
            return this;
        }

        public override long softPendingCompactionBytesLimit()
        {
            return columnFamilyOptions.softPendingCompactionBytesLimit();
        }

        public override Options setHardPendingCompactionBytesLimit(long hardPendingCompactionBytesLimit)
        {
            columnFamilyOptions.setHardPendingCompactionBytesLimit(hardPendingCompactionBytesLimit);
            return this;
        }

        public override long hardPendingCompactionBytesLimit()
        {
            return columnFamilyOptions.hardPendingCompactionBytesLimit();
        }

        public override Options setLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger)
        {
            columnFamilyOptions.setLevel0FileNumCompactionTrigger(level0FileNumCompactionTrigger);
            return this;
        }

        public override int level0FileNumCompactionTrigger()
        {
            return columnFamilyOptions.level0FileNumCompactionTrigger();
        }

        public override Options setLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger)
        {
            columnFamilyOptions.setLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger);
            return this;
        }

        public override int level0SlowdownWritesTrigger()
        {
            return columnFamilyOptions.level0SlowdownWritesTrigger();
        }

        public override Options setLevel0StopWritesTrigger(int level0StopWritesTrigger)
        {
            columnFamilyOptions.setLevel0StopWritesTrigger(level0StopWritesTrigger);
            return this;
        }

        public override int level0StopWritesTrigger()
        {
            return columnFamilyOptions.level0StopWritesTrigger();
        }

        public override Options setMaxBytesForLevelMultiplierAdditional(int[] maxBytesForLevelMultiplierAdditional)
        {
            columnFamilyOptions.setMaxBytesForLevelMultiplierAdditional(maxBytesForLevelMultiplierAdditional);
            return this;
        }

        public override int[] maxBytesForLevelMultiplierAdditional()
        {
            return columnFamilyOptions.maxBytesForLevelMultiplierAdditional();
        }

        public override Options setParanoidFileChecks(bool paranoidFileChecks)
        {
            columnFamilyOptions.setParanoidFileChecks(paranoidFileChecks);
            return this;
        }

        public override bool paranoidFileChecks()
        {
            return columnFamilyOptions.paranoidFileChecks();
        }

        public override Options setMaxWriteBufferNumberToMaintain(int maxWriteBufferNumberToMaintain)
        {
            columnFamilyOptions.setMaxWriteBufferNumberToMaintain(maxWriteBufferNumberToMaintain);
            return this;
        }

        public override int maxWriteBufferNumberToMaintain()
        {
            return columnFamilyOptions.maxWriteBufferNumberToMaintain();
        }

        public override Options setCompactionPriority(CompactionPriority compactionPriority)
        {
            columnFamilyOptions.setCompactionPriority(compactionPriority);
            return this;
        }

        public override CompactionPriority compactionPriority()
        {
            return columnFamilyOptions.compactionPriority();
        }

        public override Options setReportBgIoStats(bool reportBgIoStats)
        {
            columnFamilyOptions.setReportBgIoStats(reportBgIoStats);
            return this;
        }

        public override bool reportBgIoStats()
        {
            return columnFamilyOptions.reportBgIoStats();
        }

        public override Options setCompactionOptionsUniversal(CompactionOptionsUniversal compactionOptionsUniversal)
        {
            columnFamilyOptions.setCompactionOptionsUniversal(compactionOptionsUniversal);
            return this;
        }

        public override CompactionOptionsUniversal compactionOptionsUniversal()
        {
            return columnFamilyOptions.compactionOptionsUniversal();
        }

        public override Options setCompactionOptionsFIFO(CompactionOptionsFIFO compactionOptionsFIFO)
        {
            columnFamilyOptions.setCompactionOptionsFIFO(compactionOptionsFIFO);
            return this;
        }

        public override CompactionOptionsFIFO compactionOptionsFIFO()
        {
            return columnFamilyOptions.compactionOptionsFIFO();
        }

        public override Options setForceConsistencyChecks(bool forceConsistencyChecks)
        {
            columnFamilyOptions.setForceConsistencyChecks(forceConsistencyChecks);
            return this;
        }

        public override bool forceConsistencyChecks()
        {
            return columnFamilyOptions.forceConsistencyChecks();
        }

        public override Options setWriteBufferManager(WriteBufferManager writeBufferManager)
        {
            dbOptions.setWriteBufferManager(writeBufferManager);
            return this;
        }

        public override WriteBufferManager writeBufferManager()
        {
            return dbOptions.writeBufferManager();
        }

        public Options setCompactionFilter(AbstractCompactionFilter<AbstractSlice<object>> compactionFilter)
        {
            columnFamilyOptions.setCompactionFilter(compactionFilter);
            return this;
        }

        public Options setCompactionFilterFactory(AbstractCompactionFilterFactory<AbstractCompactionFilter<object>> compactionFilterFactory)
        {
            columnFamilyOptions.setCompactionFilterFactory(compactionFilterFactory);
            return this;
        }

        public override void close()
        {
            // ColumnFamilyOptions should be closed last
            dbOptions.close();
            columnFamilyOptions.close();
        }
    }
}