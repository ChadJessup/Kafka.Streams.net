
//            // (1) not in our control
//            // (2) is done via bulk-loading API
//            // (3) skipping because, not done in actual PrepareForBulkLoad() code in https://github.com/facebook/rocksdb/blob/master/options/options.cc
//            //columnFamilyOptions.SetMemTableConfig(new VectorMemTableConfig());
//            // (4-5) below:
//            dbOptions.SetMaxBackgroundFlushes(4);
//            columnFamilyOptions.SetDisableAutoCompactions(1);
//            columnFamilyOptions.SetLevel0FileNumCompactionTrigger(1 << 30);
//            columnFamilyOptions.SetLevel0SlowdownWritesTrigger(1 << 30);
//            columnFamilyOptions.SetLevel0StopWritesTrigger(1 << 30);

//            return this;
//        }

//        //public bool createIfMissing()
//        //{
//        //    return dbOptions.SetCreateIfMissing(true);
//        //}

//        //public bool createMissingColumnFamilies()
//        //{
//        //    return dbOptions.createMissingColumnFamilies();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optimizeForSmallDb()
//        {
//            //dbOptions.optimizeForSmallDb();
//            //columnFamilyOptions.optimizeForSmallDb();

//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optimizeForPointLookup(ulong blockCacheSizeMb)
//        {
//            columnFamilyOptions.OptimizeForPointLookup(blockCacheSizeMb);

//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optimizeLevelStyleCompaction()
//        {
//            // Default from source.
//            columnFamilyOptions.OptimizeLevelStyleCompaction(512 * 1024 * 1024);

//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optimizeLevelStyleCompaction(ulong memtableMemoryBudget)
//        {
//            columnFamilyOptions.OptimizeLevelStyleCompaction(memtableMemoryBudget);

//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optimizeUniversalStyleCompaction()
//        {
//            columnFamilyOptions.OptimizeUniversalStyleCompaction(512 * 1024 * 1024);

//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optimizeUniversalStyleCompaction(ulong memtableMemoryBudget)
//        {
//            columnFamilyOptions.OptimizeUniversalStyleCompaction(memtableMemoryBudget);

//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setComparator(Comparator builtinComparator)
//        {
//            columnFamilyOptions.SetComparator(builtinComparator);

//            return this;
//        }

//        //public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setComparator(AbstractComparator<AbstractSlice<object>> comparator)
//        //{
//        //    columnFamilyOptions.SetComparator(comparator);
//        //    return this;
//        //}

//        //public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMergeOperatorName(string name)
//        //{
//        //    columnFamilyOptions.SetMergeOperatorName(name);

//        //    return this;
//        //}

//        //public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMergeOperator(MergeOperator mergeOperator)
//        //{
//        //    columnFamilyOptions.SetMergeOperator(mergeOperator);

//        //    return this;
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWriteBufferSize(ulong writeBufferSize)
//        {
//            columnFamilyOptions.SetWriteBufferSize(writeBufferSize);

//            return this;
//        }

//        //public override long writeBufferSize()
//        //{
//        //    return columnFamilyOptions.SetWriteBufferSize();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxWriteBufferNumber(int maxWriteBufferNumber)
//        {
//            columnFamilyOptions.SetMaxWriteBufferNumber(maxWriteBufferNumber);

//            return this;
//        }

//        //public override int maxWriteBufferNumber()
//        //{
//        //    return columnFamilyOptions..maxWriteBufferNumber();
//        //}

//        //public bool errorIfExists()
//        //{
//        //    return dbOptions.errorIfExists();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setErrorIfExists(bool errorIfExists)
//        {
//            dbOptions.SetErrorIfExists(errorIfExists);

//            return this;
//        }

//        //public bool paranoidChecks()
//        //{
//        //    bool columnFamilyParanoidFileChecks = columnFamilyOptions.paranoidFileChecks();
//        //    bool dbOptionsParanoidChecks = dbOptions.paranoidChecks();

//        //    if (columnFamilyParanoidFileChecks != dbOptionsParanoidChecks)
//        //    {
//        //        throw new InvalidOperationException("Config for paranoid checks for RockDB and ColumnFamilies should be the same.");
//        //    }

//        //    return dbOptionsParanoidChecks;
//        //}

//        //public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setParanoidChecks(bool paranoidChecks)
//        //{
//        //    columnFamilyOptions.paranoidFileChecks();
//        //    dbOptions.SetParanoidChecks(paranoidChecks);
//        //    return this;
//        //}

//        //public override int maxOpenFiles()
//        //{
//        //    return dbOptions.maxOpenFiles();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxFileOpeningThreads(int maxFileOpeningThreads)
//        {
//            dbOptions.SetMaxFileOpeningThreads(maxFileOpeningThreads);

//            return this;
//        }

//        //public override int maxFileOpeningThreads()
//        //{
//        //    return dbOptions.maxFileOpeningThreads();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxTotalWalSize(ulong maxTotalWalSize)
//        {
//            dbOptions.SetMaxTotalWalSize(maxTotalWalSize);

//            return this;
//        }

//        //public override long maxTotalWalSize()
//        //{
//        //    return dbOptions.maxTotalWalSize();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxOpenFiles(int maxOpenFiles)
//        {
//            dbOptions.SetMaxOpenFiles(maxOpenFiles);
//            return this;
//        }

//        //public override bool useFsync()
//        //{
//        //    return dbOptions.useFsync();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setUseFsync(bool useFsync)
//        {
//            dbOptions.SetUseFsync(useFsync ? 1 : 0);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setDbPaths(List<DbPath> dbPaths)
//        {
//            dbOptions.SetDbLogDir(dbPaths);
//            return this;
//        }

//        //public override List<DbPath> dbPaths()
//        //{
//        //    return dbOptions.dbPaths();
//        //}

//        //public override string dbLogDir()
//        //{
//        //    return dbOptions.dbLogDir();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setDbLogDir(string dbLogDir)
//        {
//            dbOptions.SetDbLogDir(dbLogDir);
//            return this;
//        }

//        //public override string walDir()
//        //{
//        //    return dbOptions.walDir();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWalDir(string walDir)
//        {
//            dbOptions.SetWalDir(walDir);
//            return this;
//        }

//        //public override long deleteObsoleteFilesPeriodMicros()
//        //{
//        //    return dbOptions.deleteObsoleteFilesPeriodMicros();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setDeleteObsoleteFilesPeriodMicros(long micros)
//        {
//            dbOptions.SetDeleteObsoleteFilesPeriodMicros(micros);
//            return this;
//        }

//        public override int maxBackgroundCompactions()
//        {
//            return dbOptions.maxBackgroundCompactions();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setStatistics()
//        {
//            dbOptions.EnableStatistics();
//            return this;
//        }

//        //public override Statistics statistics()
//        //{
//        //    return dbOptions.statistics();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setBaseBackgroundCompactions(int baseBackgroundCompactions)
//        {
//            dbOptions.SetBaseBackgroundCompactions(baseBackgroundCompactions);

//            return this;
//        }

//        //public override int baseBackgroundCompactions()
//        //{
//        //    return dbOptions.base.baseBackgroundCompactions();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxBackgroundCompactions(int maxBackgroundCompactions)
//        {
//            dbOptions.SetMaxBackgroundCompactions(maxBackgroundCompactions);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxSubcompactions(int maxSubcompactions)
//        {
//            dbOptions.SetMaxBackgroundCompactions(maxSubcompactions);

//            return this;
//        }

//        //public override int maxSubcompactions()
//        //{
//        //    return dbOptions.maxSubcompactions();
//        //}

//        //public override int maxBackgroundFlushes()
//        //{
//        //    return dbOptions.maxBackgroundFlushes();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxBackgroundFlushes(int maxBackgroundFlushes)
//        {
//            dbOptions.SetMaxBackgroundFlushes(maxBackgroundFlushes);
//            return this;
//        }

//        //public override int maxBackgroundJobs()
//        //{
//        //    return dbOptions.maxBackgroundJobs();
//        //}

//        //public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxBackgroundJobs(int maxBackgroundJobs)
//        //{
//        //    dbOptions.SetMaxBackgroundJobs(maxBackgroundJobs);
//        //    return this;
//        //}

//        //public override long maxLogFileSize()
//        //{
//        //    return dbOptions.maxLogFileSize();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxLogFileSize(ulong maxLogFileSize)
//        {
//            dbOptions.SetMaxLogFileSize(maxLogFileSize);
//            return this;
//        }

//        //public override long logFileTimeToRoll()
//        //{
//        //    return dbOptions.logFileTimeToRoll();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLogFileTimeToRoll(ulong logFileTimeToRoll)
//        {
//            dbOptions.SetLogFileTimeToRoll(logFileTimeToRoll);
//            return this;
//        }

//        //public override long keepLogFileNum()
//        //{
//        //    return dbOptions.keepLogFileNum();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setKeepLogFileNum(ulong keepLogFileNum)
//        {
//            dbOptions.SetKeepLogFileNum(keepLogFileNum);

//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setRecycleLogFileNum(ulong recycleLogFileNum)
//        {
//            dbOptions.SetRecycleLogFileNum(recycleLogFileNum);
//            return this;
//        }

//        //public override long recycleLogFileNum()
//        //{
//        //    return dbOptions.recycleLogFileNum();
//        //}

//        //public override long maxManifestFileSize()
//        //{
//        //    return dbOptions.maxManifestFileSize();
//        //}

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxManifestFileSize(ulong maxManifestFileSize)
//        {
//            dbOptions.SetMaxManifestFileSize(maxManifestFileSize);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxTableFilesSizeFIFO(long maxTableFilesSize)
//        {
//            columnFamilyOptions.SetMaxTableFilesSizeFIFO(maxTableFilesSize);
//            return this;
//        }

//        public override long maxTableFilesSizeFIFO()
//        {
//            return columnFamilyOptions.maxTableFilesSizeFIFO();
//        }

//        public override int tableCacheNumshardbits()
//        {
//            return dbOptions.tableCacheNumshardbits();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setTableCacheNumshardbits(int tableCacheNumshardbits)
//        {
//            dbOptions.SetTableCacheNumshardbits(tableCacheNumshardbits);
//            return this;
//        }

//        public override long walTtlSeconds()
//        {
//            return dbOptions.walTtlSeconds();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWalTtlSeconds(long walTtlSeconds)
//        {
//            LOG.LogWarning("option walTtlSeconds will be ignored: Streams does not expose RocksDb ttl functionality");
//            dbOptions.SetWalTtlSeconds(walTtlSeconds);
//            return this;
//        }

//        public override long walSizeLimitMB()
//        {
//            return dbOptions.walSizeLimitMB();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWalSizeLimitMB(long sizeLimitMB)
//        {
//            dbOptions.SetWalSizeLimitMB(sizeLimitMB);
//            return this;
//        }

//        public override long manifestPreallocationSize()
//        {
//            return dbOptions.manifestPreallocationSize();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setManifestPreallocationSize(long size)
//        {
//            dbOptions.SetManifestPreallocationSize(size);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setUseDirectReads(bool useDirectReads)
//        {
//            dbOptions.SetUseDirectReads(useDirectReads);
//            return this;
//        }

//        public override bool useDirectReads()
//        {
//            return dbOptions.useDirectReads();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setUseDirectIoForFlushAndCompaction(bool useDirectIoForFlushAndCompaction)
//        {
//            dbOptions.SetUseDirectIoForFlushAndCompaction(useDirectIoForFlushAndCompaction);
//            return this;
//        }

//        public override bool useDirectIoForFlushAndCompaction()
//        {
//            return dbOptions.useDirectIoForFlushAndCompaction();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAllowFAllocate(bool allowFAllocate)
//        {
//            dbOptions.SetAllowFAllocate(allowFAllocate);
//            return this;
//        }

//        public override bool allowFAllocate()
//        {
//            return dbOptions.allowFAllocate();
//        }

//        public override bool allowMmapReads()
//        {
//            return dbOptions.allowMmapReads();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAllowMmapReads(bool allowMmapReads)
//        {
//            dbOptions.SetAllowMmapReads(allowMmapReads);
//            return this;
//        }

//        public override bool allowMmapWrites()
//        {
//            return dbOptions.allowMmapWrites();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAllowMmapWrites(bool allowMmapWrites)
//        {
//            dbOptions.SetAllowMmapWrites(allowMmapWrites);
//            return this;
//        }

//        public override bool isFdCloseOnExec()
//        {
//            return dbOptions.isFdCloseOnExec();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setIsFdCloseOnExec(bool isFdCloseOnExec)
//        {
//            dbOptions.SetIsFdCloseOnExec(isFdCloseOnExec);
//            return this;
//        }

//        public override int statsDumpPeriodSec()
//        {
//            return dbOptions.statsDumpPeriodSec();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setStatsDumpPeriodSec(int statsDumpPeriodSec)
//        {
//            dbOptions.SetStatsDumpPeriodSec(statsDumpPeriodSec);
//            return this;
//        }

//        public override bool adviseRandomOnOpen()
//        {
//            return dbOptions.adviseRandomOnOpen();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAdviseRandomOnOpen(bool adviseRandomOnOpen)
//        {
//            dbOptions.SetAdviseRandomOnOpen(adviseRandomOnOpen);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setDbWriteBufferSize(long dbWriteBufferSize)
//        {
//            dbOptions.SetDbWriteBufferSize(dbWriteBufferSize);
//            return this;
//        }

//        public override long dbWriteBufferSize()
//        {
//            return dbOptions.dbWriteBufferSize();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAccessHintOnCompactionStart(AccessHint accessHint)
//        {
//            dbOptions.SetAccessHintOnCompactionStart(accessHint);
//            return this;
//        }

//        public override AccessHint accessHintOnCompactionStart()
//        {
//            return dbOptions.accessHintOnCompactionStart();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setNewTableReaderForCompactionInputs(bool newTableReaderForCompactionInputs)
//        {
//            dbOptions.SetNewTableReaderForCompactionInputs(newTableReaderForCompactionInputs);
//            return this;
//        }

//        public override bool newTableReaderForCompactionInputs()
//        {
//            return dbOptions.newTableReaderForCompactionInputs();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setCompactionReadaheadSize(long compactionReadaheadSize)
//        {
//            dbOptions.SetCompactionReadaheadSize(compactionReadaheadSize);
//            return this;
//        }

//        public override long compactionReadaheadSize()
//        {
//            return dbOptions.compactionReadaheadSize();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setRandomAccessMaxBufferSize(long randomAccessMaxBufferSize)
//        {
//            dbOptions.SetRandomAccessMaxBufferSize(randomAccessMaxBufferSize);
//            return this;
//        }

//        public override long randomAccessMaxBufferSize()
//        {
//            return dbOptions.randomAccessMaxBufferSize();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWritableFileMaxBufferSize(long writableFileMaxBufferSize)
//        {
//            dbOptions.SetWritableFileMaxBufferSize(writableFileMaxBufferSize);
//            return this;
//        }

//        public override long writableFileMaxBufferSize()
//        {
//            return dbOptions.writableFileMaxBufferSize();
//        }

//        public override bool useAdaptiveMutex()
//        {
//            return dbOptions.useAdaptiveMutex();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setUseAdaptiveMutex(bool useAdaptiveMutex)
//        {
//            dbOptions.SetUseAdaptiveMutex(useAdaptiveMutex);
//            return this;
//        }

//        public override long bytesPerSync()
//        {
//            return dbOptions.bytesPerSync();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setBytesPerSync(long bytesPerSync)
//        {
//            dbOptions.SetBytesPerSync(bytesPerSync);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWalBytesPerSync(long walBytesPerSync)
//        {
//            dbOptions.SetWalBytesPerSync(walBytesPerSync);
//            return this;
//        }

//        public override long walBytesPerSync()
//        {
//            return dbOptions.walBytesPerSync();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setEnableThreadTracking(bool enableThreadTracking)
//        {
//            dbOptions.SetEnableThreadTracking(enableThreadTracking);
//            return this;
//        }

//        public override bool enableThreadTracking()
//        {
//            return dbOptions.enableThreadTracking();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setDelayedWriteRate(long delayedWriteRate)
//        {
//            dbOptions.SetDelayedWriteRate(delayedWriteRate);
//            return this;
//        }

//        public override long delayedWriteRate()
//        {
//            return dbOptions.delayedWriteRate();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAllowConcurrentMemtableWrite(bool allowConcurrentMemtableWrite)
//        {
//            dbOptions.SetAllowConcurrentMemtableWrite(allowConcurrentMemtableWrite);
//            return this;
//        }

//        public override bool allowConcurrentMemtableWrite()
//        {
//            return dbOptions.allowConcurrentMemtableWrite();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setEnableWriteThreadAdaptiveYield(bool enableWriteThreadAdaptiveYield)
//        {
//            dbOptions.SetEnableWriteThreadAdaptiveYield(enableWriteThreadAdaptiveYield);
//            return this;
//        }

//        public override bool enableWriteThreadAdaptiveYield()
//        {
//            return dbOptions.enableWriteThreadAdaptiveYield();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWriteThreadMaxYieldUsec(long writeThreadMaxYieldUsec)
//        {
//            dbOptions.SetWriteThreadMaxYieldUsec(writeThreadMaxYieldUsec);
//            return this;
//        }

//        public override long writeThreadMaxYieldUsec()
//        {
//            return dbOptions.writeThreadMaxYieldUsec();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWriteThreadSlowYieldUsec(long writeThreadSlowYieldUsec)
//        {
//            dbOptions.SetWriteThreadSlowYieldUsec(writeThreadSlowYieldUsec);
//            return this;
//        }

//        public override long writeThreadSlowYieldUsec()
//        {
//            return dbOptions.writeThreadSlowYieldUsec();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setSkipStatsUpdateOnDbOpen(bool skipStatsUpdateOnDbOpen)
//        {
//            dbOptions.SetSkipStatsUpdateOnDbOpen(skipStatsUpdateOnDbOpen);
//            return this;
//        }

//        public override bool skipStatsUpdateOnDbOpen()
//        {
//            return dbOptions.skipStatsUpdateOnDbOpen();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWalRecoveryMode(WALRecoveryMode walRecoveryMode)
//        {
//            dbOptions.SetWalRecoveryMode(walRecoveryMode);
//            return this;
//        }

//        public override WALRecoveryMode walRecoveryMode()
//        {
//            return dbOptions.walRecoveryMode();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAllow2pc(bool allow2pc)
//        {
//            dbOptions.SetAllow2pc(allow2pc);
//            return this;
//        }

//        public override bool allow2pc()
//        {
//            return dbOptions.allow2pc();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setRowCache(Cache rowCache)
//        {
//            dbOptions.SetRowCache(rowCache);
//            return this;
//        }

//        public override Cache rowCache()
//        {
//            return dbOptions.rowCache();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setFailIfOptionsFileError(bool failIfOptionsFileError)
//        {
//            dbOptions.SetFailIfOptionsFileError(failIfOptionsFileError);
//            return this;
//        }

//        public override bool failIfOptionsFileError()
//        {
//            return dbOptions.failIfOptionsFileError();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setDumpMallocStats(bool dumpMallocStats)
//        {
//            dbOptions.SetDumpMallocStats(dumpMallocStats);
//            return this;
//        }

//        public override bool dumpMallocStats()
//        {
//            return dbOptions.dumpMallocStats();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAvoidFlushDuringRecovery(bool avoidFlushDuringRecovery)
//        {
//            dbOptions.SetAvoidFlushDuringRecovery(avoidFlushDuringRecovery);
//            return this;
//        }

//        public override bool avoidFlushDuringRecovery()
//        {
//            return dbOptions.avoidFlushDuringRecovery();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setAvoidFlushDuringShutdown(bool avoidFlushDuringShutdown)
//        {
//            dbOptions.SetAvoidFlushDuringShutdown(avoidFlushDuringShutdown);
//            return this;
//        }

//        public override bool avoidFlushDuringShutdown()
//        {
//            return dbOptions.avoidFlushDuringShutdown();
//        }

//        public override MemTableConfig memTableConfig()
//        {
//            return columnFamilyOptions.memTableConfig();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMemTableConfig(MemTableConfig config)
//        {
//            columnFamilyOptions.SetMemTableConfig(config);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setRateLimiter(RateLimiter rateLimiter)
//        {
//            dbOptions.SetRateLimiter(rateLimiter);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setSstFileManager(SstFileManager sstFileManager)
//        {
//            dbOptions.SetSstFileManager(sstFileManager);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLogger(ILogger logger)
//        {
//            dbOptions.SetLogger(logger);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setInfoLogLevel(InfoLogLevel infoLogLevel)
//        {
//            dbOptions.SetInfoLogLevel(infoLogLevel);
//            return this;
//        }

//        public override InfoLogLevel infoLogLevel()
//        {
//            return dbOptions.infoLogLevel();
//        }

//        public override string memTableFactoryName()
//        {
//            return columnFamilyOptions.memTableFactoryName();
//        }

//        public override TableFormatConfig tableFormatConfig()
//        {
//            return columnFamilyOptions.tableFormatConfig();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setTableFormatConfig(TableFormatConfig config)
//        {
//            columnFamilyOptions.SetTableFormatConfig(config);
//            return this;
//        }

//        public override string tableFactoryName()
//        {
//            return columnFamilyOptions.tableFactoryName();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter useFixedLengthPrefixExtractor(int n)
//        {
//            columnFamilyOptions.useFixedLengthPrefixExtractor(n);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter useCappedPrefixExtractor(int n)
//        {
//            columnFamilyOptions.useCappedPrefixExtractor(n);
//            return this;
//        }

//        public override CompressionType compressionType()
//        {
//            return columnFamilyOptions.compressionType();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setCompressionPerLevel(List<CompressionType> compressionLevels)
//        {
//            columnFamilyOptions.SetCompressionPerLevel(compressionLevels);
//            return this;
//        }

//        public override List<CompressionType> compressionPerLevel()
//        {
//            return columnFamilyOptions.compressionPerLevel();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setCompressionType(CompressionType compressionType)
//        {
//            columnFamilyOptions.SetCompressionType(compressionType);
//            return this;
//        }


//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setBottommostCompressionType(CompressionType bottommostCompressionType)
//        {
//            columnFamilyOptions.SetBottommostCompressionType(bottommostCompressionType);
//            return this;
//        }

//        public override CompressionType bottommostCompressionType()
//        {
//            return columnFamilyOptions.bottommostCompressionType();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setCompressionOptions(CompressionOptions compressionOptions)
//        {
//            columnFamilyOptions.SetCompressionOptions(compressionOptions);
//            return this;
//        }

//        public override CompressionOptions compressionOptions()
//        {
//            return columnFamilyOptions.compressionOptions();
//        }

//        public override CompactionStyle compactionStyle()
//        {
//            return columnFamilyOptions.compactionStyle();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setCompactionStyle(CompactionStyle compactionStyle)
//        {
//            columnFamilyOptions.SetCompactionStyle(compactionStyle);
//            return this;
//        }

//        public override int numLevels()
//        {
//            return columnFamilyOptions.numLevels();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setNumLevels(int numLevels)
//        {
//            columnFamilyOptions.SetNumLevels(numLevels);
//            return this;
//        }

//        public override int levelZeroFileNumCompactionTrigger()
//        {
//            return columnFamilyOptions.levelZeroFileNumCompactionTrigger();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLevelZeroFileNumCompactionTrigger(int numFiles)
//        {
//            columnFamilyOptions.SetLevelZeroFileNumCompactionTrigger(numFiles);
//            return this;
//        }

//        public override int levelZeroSlowdownWritesTrigger()
//        {
//            return columnFamilyOptions.levelZeroSlowdownWritesTrigger();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLevelZeroSlowdownWritesTrigger(int numFiles)
//        {
//            columnFamilyOptions.SetLevelZeroSlowdownWritesTrigger(numFiles);
//            return this;
//        }

//        public override int levelZeroStopWritesTrigger()
//        {
//            return columnFamilyOptions.levelZeroStopWritesTrigger();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLevelZeroStopWritesTrigger(int numFiles)
//        {
//            columnFamilyOptions.SetLevelZeroStopWritesTrigger(numFiles);
//            return this;
//        }

//        public override long targetFileSizeBase()
//        {
//            return columnFamilyOptions.targetFileSizeBase();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setTargetFileSizeBase(long targetFileSizeBase)
//        {
//            columnFamilyOptions.SetTargetFileSizeBase(targetFileSizeBase);
//            return this;
//        }

//        public override int targetFileSizeMultiplier()
//        {
//            return columnFamilyOptions.targetFileSizeMultiplier();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setTargetFileSizeMultiplier(int multiplier)
//        {
//            columnFamilyOptions.SetTargetFileSizeMultiplier(multiplier);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxBytesForLevelBase(long maxBytesForLevelBase)
//        {
//            columnFamilyOptions.SetMaxBytesForLevelBase(maxBytesForLevelBase);
//            return this;
//        }

//        public override long maxBytesForLevelBase()
//        {
//            return columnFamilyOptions.maxBytesForLevelBase();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLevelCompactionDynamicLevelBytes(bool enableLevelCompactionDynamicLevelBytes)
//        {
//            columnFamilyOptions.SetLevelCompactionDynamicLevelBytes(enableLevelCompactionDynamicLevelBytes);
//            return this;
//        }

//        public override bool levelCompactionDynamicLevelBytes()
//        {
//            return columnFamilyOptions.levelCompactionDynamicLevelBytes();
//        }

//        public override double maxBytesForLevelMultiplier()
//        {
//            return columnFamilyOptions.maxBytesForLevelMultiplier();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxBytesForLevelMultiplier(double multiplier)
//        {
//            columnFamilyOptions.SetMaxBytesForLevelMultiplier(multiplier);
//            return this;
//        }

//        public override long maxCompactionBytes()
//        {
//            return columnFamilyOptions.maxCompactionBytes();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxCompactionBytes(long maxCompactionBytes)
//        {
//            columnFamilyOptions.SetMaxCompactionBytes(maxCompactionBytes);
//            return this;
//        }

//        public override long arenaBlockSize()
//        {
//            return columnFamilyOptions.arenaBlockSize();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setArenaBlockSize(long arenaBlockSize)
//        {
//            columnFamilyOptions.SetArenaBlockSize(arenaBlockSize);
//            return this;
//        }

//        public override bool disableAutoCompactions()
//        {
//            return columnFamilyOptions.disableAutoCompactions();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setDisableAutoCompactions(bool disableAutoCompactions)
//        {
//            columnFamilyOptions.SetDisableAutoCompactions(disableAutoCompactions);
//            return this;
//        }

//        public override long maxSequentialSkipInIterations()
//        {
//            return columnFamilyOptions.maxSequentialSkipInIterations();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxSequentialSkipInIterations(long maxSequentialSkipInIterations)
//        {
//            columnFamilyOptions.SetMaxSequentialSkipInIterations(maxSequentialSkipInIterations);
//            return this;
//        }

//        public override bool inplaceUpdateSupport()
//        {
//            return columnFamilyOptions.inplaceUpdateSupport();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setInplaceUpdateSupport(bool inplaceUpdateSupport)
//        {
//            columnFamilyOptions.SetInplaceUpdateSupport(inplaceUpdateSupport);
//            return this;
//        }

//        public override long inplaceUpdateNumLocks()
//        {
//            return columnFamilyOptions.inplaceUpdateNumLocks();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setInplaceUpdateNumLocks(long inplaceUpdateNumLocks)
//        {
//            columnFamilyOptions.SetInplaceUpdateNumLocks(inplaceUpdateNumLocks);
//            return this;
//        }

//        public override double memtablePrefixBloomSizeRatio()
//        {
//            return columnFamilyOptions.memtablePrefixBloomSizeRatio();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMemtablePrefixBloomSizeRatio(double memtablePrefixBloomSizeRatio)
//        {
//            columnFamilyOptions.SetMemtablePrefixBloomSizeRatio(memtablePrefixBloomSizeRatio);
//            return this;
//        }

//        public override int bloomLocality()
//        {
//            return columnFamilyOptions.bloomLocality();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setBloomLocality(int bloomLocality)
//        {
//            columnFamilyOptions.SetBloomLocality(bloomLocality);
//            return this;
//        }

//        public override long maxSuccessiveMerges()
//        {
//            return columnFamilyOptions.maxSuccessiveMerges();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxSuccessiveMerges(long maxSuccessiveMerges)
//        {
//            columnFamilyOptions.SetMaxSuccessiveMerges(maxSuccessiveMerges);
//            return this;
//        }

//        public override int minWriteBufferNumberToMerge()
//        {
//            return columnFamilyOptions.minWriteBufferNumberToMerge();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMinWriteBufferNumberToMerge(int minWriteBufferNumberToMerge)
//        {
//            columnFamilyOptions.SetMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge);
//            return this;
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setOptimizeFiltersForHits(bool optimizeFiltersForHits)
//        {
//            columnFamilyOptions.SetOptimizeFiltersForHits(optimizeFiltersForHits);
//            return this;
//        }

//        public override bool optimizeFiltersForHits()
//        {
//            return columnFamilyOptions.optimizeFiltersForHits();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMemtableHugePageSize(long memtableHugePageSize)
//        {
//            columnFamilyOptions.SetMemtableHugePageSize(memtableHugePageSize);
//            return this;
//        }

//        public override long memtableHugePageSize()
//        {
//            return columnFamilyOptions.memtableHugePageSize();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setSoftPendingCompactionBytesLimit(long softPendingCompactionBytesLimit)
//        {
//            columnFamilyOptions.SetSoftPendingCompactionBytesLimit(softPendingCompactionBytesLimit);
//            return this;
//        }

//        public override long softPendingCompactionBytesLimit()
//        {
//            return columnFamilyOptions.softPendingCompactionBytesLimit();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setHardPendingCompactionBytesLimit(long hardPendingCompactionBytesLimit)
//        {
//            columnFamilyOptions.SetHardPendingCompactionBytesLimit(hardPendingCompactionBytesLimit);
//            return this;
//        }

//        public override long hardPendingCompactionBytesLimit()
//        {
//            return columnFamilyOptions.hardPendingCompactionBytesLimit();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLevel0FileNumCompactionTrigger(int level0FileNumCompactionTrigger)
//        {
//            columnFamilyOptions.SetLevel0FileNumCompactionTrigger(level0FileNumCompactionTrigger);
//            return this;
//        }

//        public override int level0FileNumCompactionTrigger()
//        {
//            return columnFamilyOptions.level0FileNumCompactionTrigger();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLevel0SlowdownWritesTrigger(int level0SlowdownWritesTrigger)
//        {
//            columnFamilyOptions.SetLevel0SlowdownWritesTrigger(level0SlowdownWritesTrigger);
//            return this;
//        }

//        public override int level0SlowdownWritesTrigger()
//        {
//            return columnFamilyOptions.level0SlowdownWritesTrigger();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setLevel0StopWritesTrigger(int level0StopWritesTrigger)
//        {
//            columnFamilyOptions.SetLevel0StopWritesTrigger(level0StopWritesTrigger);
//            return this;
//        }

//        public override int level0StopWritesTrigger()
//        {
//            return columnFamilyOptions.level0StopWritesTrigger();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxBytesForLevelMultiplierAdditional(int[] maxBytesForLevelMultiplierAdditional)
//        {
//            columnFamilyOptions.SetMaxBytesForLevelMultiplierAdditional(maxBytesForLevelMultiplierAdditional);
//            return this;
//        }

//        public override int[] maxBytesForLevelMultiplierAdditional()
//        {
//            return columnFamilyOptions.maxBytesForLevelMultiplierAdditional();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setParanoidFileChecks(bool paranoidFileChecks)
//        {
//            columnFamilyOptions.SetParanoidFileChecks(paranoidFileChecks);
//            return this;
//        }

//        public override bool paranoidFileChecks()
//        {
//            return columnFamilyOptions.paranoidFileChecks();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setMaxWriteBufferNumberToMaintain(int maxWriteBufferNumberToMaintain)
//        {
//            columnFamilyOptions.SetMaxWriteBufferNumberToMaintain(maxWriteBufferNumberToMaintain);
//            return this;
//        }

//        public override int maxWriteBufferNumberToMaintain()
//        {
//            return columnFamilyOptions.maxWriteBufferNumberToMaintain();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setCompactionPriority(CompactionPriority compactionPriority)
//        {
//            columnFamilyOptions.SetCompactionPriority(compactionPriority);
//            return this;
//        }

//        public override CompactionPriority compactionPriority()
//        {
//            return columnFamilyOptions.compactionPriority();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setReportBgIoStats(bool reportBgIoStats)
//        {
//            columnFamilyOptions.SetReportBgIoStats(reportBgIoStats);
//            return this;
//        }

//        public override bool reportBgIoStats()
//        {
//            return columnFamilyOptions.reportBgIoStats();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setCompactionOptionsUniversal(CompactionOptionsUniversal compactionOptionsUniversal)
//        {
//            columnFamilyOptions.SetCompactionOptionsUniversal(compactionOptionsUniversal);
//            return this;
//        }

//        public override CompactionOptionsUniversal compactionOptionsUniversal()
//        {
//            return columnFamilyOptions.compactionOptionsUniversal();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setCompactionOptionsFIFO(CompactionOptionsFIFO compactionOptionsFIFO)
//        {
//            columnFamilyOptions.SetCompactionOptionsFIFO(compactionOptionsFIFO);
//            return this;
//        }

//        public override CompactionOptionsFIFO compactionOptionsFIFO()
//        {
//            return columnFamilyOptions.compactionOptionsFIFO();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setForceConsistencyChecks(bool forceConsistencyChecks)
//        {
//            columnFamilyOptions.SetForceConsistencyChecks(forceConsistencyChecks);
//            return this;
//        }

//        public override bool forceConsistencyChecks()
//        {
//            return columnFamilyOptions.forceConsistencyChecks();
//        }

//        public RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter setWriteBufferManager(WriteBufferManager writeBufferManager)
//        {
//            dbOptions.SetWriteBufferManager(writeBufferManager);
//            return this;
//        }

//        public override WriteBufferManager writeBufferManager()
//        {
//            return dbOptions.writeBufferManager();
//        }

//        public Options setCompactionFilter(AbstractCompactionFilter<AbstractSlice<object>> compactionFilter)
//        {
//            columnFamilyOptions.SetCompactionFilter(compactionFilter);
//            return this;
//        }

//        public DbOptions setCompactionFilterFactory(AbstractCompactionFilterFactory<AbstractCompactionFilter<object>> compactionFilterFactory)
//        {
//            columnFamilyOptions.SetCompactionFilterFactory(compactionFilterFactory);

//            return this;
//        }

//        public override void close()
//        {
//            // ColumnFamilyOptions should be closed last
//            dbOptions.close();
//            columnFamilyOptions.close();
//        }
//    }
//}