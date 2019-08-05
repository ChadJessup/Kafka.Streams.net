using Kafka.Common.Utils;
using Kafka.streams.state;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{
    /**
     * A persistent key-value store based on RocksDB.
     */
    public class RocksDBStore : IKeyValueStore<Bytes, byte[]>, IBulkLoadingStore
    {
        private static ILogger log = new LoggerFactory().CreateLogger<RocksDBStore>();

        private static Pattern SST_FILE_EXTENSION = Pattern.compile(".*\\.sst");

        private static CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
        private static CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
        private static long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
        private static long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
        private static long BLOCK_SIZE = 4096L;
        private static int MAX_WRITE_BUFFERS = 3;
        private static string DB_FILE_DIR = "rocksdb";

        string name;
        private string parentDir;
        HashSet<KeyValueIterator<Bytes, byte[]>> openIterators = Collections.synchronizedSet(new HashSet<KeyValueIterator<Bytes, byte[]>>());

        FileInfo dbDir;
        RocksDb db;
        IRocksDBAccessor dbAccessor;

        // the following option objects will be created in openDB and closed in the close() method
        private RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
        WriteOptions wOptions;
        FlushOptions fOptions;
        private Cache cache;
        private BloomFilter filter;

        private RocksDBConfigSetter configSetter;

        private bool prepareForBulkload = false;
        IProcessorContext internalProcessorContext;
        // visible for testing
        BatchingStateRestoreCallback batchingStateRestoreCallback = null;

        protected volatile bool open = false;

        RocksDBStore(string name)
            : this(name, DB_FILE_DIR)
        {
        }

        RocksDBStore(string name,
                     string parentDir)
        {
            this.name = name;
            this.parentDir = parentDir;
        }



        void openDB(IProcessorContext context)
        {
            // initialize the default rocksdb options

            DBOptions dbOptions = new DBOptions();
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
            userSpecifiedOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, columnFamilyOptions);

            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            cache = new LRUCache(BLOCK_CACHE_SIZE);
            tableConfig.setBlockCache(cache);
            tableConfig.setBlockSize(BLOCK_SIZE);

            filter = new BloomFilter();
            tableConfig.setFilter(filter);

            userSpecifiedOptions.optimizeFiltersForHits();
            userSpecifiedOptions.setTableFormatConfig(tableConfig);
            userSpecifiedOptions.setWriteBufferSize(WRITE_BUFFER_SIZE);
            userSpecifiedOptions.setCompressionType(COMPRESSION_TYPE);
            userSpecifiedOptions.setCompactionStyle(COMPACTION_STYLE);
            userSpecifiedOptions.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
            userSpecifiedOptions.setCreateIfMissing(true);
            userSpecifiedOptions.setErrorIfExists(false);
            userSpecifiedOptions.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
            // this is the recommended way to increase parallelism in RocksDb
            // note that the current implementation of setIncreaseParallelism affects the number
            // of compaction threads but not flush threads (the latter remains one). Also
            // the parallelism value needs to be at least two because of the code in
            // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
            // subtracts one from the value passed to determine the number of compaction threads
            // (this could be a bug in the RocksDB code and their devs have been contacted).
            userSpecifiedOptions.setIncreaseParallelism(Math.Max(Runtime.getRuntime().availableProcessors(), 2));

            wOptions = new WriteOptions();
            wOptions.setDisableWAL(true);

            fOptions = new FlushOptions();
            fOptions.setWaitForFlush(true);

            Dictionary<string, object> configs = context.appConfigs();
            Class<RocksDBConfigSetter> configSetterClass =
                (Class<RocksDBConfigSetter>)configs[StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);

            if (configSetterClass != null)
            {
                configSetter = Utils.newInstance(configSetterClass);
                configSetter.setConfig(name, userSpecifiedOptions, configs);
            }

            if (prepareForBulkload)
            {
                userSpecifiedOptions.prepareForBulkLoad();
            }

            dbDir = new File(new File(context.stateDir(), parentDir), name);

            try
            {
                Files.createDirectories(dbDir.getParentFile().toPath());
                Files.createDirectories(dbDir.getAbsoluteFile().toPath());
            }
            catch (IOException fatal)
            {
                throw new ProcessorStateException(fatal);
            }

            openRocksDB(dbOptions, columnFamilyOptions);
            open = true;
        }

        void openRocksDB(DBOptions dbOptions,
                         ColumnFamilyOptions columnFamilyOptions)
        {
            List<ColumnFamilyDescriptor> columnFamilyDescriptors
                = Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
            List<ColumnFamilyHandle> columnFamilies = new List<>(columnFamilyDescriptors.size());

            try
            {
                db = RocksDB.open(dbOptions, dbDir.FullName, columnFamilyDescriptors, columnFamilies);
                dbAccessor = new SingleColumnFamilyAccessor(columnFamilies[0));
            }
            catch (RocksDBException e)
            {
                throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.ToString(), e);
            }
        }

        public void init(IProcessorContext context,
                         IStateStore root)
        {
            // open the DB dir
            internalProcessorContext = context;
            openDB(context);
            batchingStateRestoreCallback = new RocksDBBatchingRestoreCallback(this);

            // value getter should always read directly from rocksDB
            // since it is only for values that are already flushed
            context.register(root, batchingStateRestoreCallback);
        }

        // visible for testing
        bool isPrepareForBulkload()
        {
            return prepareForBulkload;
        }

        public override string name()
        {
            return name;
        }

        public override bool persistent()
        {
            return true;
        }

        public override bool isOpen()
        {
            return open;
        }

        private void validateStoreOpen()
        {
            if (!open)
            {
                throw new InvalidStateStoreException("Store " + name + " is currently closed");
            }
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void put(Bytes key,
                                     byte[] value)
        {
            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
            validateStoreOpen();
            dbAccessor.Add(key(), value);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override byte[] putIfAbsent(Bytes key,
                                               byte[] value)
        {
            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
            byte[] originalValue = get(key);
            if (originalValue == null)
            {
                put(key, value);
            }
            return originalValue;
        }

        public override void putAll(List<KeyValue<Bytes, byte[]>> entries)
        {
            try (WriteBatch batch = new WriteBatch())
{
                dbAccessor.prepareBatch(entries, batch);
                write(batch);
            } catch (RocksDBException e)
            {
                throw new ProcessorStateException("Error while batch writing to store " + name, e);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override byte[] get(Bytes key)
        {
            validateStoreOpen();
            try
            {
                return dbAccessor[key());
            }
            catch (RocksDBException e)
            {
                // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while getting value for key from store " + name, e);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override byte[] delete(Bytes key)
        {
            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
            byte[] oldValue;
            try
            {
                oldValue = dbAccessor.getOnly(key());
            }
            catch (RocksDBException e)
            {
                // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while getting value for key from store " + name, e);
            }
            put(key, null);
            return oldValue;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                                  Bytes to)
        {
            from = from ?? throw new System.ArgumentNullException("from cannot be null", nameof(from));
            to = to ?? throw new System.ArgumentNullException("to cannot be null", nameof(to));

            if (from.compareTo(to) > 0)
            {
                log.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
                return KeyValueIterators.emptyIterator();
            }

            validateStoreOpen();

            KeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = dbAccessor.range(from, to);
            openIterators.Add(rocksDBRangeIterator);

            return rocksDBRangeIterator;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override KeyValueIterator<Bytes, byte[]> all()
        {
            validateStoreOpen();
            KeyValueIterator<Bytes, byte[]> rocksDbIterator = dbAccessor.all();
            openIterators.Add(rocksDbIterator);
            return rocksDbIterator;
        }

        /**
         * Return an approximate count of key-value mappings in this store.
         *
         * <code>RocksDB</code> cannot return an exact entry count without doing a
         * full scan, so this method relies on the <code>rocksdb.estimate-num-keys</code>
         * property to get an approximate count. The returned size also includes
         * a count of dirty keys in the store's in-memory cache, which may lead to some
         * double-counting of entries and inflate the estimate.
         *
         * @return an approximate count of key-value mappings in the store.
         */
        public override long approximateNumEntries()
        {
            validateStoreOpen();
            long numEntries;
            try
            {
                numEntries = dbAccessor.approximateNumEntries();
            }
            catch (RocksDBException e)
            {
                throw new ProcessorStateException("Error fetching property from store " + name, e);
            }
            if (isOverflowing(numEntries))
            {
                return long.MaxValue;
            }
            return numEntries;
        }

        private bool isOverflowing(long value)
        {
            // RocksDB returns an unsigned 8-byte integer, which could overflow long
            // and manifest as a negative value.
            return value < 0;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void flush()
        {
            if (db == null)
            {
                return;
            }
            try
            {
                dbAccessor.flush();
            }
            catch (RocksDBException e)
            {
                throw new ProcessorStateException("Error while executing flush from store " + name, e);
            }
        }

        public override void toggleDbForBulkLoading(bool prepareForBulkload)
        {
            if (prepareForBulkload)
            {
                // if the store is not empty, we need to compact to get around the num.levels check for bulk loading
                string[] sstFileNames = dbDir.list((dir, name)->SST_FILE_EXTENSION.matcher(name).matches());

                if (sstFileNames != null && sstFileNames.Length > 0)
                {
                    dbAccessor.toggleDbForBulkLoading();
                }
            }

            close();
            this.prepareForBulkload = prepareForBulkload;
            openDB(internalProcessorContext);
        }

        public override void addToBatch(KeyValue<byte[], byte[]> record,
                               WriteBatch batch)
        {
            dbAccessor.AddToBatch(record.key, record.value, batch);
        }

        public override void write(WriteBatch batch)
        {
            db.write(wOptions, batch);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void close()
        {
            if (!open)
            {
                return;
            }

            open = false;
            closeOpenIterators();

            if (configSetter != null)
            {
                configSetter.close(name, userSpecifiedOptions);
                configSetter = null;
            }

            // Important: do not rearrange the order in which the below objects are closed!
            // Order of closing must follow: ColumnFamilyHandle > RocksDB > DBOptions > ColumnFamilyOptions
            dbAccessor.close();
            db.close();
            userSpecifiedOptions.close();
            wOptions.close();
            fOptions.close();
            filter.close();
            cache.close();

            dbAccessor = null;
            userSpecifiedOptions = null;
            wOptions = null;
            fOptions = null;
            db = null;
            filter = null;
            cache = null;
        }

        private void closeOpenIterators()
        {
            HashSet<KeyValueIterator<Bytes, byte[]>> iterators;
            synchronized(openIterators)
    {
                iterators = new HashSet<>(openIterators);
            }
            if (iterators.size() != 0)
            {
                log.LogWarning("Closing {} open iterators for store {}", iterators.size(), name);
                foreach (KeyValueIterator<Bytes, byte[]> iterator in iterators)
                {
                    iterator.close();
                }
            }
        }
    }

    class SingleColumnFamilyAccessor : IRocksDBAccessor
    {
        private ColumnFamilyHandle columnFamily;

        SingleColumnFamilyAccessor(ColumnFamilyHandle columnFamily)
        {
            this.columnFamily = columnFamily;
        }


        public void put(byte[] key,
                        byte[] value)
        {
            if (value == null)
            {
                try
                {
                    db.delete(columnFamily, wOptions, key);
                }
                catch (RocksDBException e)
                {
                    // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }
            }
            else
            {
                try
                {
                    db.Add(columnFamily, wOptions, key, value);
                }
                catch (RocksDBException e)
                {
                    // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                }
            }
        }


        public void prepareBatch(List<KeyValue<Bytes, byte[]>> entries,
                                 WriteBatch batch)
        {
            foreach (KeyValue<Bytes, byte[]> entry in entries)
            {
                entry.key = entry.key ?? throw new System.ArgumentNullException("key cannot be null", nameof(entry.key));
               .AddToBatch(entry.key(), entry.value, batch);
            }
        }


        public byte[] get(byte[] key)
        {
            return db[columnFamily, key];
        }


        public byte[] getOnly(byte[] key)
        {
            return db[columnFamily, key];
        }


        public KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                     Bytes to)
        {
            return new RocksDBRangeIterator(
                name,
                db.newIterator(columnFamily),
                openIterators,
                from,
                to);
        }


        public KeyValueIterator<Bytes, byte[]> all()
        {
            RocksIterator innerIterWithTimestamp = db.newIterator(columnFamily);
            innerIterWithTimestamp.seekToFirst();
            return new RocksDbIterator(name, innerIterWithTimestamp, openIterators);
        }


        public long approximateNumEntries()
        {
            return db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
        }


        public void flush()
        {
            db.flush(fOptions, columnFamily);
        }


        public void prepareBatchForRestore(Collection<KeyValue<byte[], byte[]>> records,
                                           WriteBatch batch)
        {
            foreach (KeyValue<byte[], byte[]> record in records)
            {
               .AddToBatch(record.key, record.value, batch);
            }
        }


        public void addToBatch(byte[] key,
                               byte[] value,
                               WriteBatch batch)
        {
            if (value == null)
            {
                batch.delete(columnFamily, key);
            }
            else
            {
                batch.Add(columnFamily, key, value);
            }
        }


        public void close()
        {
            columnFamily.close();
        }



        public void toggleDbForBulkLoading()
        {
            try
            {
                db.compactRange(columnFamily, true, 1, 0);
            }
            catch (RocksDBException e)
            {
                throw new ProcessorStateException("Error while range compacting during restoring  store " + name, e);
            }
        }
    }

    // not private for testing
    static class RocksDBBatchingRestoreCallback : AbstractNotifyingBatchingRestoreCallback
    {

        private RocksDBStore rocksDBStore;

        RocksDBBatchingRestoreCallback(RocksDBStore rocksDBStore)
        {
            this.rocksDBStore = rocksDBStore;
        }


        public void restoreAll(Collection<KeyValue<byte[], byte[]>> records)
        {
            try (WriteBatch batch = new WriteBatch())
{
                rocksDBStore.dbAccessor.prepareBatchForRestore(records, batch);
                rocksDBStore.write(batch);
            } catch (RocksDBException e)
            {
                throw new ProcessorStateException("Error restoring batch to store " + rocksDBStore.name, e);
            }
        }


        public void onRestoreStart(TopicPartition topicPartition,
                                   string storeName,
                                   long startingOffset,
                                   long endingOffset)
        {
            rocksDBStore.toggleDbForBulkLoading(true);
        }


        public void onRestoreEnd(TopicPartition topicPartition,
                                 string storeName,
                                 long totalRestored)
        {
            rocksDBStore.toggleDbForBulkLoading(false);
        }

        // for testing
        public Options getOptions()
        {
            return userSpecifiedOptions;
        }
    }
}