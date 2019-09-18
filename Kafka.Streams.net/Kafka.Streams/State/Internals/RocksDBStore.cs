using Kafka.Common.Utils;
using Kafka.Streams.Errors;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace Kafka.Streams.State.Internals
{
    /**
     * A persistent key-value store based on RocksDb.
     */
    public class RocksDbStore : IKeyValueStore<Bytes, byte[]>, IBulkLoadingStore
    {
        private static readonly ILogger log = new LoggerFactory().CreateLogger<RocksDbStore>();

        private static readonly Regex SST_FILE_EXTENSION = new Regex(".*\\.sst", RegexOptions.Compiled);

        private static readonly Compression COMPRESSION_TYPE = Compression.No;
        private static readonly Compaction COMPACTION_STYLE = Compaction.Universal;

        private static readonly ulong WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
        private static readonly ulong BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
        private static readonly ulong BLOCK_SIZE = 4096L;
        private static readonly int MAX_WRITE_BUFFERS = 3;
        private static readonly string DB_FILE_DIR = "rocksdb";

        public string name { get; }
        private readonly string parentDir;
        protected HashSet<IKeyValueIterator<Bytes, byte[]>> openIterators { get; } = new HashSet<IKeyValueIterator<Bytes, byte[]>>();

        protected DirectoryInfo dbDir { get; private set; }
        public RocksDb db { get; protected set; }
        public IRocksDbAccessor dbAccessor { get; protected set; }

        // the following option objects will be created in openDB and closed in the close() method
        //private RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
        protected WriteOptions wOptions { get; set; }

        // FlushOptions fOptions;
        private Cache cache;

        private BloomFilterPolicy filter;

        private IRocksDbConfigSetter configSetter;

        private bool prepareForBulkload = false;
        public IProcessorContext internalProcessorContext { get; private set; }

        // visible for testing
        IBatchingStateRestoreCallback batchingStateRestoreCallback = null;
        private readonly DbOptions dbOptions = new DbOptions();
        private readonly ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        private readonly BlockBasedTableOptions tableConfig = new BlockBasedTableOptions();

        protected volatile bool open = false;

        public RocksDbStore(string name)
            : this(name, DB_FILE_DIR)
        {
        }

        public RocksDbStore(string name, string parentDir)
        {
            this.name = name;
            this.parentDir = parentDir;
        }

        void openDB(IProcessorContext context)
        {
            // initialize the default rocksdb options

            //  userSpecifiedOptions = new RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, columnFamilyOptions);

            tableConfig.SetBlockCache(Cache.CreateLru(BLOCK_CACHE_SIZE));
            tableConfig.SetBlockSize(BLOCK_SIZE);

            tableConfig.SetFilterPolicy(BloomFilterPolicy.Create());

            columnFamilyOptions.SetOptimizeFiltersForHits(1);
            columnFamilyOptions.SetBlockBasedTableFactory(tableConfig);
            dbOptions.SetWriteBufferSize(WRITE_BUFFER_SIZE);
            dbOptions.SetCompression(COMPRESSION_TYPE);
            dbOptions.SetCompactionStyle(COMPACTION_STYLE);
            dbOptions.SetMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
            dbOptions.SetCreateIfMissing(true);
            dbOptions.SetErrorIfExists(false);
            dbOptions.SetInfoLogLevel((int)LogLevel.Error);
            // this is the recommended way to increase parallelism in RocksDb
            // note that the current implementation of setIncreaseParallelism affects the number
            // of compaction threads but not flush threads (the latter remains one). Also
            // the parallelism value needs to be at least two because of the code in
            // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
            // subtracts one from the value passed to determine the number of compaction threads
            // (this could be a bug in the RocksDb code and their devs have been contacted).
            dbOptions.IncreaseParallelism(Math.Max(Environment.ProcessorCount, 2));

            wOptions = new WriteOptions();
            wOptions.DisableWal(1);

            //fOptions = new FlushOptions();
            //fOptions.setWaitForFlush(true);

            Dictionary<string, object> configs = context.appConfigs();
            IRocksDbConfigSetter configSetterClass =
                (IRocksDbConfigSetter)configs[StreamsConfigPropertyNames.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG];

            if (configSetterClass != null)
            {
                configSetter = (IRocksDbConfigSetter)Activator.CreateInstance(configSetterClass.GetType());
                configSetter.setConfig(name, dbOptions, configs);
            }

            if (prepareForBulkload)
            {
                //userSpecifiedOptions.prepareForBulkLoad();
            }

            dbDir = new DirectoryInfo(Path.Combine(Path.Combine(context.stateDir.FullName, parentDir), name));

            try
            {
                Directory.CreateDirectory(dbDir.FullName);
            }
            catch (IOException fatal)
            {
                throw new ProcessorStateException(fatal.ToString());
            }

            openRocksDb(dbOptions, columnFamilyOptions);
            open = true;
        }

        void openRocksDb(
            DbOptions dbOptions,
            ColumnFamilyOptions columnFamilyOptions)
        {
            ColumnFamilies columnFamilyDescriptors = new ColumnFamilies(columnFamilyOptions);

            List<ColumnFamilyHandle> columnFamilies = new List<ColumnFamilyHandle>(columnFamilyDescriptors.Count());

            try
            {
                db = RocksDb.Open(
                    dbOptions,
                    dbDir.FullName,
                    columnFamilyDescriptors);

                dbAccessor = new SingleColumnFamilyAccessor(
                    name,
                    db,
                    wOptions,
                    openIterators,
                    columnFamilies[0]);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir.ToString(), e);
            }
        }

        public void init(IProcessorContext context, IStateStore root)
        {
            // open the DB dir
            internalProcessorContext = context;
            openDB(context);

            batchingStateRestoreCallback = new RocksDbBatchingRestoreCallback(this);

            // value getter should always read directly from rocksDB
            // since it is only for values that are already flushed
            context.register(root, batchingStateRestoreCallback);
        }

        // visible for testing
        bool isPrepareForBulkload()
        {
            return prepareForBulkload;
        }

        public bool persistent()
        {
            return true;
        }

        public bool isOpen()
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
        public void put(Bytes key, byte[] value)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            validateStoreOpen();
            dbAccessor.put(key.get(), value);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] putIfAbsent(Bytes key, byte[] value)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            byte[] originalValue = get(key);

            if (originalValue == null)
            {
                put(key, value);
            }

            return originalValue;
        }

        public void putAll(List<KeyValue<Bytes, byte[]>> entries)
        {
            try
            {
                using (WriteBatch batch = new WriteBatch())
                {
                    dbAccessor.prepareBatch(entries, batch);
                    write(batch);
                }
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while batch writing to store " + name, e);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] get(Bytes key)
        {
            validateStoreOpen();
            try
            {
                return dbAccessor.get(key.get());
            }
            catch (RocksDbException e)
            {
                // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while getting value for key from store " + name, e);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] delete(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            byte[] oldValue;
            try
            {
                oldValue = dbAccessor.getOnly(key.get());
            }
            catch (RocksDbException e)
            {
                // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while getting value for key from store " + name, e);
            }

            put(key, null);
            return oldValue;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IKeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to)
        {
            from = from ?? throw new ArgumentNullException(nameof(from));
            to = to ?? throw new ArgumentNullException(nameof(to));

            if (from.CompareTo(to) > 0)
            {
                log.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");

                return null; // KeyValueIterators.emptyIterator();
            }

            validateStoreOpen();

            IKeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = dbAccessor.range(from, to);
            openIterators.Add(rocksDBRangeIterator);

            return rocksDBRangeIterator;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IKeyValueIterator<Bytes, byte[]> all()
        {
            validateStoreOpen();
            IKeyValueIterator<Bytes, byte[]> rocksDbIterator = dbAccessor.all();
            openIterators.Add(rocksDbIterator);
            return rocksDbIterator;
        }

        /**
         * Return an approximate count of key-value mappings in this store.
         *
         * <code>RocksDb</code> cannot return an exact entry count without doing a
         * full scan, so this method relies on the <code>rocksdb.estimate-num-keys</code>
         * property to get an approximate count. The returned size also includes
         * a count of dirty keys in the store's in-memory cache, which may lead to some
         * double-counting of entries and inflate the estimate.
         *
         * @return an approximate count of key-value mappings in the store.
         */
        public long approximateNumEntries()
        {
            validateStoreOpen();
            long numEntries;
            try
            {
                numEntries = dbAccessor.approximateNumEntries();
            }
            catch (RocksDbException e)
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
            // RocksDb returns an unsigned 8-byte integer, which could overflow long
            // and manifest as a negative value.
            return value < 0;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void flush()
        {
            if (db == null)
            {
                return;
            }
            try
            {
                dbAccessor.flush();
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while executing flush from store " + name, e);
            }
        }

        public void toggleDbForBulkLoading(bool prepareForBulkload)
        {
            if (prepareForBulkload)
            {
                // if the store is not empty, we need to compact to get around the num.levels check for bulk loading
                string[] sstFileNames = dbDir
                    .GetFiles()
                    .Where(dir => SST_FILE_EXTENSION.IsMatch(dir.FullName))
                    .Select(fi => fi.FullName).ToArray();

                if (sstFileNames != null && sstFileNames.Length > 0)
                {
                    dbAccessor.toggleDbForBulkLoading();
                }
            }

            close();
            this.prepareForBulkload = prepareForBulkload;
            openDB(internalProcessorContext);
        }

        public void addToBatch(KeyValue<byte[], byte[]> record,
                               WriteBatch batch)
        {
            dbAccessor.addToBatch(record.key, record.value, batch);
        }

        public void write(WriteBatch batch)
        {
            db.Write(batch, wOptions);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void close()
        {
            if (!open)
            {
                return;
            }

            open = false;
            closeOpenIterators();

            if (configSetter != null)
            {
                configSetter.close(name, dbOptions);
                configSetter = null;
            }

            // Important: do not rearrange the order in which the below objects are closed!
            // Order of closing must follow: ColumnFamilyHandle > RocksDb > DBOptions > ColumnFamilyOptions
            dbAccessor.close();
            //db.close();
            //wOptions.close();
            //fOptions.close();
            //filter.close();
            //cache.close();

            dbAccessor = null;
            wOptions = null;
            //fOptions = null;
            db = null;
            filter = null;
            cache = null;
        }

        private void closeOpenIterators()
        {
            HashSet<IKeyValueIterator<Bytes, byte[]>> iterators;
            lock (openIterators)
            {
                iterators = new HashSet<IKeyValueIterator<Bytes, byte[]>>(openIterators);
            }

            if (iterators.Count != 0)
            {
                log.LogWarning("Closing {} open iterators for store {}", iterators.Count, name);

                foreach (IKeyValueIterator<Bytes, byte[]> iterator in iterators)
                {
                    iterator.close();
                }
            }
        }

        public void Add<K, V>(K key, V value)
        {
            throw new NotImplementedException();
        }

        public bool isPresent()
        {
            throw new NotImplementedException();
        }

        public void Add(Bytes key, byte[] value)
        {
            throw new NotImplementedException();
        }
    }
}