using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace Kafka.Streams.State.RocksDbState
{
    /**
     * A persistent key-value store based on RocksDb.
     */
    public class RocksDbStore : IKeyValueStore<Bytes, byte[]>, IBulkLoadingStore
    {
        private static readonly ILogger log = new LoggerFactory().CreateLogger<RocksDbStore>();

        private static readonly Regex SST_FILE_EXTENSION = new Regex(".*\\.sst", RegexOptions.Compiled);

        private const Compression COMPRESSION_TYPE = Compression.No;
        private const Compaction COMPACTION_STYLE = Compaction.Universal;

        private const ulong WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
        private const ulong BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
        private const ulong BLOCK_SIZE = 4096L;
        private const int MAX_WRITE_BUFFERS = 3;
        private const string DB_FILE_DIR = "rocksdb";

        public string name { get; }
        private readonly string parentDir;
        protected HashSet<IKeyValueIterator<Bytes, byte[]>> OpenIterators { get; } = new HashSet<IKeyValueIterator<Bytes, byte[]>>();

        protected DirectoryInfo DbDir { get; private set; }
        public RocksDb Db { get; protected set; }
        public IRocksDbAccessor DbAccessor { get; protected set; }

        // the following option objects will be created in openDB and closed in the close() method
        //private RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
        protected WriteOptions WOptions { get; set; }

        // FlushOptions fOptions;
        private Cache cache;

        private BloomFilterPolicy filter;

        private IRocksDbConfigSetter configSetter;

        private bool prepareForBulkload = false;
        public IProcessorContext InternalProcessorContext { get; private set; }

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

        public virtual void OpenDB(IProcessorContext context)
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

            WOptions = new WriteOptions();
            WOptions.DisableWal(1);

            //fOptions = new FlushOptions();
            //fOptions.setWaitForFlush(true);

            Dictionary<string, object> configs = context.AppConfigs();
            var configSetterClass =
                (IRocksDbConfigSetter)configs[StreamsConfigPropertyNames.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG];

            if (configSetterClass != null)
            {
                configSetter = (IRocksDbConfigSetter)Activator.CreateInstance(configSetterClass.GetType());
                configSetter.SetConfig(name, dbOptions, configs);
            }

            if (prepareForBulkload)
            {
                //userSpecifiedOptions.prepareForBulkLoad();
            }

            DbDir = new DirectoryInfo(Path.Combine(Path.Combine(context.stateDir.FullName, parentDir), name));

            try
            {
                Directory.CreateDirectory(DbDir.FullName);
            }
            catch (IOException fatal)
            {
                throw new ProcessorStateException(fatal.ToString());
            }

            OpenRocksDb(dbOptions, columnFamilyOptions);
            open = true;
        }

        void OpenRocksDb(
            DbOptions dbOptions,
            ColumnFamilyOptions columnFamilyOptions)
        {
            var columnFamilyDescriptors = new ColumnFamilies(columnFamilyOptions);

            var columnFamilies = new List<ColumnFamilyHandle>(columnFamilyDescriptors.Count());

            try
            {
                Db = RocksDb.Open(
                    dbOptions,
                    DbDir.FullName,
                    columnFamilyDescriptors);

                DbAccessor = new SingleColumnFamilyAccessor(
                    name,
                    Db,
                    WOptions,
                    OpenIterators,
                    columnFamilies[0]);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error opening store " + name + " at location " + DbDir.ToString(), e);
            }
        }

        public void Init(IProcessorContext context, IStateStore root)
        {
            // open the DB dir
            InternalProcessorContext = context;
            OpenDB(context);

            batchingStateRestoreCallback = new RocksDbBatchingRestoreCallback(this);

            // value getter should always read directly from rocksDB
            // since it is only for values that are already flushed
            context.Register(root, batchingStateRestoreCallback);
        }

        public bool Persistent()
        {
            return true;
        }

        public bool IsOpen()
        {
            return open;
        }

        private void ValidateStoreOpen()
        {
            if (!open)
            {
                throw new InvalidStateStoreException("Store " + name + " is currently closed");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Put(Bytes key, byte[] value)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            ValidateStoreOpen();
            DbAccessor.Put(key.Get(), value);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            var originalValue = Get(key);

            if (originalValue == null)
            {
                Put(key, value);
            }

            return originalValue;
        }

        public void PutAll(List<KeyValuePair<Bytes, byte[]>> entries)
        {
            try
            {
                using var batch = new WriteBatch();

                DbAccessor.PrepareBatch(entries, batch);
                Write(batch);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while batch writing to store " + name, e);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] Get(Bytes key)
        {
            ValidateStoreOpen();
            try
            {
                return DbAccessor.Get(key.Get());
            }
            catch (RocksDbException e)
            {
                // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while getting value for key from store " + name, e);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] Delete(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            byte[] oldValue;
            try
            {
                oldValue = DbAccessor.GetOnly(key.Get());
            }
            catch (RocksDbException e)
            {
                // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while getting value for key from store " + name, e);
            }

            Put(key, null);
            return oldValue;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IKeyValueIterator<Bytes, byte[]> Range(Bytes from, Bytes to)
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

            ValidateStoreOpen();

            IKeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = DbAccessor.Range(from, to);
            OpenIterators.Add(rocksDBRangeIterator);

            return rocksDBRangeIterator;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IKeyValueIterator<Bytes, byte[]> All()
        {
            ValidateStoreOpen();
            IKeyValueIterator<Bytes, byte[]> rocksDbIterator = DbAccessor.All();
            OpenIterators.Add(rocksDbIterator);
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
        public long approximateNumEntries
        {
            get
            {
                ValidateStoreOpen();
                long numEntries;
                try
                {
                    numEntries = DbAccessor.ApproximateNumEntries();
                }
                catch (RocksDbException e)
                {
                    throw new ProcessorStateException("Error fetching property from store " + name, e);
                }
                
                if (IsOverflowing(numEntries))
                {
                    return long.MaxValue;
                }
             
                return numEntries;
            }
        }

        private bool IsOverflowing(long value)
        {
            // RocksDb returns an unsigned 8-byte integer, which could overflow long
            // and manifest as a negative value.
            return value < 0;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Flush()
        {
            if (Db == null)
            {
                return;
            }
            try
            {
                DbAccessor.Flush();
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while executing flush from store " + name, e);
            }
        }

        public void ToggleDbForBulkLoading(bool prepareForBulkload)
        {
            if (prepareForBulkload)
            {
                // if the store is not empty, we need to compact to get around the num.levels check for bulk loading
                var sstFileNames = DbDir
                    .GetFiles()
                    .Where(dir => SST_FILE_EXTENSION.IsMatch(dir.FullName))
                    .Select(fi => fi.FullName).ToArray();

                if (sstFileNames != null && sstFileNames.Length > 0)
                {
                    DbAccessor.ToggleDbForBulkLoading();
                }
            }

            Close();
            this.prepareForBulkload = prepareForBulkload;
            OpenDB(InternalProcessorContext);
        }

        public void AddToBatch(KeyValuePair<byte[], byte[]> record,
                               WriteBatch batch)
        {
            DbAccessor.AddToBatch(record.Key, record.Value, batch);
        }

        public void Write(WriteBatch batch)
        {
            Db.Write(batch, WOptions);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Close()
        {
            if (!open)
            {
                return;
            }

            open = false;
            CloseOpenIterators();

            if (configSetter != null)
            {
                configSetter.Close(name, dbOptions);
                configSetter = null;
            }

            // Important: do not rearrange the order in which the below objects are closed!
            // Order of closing must follow: ColumnFamilyHandle > RocksDb > DBOptions > ColumnFamilyOptions
            DbAccessor.Close();
            //db.close();
            //wOptions.close();
            //fOptions.close();
            //filter.close();
            //cache.close();

            DbAccessor = null;
            WOptions = null;
            //fOptions = null;
            Db = null;
            filter = null;
            cache = null;
        }

        private void CloseOpenIterators()
        {
            HashSet<IKeyValueIterator<Bytes, byte[]>> iterators;
            lock (OpenIterators)
            {
                iterators = new HashSet<IKeyValueIterator<Bytes, byte[]>>(OpenIterators);
            }

            if (iterators.Count != 0)
            {
                log.LogWarning("Closing {} open iterators for store {}", iterators.Count, name);

                foreach (IKeyValueIterator<Bytes, byte[]> iterator in iterators)
                {
                    iterator.Close();
                }
            }
        }

        public void Add<K, V>(K key, V value)
        {
            throw new NotImplementedException();
        }

        public bool IsPresent()
        {
            throw new NotImplementedException();
        }

        public void Add(Bytes key, byte[] value)
        {
            throw new NotImplementedException();
        }
    }
}