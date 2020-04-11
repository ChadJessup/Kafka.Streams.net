using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
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

namespace Kafka.Streams.RocksDbState
{
    /**
     * A Persistent key-value store based on RocksDb.
     */
    public class RocksDbStore : IKeyValueStore<Bytes, byte[]>, IBulkLoadingStore, IDisposable
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

        public string Name { get; }
        private readonly string parentDir;
        protected HashSet<IKeyValueIterator<Bytes, byte[]>> OpenIterators { get; } = new HashSet<IKeyValueIterator<Bytes, byte[]>>();

        protected DirectoryInfo DbDir { get; private set; }
        public RocksDb Db { get; protected set; }
        public IRocksDbAccessor DbAccessor { get; protected set; }

        // the following option objects will be created in openDB and closed in the Close() method
        //private RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
        protected WriteOptions WOptions { get; set; }

        // FlushOptions fOptions;
        private Cache cache;

        private BloomFilterPolicy filter;

        private IRocksDbConfigSetter configSetter;

        private bool prepareForBulkload = false;
        public IProcessorContext InternalProcessorContext { get; set; }

        // visible for testing
        IBatchingStateRestoreCallback batchingStateRestoreCallback = null;
        private readonly DbOptions dbOptions = new DbOptions();
        private readonly ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        private readonly BlockBasedTableOptions tableConfig = new BlockBasedTableOptions();

        protected volatile bool open = false;

        public RocksDbStore(string Name)
            : this(Name, DB_FILE_DIR)
        {
        }

        public RocksDbStore(string Name, string parentDir)
        {
            this.Name = Name;
            this.parentDir = parentDir;
        }

        public virtual IDisposable OpenDB(IProcessorContext context)
        {
            // initialize the default rocksdb options

            //  userSpecifiedOptions = new RocksDbGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, columnFamilyOptions);

            this.tableConfig.SetBlockCache(Cache.CreateLru(BLOCK_CACHE_SIZE));
            this.tableConfig.SetBlockSize(BLOCK_SIZE);

            this.tableConfig.SetFilterPolicy(BloomFilterPolicy.Create());

            this.columnFamilyOptions.SetOptimizeFiltersForHits(1);
            this.columnFamilyOptions.SetBlockBasedTableFactory(this.tableConfig);
            this.dbOptions.SetWriteBufferSize(WRITE_BUFFER_SIZE);
            this.dbOptions.SetCompression(COMPRESSION_TYPE);
            this.dbOptions.SetCompactionStyle(COMPACTION_STYLE);
            this.dbOptions.SetMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
            this.dbOptions.SetCreateIfMissing(true);
            this.dbOptions.SetErrorIfExists(false);
            this.dbOptions.SetInfoLogLevel((int)LogLevel.Error);
            // this is the recommended way to increase parallelism in RocksDb
            // note that the current implementation of setIncreaseParallelism affects the number
            // of compaction threads but not Flush threads (the latter remains one). Also
            // the parallelism value needs to be at least two because of the code in
            // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
            // subtracts one from the value passed to determine the number of compaction threads
            // (this could be a bug in the RocksDb code and their devs have been contacted).
            this.dbOptions.IncreaseParallelism(Math.Max(Environment.ProcessorCount, 2));

            this.WOptions = new WriteOptions();
            this.WOptions.DisableWal(1);
            
            // fOptions = new FlushOptions();
            // fOptions.setWaitForFlush(true);

            Dictionary<string, object> configs = context.AppConfigs();


            if (configs.TryGetValue(StreamsConfigPropertyNames.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, out var classString))
            {
                var configSetterClass = Type.GetType(classString.ToString());
                if (configSetterClass != null)
                {
                    this.configSetter = (IRocksDbConfigSetter)Activator.CreateInstance(configSetterClass.GetType());
                    this.configSetter.SetConfig(this.Name, this.dbOptions, configs);
                }
            }

            if (this.prepareForBulkload)
            {
                // userSpecifiedOptions.prepareForBulkLoad();
            }

            this.DbDir = new DirectoryInfo(Path.Combine(context.StateDir.FullName, this.parentDir, this.Name));

            try
            {
                Directory.CreateDirectory(this.DbDir.FullName);
            }
            catch (IOException fatal)
            {
                throw new ProcessorStateException(fatal.ToString());
            }

            this.OpenRocksDb(this.dbOptions, this.columnFamilyOptions);
            this.open = true;

            return this;
        }

        void OpenRocksDb(
            DbOptions dbOptions,
            ColumnFamilyOptions columnFamilyOptions)
        {
            var columnFamilyDescriptors = new ColumnFamilies(columnFamilyOptions);

            try
            {
                this.Db = RocksDb.Open(
                    dbOptions,
                    this.DbDir.FullName,
                    columnFamilyDescriptors);

                var columnFamilyHandle = this.Db.GetDefaultColumnFamily();
                this.DbAccessor = new SingleColumnFamilyAccessor(
                    this.Name,
                    this.Db,
                    this.WOptions,
                    this.OpenIterators,
                    columnFamilyHandle);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error opening store " + this.Name + " at location " + this.DbDir.ToString(), e);
            }
        }

        public void Init(IProcessorContext context, IStateStore root)
        {
            // open the DB dir
            this.InternalProcessorContext = context;
            this.OpenDB(context);

            this.batchingStateRestoreCallback = new RocksDbBatchingRestoreCallback(this);

            // value getter should always read directly from rocksDB
            // since it is only for values that are already flushed
            context.Register(root, this.batchingStateRestoreCallback);
        }

        public bool Persistent()
        {
            return true;
        }

        public bool IsOpen()
        {
            return this.open;
        }

        private void ValidateStoreOpen()
        {
            if (!this.open)
            {
                throw new InvalidStateStoreException("Store " + this.Name + " is currently closed");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Put(Bytes key, byte[] value)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            this.ValidateStoreOpen();
            this.DbAccessor.Put(key.Get(), value);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            var originalValue = this.Get(key);

            if (originalValue == null)
            {
                this.Put(key, value);
            }

            return originalValue;
        }

        public void PutAll(List<KeyValuePair<Bytes, byte[]>> entries)
        {
            try
            {
                using var batch = new WriteBatch();

                this.DbAccessor.PrepareBatch(entries, batch);
                this.Write(batch);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while batch writing to store " + this.Name, e);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] Get(Bytes key)
        {
            this.ValidateStoreOpen();
            try
            {
                return this.DbAccessor.Get(key.Get());
            }
            catch (RocksDbException e)
            {
                // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while getting value for key from store " + this.Name, e);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public byte[] Delete(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            byte[] oldValue;
            try
            {
                oldValue = this.DbAccessor.GetOnly(key.Get());
            }
            catch (RocksDbException e)
            {
                // string string.Format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while getting value for key from store " + this.Name, e);
            }

            this.Put(key, null);

            return oldValue;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IKeyValueIterator<Bytes, byte[]> Range(Bytes from, Bytes to)
        {
            from = from ?? throw new ArgumentNullException(nameof(from));
            to = to ?? throw new ArgumentNullException(nameof(to));

            if (from.CompareTo(to) > 0)
            {
                log.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");

                return null; // KeyValueIterators.emptyIterator();
            }

            this.ValidateStoreOpen();

            IKeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = this.DbAccessor.Range(from, to);
            this.OpenIterators.Add(rocksDBRangeIterator);

            return rocksDBRangeIterator;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IKeyValueIterator<Bytes, byte[]> All()
        {
            this.ValidateStoreOpen();
            IKeyValueIterator<Bytes, byte[]> rocksDbIterator = this.DbAccessor.All();
            this.OpenIterators.Add(rocksDbIterator);
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
                this.ValidateStoreOpen();
                long numEntries;
                try
                {
                    numEntries = this.DbAccessor.ApproximateNumEntries();
                }
                catch (RocksDbException e)
                {
                    throw new ProcessorStateException("Error fetching property from store " + this.Name, e);
                }

                if (this.IsOverflowing(numEntries))
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
            if (this.Db == null)
            {
                return;
            }
            try
            {
                this.DbAccessor.Flush();
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error while executing Flush from store " + this.Name, e);
            }
        }

        public void ToggleDbForBulkLoading(bool prepareForBulkload)
        {
            if (prepareForBulkload)
            {
                // if the store is not empty, we need to compact to get around the num.levels check for bulk loading
                var sstFileNames = this.DbDir
                    .GetFiles()
                    .Where(dir => SST_FILE_EXTENSION.IsMatch(dir.FullName))
                    .Select(fi => fi.FullName).ToArray();

                if (sstFileNames != null && sstFileNames.Length > 0)
                {
                    this.DbAccessor.ToggleDbForBulkLoading();
                }
            }

            this.Close();
            this.prepareForBulkload = prepareForBulkload;
            this.OpenDB(this.InternalProcessorContext);
        }

        public void AddToBatch(
            KeyValuePair<byte[], byte[]> record,
            WriteBatch batch)
        {
            this.DbAccessor.AddToBatch(record.Key, record.Value, batch);
        }

        public void Write(WriteBatch batch)
        {
            this.Db.Write(batch, this.WOptions);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Close()
        {
            if (!this.open)
            {
                return;
            }

            this.open = false;
            this.CloseOpenIterators();

            if (this.configSetter != null)
            {
                this.configSetter.Close(this.Name, this.dbOptions);
                this.configSetter = null;
            }

            // Important: do not rearrange the order in which the below objects are closed!
            // Order of closing must follow: ColumnFamilyHandle > RocksDb > DBOptions > ColumnFamilyOptions
            this.DbAccessor.Close();
            this.Db?.Dispose();

            this.DbAccessor = null;
            this.WOptions = null;
            this.Db = null;
            this.filter = null;
            this.cache = null;
        }

        private void CloseOpenIterators()
        {
            HashSet<IKeyValueIterator<Bytes, byte[]>> iterators;
            lock (this.OpenIterators)
            {
                iterators = new HashSet<IKeyValueIterator<Bytes, byte[]>>(this.OpenIterators);
            }

            if (iterators.Count != 0)
            {
                log.LogWarning("Closing {} open iterators for store {}", iterators.Count, this.Name);

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

        public void AddToBatch(KeyValuePair<byte[], byte[]> record)
        {
            throw new NotImplementedException();
        }

        public void Write()
        {
            throw new NotImplementedException();
        }

        private bool disposedValue = false; // To detect redundant calls
        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    this.Db?.Dispose();
                }

                this.disposedValue = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
        }
    }
}
