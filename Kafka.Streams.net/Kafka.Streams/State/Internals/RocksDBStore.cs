/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.state.internals;

using Kafka.Common.TopicPartition;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Utils;
using Kafka.Streams.KeyValue;
using Kafka.Streams.StreamsConfig;
using Kafka.Streams.Errors.InvalidStateStoreException;
using Kafka.Streams.Errors.ProcessorStateException;
using Kafka.Streams.Processor.AbstractNotifyingBatchingRestoreCallback;
using Kafka.Streams.Processor.BatchingStateRestoreCallback;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.KeyValueStore;
using Kafka.Streams.State.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A persistent key-value store based on RocksDB.
 */
public class RocksDBStore : IKeyValueStore<Bytes, byte[]>, IBulkLoadingStore
{
    private static Logger log = LoggerFactory.getLogger(RocksDBStore.class);

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
    Set<KeyValueIterator<Bytes, byte[]>> openIterators = Collections.synchronizedSet(new HashSet<>()];

    File dbDir;
    RocksDB db;
    RocksDBAccessor dbAccessor;

    // the following option objects will be created in openDB and closed in the close() method
    private RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter userSpecifiedOptions;
    WriteOptions wOptions;
    FlushOptions fOptions;
    private Cache cache;
    private BloomFilter filter;

    private RocksDBConfigSetter configSetter;

    private volatile bool prepareForBulkload = false;
    IProcessorContext internalProcessorContext;
    // visible for testing
    volatile BatchingStateRestoreCallback batchingStateRestoreCallback = null;

    protected volatile bool open = false;

    RocksDBStore(string name)
{
        this(name, DB_FILE_DIR);
    }

    RocksDBStore(string name,
                 string parentDir)
{
        this.name = name;
        this.parentDir = parentDir;
    }


    @SuppressWarnings("unchecked")
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
            (Class<RocksDBConfigSetter>) configs[StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG];

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
        } catch (IOException fatal)
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
            db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilies);
            dbAccessor = new SingleColumnFamilyAccessor(columnFamilies[0)];
        } catch (RocksDBException e)
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

    @SuppressWarnings("unchecked")
    public override synchronized void put(Bytes key,
                                 byte[] value)
{
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        dbAccessor.Add(key(), value];
    }

    public override synchronized byte[] putIfAbsent(Bytes key,
                                           byte[] value)
{
        Objects.requireNonNull(key, "key cannot be null");
        byte[] originalValue = get(key];
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

    public override synchronized byte[] get(Bytes key)
{
        validateStoreOpen();
        try
{
            return dbAccessor[key()];
        } catch (RocksDBException e)
{
            // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while getting value for key from store " + name, e);
        }
    }

    public override synchronized byte[] delete(Bytes key)
{
        Objects.requireNonNull(key, "key cannot be null");
        byte[] oldValue;
        try
{
            oldValue = dbAccessor.getOnly(key()];
        } catch (RocksDBException e)
{
            // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
            throw new ProcessorStateException("Error while getting value for key from store " + name, e);
        }
        put(key, null);
        return oldValue;
    }

    public override synchronized KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                              Bytes to)
{
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");

        if (from.compareTo(to) > 0)
{
            log.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();

        KeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = dbAccessor.range(from, to];
        openIterators.add(rocksDBRangeIterator);

        return rocksDBRangeIterator;
    }

    public override synchronized KeyValueIterator<Bytes, byte[]> all()
{
        validateStoreOpen();
        KeyValueIterator<Bytes, byte[]> rocksDbIterator = dbAccessor.all();
        openIterators.add(rocksDbIterator);
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
        } catch (RocksDBException e)
{
            throw new ProcessorStateException("Error fetching property from store " + name, e);
        }
        if (isOverflowing(numEntries))
{
            return Long.MAX_VALUE;
        }
        return numEntries;
    }

    private bool isOverflowing(long value)
{
        // RocksDB returns an unsigned 8-byte integer, which could overflow long
        // and manifest as a negative value.
        return value < 0;
    }

    public override synchronized void flush()
{
        if (db == null)
{
            return;
        }
        try
{
            dbAccessor.flush();
        } catch (RocksDBException e)
{
            throw new ProcessorStateException("Error while executing flush from store " + name, e);
        }
    }

    public override void toggleDbForBulkLoading(bool prepareForBulkload)
{
        if (prepareForBulkload)
{
            // if the store is not empty, we need to compact to get around the num.levels check for bulk loading
            string[] sstFileNames = dbDir.list((dir, name) -> SST_FILE_EXTENSION.matcher(name).matches()];

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
                           WriteBatch batch) throws RocksDBException
{
        dbAccessor.addToBatch(record.key, record.value, batch);
    }

    public override void write(WriteBatch batch) throws RocksDBException
{
        db.write(wOptions, batch);
    }

    public override synchronized void close()
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
        synchronized (openIterators)
{
            iterators = new HashSet<>(openIterators);
        }
        if (iterators.size() != 0)
{
            log.warn("Closing {} open iterators for store {}", iterators.size(), name);
            foreach (KeyValueIterator<Bytes, byte[]> iterator in iterators)
{
                iterator.close();
            }
        }
    }



    interface RocksDBAccessor
{

        void put(byte[] key,
                 byte[] value];

        void prepareBatch(List<KeyValue<Bytes, byte[]>> entries,
                          WriteBatch batch) throws RocksDBException;

        byte[] get(byte[] key] throws RocksDBException;

        /**
         * In contrast to get(), we don't migrate the key to new CF.
         * <p>
         * Use for get() within delete() -- no need to migrate, as it's deleted anyway
         */
        byte[] getOnly(byte[] key] throws RocksDBException;

        KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                              Bytes to);

        KeyValueIterator<Bytes, byte[]> all();

        long approximateNumEntries() throws RocksDBException;

        void flush() throws RocksDBException;

        void prepareBatchForRestore(Collection<KeyValue<byte[], byte[]>> records,
                                    WriteBatch batch) throws RocksDBException;

        void addToBatch(byte[] key,
                        byte[] value,
                        WriteBatch batch) throws RocksDBException;

        void close();

        void toggleDbForBulkLoading();
    }

    class SingleColumnFamilyAccessor : RocksDBAccessor
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
                } catch (RocksDBException e)
{
                    // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while removing key from store " + name, e);
                }
            } else
{
                try
{
                    db.Add(columnFamily, wOptions, key, value);
                } catch (RocksDBException e)
{
                    // string format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                    throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                }
            }
        }

        
        public void prepareBatch(List<KeyValue<Bytes, byte[]>> entries,
                                 WriteBatch batch) throws RocksDBException
{
            foreach (KeyValue<Bytes, byte[]> entry in entries)
{
                Objects.requireNonNull(entry.key, "key cannot be null");
                addToBatch(entry.key(), entry.value, batch];
            }
        }

        
        public byte[] get(byte[] key] throws RocksDBException
{
            return db[columnFamily, key];
        }

        
        public byte[] getOnly(byte[] key] throws RocksDBException
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

        
        public long approximateNumEntries() throws RocksDBException
{
            return db.getLongProperty(columnFamily, "rocksdb.estimate-num-keys");
        }

        
        public void flush() throws RocksDBException
{
            db.flush(fOptions, columnFamily);
        }

        
        public void prepareBatchForRestore(Collection<KeyValue<byte[], byte[]>> records,
                                           WriteBatch batch) throws RocksDBException
{
            foreach (KeyValue<byte[], byte[]> record in records)
{
                addToBatch(record.key, record.value, batch);
            }
        }

        
        public void addToBatch(byte[] key,
                               byte[] value,
                               WriteBatch batch) throws RocksDBException
{
            if (value == null)
{
                batch.delete(columnFamily, key);
            } else
{
                batch.Add(columnFamily, key, value);
            }
        }

        
        public void close()
{
            columnFamily.close();
        }

        
        @SuppressWarnings("deprecation")
        public void toggleDbForBulkLoading()
{
            try
{
                db.compactRange(columnFamily, true, 1, 0);
            } catch (RocksDBException e)
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
    }

    // for testing
    public Options getOptions()
{
        return userSpecifiedOptions;
    }
}
