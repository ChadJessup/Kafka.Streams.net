//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.State
//{
//    /**
//     * A component that provides a {@link #context ProcessingContext} that can be supplied to a {@link IKeyValueStore} so that
//     * All entries written to the Kafka topic by the store during {@link IKeyValueStore#Flush()} are captured for testing purposes.
//     * This class simplifies testing of various {@link IKeyValueStore} instances, especially those that use
//     * {@link MeteredKeyValueStore} to monitor and write its entries to the Kafka topic.
//     *
//     * <h2>Basic usage</h2>
//     * This component can be used to help test a {@link IKeyValueStore}'s ability to read and write entries.
//     *
//     * <pre>
//     * // Create the test driver ...
//     * KeyValueStoreTestDriver&lt;int, string> driver = KeyValueStoreTestDriver.Create();
//     * IKeyValueStore&lt;int, string> store = Stores.Create("my-store", driver.context)
//     *                                              .withIntegerKeys().withStringKeys()
//     *                                              .inMemory().Build();
//     *
//     * // Verify that the store reads and writes correctly ...
//     * store.Put(0, "zero");
//     * store.Put(1, "one");
//     * store.Put(2, "two");
//     * store.Put(4, "four");
//     * store.Put(5, "five");
//     * Assert.Equal(5, driver.sizeOf(store));
//     * Assert.Equal("zero", store.Get(0));
//     * Assert.Equal("one", store.Get(1));
//     * Assert.Equal("two", store.Get(2));
//     * Assert.Equal("four", store.Get(4));
//     * Assert.Equal("five", store.Get(5));
//     * Assert.Null(store.Get(3));
//     * store.Delete(5);
//     *
//     * // Flush the store and verify All current entries were properly flushed ...
//     * store.Flush();
//     * Assert.Equal("zero", driver.flushedEntryStored(0));
//     * Assert.Equal("one", driver.flushedEntryStored(1));
//     * Assert.Equal("two", driver.flushedEntryStored(2));
//     * Assert.Equal("four", driver.flushedEntryStored(4));
//     * Assert.Equal(null, driver.flushedEntryStored(5));
//     *
//     * Assert.Equal(false, driver.flushedEntryRemoved(0));
//     * Assert.Equal(false, driver.flushedEntryRemoved(1));
//     * Assert.Equal(false, driver.flushedEntryRemoved(2));
//     * Assert.Equal(false, driver.flushedEntryRemoved(4));
//     * Assert.Equal(true, driver.flushedEntryRemoved(5));
//     * </pre>
//     *
//     *
//     * <h2>Restoring a store</h2>
//     * This component can be used to test whether a {@link IKeyValueStore} implementation properly
//     * {@link ProcessorContext#register(IStateStore, StateRestoreCallback) registers itself} with the {@link ProcessorContext}, so that
//     * the persisted contents of a store are properly restored from the flushed entries when the store instance is started.
//     * <p>
//     * To do this, Create an instance of this driver component, {@link #addEntryToRestoreLog(object, object) add entries} that will be
//     * passed to the store upon creation (simulating the entries that were previously flushed to the topic), and then Create the store
//     * using this driver's {@link #context ProcessorContext}:
//     *
//     * <pre>
//     * // Create the test driver ...
//     * KeyValueStoreTestDriver&lt;int, string> driver = KeyValueStoreTestDriver.Create(int, string);
//     *
//     * // Add any entries that will be restored to any store that uses the driver's context ...
//     * driver.addRestoreEntry(0, "zero");
//     * driver.addRestoreEntry(1, "one");
//     * driver.addRestoreEntry(2, "two");
//     * driver.addRestoreEntry(4, "four");
//     *
//     * // Create the store, which should register with the context and automatically
//     * // receive the restore entries ...
//     * IKeyValueStore&lt;int, string> store = Stores.Create("my-store", driver.context)
//     *                                              .withIntegerKeys().withStringKeys()
//     *                                              .inMemory().Build();
//     *
//     * // Verify that the store's contents were properly restored ...
//     * Assert.Equal(0, driver.checkForRestoredEntries(store));
//     *
//     * // and there are no other entries ...
//     * Assert.Equal(4, driver.sizeOf(store));
//     * </pre>
//     *
//     * @param <K> the type of keys placed in the store
//     * @param <V> the type of values placed in the store
//     */
//    public class KeyValueStoreTestDriver<K, V>
//    {
//        private StreamsConfig props;

//        /**
//         * Create a driver object that will have a {@link #context} that records messages
//         * {@link ProcessorContext#forward(object, object) forwarded} by the store and that provides default serializers and
//         * deserializers for the given built-in key and value types (e.g., {@code string}, {@code int},
//         * {@code long}, and {@code byte[]}). This can be used when store is created to rely upon the
//         * ProcessorContext's default key and value serializers and deserializers.
//         *
//         * @param keyClass   the class for the keys; must be one of {@code string}, {@code int},
//         *                   {@code long}, or {@code byte[]}
//         * @param valueClass the class for the values; must be one of {@code string}, {@code int},
//         *                   {@code long}, or {@code byte[]}
//         * @return the test driver; never null
//         */
//        public static KeyValueStoreTestDriver<K, V> Create(Type keyClass, Type valueClass)
//        {
//            StateSerdes<K, V> serdes = StateSerdes.WithBuiltinTypes("unexpected", keyClass, valueClass);
//            return new KeyValueStoreTestDriver<>(serdes);
//        }

//        /**
//         * Create a driver object that will have a {@link #context} that records messages
//         * {@link ProcessorContext#forward(object, object) forwarded} by the store and that provides the specified serializers and
//         * deserializers. This can be used when store is created to rely upon the ProcessorContext's default key and value serializers
//         * and deserializers.
//         *
//         * @param keySerializer     the key serializer for the {@link ProcessorContext}; may not be null
//         * @param keyDeserializer   the key deserializer for the {@link ProcessorContext}; may not be null
//         * @param valueSerializer   the value serializer for the {@link ProcessorContext}; may not be null
//         * @param valueDeserializer the value deserializer for the {@link ProcessorContext}; may not be null
//         * @return the test driver; never null
//         */
//        public static KeyValueStoreTestDriver<K, V> Create(
//            ISerializer<K> keySerializer,
//            IDeserializer<K> keyDeserializer,
//            ISerializer<V> valueSerializer,
//            IDeserializer<V> valueDeserializer)
//        {
//            StateSerdes<K, V> serdes = new StateSerdes<K, V>(
//                "unexpected",
//                Serdes.SerdeFrom(keySerializer, keyDeserializer),
//                Serdes.SerdeFrom(valueSerializer, valueDeserializer));
//            return new KeyValueStoreTestDriver<K, V>(serdes);
//        }

//        private Dictionary<K, V> flushedEntries = new Dictionary<K, V>();
//        private HashSet<K> flushedRemovals = new HashSet<K>();
//        private List<KeyValuePair<byte[], byte[]>> restorableEntries = new List<KeyValuePair<byte[], byte[]>>();

//        private InternalMockProcessorContext context;
//        private StateSerdes<K, V> stateSerdes;

//        // private KeyValueStoreTestDriver(StateSerdes<K, V> serdes)
//        // {
//        //     ByteArraySerializer rawSerializer = new ByteArraySerializer();
//        //     Producer<byte[], byte[]> producer = new MockProducer<>(true, rawSerializer, rawSerializer);
//        // 
//        //     RecordCollector recordCollector = new RecordCollectorImpl(
//        //         "KeyValueStoreTestDriver",
//        //         new LogContext("KeyValueStoreTestDriver "),
//        //         new DefaultProductionExceptionHandler(),
//        //         new Metrics().sensor("skipped-records")
//        //     )
//        //     {
//        // 
//        // 
//        //     public void send<K1, V1>(string topic,
//        //                               K1 key,
//        //                               V1 value,
//        //                               Headers headers,
//        //                               int partition,
//        //                               long timestamp,
//        //                               Serializer<K1> keySerializer,
//        //                               Serializer<V1> valueSerializer) {
//        //         // for byte arrays we need to wrap it for comparison
//        // 
//        //         K keyTest = serdes.keyFrom(keySerializer.Serialize(topic, headers, key));
//        //         V valueTest = serdes.valueFrom(valueSerializer.Serialize(topic, headers, value));
//        // 
//        //         recordFlushed(keyTest, valueTest);
//        //     }
//        // 
//        // 
//        //     public void send<K1, V1>(string topic,
//        //                               K1 key,
//        //                               V1 value,
//        //                               Headers headers,
//        //                               long timestamp,
//        //                               Serializer<K1> keySerializer,
//        //                               Serializer<V1> valueSerializer,
//        //                               StreamPartitioner<? super K1, ? super V1> partitioner) {
//        //         throw new UnsupportedOperationException();
//        //     }
//        // };
//        recordCollector.Init(producer);

//        File stateDir = TestUtils.GetTempDirectory();
//        //noinspection ResultOfMethodCallIgnored
//        stateDir.mkdirs();
//        stateSerdes = serdes;

//        var props = new StreamsConfig();
//        props.Put(StreamsConfig.ApplicationIdConfig, "application-id");
//        props.Put(StreamsConfig.BootstrapServersConfig, "localhost:9092");
//        props.Put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor);
//        props.Put(StreamsConfig.DefaultKeySerdeClassConfig, serdes.keySerde().GetType());
//        props.Put(StreamsConfig.DefaultValueSerdeClassConfig, ISerde.valueSerde().GetType());
//        props.Put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBKeyValueStoreTest.TheRocksDbConfigSetter);

//        // context = new InternalMockProcessorContext(stateDir, serdes.keySerde(), serdes.valueSerde(), recordCollector, null)
//        // {
//        //     ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1024 * 1024L, metrics());
//        // 
//        // 
//        //     public ThreadCache getCache()
//        //     {
//        //         return cache;
//        //     }
//        // 
//        // 
//        //     public Dictionary<string, object> appConfigs()
//        //     {
//        //         return new StreamsConfig(props).originals();
//        //     }
//        // 
//        // 
//        //     public Dictionary<string, object> appConfigsWithPrefix(string prefix)
//        //     {
//        //         return new StreamsConfig(props).originalsWithPrefix(prefix);
//        //     }
//        // };
//    }

//    private void RecordFlushed(K key, V value)
//    {
//        if (value == null)
//        {
//            // This is a removal ...
//            flushedRemovals.Add(key);
//            flushedEntries.remove(key);
//        }
//        else
//        {
//            // This is a normal add
//            flushedEntries.Put(key, value);
//            flushedRemovals.remove(key);
//        }
//    }

//    /**
//     * Get the entries that are restored to a IKeyValueStore when it is constructed with this driver's {@link #context
//     * ProcessorContext}.
//     *
//     * @return the restore entries; never null but possibly a null iterator
//     */
//    public Iterable<KeyValuePair<byte[], byte[]>> RestoredEntries()
//    {
//        return restorableEntries;
//    }

//    /**
//     * This method adds an entry to the "restore log" for the {@link IKeyValueStore}, and is used <em>only</em> when testing the
//     * restore functionality of a {@link IKeyValueStore} implementation.
//     * <p>
//     * To Create such a test, Create the test driver, call this method one or more times, and then Create the
//     * {@link IKeyValueStore}. Your tests can then check whether the store contains the entries from the log.
//     *
//     * <pre>
//     * // Set up the driver and pre-populate the log ...
//     * KeyValueStoreTestDriver&lt;int, string> driver = KeyValueStoreTestDriver.Create();
//     * driver.addRestoreEntry(1,"value1");
//     * driver.addRestoreEntry(2,"value2");
//     * driver.addRestoreEntry(3,"value3");
//     *
//     * // Create the store using the driver's context ...
//     * ProcessorContext context = driver.context;
//     * IKeyValueStore&lt;int, string> store = ...
//     *
//     * // Verify that the store's contents were properly restored from the log ...
//     * Assert.Equal(0, driver.checkForRestoredEntries(store));
//     *
//     * // and there are no other entries ...
//     * Assert.Equal(3, driver.sizeOf(store));
//     * </pre>
//     *
//     * @param key   the key for the entry
//     * @param value the value for the entry
//     * @see #checkForRestoredEntries(IKeyValueStore)
//     */
//    public void AddEntryToRestoreLog(K key, V value)
//    {
//        restorableEntries.Add(KeyValuePair.Create(stateSerdes.rawKey(key), stateSerdes.RawValue(value)));
//    }

//    /**
//     * Get the context that should be supplied to a {@link IKeyValueStore}'s constructor. This context records any messages
//     * written by the store to the Kafka topic, making them available via the {@link #flushedEntryStored(object)} and
//     * {@link #flushedEntryRemoved(object)} methods.
//     * <p>
//     * If the {@link IKeyValueStore}'s are to be restored upon its startup, be sure to {@link #addEntryToRestoreLog(object, object)
//     * add the restore entries} before creating the store with the {@link ProcessorContext} returned by this method.
//     *
//     * @return the processing context; never null
//     * @see #addEntryToRestoreLog(object, object)
//     */
//    public ProcessorContext Context()
//    {
//        return context;
//    }

//    /**
//     * Utility method that will count the number of {@link #addEntryToRestoreLog(object, object) restore entries} missing from the
//     * supplied store.
//     *
//     * @param store the store that is to have All of the {@link #restoredEntries() restore entries}
//     * @return the number of restore entries missing from the store, or 0 if All restore entries were found
//     * @see #addEntryToRestoreLog(object, object)
//     */
//    public int CheckForRestoredEntries(IKeyValueStore<K, V> store)
//    {
//        int missing = 0;
//        foreach (KeyValuePair<byte[], byte[]> kv in restorableEntries)
//        {
//            if (kv != null)
//            {
//                V value = store.Get(stateSerdes.keyFrom(kv.Key));
//                if (!Objects.Equals(value, stateSerdes.valueFrom(kv.Value)))
//                {
//                    ++missing;
//                }
//            }
//        }
//        return missing;
//    }

//    /**
//     * Utility method to compute the number of entries within the store.
//     *
//     * @param store the key value store using this {@link #context}.
//     * @return the number of entries
//     */
//    public int SizeOf(IKeyValueStore<K, V> store)
//    {
//        int size = 0;
//        IKeyValueIterator iterator = store.All<K, V>();
//        while (iterator.HasNext())
//        {
//            iterator.MoveNext();
//            ++size;
//        }
//    }
//        return size;
//    }

///**
// * Retrieve the value that the store {@link IKeyValueStore#Flush() flushed} with the given key.
// *
// * @param key the key
// * @return the value that was flushed with the key, or {@code null} if no such key was flushed or if the entry with this
// * key was removed upon Flush
// */
//public V FlushedEntryStored(K key)
//{
//    return flushedEntries.Get(key);
//}

///**
// * Determine whether the store {@link IKeyValueStore#Flush() flushed} the removal of the given key.
// *
// * @param key the key
// * @return {@code true} if the entry with the given key was removed when flushed, or {@code false} if the entry was not
// * removed when last flushed
// */
//public bool FlushedEntryRemoved(K key)
//{
//    return flushedRemovals.Contains(key);
//}

///**
// * Return number of removed entry
// */
//public int NumFlushedEntryStored()
//{
//    return flushedEntries.Count;
//}

///**
// * Return number of removed entry
// */
//public int NumFlushedEntryRemoved()
//{
//    return flushedRemovals.Count;
//}

///**
// * Remove All {@link #flushedEntryStored(object) flushed entries}, {@link #flushedEntryRemoved(object) flushed removals},
// */
//public void Clear()
//{
//    restorableEntries.Clear();
//    flushedEntries.Clear();
//    flushedRemovals.Clear();
//}
//}
