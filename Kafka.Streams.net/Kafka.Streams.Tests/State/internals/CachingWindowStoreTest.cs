//using Kafka.Streams;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Windowed;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class CachingWindowStoreTest
//    {
//        private const int MAX_CACHE_SIZE_BYTES = 150;
//        private const long DEFAULT_TIMESTAMP = 10L;
//        private const long WINDOW_SIZE = 10L;
//        private const long SEGMENT_INTERVAL = 100L;
//        private InternalMockProcessorContext context;
//        private RocksDBSegmentedBytesStore underlying;
//        private CachingWindowStore cachingStore;
//        private CachingKeyValueStoreTest.CacheFlushListenerStub<IWindowed<string>, string> cacheListener;
//        private ThreadCache cache;
//        private string topic;
//        private WindowKeySchema keySchema;


//        public void SetUp()
//        {
//            keySchema = new WindowKeySchema();
//            underlying = new RocksDBSegmentedBytesStore("test", "metrics-scope", 0, SEGMENT_INTERVAL, keySchema);
//            RocksDBWindowStore windowStore = new RocksDBWindowStore(
//                underlying,
//                false,
//                WINDOW_SIZE);
//            TimeWindowedDeserializer<string> keyDeserializer = new TimeWindowedDeserializer<>(new Serdes.String().Deserializer(), WINDOW_SIZE);
//            keyDeserializer.setIsChangelogTopic(true);
//            cacheListener = new CachingKeyValueStoreTest.CacheFlushListenerStub<>(keyDeserializer, new Serdes.String().Deserializer());
//            cachingStore = new CachingWindowStore(windowStore, WINDOW_SIZE, SEGMENT_INTERVAL);
//            cachingStore.SetFlushListener(cacheListener, false);
//            cache = new ThreadCache(new LogContext("testCache "), MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
//            topic = "topic";
//            context = new InternalMockProcessorContext(TestUtils.GetTempDirectory(), null, null, null, cache);
//            context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, topic, null));
//            cachingStore.Init(context, cachingStore);
//        }

//        public void CloseStore()
//        {
//            cachingStore.Close();
//        }

//        [Fact]
//        public void ShouldNotReturnDuplicatesInRanges()
//        {
//            StreamsBuilder builder = new StreamsBuilder();

//            IStoreBuilder<IWindowStore<string, string>> storeBuilder = Stores.windowStoreBuilder(
//                Stores.PersistentWindowStore("store-Name", ofHours(1L), ofMinutes(1L), false),
//                Serdes.String(),
//                Serdes.String())
//                .withCachingEnabled();

//            builder.AddStateStore(storeBuilder);

//            builder.Stream(topic,
//                Consumed.With(Serdes.String(), Serdes.String()))
//                .transform(() => new Transformer<string, string, KeyValuePair<string, string>>()
//                {
//                    private IWindowStore<string, string> store;
//        private int numRecordsProcessed;



//        public void Init(IProcessorContext processorContext)
//        {
//            this.store = (IWindowStore<string, string>)processorContext.GetStateStore("store-Name");
//            int count = 0;

//            IKeyValueIterator<IWindowed<string>, string> All = store.All();
//            while (All.HasNext())
//            {
//                count++;
//                All.MoveNext();
//            }

//            Assert.Equal(count, (0));
//        }


//        public KeyValuePair<string, string> Transform(string key, string value)
//        {
//            int count = 0;

//            IKeyValueIterator<IWindowed<string>, string> All = store.All();
//            while (All.HasNext())
//            {
//                count++;
//                All.MoveNext();
//            }
//            Assert.Equal(count, (numRecordsProcessed));

//            store.Put(value, value);

//            numRecordsProcessed++;

//            return KeyValuePair.Create(key, value);
//        }


//        public void Close() { }
//    }, "store-Name");

//        string bootstrapServers = "localhost:9092";
//    StreamsConfig streamsConfiguration = new StreamsConfig();
//    streamsConfiguration.Put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        streamsConfiguration.Put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
//        streamsConfiguration.Put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        streamsConfiguration.Put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName);
//        streamsConfiguration.Put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().GetType().FullName);
//        streamsConfiguration.Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
//        streamsConfiguration.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

//        long initialWallClockTime = 0L;
//    TopologyTestDriver driver = new TopologyTestDriver(builder.Build(), streamsConfiguration, initialWallClockTime);

//    ConsumerRecordFactory<string, string> recordFactory = new ConsumerRecordFactory<>(
//        Serdes.String().Serializer,
//        Serdes.String().Serializer,
//        initialWallClockTime);

//        for (int i = 0; i< 5; i++) {
//            driver.PipeInput(recordFactory.Create(topic, UUID.randomUUID().ToString(), UUID.randomUUID().ToString()));
//        }
//driver.advanceWallClockTime(10 * 1000L);
//        recordFactory.advanceTimeMs(10 * 1000L);
//        for (int i = 0; i< 5; i++) {
//            driver.PipeInput(recordFactory.Create(topic, UUID.randomUUID().ToString(), UUID.randomUUID().ToString()));
//        }
//        driver.advanceWallClockTime(10 * 1000L);
//        recordFactory.advanceTimeMs(10 * 1000L);
//        for (int i = 0; i< 5; i++) {
//            driver.PipeInput(recordFactory.Create(topic, UUID.randomUUID().ToString(), UUID.randomUUID().ToString()));
//        }
//        driver.advanceWallClockTime(10 * 1000L);
//        recordFactory.advanceTimeMs(10 * 1000L);
//        for (int i = 0; i< 5; i++) {
//            driver.PipeInput(recordFactory.Create(topic, UUID.randomUUID().ToString(), UUID.randomUUID().ToString()));
//        }
//    }

//    [Fact]
//public void ShouldPutFetchFromCache()
//{
//    cachingStore.Put(bytesKey("a"), bytesValue("a"));
//    cachingStore.Put(bytesKey("b"), bytesValue("b"));

//    Assert.Equal(cachingStore.Fetch(bytesKey("a"), 10), (bytesValue("a")));
//    Assert.Equal(cachingStore.Fetch(bytesKey("b"), 10), (bytesValue("b")));
//    Assert.Equal(cachingStore.Fetch(bytesKey("c"), 10), (null));
//    Assert.Equal(cachingStore.Fetch(bytesKey("a"), 0), (null));

//    IWindowStoreIterator<byte[]> a = cachingStore.Fetch(bytesKey("a"), ofEpochMilli(10), ofEpochMilli(10));
//    IWindowStoreIterator<byte[]> b = cachingStore.Fetch(bytesKey("b"), ofEpochMilli(10), ofEpochMilli(10));
//    verifyKeyValue(a.MoveNext(), DEFAULT_TIMESTAMP, "a");
//    verifyKeyValue(b.MoveNext(), DEFAULT_TIMESTAMP, "b");
//    Assert.False(a.HasNext());
//    Assert.False(b.HasNext());
//    Assert.Equal(2, cache.Count);
//}

//private void VerifyKeyValue(KeyValuePair<long, byte[]> next,
//                            long expectedKey,
//                            string expectedValue)
//{
//    Assert.Equal(next.Key, (expectedKey));
//    Assert.Equal(next.Value, (bytesValue(expectedValue)));
//}

//private static byte[] BytesValue(string value)
//{
//    return value.getBytes();
//}

//private static Bytes BytesKey(string key)
//{
//    return Bytes.Wrap(key.getBytes());
//}

//private string StringFrom(byte[] from)
//{
//    return Serdes.String().deserializer().Deserialize("", from);
//}

//[Fact]
//public void ShouldPutFetchRangeFromCache()
//{
//    cachingStore.Put(bytesKey("a"), bytesValue("a"));
//    cachingStore.Put(bytesKey("b"), bytesValue("b"));

//    IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator =
//        cachingStore.Fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(10), ofEpochMilli(10));
//    verifyWindowedKeyValue(
//        iterator.MoveNext(),
//        new Windowed2<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
//        "a");
//    verifyWindowedKeyValue(
//        iterator.MoveNext(),
//        new Windowed2<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
//        "b");
//    Assert.False(iterator.HasNext());
//    Assert.Equal(2, cache.Count);
//}

//[Fact]
//public void ShouldGetAllFromCache()
//{
//    cachingStore.Put(bytesKey("a"), bytesValue("a"));
//    cachingStore.Put(bytesKey("b"), bytesValue("b"));
//    cachingStore.Put(bytesKey("c"), bytesValue("c"));
//    cachingStore.Put(bytesKey("d"), bytesValue("d"));
//    cachingStore.Put(bytesKey("e"), bytesValue("e"));
//    cachingStore.Put(bytesKey("f"), bytesValue("f"));
//    cachingStore.Put(bytesKey("g"), bytesValue("g"));
//    cachingStore.Put(bytesKey("h"), bytesValue("h"));

//    IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator = cachingStore.All();
//    string[] array = { "a", "b", "c", "d", "e", "f", "g", "h" };
//    foreach (string s in array)
//    {
//        verifyWindowedKeyValue(
//            iterator.MoveNext(),
//            new Windowed2<>(bytesKey(s), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
//            s);
//    }
//    Assert.False(iterator.HasNext());
//}

//[Fact]
//public void ShouldFetchAllWithinTimestampRange()
//{
//    string[] array = { "a", "b", "c", "d", "e", "f", "g", "h" };
//    for (int i = 0; i < array.Length; i++)
//    {
//        context.setTime(i);
//        cachingStore.Put(bytesKey(array[i]), bytesValue(array[i]));
//    }

//    IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator =
//        cachingStore.FetchAll(ofEpochMilli(0), ofEpochMilli(7));
//    for (int i = 0; i < array.Length; i++)
//    {
//        string str = array[i];
//        verifyWindowedKeyValue(
//            iterator.MoveNext(),
//            new Windowed2<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
//            str);
//    }
//    Assert.False(iterator.HasNext());

//    IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator1 =
//        cachingStore.FetchAll(ofEpochMilli(2), ofEpochMilli(4));
//    for (int i = 2; i <= 4; i++)
//    {
//        string str = array[i];
//        verifyWindowedKeyValue(
//            iterator1.MoveNext(),
//            new Windowed2<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
//            str);
//    }
//    Assert.False(iterator1.HasNext());

//    IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator2 =
//        cachingStore.FetchAll(ofEpochMilli(5), ofEpochMilli(7));
//    for (int i = 5; i <= 7; i++)
//    {
//        string str = array[i];
//        verifyWindowedKeyValue(
//            iterator2.MoveNext(),
//            new Windowed2<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
//            str);
//    }
//    Assert.False(iterator2.HasNext());
//}

//[Fact]
//public void ShouldFlushEvictedItemsIntoUnderlyingStore()
//{
//    int added = addItemsToCache();
//    // All dirty entries should have been flushed
//    IKeyValueIterator<Bytes, byte[]> iter = underlying.Fetch(
//        Bytes.Wrap("0".getBytes(StandardCharsets.UTF_8)),
//        DEFAULT_TIMESTAMP,
//        DEFAULT_TIMESTAMP);
//    KeyValuePair<Bytes, byte[]> next = iter.MoveNext();
//    Assert.Equal(DEFAULT_TIMESTAMP, keySchema.segmentTimestamp(next.Key));
//    assertArrayEquals("0".getBytes(), next.Value);
//    Assert.False(iter.HasNext());
//    Assert.Equal(added - 1, cache.Count);
//}

//[Fact]
//public void ShouldForwardDirtyItemsWhenFlushCalled()
//{
//    IWindowed<string> windowedKey =
//        new Windowed2<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
//    cachingStore.Put(bytesKey("1"), bytesValue("a"));
//    cachingStore.Flush();
//    Assert.Equal("a", cacheListener.forwarded.Get(windowedKey).newValue);
//    Assert.Null(cacheListener.forwarded.Get(windowedKey).oldValue);
//}

//[Fact]
//public void ShouldSetFlushListener()
//{
//    Assert.True(cachingStore.SetFlushListener(null, true));
//    Assert.True(cachingStore.SetFlushListener(null, false));
//}

//[Fact]
//public void ShouldForwardOldValuesWhenEnabled()
//{
//    cachingStore.SetFlushListener(cacheListener, true);
//    IWindowed<string> windowedKey =
//        new Windowed2<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
//    cachingStore.Put(bytesKey("1"), bytesValue("a"));
//    cachingStore.Put(bytesKey("1"), bytesValue("b"));
//    cachingStore.Flush();
//    Assert.Equal("b", cacheListener.forwarded.Get(windowedKey).newValue);
//    Assert.Null(cacheListener.forwarded.Get(windowedKey).oldValue);
//    cacheListener.forwarded.Clear();
//    cachingStore.Put(bytesKey("1"), bytesValue("c"));
//    cachingStore.Flush();
//    Assert.Equal("c", cacheListener.forwarded.Get(windowedKey).newValue);
//    Assert.Equal("b", cacheListener.forwarded.Get(windowedKey).oldValue);
//    cachingStore.Put(bytesKey("1"), null);
//    cachingStore.Flush();
//    Assert.Null(cacheListener.forwarded.Get(windowedKey).newValue);
//    Assert.Equal("c", cacheListener.forwarded.Get(windowedKey).oldValue);
//    cacheListener.forwarded.Clear();
//    cachingStore.Put(bytesKey("1"), bytesValue("a"));
//    cachingStore.Put(bytesKey("1"), bytesValue("b"));
//    cachingStore.Put(bytesKey("1"), null);
//    cachingStore.Flush();
//    Assert.Null(cacheListener.forwarded.Get(windowedKey));
//    cacheListener.forwarded.Clear();
//}

//[Fact]
//public void ShouldForwardOldValuesWhenDisabled()
//{
//    IWindowed<string> windowedKey =
//        new Windowed2<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
//    cachingStore.Put(bytesKey("1"), bytesValue("a"));
//    cachingStore.Put(bytesKey("1"), bytesValue("b"));
//    cachingStore.Flush();
//    Assert.Equal("b", cacheListener.forwarded.Get(windowedKey).newValue);
//    Assert.Null(cacheListener.forwarded.Get(windowedKey).oldValue);
//    cachingStore.Put(bytesKey("1"), bytesValue("c"));
//    cachingStore.Flush();
//    Assert.Equal("c", cacheListener.forwarded.Get(windowedKey).newValue);
//    Assert.Null(cacheListener.forwarded.Get(windowedKey).oldValue);
//    cachingStore.Put(bytesKey("1"), null);
//    cachingStore.Flush();
//    Assert.Null(cacheListener.forwarded.Get(windowedKey).newValue);
//    Assert.Null(cacheListener.forwarded.Get(windowedKey).oldValue);
//    cacheListener.forwarded.Clear();
//    cachingStore.Put(bytesKey("1"), bytesValue("a"));
//    cachingStore.Put(bytesKey("1"), bytesValue("b"));
//    cachingStore.Put(bytesKey("1"), null);
//    cachingStore.Flush();
//    Assert.Null(cacheListener.forwarded.Get(windowedKey));
//    cacheListener.forwarded.Clear();
//}

//[Fact]
//public void ShouldForwardDirtyItemToListenerWhenEvicted()
//{
//    int numRecords = addItemsToCache();
//    Assert.Equal(numRecords, cacheListener.forwarded.Count);
//}

//[Fact]
//public void ShouldTakeValueFromCacheIfSameTimestampFlushedToRocks()
//{
//    cachingStore.Put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
//    cachingStore.Flush();
//    cachingStore.Put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);

//    IWindowStoreIterator<byte[]> Fetch =
//        cachingStore.Fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP));
//    verifyKeyValue(Fetch.MoveNext(), DEFAULT_TIMESTAMP, "b");
//    Assert.False(Fetch.HasNext());
//}

//[Fact]
//public void ShouldIterateAcrossWindows()
//{
//    cachingStore.Put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
//    cachingStore.Put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

//    IWindowStoreIterator<byte[]> Fetch =
//        cachingStore.Fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
//    verifyKeyValue(Fetch.MoveNext(), DEFAULT_TIMESTAMP, "a");
//    verifyKeyValue(Fetch.MoveNext(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
//    Assert.False(Fetch.HasNext());
//}

//[Fact]
//public void ShouldIterateCacheAndStore()
//{
//    Bytes key = Bytes.Wrap("1".getBytes());
//    underlying.Put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
//    cachingStore.Put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
//    IWindowStoreIterator<byte[]> Fetch =
//        cachingStore.Fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
//    verifyKeyValue(Fetch.MoveNext(), DEFAULT_TIMESTAMP, "a");
//    verifyKeyValue(Fetch.MoveNext(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
//    Assert.False(Fetch.HasNext());
//}

//[Fact]
//public void ShouldIterateCacheAndStoreKeyRange()
//{
//    Bytes key = Bytes.Wrap("1".getBytes());
//    underlying.Put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
//    cachingStore.Put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

//    IKeyValueIterator<IWindowed<Bytes>, byte[]> fetchRange =
//        cachingStore.Fetch(key, bytesKey("2"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
//    verifyWindowedKeyValue(
//        fetchRange.MoveNext(),
//        new Windowed2<>(key, new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
//        "a");
//    verifyWindowedKeyValue(
//        fetchRange.MoveNext(),
//        new Windowed2<>(key, new TimeWindow(DEFAULT_TIMESTAMP + WINDOW_SIZE, DEFAULT_TIMESTAMP + WINDOW_SIZE + WINDOW_SIZE)),
//        "b");
//    Assert.False(fetchRange.HasNext());
//}

//[Fact]
//public void ShouldClearNamespaceCacheOnClose()
//{
//    cachingStore.Put(bytesKey("a"), bytesValue("a"));
//    Assert.Equal(1, cache.Count);
//    cachingStore.Close();
//    Assert.Equal(0, cache.Count);
//}

//[Fact]// (expected = InvalidStateStoreException)
//public void ShouldThrowIfTryingToFetchFromClosedCachingStore()
//{
//    cachingStore.Close();
//    cachingStore.Fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(10));
//}

//[Fact]// (expected = InvalidStateStoreException)
//public void ShouldThrowIfTryingToFetchRangeFromClosedCachingStore()
//{
//    cachingStore.Close();
//    cachingStore.Fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(0), ofEpochMilli(10));
//}

//[Fact]// (expected = InvalidStateStoreException)
//public void ShouldThrowIfTryingToWriteToClosedCachingStore()
//{
//    cachingStore.Close();
//    cachingStore.Put(bytesKey("a"), bytesValue("a"));
//}

//[Fact]
//public void ShouldFetchAndIterateOverExactKeys()
//{
//    cachingStore.Put(bytesKey("a"), bytesValue("0001"), 0);
//    cachingStore.Put(bytesKey("aa"), bytesValue("0002"), 0);
//    cachingStore.Put(bytesKey("a"), bytesValue("0003"), 1);
//    cachingStore.Put(bytesKey("aa"), bytesValue("0004"), 1);
//    cachingStore.Put(bytesKey("a"), bytesValue("0005"), SEGMENT_INTERVAL);

//    List<KeyValuePair<long, byte[]>> expected = Arrays.asList(
//        KeyValuePair.Create(0L, bytesValue("0001")),
//        KeyValuePair.Create(1L, bytesValue("0003")),
//        KeyValuePair.Create(SEGMENT_INTERVAL, bytesValue("0005"))
//    );
//    List<KeyValuePair<long, byte[]>> actual =
//        toList(cachingStore.Fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(long.MaxValue)));
//    verifyKeyValueList(expected, actual);
//}

//[Fact]
//public void ShouldFetchAndIterateOverKeyRange()
//{
//    cachingStore.Put(bytesKey("a"), bytesValue("0001"), 0);
//    cachingStore.Put(bytesKey("aa"), bytesValue("0002"), 0);
//    cachingStore.Put(bytesKey("a"), bytesValue("0003"), 1);
//    cachingStore.Put(bytesKey("aa"), bytesValue("0004"), 1);
//    cachingStore.Put(bytesKey("a"), bytesValue("0005"), SEGMENT_INTERVAL);

//    verifyKeyValueList(
//        Arrays.asList(
//            windowedPair("a", "0001", 0),
//            windowedPair("a", "0003", 1),
//            windowedPair("a", "0005", SEGMENT_INTERVAL)
//        ),
//        toList(cachingStore.Fetch(bytesKey("a"), bytesKey("a"), ofEpochMilli(0), ofEpochMilli(long.MaxValue)))
//    );

//    verifyKeyValueList(
//        Arrays.asList(
//            windowedPair("aa", "0002", 0),
//            windowedPair("aa", "0004", 1)),
//        toList(cachingStore.Fetch(bytesKey("aa"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(long.MaxValue)))
//    );

//    verifyKeyValueList(
//        Arrays.asList(
//            windowedPair("a", "0001", 0),
//            windowedPair("a", "0003", 1),
//            windowedPair("aa", "0002", 0),
//            windowedPair("aa", "0004", 1),
//            windowedPair("a", "0005", SEGMENT_INTERVAL)
//        ),
//        toList(cachingStore.Fetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(long.MaxValue)))
//    );
//}

//[Fact]
//public void ShouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeFetch()
//{
//    cachingStore.Put(bytesKey("a"), bytesValue("0001"), 0);
//    cachingStore.Put(bytesKey("aa"), bytesValue("0002"), 1);
//    cachingStore.Put(bytesKey("aa"), bytesValue("0003"), 2);
//    cachingStore.Put(bytesKey("aaa"), bytesValue("0004"), 3);

//    IWindowStoreIterator<byte[]> singleKeyIterator = cachingStore.Fetch(bytesKey("aa"), 0L, 5L);
//    IKeyValueIterator<IWindowed<Bytes>, byte[]> keyRangeIterator = cachingStore.Fetch(bytesKey("aa"), bytesKey("aa"), 0L, 5L);

//    Assert.Equal(stringFrom(singleKeyIterator.MoveNext().Value), stringFrom(keyRangeIterator.MoveNext().Value));
//    Assert.Equal(stringFrom(singleKeyIterator.MoveNext().Value), stringFrom(keyRangeIterator.MoveNext().Value));
//    Assert.False(singleKeyIterator.HasNext());
//    Assert.False(keyRangeIterator.HasNext());
//}

//[Fact]// (expected = NullPointerException)
//public void ShouldThrowNullPointerExceptionOnPutNullKey()
//{
//    cachingStore.Put(null, bytesValue("anyValue"));
//}

//[Fact]
//public void ShouldNotThrowNullPointerExceptionOnPutNullValue()
//{
//    cachingStore.Put(bytesKey("a"), null);
//}

//[Fact]// (expected = NullPointerException)
//public void ShouldThrowNullPointerExceptionOnFetchNullKey()
//{
//    cachingStore.Fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
//}

//[Fact]// (expected = NullPointerException)
//public void ShouldThrowNullPointerExceptionOnRangeNullFromKey()
//{
//    cachingStore.Fetch(null, bytesKey("anyTo"), ofEpochMilli(1L), ofEpochMilli(2L));
//}

//[Fact]// (expected = NullPointerException)
//public void ShouldThrowNullPointerExceptionOnRangeNullToKey()
//{
//    cachingStore.Fetch(bytesKey("anyFrom"), null, ofEpochMilli(1L), ofEpochMilli(2L));
//}

//[Fact]
//public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey()
//{
//    LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore);
//    LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//    Bytes keyFrom = Bytes.Wrap(Serdes.Int().Serializer.Serialize("", -1));
//    Bytes keyTo = Bytes.Wrap(Serdes.Int().Serializer.Serialize("", 1));

//    IKeyValueIterator<IWindowed<Bytes>, byte[]> iterator = cachingStore.Fetch(keyFrom, keyTo, 0L, 10L);
//    Assert.False(iterator.HasNext());

//    List<string> messages = appender.getMessages();
//    Assert.Equal(messages, hasItem("Returning empty iterator for Fetch with invalid key range: from > to. "
//        + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
//        + "Note that the built-in numerical serdes do not follow this for negative numbers"));
//}

//private static KeyValuePair<IWindowed<Bytes>, byte[]> WindowedPair(string key, string value, long timestamp)
//{
//    return KeyValuePair.Create(
//        new Windowed2<>(bytesKey(key), new TimeWindow(timestamp, timestamp + WINDOW_SIZE)),
//        bytesValue(value));
//}

//private int AddItemsToCache()
//{
//    int cachedSize = 0;
//    int i = 0;
//    while (cachedSize < MAX_CACHE_SIZE_BYTES)
//    {
//        string kv = string.valueOf(i++);
//        cachingStore.Put(bytesKey(kv), bytesValue(kv));
//        cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic) +
//            8 + // timestamp
//            4; // sequenceNumber
//    }
//    return i;
//}

//}
//}
