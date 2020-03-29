/*






 *

 *





 */
























































public class CachingWindowStoreTest {

    private static int MAX_CACHE_SIZE_BYTES = 150;
    private static long DEFAULT_TIMESTAMP = 10L;
    private static long WINDOW_SIZE = 10L;
    private static long SEGMENT_INTERVAL = 100L;
    private InternalMockProcessorContext context;
    private RocksDBSegmentedBytesStore underlying;
    private CachingWindowStore cachingStore;
    private CachingKeyValueStoreTest.CacheFlushListenerStub<Windowed<string>, string> cacheListener;
    private ThreadCache cache;
    private string topic;
    private WindowKeySchema keySchema;

    
    public void setUp() {
        keySchema = new WindowKeySchema();
        underlying = new RocksDBSegmentedBytesStore("test", "metrics-scope", 0, SEGMENT_INTERVAL, keySchema);
        RocksDBWindowStore windowStore = new RocksDBWindowStore(
            underlying,
            false,
            WINDOW_SIZE);
        TimeWindowedDeserializer<string> keyDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer(), WINDOW_SIZE);
        keyDeserializer.setIsChangelogTopic(true);
        cacheListener = new CachingKeyValueStoreTest.CacheFlushListenerStub<>(keyDeserializer, new StringDeserializer());
        cachingStore = new CachingWindowStore(windowStore, WINDOW_SIZE, SEGMENT_INTERVAL);
        cachingStore.setFlushListener(cacheListener, false);
        cache = new ThreadCache(new LogContext("testCache "), MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        topic = "topic";
        context = new InternalMockProcessorContext(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, topic, null));
        cachingStore.init(context, cachingStore);
    }

    
    public void closeStore() {
        cachingStore.close();
    }

    [Xunit.Fact]
    public void shouldNotReturnDuplicatesInRanges() {
        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder<WindowStore<string, string>> storeBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("store-name", ofHours(1L), ofMinutes(1L), false),
            Serdes.String(),
            Serdes.String())
            .withCachingEnabled();

        builder.addStateStore(storeBuilder);

        builder.stream(topic,
            Consumed.with(Serdes.String(), Serdes.String()))
            .transform(() => new Transformer<string, string, KeyValuePair<string, string>>() {
                private WindowStore<string, string> store;
                private int numRecordsProcessed;

                
                
                public void init(ProcessorContext processorContext) {
                    this.store = (WindowStore<string, string>) processorContext.getStateStore("store-name");
                    int count = 0;

                    KeyValueIterator<Windowed<string>, string> all = store.all();
                    while (all.hasNext()) {
                        count++;
                        all.next();
                    }

                    Assert.Equal(count, (0));
                }

                
                public KeyValuePair<string, string> transform(string key, string value) {
                    int count = 0;

                    KeyValueIterator<Windowed<string>, string> all = store.all();
                    while (all.hasNext()) {
                        count++;
                        all.next();
                    }
                    Assert.Equal(count, (numRecordsProcessed));

                    store.put(value, value);

                    numRecordsProcessed++;

                    return new KeyValuePair<>(key, value);
                }

                
                public void close() {}
            }, "store-name");

        string bootstrapServers = "localhost:9092";
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        long initialWallClockTime = 0L;
        TopologyTestDriver driver = new TopologyTestDriver(builder.build(), streamsConfiguration, initialWallClockTime);

        ConsumerRecordFactory<string, string> recordFactory = new ConsumerRecordFactory<>(
            Serdes.String().Serializer,
            Serdes.String().Serializer,
            initialWallClockTime);

        for (int i = 0; i < 5; i++) {
            driver.pipeInput(recordFactory.create(topic, UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        driver.advanceWallClockTime(10 * 1000L);
        recordFactory.advanceTimeMs(10 * 1000L);
        for (int i = 0; i < 5; i++) {
            driver.pipeInput(recordFactory.create(topic, UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        driver.advanceWallClockTime(10 * 1000L);
        recordFactory.advanceTimeMs(10 * 1000L);
        for (int i = 0; i < 5; i++) {
            driver.pipeInput(recordFactory.create(topic, UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
        driver.advanceWallClockTime(10 * 1000L);
        recordFactory.advanceTimeMs(10 * 1000L);
        for (int i = 0; i < 5; i++) {
            driver.pipeInput(recordFactory.create(topic, UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        }
    }

    [Xunit.Fact]
    public void shouldPutFetchFromCache() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));

        Assert.Equal(cachingStore.fetch(bytesKey("a"), 10), (bytesValue("a")));
        Assert.Equal(cachingStore.fetch(bytesKey("b"), 10), (bytesValue("b")));
        Assert.Equal(cachingStore.fetch(bytesKey("c"), 10), (null));
        Assert.Equal(cachingStore.fetch(bytesKey("a"), 0), (null));

        WindowStoreIterator<byte[]> a = cachingStore.fetch(bytesKey("a"), ofEpochMilli(10), ofEpochMilli(10));
        WindowStoreIterator<byte[]> b = cachingStore.fetch(bytesKey("b"), ofEpochMilli(10), ofEpochMilli(10));
        verifyKeyValue(a.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(b.next(), DEFAULT_TIMESTAMP, "b");
        Assert.False(a.hasNext());
        Assert.False(b.hasNext());
        Assert.Equal(2, cache.Count);
    }

    private void verifyKeyValue(KeyValuePair<long, byte[]> next,
                                long expectedKey,
                                string expectedValue) {
        Assert.Equal(next.key, (expectedKey));
        Assert.Equal(next.value, (bytesValue(expectedValue)));
    }

    private static byte[] bytesValue(string value) {
        return value.getBytes();
    }

    private static Bytes bytesKey(string key) {
        return Bytes.wrap(key.getBytes());
    }

    private string stringFrom(byte[] from) {
        return Serdes.String().deserializer().deserialize("", from);
    }

    [Xunit.Fact]
    public void shouldPutFetchRangeFromCache() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));

        KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
            cachingStore.fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(10), ofEpochMilli(10));
        verifyWindowedKeyValue(
            iterator.next(),
            new Windowed<>(bytesKey("a"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
            "a");
        verifyWindowedKeyValue(
            iterator.next(),
            new Windowed<>(bytesKey("b"), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
            "b");
        Assert.False(iterator.hasNext());
        Assert.Equal(2, cache.Count);
    }

    [Xunit.Fact]
    public void shouldGetAllFromCache() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        cachingStore.put(bytesKey("b"), bytesValue("b"));
        cachingStore.put(bytesKey("c"), bytesValue("c"));
        cachingStore.put(bytesKey("d"), bytesValue("d"));
        cachingStore.put(bytesKey("e"), bytesValue("e"));
        cachingStore.put(bytesKey("f"), bytesValue("f"));
        cachingStore.put(bytesKey("g"), bytesValue("g"));
        cachingStore.put(bytesKey("h"), bytesValue("h"));

        KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.all();
        string[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
        foreach (string s in array) {
            verifyWindowedKeyValue(
                iterator.next(),
                new Windowed<>(bytesKey(s), new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
                s);
        }
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void shouldFetchAllWithinTimestampRange() {
        string[] array = {"a", "b", "c", "d", "e", "f", "g", "h"};
        for (int i = 0; i < array.Length; i++) {
            context.setTime(i);
            cachingStore.put(bytesKey(array[i]), bytesValue(array[i]));
        }

        KeyValueIterator<Windowed<Bytes>, byte[]> iterator =
            cachingStore.fetchAll(ofEpochMilli(0), ofEpochMilli(7));
        for (int i = 0; i < array.Length; i++) {
            string str = array[i];
            verifyWindowedKeyValue(
                iterator.next(),
                new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                str);
        }
        Assert.False(iterator.hasNext());

        KeyValueIterator<Windowed<Bytes>, byte[]> iterator1 =
            cachingStore.fetchAll(ofEpochMilli(2), ofEpochMilli(4));
        for (int i = 2; i <= 4; i++) {
            string str = array[i];
            verifyWindowedKeyValue(
                iterator1.next(),
                new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                str);
        }
        Assert.False(iterator1.hasNext());

        KeyValueIterator<Windowed<Bytes>, byte[]> iterator2 =
            cachingStore.fetchAll(ofEpochMilli(5), ofEpochMilli(7));
        for (int i = 5; i <= 7; i++) {
            string str = array[i];
            verifyWindowedKeyValue(
                iterator2.next(),
                new Windowed<>(bytesKey(str), new TimeWindow(i, i + WINDOW_SIZE)),
                str);
        }
        Assert.False(iterator2.hasNext());
    }

    [Xunit.Fact]
    public void shouldFlushEvictedItemsIntoUnderlyingStore() {
        int added = addItemsToCache();
        // all dirty entries should have been flushed
        KeyValueIterator<Bytes, byte[]> iter = underlying.fetch(
            Bytes.wrap("0".getBytes(StandardCharsets.UTF_8)),
            DEFAULT_TIMESTAMP,
            DEFAULT_TIMESTAMP);
        KeyValuePair<Bytes, byte[]> next = iter.next();
        Assert.Equal(DEFAULT_TIMESTAMP, keySchema.segmentTimestamp(next.key));
        assertArrayEquals("0".getBytes(), next.value);
        Assert.False(iter.hasNext());
        Assert.Equal(added - 1, cache.Count);
    }

    [Xunit.Fact]
    public void shouldForwardDirtyItemsWhenFlushCalled() {
        Windowed<string> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.flush();
        Assert.Equal("a", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
    }

    [Xunit.Fact]
    public void shouldSetFlushListener() {
        Assert.True(cachingStore.setFlushListener(null, true));
        Assert.True(cachingStore.setFlushListener(null, false));
    }

    [Xunit.Fact]
    public void shouldForwardOldValuesWhenEnabled() {
        cachingStore.setFlushListener(cacheListener, true);
        Windowed<string> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.flush();
        Assert.Equal("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.Clear();
        cachingStore.put(bytesKey("1"), bytesValue("c"));
        cachingStore.flush();
        Assert.Equal("c", cacheListener.forwarded.get(windowedKey).newValue);
        Assert.Equal("b", cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), null);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey).newValue);
        Assert.Equal("c", cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.Clear();
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.put(bytesKey("1"), null);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey));
        cacheListener.forwarded.Clear();
    }

    [Xunit.Fact]
    public void shouldForwardOldValuesWhenDisabled() {
        Windowed<string> windowedKey =
            new Windowed<>("1", new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE));
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.flush();
        Assert.Equal("b", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), bytesValue("c"));
        cachingStore.flush();
        Assert.Equal("c", cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cachingStore.put(bytesKey("1"), null);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey).newValue);
        assertNull(cacheListener.forwarded.get(windowedKey).oldValue);
        cacheListener.forwarded.Clear();
        cachingStore.put(bytesKey("1"), bytesValue("a"));
        cachingStore.put(bytesKey("1"), bytesValue("b"));
        cachingStore.put(bytesKey("1"), null);
        cachingStore.flush();
        assertNull(cacheListener.forwarded.get(windowedKey));
        cacheListener.forwarded.Clear();
    }

    [Xunit.Fact]
    public void shouldForwardDirtyItemToListenerWhenEvicted() {
        int numRecords = addItemsToCache();
        Assert.Equal(numRecords, cacheListener.forwarded.Count);
    }

    [Xunit.Fact]
    public void shouldTakeValueFromCacheIfSameTimestampFlushedToRocks() {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.flush();
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP);

        WindowStoreIterator<byte[]> fetch =
            cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP));
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "b");
        Assert.False(fetch.hasNext());
    }

    [Xunit.Fact]
    public void shouldIterateAcrossWindows() {
        cachingStore.put(bytesKey("1"), bytesValue("a"), DEFAULT_TIMESTAMP);
        cachingStore.put(bytesKey("1"), bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        WindowStoreIterator<byte[]> fetch =
            cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
        Assert.False(fetch.hasNext());
    }

    [Xunit.Fact]
    public void shouldIterateCacheAndStore() {
        Bytes key = Bytes.wrap("1".getBytes());
        underlying.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);
        WindowStoreIterator<byte[]> fetch =
            cachingStore.fetch(bytesKey("1"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP, "a");
        verifyKeyValue(fetch.next(), DEFAULT_TIMESTAMP + WINDOW_SIZE, "b");
        Assert.False(fetch.hasNext());
    }

    [Xunit.Fact]
    public void shouldIterateCacheAndStoreKeyRange() {
        Bytes key = Bytes.wrap("1".getBytes());
        underlying.put(WindowKeySchema.toStoreKeyBinary(key, DEFAULT_TIMESTAMP, 0), "a".getBytes());
        cachingStore.put(key, bytesValue("b"), DEFAULT_TIMESTAMP + WINDOW_SIZE);

        KeyValueIterator<Windowed<Bytes>, byte[]> fetchRange =
            cachingStore.fetch(key, bytesKey("2"), ofEpochMilli(DEFAULT_TIMESTAMP), ofEpochMilli(DEFAULT_TIMESTAMP + WINDOW_SIZE));
        verifyWindowedKeyValue(
            fetchRange.next(),
            new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP, DEFAULT_TIMESTAMP + WINDOW_SIZE)),
            "a");
        verifyWindowedKeyValue(
            fetchRange.next(),
            new Windowed<>(key, new TimeWindow(DEFAULT_TIMESTAMP + WINDOW_SIZE, DEFAULT_TIMESTAMP + WINDOW_SIZE + WINDOW_SIZE)),
            "b");
        Assert.False(fetchRange.hasNext());
    }

    [Xunit.Fact]
    public void shouldClearNamespaceCacheOnClose() {
        cachingStore.put(bytesKey("a"), bytesValue("a"));
        Assert.Equal(1, cache.Count);
        cachingStore.close();
        Assert.Equal(0, cache.Count);
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToFetchFromClosedCachingStore() {
        cachingStore.close();
        cachingStore.fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(10));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToFetchRangeFromClosedCachingStore() {
        cachingStore.close();
        cachingStore.fetch(bytesKey("a"), bytesKey("b"), ofEpochMilli(0), ofEpochMilli(10));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowIfTryingToWriteToClosedCachingStore() {
        cachingStore.close();
        cachingStore.put(bytesKey("a"), bytesValue("a"));
    }

    [Xunit.Fact]
    public void shouldFetchAndIterateOverExactKeys() {
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), SEGMENT_INTERVAL);

        List<KeyValuePair<long, byte[]>> expected = asList(
            KeyValuePair.Create(0L, bytesValue("0001")),
            KeyValuePair.Create(1L, bytesValue("0003")),
            KeyValuePair.Create(SEGMENT_INTERVAL, bytesValue("0005"))
        );
        List<KeyValuePair<long, byte[]>> actual =
            toList(cachingStore.fetch(bytesKey("a"), ofEpochMilli(0), ofEpochMilli(long.MaxValue)));
        verifyKeyValueList(expected, actual);
    }

    [Xunit.Fact]
    public void shouldFetchAndIterateOverKeyRange() {
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 0);
        cachingStore.put(bytesKey("a"), bytesValue("0003"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0004"), 1);
        cachingStore.put(bytesKey("a"), bytesValue("0005"), SEGMENT_INTERVAL);

        verifyKeyValueList(
            asList(
                windowedPair("a", "0001", 0),
                windowedPair("a", "0003", 1),
                windowedPair("a", "0005", SEGMENT_INTERVAL)
            ),
            toList(cachingStore.fetch(bytesKey("a"), bytesKey("a"), ofEpochMilli(0), ofEpochMilli(long.MaxValue)))
        );

        verifyKeyValueList(
            asList(
                windowedPair("aa", "0002", 0),
                windowedPair("aa", "0004", 1)),
            toList(cachingStore.fetch(bytesKey("aa"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(long.MaxValue)))
        );

        verifyKeyValueList(
            asList(
                windowedPair("a", "0001", 0),
                windowedPair("a", "0003", 1),
                windowedPair("aa", "0002", 0),
                windowedPair("aa", "0004", 1),
                windowedPair("a", "0005", SEGMENT_INTERVAL)
            ),
            toList(cachingStore.fetch(bytesKey("a"), bytesKey("aa"), ofEpochMilli(0), ofEpochMilli(long.MaxValue)))
        );
    }

    [Xunit.Fact]
    public void shouldReturnSameResultsForSingleKeyFetchAndEqualKeyRangeFetch() {
        cachingStore.put(bytesKey("a"), bytesValue("0001"), 0);
        cachingStore.put(bytesKey("aa"), bytesValue("0002"), 1);
        cachingStore.put(bytesKey("aa"), bytesValue("0003"), 2);
        cachingStore.put(bytesKey("aaa"), bytesValue("0004"), 3);

        WindowStoreIterator<byte[]> singleKeyIterator = cachingStore.fetch(bytesKey("aa"), 0L, 5L);
        KeyValueIterator<Windowed<Bytes>, byte[]> keyRangeIterator = cachingStore.fetch(bytesKey("aa"), bytesKey("aa"), 0L, 5L);

        Assert.Equal(stringFrom(singleKeyIterator.next().value), stringFrom(keyRangeIterator.next().value));
        Assert.Equal(stringFrom(singleKeyIterator.next().value), stringFrom(keyRangeIterator.next().value));
        Assert.False(singleKeyIterator.hasNext());
        Assert.False(keyRangeIterator.hasNext());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnPutNullKey() {
        cachingStore.put(null, bytesValue("anyValue"));
    }

    [Xunit.Fact]
    public void shouldNotThrowNullPointerExceptionOnPutNullValue() {
        cachingStore.put(bytesKey("a"), null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnFetchNullKey() {
        cachingStore.fetch(null, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnRangeNullFromKey() {
        cachingStore.fetch(null, bytesKey("anyTo"), ofEpochMilli(1L), ofEpochMilli(2L));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerExceptionOnRangeNullToKey() {
        cachingStore.fetch(bytesKey("anyFrom"), null, ofEpochMilli(1L), ofEpochMilli(2L));
    }

    [Xunit.Fact]
    public void shouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore);
        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        Bytes keyFrom = Bytes.wrap(Serdes.Int().Serializer.serialize("", -1));
        Bytes keyTo = Bytes.wrap(Serdes.Int().Serializer.serialize("", 1));

        KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.fetch(keyFrom, keyTo, 0L, 10L);
        Assert.False(iterator.hasNext());

        List<string> messages = appender.getMessages();
        Assert.Equal(messages, hasItem("Returning empty iterator for fetch with invalid key range: from > to. "
            + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
            + "Note that the built-in numerical serdes do not follow this for negative numbers"));
    }

    private static KeyValuePair<Windowed<Bytes>, byte[]> windowedPair(string key, string value, long timestamp) {
        return KeyValuePair.Create(
            new Windowed<>(bytesKey(key), new TimeWindow(timestamp, timestamp + WINDOW_SIZE)),
            bytesValue(value));
    }

    private int addItemsToCache() {
        int cachedSize = 0;
        int i = 0;
        while (cachedSize < MAX_CACHE_SIZE_BYTES) {
            string kv = string.valueOf(i++);
            cachingStore.put(bytesKey(kv), bytesValue(kv));
            cachedSize += memoryCacheEntrySize(kv.getBytes(), kv.getBytes(), topic) +
                8 + // timestamp
                4; // sequenceNumber
        }
        return i;
    }

}
