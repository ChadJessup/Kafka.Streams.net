/*






 *

 *





 */

















































public abstract class SessionBytesStoreTest {

    protected static long SEGMENT_INTERVAL = 60_000L;
    protected static long RETENTION_PERIOD = 10_000L;

    protected SessionStore<string, long> sessionStore;
    protected InternalMockProcessorContext context;

    private List<KeyValuePair<byte[], byte[]>> changeLog = new ArrayList<>();

    private Producer<byte[], byte[]> producer = new MockProducer<>(true,
        Serdes.ByteArray().Serializer,
        Serdes.ByteArray().Serializer);

    abstract SessionStore<K, V> BuildSessionStore<K, V>(long retentionPeriod,
                                                          Serde<K> keySerde,
                                                          Serde<V> valueSerde);

    abstract string GetMetricsScope();

    abstract void SetClassLoggerToDebug();

    private RecordCollectorImpl CreateRecordCollector(string name) {
        return new RecordCollectorImpl(name,
            new LogContext(name),
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records")) {
            
            public void send<K1, V1>(string topic,
                K1 key,
                V1 value,
                Headers headers,
                int partition,
                long timestamp,
                Serializer<K1> keySerializer,
                Serializer<V1> valueSerializer) {
                changeLog.add(new KeyValuePair<>(
                    keySerializer.serialize(topic, headers, key),
                    valueSerializer.serialize(topic, headers, value))
                );
            }
        };
    }

    
    public void SetUp() {
        sessionStore = buildSessionStore(RETENTION_PERIOD, Serdes.String(), Serdes.Long());

        RecordCollector recordCollector = createRecordCollector(sessionStore.name());
        recordCollector.init(producer);

        context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            recordCollector,
            new ThreadCache(
                new LogContext("testCache"),
                0,
                new MockStreamsMetrics(new Metrics())));

        sessionStore.init(context, sessionStore);
    }

    
    public void After() {
        sessionStore.close();
    }

    [Xunit.Fact]
    public void ShouldPutAndFindSessionsInRange() {
        string key = "a";
        Windowed<string> a1 = new Windowed<>(key, new SessionWindow(10, 10L));
        Windowed<string> a2 = new Windowed<>(key, new SessionWindow(500L, 1000L));
        sessionStore.put(a1, 1L);
        sessionStore.put(a2, 2L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1500L, 2000L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(2500L, 3000L)), 2L);

        List<KeyValuePair<Windowed<string>, long>> expected =
            Array.asList(KeyValuePair.Create(a1, 1L), KeyValuePair.Create(a2, 2L));

        try (KeyValueIterator<Windowed<string>, long> values = sessionStore.findSessions(key, 0, 1000L)
        ) {
            Assert.Equal(new HashSet<>(expected), toSet(values));
        }

        List<KeyValuePair<Windowed<string>, long>> expected2 =
            Collections.singletonList(KeyValuePair.Create(a2, 2L));

        try (KeyValueIterator<Windowed<string>, long> values2 = sessionStore.findSessions(key, 400L, 600L)
        ) {
            Assert.Equal(new HashSet<>(expected2), toSet(values2));
        }
    }

    [Xunit.Fact]
    public void ShouldFetchAllSessionsWithSameRecordKey() {
        List<KeyValuePair<Windowed<string>, long>> expected = Array.asList(
            KeyValuePair.Create(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
            KeyValuePair.Create(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
            KeyValuePair.Create(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
            KeyValuePair.Create(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));

        foreach (KeyValuePair<Windowed<string>, long> kv in expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 0)), 5L);

        try { 
 (KeyValueIterator<Windowed<string>, long> values = sessionStore.fetch("a"));
            Assert.Equal(new HashSet<>(expected), toSet(values));
        }
    }

    [Xunit.Fact]
    public void ShouldFetchAllSessionsWithinKeyRange() {
        List<KeyValuePair<Windowed<string>, long>> expected = Array.asList(
            KeyValuePair.Create(new Windowed<>("aa", new SessionWindow(10, 10)), 2L),
            KeyValuePair.Create(new Windowed<>("b", new SessionWindow(1000, 1000)), 4L),

            KeyValuePair.Create(new Windowed<>("aaa", new SessionWindow(100, 100)), 3L),
            KeyValuePair.Create(new Windowed<>("bb", new SessionWindow(1500, 2000)), 5L));

        foreach (KeyValuePair<Windowed<string>, long> kv in expected) {
            sessionStore.put(kv.key, kv.value);
        }

        // add some that shouldn't appear in the results
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("bbb", new SessionWindow(2500, 3000)), 6L);

        try { 
 (KeyValueIterator<Windowed<string>, long> values = sessionStore.fetch("aa", "bb"));
            Assert.Equal(new HashSet<>(expected), toSet(values));
        }
    }

    [Xunit.Fact]
    public void ShouldFetchExactSession() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 4)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 3)), 2L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 4)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(1, 4)), 4L);
        sessionStore.put(new Windowed<>("aaa", new SessionWindow(0, 4)), 5L);

        long result = sessionStore.fetchSession("aa", 0, 4);
        Assert.Equal(3L, result);
    }

    [Xunit.Fact]
    public void ShouldReturnNullOnSessionNotFound() {
        assertNull(sessionStore.fetchSession("any key", 0L, 5L));
    }

    [Xunit.Fact]
    public void ShouldFindValuesWithinMergingSessionWindowRange() {
        string key = "a";
        sessionStore.put(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L);
        sessionStore.put(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L);

        List<KeyValuePair<Windowed<string>, long>> expected = Array.asList(
            KeyValuePair.Create(new Windowed<>(key, new SessionWindow(0L, 0L)), 1L),
            KeyValuePair.Create(new Windowed<>(key, new SessionWindow(1000L, 1000L)), 2L));

        try { 
 (KeyValueIterator<Windowed<string>, long> results = sessionStore.findSessions(key, -1, 1000L));
            Assert.Equal(new HashSet<>(expected), toSet(results));
        }
    }

    [Xunit.Fact]
    public void ShouldRemove() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1000)));

        try { 
 (KeyValueIterator<Windowed<string>, long> results = sessionStore.findSessions("a", 0L, 1000L));
            Assert.False(results.hasNext());
        }

        try { 
 (KeyValueIterator<Windowed<string>, long> results = sessionStore.findSessions("a", 1500L, 2500L));
            Assert.True(results.hasNext());
        }
    }

    [Xunit.Fact]
    public void ShouldRemoveOnNullAggValue() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), 1L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(1500, 2500)), 2L);

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1000)), null);

        try { 
 (KeyValueIterator<Windowed<string>, long> results = sessionStore.findSessions("a", 0L, 1000L));
            Assert.False(results.hasNext());
        }

        try { 
 (KeyValueIterator<Windowed<string>, long> results = sessionStore.findSessions("a", 1500L, 2500L));
            Assert.True(results.hasNext());
        }
    }

    [Xunit.Fact]
    public void ShouldFindSessionsToMerge() {
        Windowed<string> session1 = new Windowed<>("a", new SessionWindow(0, 100));
        Windowed<string> session2 = new Windowed<>("a", new SessionWindow(101, 200));
        Windowed<string> session3 = new Windowed<>("a", new SessionWindow(201, 300));
        Windowed<string> session4 = new Windowed<>("a", new SessionWindow(301, 400));
        Windowed<string> session5 = new Windowed<>("a", new SessionWindow(401, 500));
        sessionStore.put(session1, 1L);
        sessionStore.put(session2, 2L);
        sessionStore.put(session3, 3L);
        sessionStore.put(session4, 4L);
        sessionStore.put(session5, 5L);

        List<KeyValuePair<Windowed<string>, long>> expected =
            Array.asList(KeyValuePair.Create(session2, 2L), KeyValuePair.Create(session3, 3L));

        try { 
 (KeyValueIterator<Windowed<string>, long> results = sessionStore.findSessions("a", 150, 300));
            Assert.Equal(new HashSet<>(expected), toSet(results));
        }
    }

    [Xunit.Fact]
    public void ShouldFetchExactKeys() {
        sessionStore = buildSessionStore(0x7a00000000000000L, Serdes.String(), Serdes.Long());
        sessionStore.init(context, sessionStore);

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);
        sessionStore.put(new Windowed<>("a",
            new SessionWindow(0x7a00000000000000L - 2, 0x7a00000000000000L - 1)), 5L);

        try (KeyValueIterator<Windowed<string>, long> iterator =
            sessionStore.findSessions("a", 0, long.MaxValue)
        ) {
            Assert.Equal(valuesToSet(iterator), (new HashSet<>(asList(1L, 3L, 5L))));
        }

        try (KeyValueIterator<Windowed<string>, long> iterator =
            sessionStore.findSessions("aa", 0, long.MaxValue)
        ) {
            Assert.Equal(valuesToSet(iterator), (new HashSet<>(asList(2L, 4L))));
        }

        try (KeyValueIterator<Windowed<string>, long> iterator =
            sessionStore.findSessions("a", "aa", 0, long.MaxValue)
        ) {
            Assert.Equal(valuesToSet(iterator), (new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));
        }

        try (KeyValueIterator<Windowed<string>, long> iterator =
            sessionStore.findSessions("a", "aa", 10, 0)
        ) {
            Assert.Equal(valuesToSet(iterator), (new HashSet<>(Collections.singletonList(2L))));
        }
    }

    [Xunit.Fact]
    public void ShouldFetchAndIterateOverExactBinaryKeys() {
        SessionStore<Bytes, string> sessionStore =
            buildSessionStore(RETENTION_PERIOD, Serdes.Bytes(), Serdes.String());

        sessionStore.init(context, sessionStore);

        Bytes key1 = Bytes.wrap(new byte[]{0});
        Bytes key2 = Bytes.wrap(new byte[]{0, 0});
        Bytes key3 = Bytes.wrap(new byte[]{0, 0, 0});

        sessionStore.put(new Windowed<>(key1, new SessionWindow(1, 100)), "1");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(2, 100)), "2");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(3, 100)), "3");
        sessionStore.put(new Windowed<>(key1, new SessionWindow(4, 100)), "4");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(5, 100)), "5");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(6, 100)), "6");
        sessionStore.put(new Windowed<>(key1, new SessionWindow(7, 100)), "7");
        sessionStore.put(new Windowed<>(key2, new SessionWindow(8, 100)), "8");
        sessionStore.put(new Windowed<>(key3, new SessionWindow(9, 100)), "9");

        HashSet<string> expectedKey1 = new HashSet<>(asList("1", "4", "7"));
        Assert.Equal(valuesToSet(sessionStore.findSessions(key1, 0L, long.MaxValue)), (expectedKey1));
        HashSet<string> expectedKey2 = new HashSet<>(asList("2", "5", "8"));
        Assert.Equal(valuesToSet(sessionStore.findSessions(key2, 0L, long.MaxValue)), (expectedKey2));
        HashSet<string> expectedKey3 = new HashSet<>(asList("3", "6", "9"));
        Assert.Equal(valuesToSet(sessionStore.findSessions(key3, 0L, long.MaxValue)), (expectedKey3));
    }

    [Xunit.Fact]
    public void TestIteratorPeek() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 20)), 4L);

        KeyValueIterator<Windowed<string>, long> iterator = sessionStore.findSessions("a", 0L, 20);

        Assert.Equal(iterator.peekNextKey(), new Windowed<>("a", new SessionWindow(0L, 0L)));
        Assert.Equal(iterator.peekNextKey(), iterator.next().key);
        Assert.Equal(iterator.peekNextKey(), iterator.next().key);
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldRestore() {
        List<KeyValuePair<Windowed<string>, long>> expected = Array.asList(
            KeyValuePair.Create(new Windowed<>("a", new SessionWindow(0, 0)), 1L),
            KeyValuePair.Create(new Windowed<>("a", new SessionWindow(10, 10)), 2L),
            KeyValuePair.Create(new Windowed<>("a", new SessionWindow(100, 100)), 3L),
            KeyValuePair.Create(new Windowed<>("a", new SessionWindow(1000, 1000)), 4L));

        foreach (KeyValuePair<Windowed<string>, long> kv in expected) {
            sessionStore.put(kv.key, kv.value);
        }

        try { 
 (KeyValueIterator<Windowed<string>, long> values = sessionStore.fetch("a"));
            Assert.Equal(new HashSet<>(expected), toSet(values));
        }

        sessionStore.close();

        try { 
 (KeyValueIterator<Windowed<string>, long> values = sessionStore.fetch("a"));
            Assert.Equal(Collections.emptySet(), toSet(values));
        }

        context.restore(sessionStore.name(), changeLog);

        try { 
 (KeyValueIterator<Windowed<string>, long> values = sessionStore.fetch("a"));
            Assert.Equal(new HashSet<>(expected), toSet(values));
        }
    }

    [Xunit.Fact]
    public void ShouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("b", new SessionWindow(10, 50)), 2L);
        sessionStore.put(new Windowed<>("c", new SessionWindow(100, 500)), 3L);

        KeyValueIterator<Windowed<string>, long> iterator = sessionStore.fetch("a");
        Assert.True(iterator.hasNext());
        sessionStore.close();

        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldReturnSameResultsForSingleKeyFindSessionsAndEqualKeyRangeFindSessions() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 1)), 0L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(2, 3)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(4, 5)), 2L);
        sessionStore.put(new Windowed<>("aaa", new SessionWindow(6, 7)), 3L);

        KeyValueIterator<Windowed<string>, long> singleKeyIterator = sessionStore.findSessions("aa", 0L, 10L);
        KeyValueIterator<Windowed<string>, long> rangeIterator = sessionStore.findSessions("aa", "aa", 0L, 10L);

        Assert.Equal(singleKeyIterator.next(), rangeIterator.next());
        Assert.Equal(singleKeyIterator.next(), rangeIterator.next());
        Assert.False(singleKeyIterator.hasNext());
        Assert.False(rangeIterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldLogAndMeasureExpiredRecords() {
        setClassLoggerToDebug();
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
        // Note that rocksdb will only expire segments at a time (where segment interval = 60,000 for this retention period)
        sessionStore.put(new Windowed<>("initial record", new SessionWindow(0, 2 * SEGMENT_INTERVAL)), 0L);

        // Try inserting a record with timestamp 0 -- should be dropped
        sessionStore.put(new Windowed<>("late record", new SessionWindow(0, 0)), 0L);
        sessionStore.put(new Windowed<>("another on-time record", new SessionWindow(0, 2 * SEGMENT_INTERVAL)), 0L);

        LogCaptureAppender.Unregister(appender);

        Dictionary<MetricName, ? : Metric> metrics = context.metrics().metrics();

        string metricScope = getMetricsScope();

        Metric dropTotal = metrics.get(new MetricName(
            "expired-window-record-drop-total",
            "stream-" + metricScope + "-metrics",
            "The total number of occurrence of expired-window-record-drop operations.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry(metricScope + "-id", sessionStore.name())
            )
        ));

        Metric dropRate = metrics.get(new MetricName(
            "expired-window-record-drop-rate",
            "stream-" + metricScope + "-metrics",
            "The average number of occurrence of expired-window-record-drop operation per second.",
            mkMap(
                mkEntry("client-id", "mock"),
                mkEntry("task-id", "0_0"),
                mkEntry(metricScope + "-id", sessionStore.name())
            )
        ));

        Assert.Equal(1.0, dropTotal.metricValue());
        Assert.NotEqual(0.0, dropRate.metricValue());
        List<string> messages = appender.getMessages();
        Assert.Equal(messages, hasItem("Skipping record for expired segment."));
    }

    [Xunit.Fact]
    public void ShouldNotThrowExceptionRemovingNonexistentKey() {
        sessionStore.remove(new Windowed<>("a", new SessionWindow(0, 1)));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFindSessionsNullKey() {
        sessionStore.findSessions(null, 1L, 2L);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFindSessionsNullFromKey() {
        sessionStore.findSessions(null, "anyKeyTo", 1L, 2L);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFindSessionsNullToKey() {
        sessionStore.findSessions("anyKeyFrom", null, 1L, 2L);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFetchNullFromKey() {
        sessionStore.fetch(null, "anyToKey");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFetchNullToKey() {
        sessionStore.fetch("anyFromKey", null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFetchNullKey() {
        sessionStore.fetch(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnRemoveNullKey() {
        sessionStore.remove(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnPutNullKey() {
        sessionStore.put(null, 1L);
    }

    [Xunit.Fact]
    public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        setClassLoggerToDebug();
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        string keyFrom = Serdes.String().deserializer()
            .deserialize("", Serdes.Int().Serializer.serialize("", -1));
        string keyTo = Serdes.String().deserializer()
            .deserialize("", Serdes.Int().Serializer.serialize("", 1));

        KeyValueIterator<Windowed<string>, long> iterator = sessionStore.findSessions(keyFrom, keyTo, 0L, 10L);
        Assert.False(iterator.hasNext());

        List<string> messages = appender.getMessages();
        Assert.Equal(messages,
            hasItem("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
                + "Note that the built-in numerical serdes do not follow this for negative numbers"));
    }

    protected static HashSet<V> ValuesToSet<K, V>(Iterator<KeyValuePair<K, V>> iterator) {
        HashSet<V> results = new HashSet<>();

        while (iterator.hasNext()) {
            results.add(iterator.next().value);
        }
        return results;
    }

    protected static HashSet<KeyValuePair<K, V>> ToSet<K, V>(Iterator<KeyValuePair<K, V>> iterator) {
        HashSet<KeyValuePair<K, V>> results = new HashSet<>();

        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        return results;
    }

}
