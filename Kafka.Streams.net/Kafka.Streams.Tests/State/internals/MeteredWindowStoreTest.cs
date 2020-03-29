/*






 *

 *





 */









































public class MeteredWindowStoreTest {
    private InternalMockProcessorContext context;
    
    private WindowStore<Bytes, byte[]> innerStoreMock = createNiceMock(WindowStore);
    private MeteredWindowStore<string, string> store = new MeteredWindowStore<>(
        innerStoreMock,
        10L, // any size
        "scope",
        new MockTime(),
        Serdes.String(),
        new SerdeThatDoesntHandleNull()
    );
    private Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));

    {
        expect(innerStoreMock.name()).andReturn("mocked-store").anyTimes();
    }

    
    public void setUp() {
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "test");

        context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            streamsMetrics,
            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
            NoOpRecordCollector::new,
            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics)
        );
    }

    [Xunit.Fact]
    public void testMetrics() {
        replay(innerStoreMock);
        store.init(context, store);
        JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
                "scope", "test", context.taskId().toString(), "scope", "mocked-store")));
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
                "scope", "test", context.taskId().toString(), "scope", "all")));
    }

    [Xunit.Fact]
    public void shouldRecordRestoreLatencyOnInit() {
        innerStoreMock.init(context, store);
        expectLastCall();
        replay(innerStoreMock);
        store.init(context, store);
        Dictionary<MetricName, ? : Metric> metrics = context.metrics().metrics();
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "restore-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "restore-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
    }

    [Xunit.Fact]
    public void shouldRecordPutLatency() {
        byte[] bytes = "a".getBytes();
        innerStoreMock.put(eq(Bytes.wrap(bytes)), anyObject(), eq(context.Timestamp));
        expectLastCall();
        replay(innerStoreMock);

        store.init(context, store);
        store.put("a", "a");
        Dictionary<MetricName, ? : Metric> metrics = context.metrics().metrics();
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "put-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "put-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
        verify(innerStoreMock);
    }

    [Xunit.Fact]
    public void shouldRecordFetchLatency() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 1, 1)).andReturn(KeyValueIterators.<byte[]>emptyWindowStoreIterator());
        replay(innerStoreMock);

        store.init(context, store);
        store.fetch("a", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        Dictionary<MetricName, ? : Metric> metrics = context.metrics().metrics();
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
        verify(innerStoreMock);
    }

    [Xunit.Fact]
    public void shouldRecordFetchRangeLatency() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), Bytes.wrap("b".getBytes()), 1, 1)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());
        replay(innerStoreMock);

        store.init(context, store);
        store.fetch("a", "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
        Dictionary<MetricName, ? : Metric> metrics = context.metrics().metrics();
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
        verify(innerStoreMock);
    }

    [Xunit.Fact]
    public void shouldRecordFlushLatency() {
        innerStoreMock.flush();
        expectLastCall();
        replay(innerStoreMock);

        store.init(context, store);
        store.flush();
        Dictionary<MetricName, ? : Metric> metrics = context.metrics().metrics();
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "flush-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "flush-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
        verify(innerStoreMock);
    }

    [Xunit.Fact]
    public void shouldCloseUnderlyingStore() {
        innerStoreMock.close();
        expectLastCall();
        replay(innerStoreMock);

        store.init(context, store);
        store.close();
        verify(innerStoreMock);
    }

    [Xunit.Fact]
    public void shouldNotThrowNullPointerExceptionIfFetchReturnsNull() {
        expect(innerStoreMock.fetch(Bytes.wrap("a".getBytes()), 0)).andReturn(null);
        replay(innerStoreMock);

        store.init(context, store);
        assertNull(store.fetch("a", 0));
    }

    private interface CachedWindowStore : WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    
    [Xunit.Fact]
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        CachedWindowStore cachedWindowStore = mock(CachedWindowStore);

        expect(cachedWindowStore.setFlushListener(anyObject(CacheFlushListener), eq(false))).andReturn(true);
        replay(cachedWindowStore);

        MeteredWindowStore<string, string> metered = new MeteredWindowStore<>(
            cachedWindowStore,
            10L, // any size
            "scope",
            new MockTime(),
            Serdes.String(),
            new SerdeThatDoesntHandleNull()
        );
        Assert.True(metered.setFlushListener(null, false));

        verify(cachedWindowStore);
    }

    [Xunit.Fact]
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        Assert.False(store.setFlushListener(null, false));
    }

}
