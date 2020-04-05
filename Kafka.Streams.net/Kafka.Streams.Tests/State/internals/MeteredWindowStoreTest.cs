//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */









































//    public class MeteredWindowStoreTest
//    {
//        private InternalMockProcessorContext context;

//        private IWindowStore<Bytes, byte[]> innerStoreMock = createNiceMock(IWindowStore);
//        private MeteredWindowStore<string, string> store = new MeteredWindowStore<>(
//            innerStoreMock,
//            10L, // any size
//            "scope",
//            new MockTime(),
//            Serdes.String(),
//            new SerdeThatDoesntHandleNull()
//        );
//        private Metrics metrics = new Metrics(new MetricConfig().recordLevel(Sensor.RecordingLevel.DEBUG));

//    {
//        expect(innerStoreMock.name()).andReturn("mocked-store").anyTimes();
//    }


//    public void SetUp()
//    {
//        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, "test");

//        context = new InternalMockProcessorContext(
//            TestUtils.GetTempDirectory(),
//            Serdes.String(),
//            Serdes.Long(),
//            streamsMetrics,
//            new StreamsConfig(StreamsTestUtils.getStreamsConfig()),
//            NoOpRecordCollector::new,
//            new ThreadCache(new LogContext("testCache "), 0, streamsMetrics)
//        );
//    }

//    [Fact]
//    public void TestMetrics()
//    {
//        replay(innerStoreMock);
//        store.Init(context, store);
//        JmxReporter reporter = new JmxReporter("kafka.streams");
//        metrics.addReporter(reporter);
//        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
//                "scope", "test", context.taskId().ToString(), "scope", "mocked-store")));
//        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
//                "scope", "test", context.taskId().ToString(), "scope", "all")));
//    }

//    [Fact]
//    public void ShouldRecordRestoreLatencyOnInit()
//    {
//        innerStoreMock.Init(context, store);
//        expectLastCall();
//        replay(innerStoreMock);
//        store.Init(context, store);
//        Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "restore-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "restore-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
//    }

//    [Fact]
//    public void ShouldRecordPutLatency()
//    {
//        byte[] bytes = "a".getBytes();
//        innerStoreMock.put(eq(Bytes.Wrap(bytes)), anyObject(), eq(context.Timestamp));
//        expectLastCall();
//        replay(innerStoreMock);

//        store.Init(context, store);
//        store.put("a", "a");
//        Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "put-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "put-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
//        verify(innerStoreMock);
//    }

//    [Fact]
//    public void ShouldRecordFetchLatency()
//    {
//        expect(innerStoreMock.Fetch(Bytes.Wrap("a".getBytes()), 1, 1)).andReturn(KeyValueIterators.< byte[] > emptyWindowStoreIterator());
//        replay(innerStoreMock);

//        store.Init(context, store);
//        store.Fetch("a", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
//        Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
//        verify(innerStoreMock);
//    }

//    [Fact]
//    public void ShouldRecordFetchRangeLatency()
//    {
//        expect(innerStoreMock.Fetch(Bytes.Wrap("a".getBytes()), Bytes.Wrap("b".getBytes()), 1, 1)).andReturn(KeyValueIterators.< Windowed<Bytes>, byte[] > emptyIterator());
//        replay(innerStoreMock);

//        store.Init(context, store);
//        store.Fetch("a", "b", ofEpochMilli(1), ofEpochMilli(1)).close(); // recorded on close;
//        Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "fetch-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
//        verify(innerStoreMock);
//    }

//    [Fact]
//    public void ShouldRecordFlushLatency()
//    {
//        innerStoreMock.flush();
//        expectLastCall();
//        replay(innerStoreMock);

//        store.Init(context, store);
//        store.flush();
//        Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "flush-total", "stream-scope-metrics", singletonMap("scope-id", "all")).metricValue());
//        Assert.Equal(1.0, getMetricByNameFilterByTags(metrics, "flush-total", "stream-scope-metrics", singletonMap("scope-id", "mocked-store")).metricValue());
//        verify(innerStoreMock);
//    }

//    [Fact]
//    public void ShouldCloseUnderlyingStore()
//    {
//        innerStoreMock.close();
//        expectLastCall();
//        replay(innerStoreMock);

//        store.Init(context, store);
//        store.close();
//        verify(innerStoreMock);
//    }

//    [Fact]
//    public void ShouldNotThrowNullPointerExceptionIfFetchReturnsNull()
//    {
//        expect(innerStoreMock.Fetch(Bytes.Wrap("a".getBytes()), 0)).andReturn(null);
//        replay(innerStoreMock);

//        store.Init(context, store);
//        Assert.Null(store.Fetch("a", 0));
//    }

//    private interface CachedWindowStore : IWindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }


//    [Fact]
//    public void ShouldSetFlushListenerOnWrappedCachingStore()
//    {
//        CachedWindowStore cachedWindowStore = mock(CachedWindowStore);

//        expect(cachedWindowStore.setFlushListener(anyObject(CacheFlushListener), eq(false))).andReturn(true);
//        replay(cachedWindowStore);

//        MeteredWindowStore<string, string> metered = new MeteredWindowStore<>(
//            cachedWindowStore,
//            10L, // any size
//            "scope",
//            new MockTime(),
//            Serdes.String(),
//            new SerdeThatDoesntHandleNull()
//        );
//        Assert.True(metered.setFlushListener(null, false));

//        verify(cachedWindowStore);
//    }

//    [Fact]
//    public void ShouldNotSetFlushListenerOnWrappedNoneCachingStore()
//    {
//        Assert.False(store.setFlushListener(null, false));
//    }

//}
//}
///*






//*

//*





//*/























































