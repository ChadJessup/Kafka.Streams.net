/*






 *

 *





 */















































public class MeteredSessionStoreTest {

    private TaskId taskId = new TaskId(0, 0);
    private Dictionary<string, string> tags = mkMap(
        mkEntry("client-id", "test"),
        mkEntry("task-id", taskId.toString()),
        mkEntry("scope-id", "metered")
    );
    private Metrics metrics = new Metrics();
    private MeteredSessionStore<string, string> metered;
    @Mock(type = MockType.NICE)
    private SessionStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContext context;

    private string key = "a";
    private byte[] keyBytes = key.getBytes();
    private Windowed<Bytes> windowedKeyBytes = new Windowed<>(Bytes.wrap(keyBytes), new SessionWindow(0, 0));

    
    public void Before() {
        metered = new MeteredSessionStore<>(
            inner,
            "scope",
            Serdes.String(),
            Serdes.String(),
            new MockTime());
        metrics.config().recordLevel(Sensor.RecordingLevel.DEBUG);
        expect(context.metrics()).andReturn(new MockStreamsMetrics(metrics));
        expect(context.taskId()).andReturn(taskId);
        expect(inner.name()).andReturn("metered").anyTimes();
    }

    private void Init() {
        replay(inner, context);
        metered.init(context, metered);
    }

    [Xunit.Fact]
    public void TestMetrics() {
        Init();
        JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
                "scope", "test", taskId.toString(), "scope", "metered")));
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
                "scope", "test", taskId.toString(), "scope", "all")));
    }

    [Xunit.Fact]
    public void ShouldWriteBytesToInnerStoreAndRecordPutMetric() {
        inner.put(eq(windowedKeyBytes), aryEq(keyBytes));
        expectLastCall();
        Init();

        metered.put(new Windowed<>(key, new SessionWindow(0, 0)), key);

        KafkaMetric metric = metric("put-rate");
        Assert.True(((Double) metric.metricValue()) > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldFindSessionsFromStoreAndRecordFetchMetric() {
        expect(inner.findSessions(Bytes.wrap(keyBytes), 0, 0))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
        Init();

        KeyValueIterator<Windowed<string>, string> iterator = metered.findSessions(key, 0, 0);
        Assert.Equal(iterator.next().value, (key));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("fetch-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldFindSessionRangeFromStoreAndRecordFetchMetric() {
        expect(inner.findSessions(Bytes.wrap(keyBytes), Bytes.wrap(keyBytes), 0, 0))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
        Init();

        KeyValueIterator<Windowed<string>, string> iterator = metered.findSessions(key, key, 0, 0);
        Assert.Equal(iterator.next().value, (key));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("fetch-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldRemoveFromStoreAndRecordRemoveMetric() {
        inner.remove(windowedKeyBytes);
        expectLastCall();

        Init();

        metered.remove(new Windowed<>(key, new SessionWindow(0, 0)));

        KafkaMetric metric = metric("remove-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldFetchForKeyAndRecordFetchMetric() {
        expect(inner.fetch(Bytes.wrap(keyBytes)))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
        Init();

        KeyValueIterator<Windowed<string>, string> iterator = metered.fetch(key);
        Assert.Equal(iterator.next().value, (key));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("fetch-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldFetchRangeFromStoreAndRecordFetchMetric() {
        expect(inner.fetch(Bytes.wrap(keyBytes), Bytes.wrap(keyBytes)))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
        Init();

        KeyValueIterator<Windowed<string>, string> iterator = metered.fetch(key, key);
        Assert.Equal(iterator.next().value, (key));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("fetch-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldRecordRestoreTimeOnInit() {
        Init();
        KafkaMetric metric = metric("restore-rate");
        Assert.True((Double) metric.metricValue() > 0);
    }

    [Xunit.Fact]
    public void ShouldNotThrowNullPointerExceptionIfFetchSessionReturnsNull() {
        expect(inner.fetchSession(Bytes.wrap("a".getBytes()), 0, long.MaxValue)).andReturn(null);

        Init();
        assertNull(metered.fetchSession("a", 0, long.MaxValue));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerOnPutIfKeyIsNull() {
        metered.put(null, "a");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerOnRemoveIfKeyIsNull() {
        metered.remove(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerOnFetchIfKeyIsNull() {
        metered.fetch(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerOnFetchRangeIfFromIsNull() {
        metered.fetch(null, "to");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerOnFetchRangeIfToIsNull() {
        metered.fetch("from", null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerOnFindSessionsIfKeyIsNull() {
        metered.findSessions(null, 0, 0);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerOnFindSessionsRangeIfFromIsNull() {
        metered.findSessions(null, "a", 0, 0);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerOnFindSessionsRangeIfToIsNull() {
        metered.findSessions("a", null, 0, 0);
    }

    private interface CachedSessionStore : SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    
    [Xunit.Fact]
    public void ShouldSetFlushListenerOnWrappedCachingStore() {
        CachedSessionStore cachedSessionStore = mock(CachedSessionStore);

        expect(cachedSessionStore.setFlushListener(anyObject(CacheFlushListener), eq(false))).andReturn(true);
        replay(cachedSessionStore);

        metered = new MeteredSessionStore<>(
            cachedSessionStore,
            "scope",
            Serdes.String(),
            Serdes.String(),
            new MockTime());
        Assert.True(metered.setFlushListener(null, false));

        verify(cachedSessionStore);
    }

    [Xunit.Fact]
    public void ShouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        Assert.False(metered.setFlushListener(null, false));
    }

    private KafkaMetric Metric(string name) {
        return this.metrics.metric(new MetricName(name, "stream-scope-metrics", "", this.tags));
    }

}
