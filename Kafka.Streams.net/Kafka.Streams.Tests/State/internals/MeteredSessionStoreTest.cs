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

    
    public void before() {
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

    private void init() {
        replay(inner, context);
        metered.init(context, metered);
    }

    [Xunit.Fact]
    public void testMetrics() {
        init();
        JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
                "scope", "test", taskId.toString(), "scope", "metered")));
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-%s-metrics,client-id=%s,task-id=%s,%s-id=%s",
                "scope", "test", taskId.toString(), "scope", "all")));
    }

    [Xunit.Fact]
    public void shouldWriteBytesToInnerStoreAndRecordPutMetric() {
        inner.put(eq(windowedKeyBytes), aryEq(keyBytes));
        expectLastCall();
        init();

        metered.put(new Windowed<>(key, new SessionWindow(0, 0)), key);

        KafkaMetric metric = metric("put-rate");
        Assert.True(((Double) metric.metricValue()) > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldFindSessionsFromStoreAndRecordFetchMetric() {
        expect(inner.findSessions(Bytes.wrap(keyBytes), 0, 0))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
        init();

        KeyValueIterator<Windowed<string>, string> iterator = metered.findSessions(key, 0, 0);
        Assert.Equal(iterator.next().value, (key));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("fetch-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldFindSessionRangeFromStoreAndRecordFetchMetric() {
        expect(inner.findSessions(Bytes.wrap(keyBytes), Bytes.wrap(keyBytes), 0, 0))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
        init();

        KeyValueIterator<Windowed<string>, string> iterator = metered.findSessions(key, key, 0, 0);
        Assert.Equal(iterator.next().value, (key));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("fetch-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldRemoveFromStoreAndRecordRemoveMetric() {
        inner.remove(windowedKeyBytes);
        expectLastCall();

        init();

        metered.remove(new Windowed<>(key, new SessionWindow(0, 0)));

        KafkaMetric metric = metric("remove-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldFetchForKeyAndRecordFetchMetric() {
        expect(inner.fetch(Bytes.wrap(keyBytes)))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
        init();

        KeyValueIterator<Windowed<string>, string> iterator = metered.fetch(key);
        Assert.Equal(iterator.next().value, (key));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("fetch-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldFetchRangeFromStoreAndRecordFetchMetric() {
        expect(inner.fetch(Bytes.wrap(keyBytes), Bytes.wrap(keyBytes)))
                .andReturn(new KeyValueIteratorStub<>(
                        Collections.singleton(KeyValuePair.Create(windowedKeyBytes, keyBytes)).iterator()));
        init();

        KeyValueIterator<Windowed<string>, string> iterator = metered.fetch(key, key);
        Assert.Equal(iterator.next().value, (key));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("fetch-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldRecordRestoreTimeOnInit() {
        init();
        KafkaMetric metric = metric("restore-rate");
        Assert.True((Double) metric.metricValue() > 0);
    }

    [Xunit.Fact]
    public void shouldNotThrowNullPointerExceptionIfFetchSessionReturnsNull() {
        expect(inner.fetchSession(Bytes.wrap("a".getBytes()), 0, long.MaxValue)).andReturn(null);

        init();
        assertNull(metered.fetchSession("a", 0, long.MaxValue));
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnPutIfKeyIsNull() {
        metered.put(null, "a");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnRemoveIfKeyIsNull() {
        metered.remove(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnFetchIfKeyIsNull() {
        metered.fetch(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnFetchRangeIfFromIsNull() {
        metered.fetch(null, "to");
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnFetchRangeIfToIsNull() {
        metered.fetch("from", null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnFindSessionsIfKeyIsNull() {
        metered.findSessions(null, 0, 0);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnFindSessionsRangeIfFromIsNull() {
        metered.findSessions(null, "a", 0, 0);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnFindSessionsRangeIfToIsNull() {
        metered.findSessions("a", null, 0, 0);
    }

    private interface CachedSessionStore : SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    
    [Xunit.Fact]
    public void shouldSetFlushListenerOnWrappedCachingStore() {
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
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        Assert.False(metered.setFlushListener(null, false));
    }

    private KafkaMetric metric(string name) {
        return this.metrics.metric(new MetricName(name, "stream-scope-metrics", "", this.tags));
    }

}
