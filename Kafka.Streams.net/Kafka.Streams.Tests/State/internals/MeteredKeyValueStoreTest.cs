/*






 *

 *





 */













































public class MeteredKeyValueStoreTest {

    private TaskId taskId = new TaskId(0, 0);
    private Dictionary<string, string> tags = mkMap(
        mkEntry("client-id", "test"),
        mkEntry("task-id", taskId.toString()),
        mkEntry("scope-id", "metered")
    );
    @Mock(type = MockType.NICE)
    private KeyValueStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContext context;

    private MeteredKeyValueStore<string, string> metered;
    private string key = "key";
    private Bytes keyBytes = Bytes.wrap(key.getBytes());
    private string value = "value";
    private byte[] valueBytes = value.getBytes();
    private KeyValuePair<Bytes, byte[]> byteKeyValuePair = KeyValuePair.Create(keyBytes, valueBytes);
    private Metrics metrics = new Metrics();

    
    public void Before() {
        metered = new MeteredKeyValueStore<>(
            inner,
            "scope",
            new MockTime(),
            Serdes.String(),
            Serdes.String()
        );
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
        inner.put(eq(keyBytes), aryEq(valueBytes));
        expectLastCall();
        Init();

        metered.put(key, value);

        KafkaMetric metric = metric("put-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldGetBytesFromInnerStoreAndReturnGetMetric() {
        expect(inner.get(keyBytes)).andReturn(valueBytes);
        Init();

        Assert.Equal(metered.get(key), (value));

        KafkaMetric metric = metric("get-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        expect(inner.putIfAbsent(eq(keyBytes), aryEq(valueBytes))).andReturn(null);
        Init();

        metered.putIfAbsent(key, value);

        KafkaMetric metric = metric("put-if-absent-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    private KafkaMetric Metric(string name) {
        return this.metrics.metric(new MetricName(name, "stream-scope-metrics", "", this.tags));
    }

    
    [Xunit.Fact]
    public void ShouldPutAllToInnerStoreAndRecordPutAllMetric() {
        inner.putAll(anyObject(List));
        expectLastCall();
        Init();

        metered.putAll(Collections.singletonList(KeyValuePair.Create(key, value)));

        KafkaMetric metric = metric("put-all-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        expect(inner.delete(keyBytes)).andReturn(valueBytes);
        Init();

        metered.delete(key);

        KafkaMetric metric = metric("delete-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        expect(inner.range(keyBytes, keyBytes))
            .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValuePair).iterator()));
        Init();

        KeyValueIterator<string, string> iterator = metered.range(key, key);
        Assert.Equal(iterator.next().value, (value));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("range-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldGetAllFromInnerStoreAndRecordAllMetric() {
        expect(inner.all()).andReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValuePair).iterator()));
        Init();

        KeyValueIterator<string, string> iterator = metered.all();
        Assert.Equal(iterator.next().value, (value));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric(new MetricName("all-rate", "stream-scope-metrics", "", tags));
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void ShouldFlushInnerWhenFlushTimeRecords() {
        inner.flush();
        expectLastCall().once();
        Init();

        metered.flush();

        KafkaMetric metric = metric("flush-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    private interface CachedKeyValueStore : KeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    
    [Xunit.Fact]
    public void ShouldSetFlushListenerOnWrappedCachingStore() {
        CachedKeyValueStore cachedKeyValueStore = mock(CachedKeyValueStore);

        expect(cachedKeyValueStore.setFlushListener(anyObject(CacheFlushListener), eq(false))).andReturn(true);
        replay(cachedKeyValueStore);

        metered = new MeteredKeyValueStore<>(
            cachedKeyValueStore,
            "scope",
            new MockTime(),
            Serdes.String(),
            Serdes.String()
        );
        Assert.True(metered.setFlushListener(null, false));

        verify(cachedKeyValueStore);
    }

    [Xunit.Fact]
    public void ShouldNotThrowNullPointerExceptionIfGetReturnsNull() {
        expect(inner.get(Bytes.wrap("a".getBytes()))).andReturn(null);

        Init();
        assertNull(metered.get("a"));
    }

    [Xunit.Fact]
    public void ShouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        Assert.False(metered.setFlushListener(null, false));
    }

    private KafkaMetric Metric(MetricName metricName) {
        return this.metrics.metric(metricName);
    }

}