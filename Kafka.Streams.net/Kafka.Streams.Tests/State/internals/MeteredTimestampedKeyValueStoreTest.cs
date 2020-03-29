/*






 *

 *





 */
















































public class MeteredTimestampedKeyValueStoreTest {

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

    private MeteredTimestampedKeyValueStore<string, string> metered;
    private string key = "key";
    private Bytes keyBytes = Bytes.wrap(key.getBytes());
    private string value = "value";
    private ValueAndTimestamp<string> valueAndTimestamp = ValueAndTimestamp.make("value", 97L);
    // timestamp is 97 what is ASCII of 'a'
    private byte[] valueAndTimestampBytes = "\0\0\0\0\0\0\0avalue".getBytes();
    private KeyValuePair<Bytes, byte[]> byteKeyValueTimestampPair = KeyValuePair.Create(keyBytes, valueAndTimestampBytes);
    private Metrics metrics = new Metrics();

    
    public void before() {
        metered = new MeteredTimestampedKeyValueStore<>(
            inner,
            "scope",
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String())
        );
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
        inner.put(eq(keyBytes), aryEq(valueAndTimestampBytes));
        expectLastCall();
        init();

        metered.put(key, valueAndTimestamp);

        KafkaMetric metric = metric("put-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldGetBytesFromInnerStoreAndReturnGetMetric() {
        expect(inner.get(keyBytes)).andReturn(valueAndTimestampBytes);
        init();

        Assert.Equal(metered.get(key), (valueAndTimestamp));

        KafkaMetric metric = metric("get-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldPutIfAbsentAndRecordPutIfAbsentMetric() {
        expect(inner.putIfAbsent(eq(keyBytes), aryEq(valueAndTimestampBytes))).andReturn(null);
        init();

        metered.putIfAbsent(key, valueAndTimestamp);

        KafkaMetric metric = metric("put-if-absent-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    private KafkaMetric metric(string name) {
        return this.metrics.metric(new MetricName(name, "stream-scope-metrics", "", this.tags));
    }

    
    [Xunit.Fact]
    public void shouldPutAllToInnerStoreAndRecordPutAllMetric() {
        inner.putAll(anyObject(List));
        expectLastCall();
        init();

        metered.putAll(Collections.singletonList(KeyValuePair.Create(key, valueAndTimestamp)));

        KafkaMetric metric = metric("put-all-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldDeleteFromInnerStoreAndRecordDeleteMetric() {
        expect(inner.delete(keyBytes)).andReturn(valueAndTimestampBytes);
        init();

        metered.delete(key);

        KafkaMetric metric = metric("delete-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldGetRangeFromInnerStoreAndRecordRangeMetric() {
        expect(inner.range(keyBytes, keyBytes)).andReturn(
            new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
        init();

        KeyValueIterator<string, ValueAndTimestamp<string>> iterator = metered.range(key, key);
        Assert.Equal(iterator.next().value, (valueAndTimestamp));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric("range-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldGetAllFromInnerStoreAndRecordAllMetric() {
        expect(inner.all())
            .andReturn(new KeyValueIteratorStub<>(Collections.singletonList(byteKeyValueTimestampPair).iterator()));
        init();

        KeyValueIterator<string, ValueAndTimestamp<string>> iterator = metered.all();
        Assert.Equal(iterator.next().value, (valueAndTimestamp));
        Assert.False(iterator.hasNext());
        iterator.close();

        KafkaMetric metric = metric(new MetricName("all-rate", "stream-scope-metrics", "", tags));
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    [Xunit.Fact]
    public void shouldFlushInnerWhenFlushTimeRecords() {
        inner.flush();
        expectLastCall().once();
        init();

        metered.flush();

        KafkaMetric metric = metric("flush-rate");
        Assert.True((Double) metric.metricValue() > 0);
        verify(inner);
    }

    private interface CachedKeyValueStore : KeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]> { }

    
    [Xunit.Fact]
    public void shouldSetFlushListenerOnWrappedCachingStore() {
        CachedKeyValueStore cachedKeyValueStore = mock(CachedKeyValueStore);

        expect(cachedKeyValueStore.setFlushListener(anyObject(CacheFlushListener), eq(false))).andReturn(true);
        replay(cachedKeyValueStore);

        metered = new MeteredTimestampedKeyValueStore<>(
            cachedKeyValueStore,
            "scope",
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.String()));
        Assert.True(metered.setFlushListener(null, false));

        verify(cachedKeyValueStore);
    }

    [Xunit.Fact]
    public void shouldNotSetFlushListenerOnWrappedNoneCachingStore() {
        Assert.False(metered.setFlushListener(null, false));
    }

    private KafkaMetric metric(MetricName metricName) {
        return this.metrics.metric(metricName);
    }

    [Xunit.Fact]
    
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromProcessorContext() {
        expect(context.keySerde()).andStubReturn((Serde) Serdes.String());
        expect(context.valueSerde()).andStubReturn((Serde) Serdes.Long());
        MeteredTimestampedKeyValueStore<string, long> store = new MeteredTimestampedKeyValueStore<>(
            inner,
            "scope",
            new MockTime(),
            null,
            null
        );
        replay(inner, context);
        store.init(context, inner);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
        } catch (StreamsException exception) {
            if (exception.getCause() is ClassCastException) {
                Assert.True(false, "Serdes are not correctly set from processor context.");
            }
            throw exception;
        }
    }

    [Xunit.Fact]
    
    public void shouldNotThrowExceptionIfSerdesCorrectlySetFromConstructorParameters() {
        expect(context.keySerde()).andStubReturn((Serde) Serdes.String());
        expect(context.valueSerde()).andStubReturn((Serde) Serdes.Long());
        MeteredTimestampedKeyValueStore<string, long> store = new MeteredTimestampedKeyValueStore<>(
            inner,
            "scope",
            new MockTime(),
            Serdes.String(),
            new ValueAndTimestampSerde<>(Serdes.Long())
        );
        replay(inner, context);
        store.init(context, inner);

        try {
            store.put("key", ValueAndTimestamp.make(42L, 60000));
        } catch (StreamsException exception) {
            if (exception.getCause() is ClassCastException) {
                Assert.True(false, "Serdes are not correctly set from constructor parameters.");
            }
            throw exception;
        }
    }
}