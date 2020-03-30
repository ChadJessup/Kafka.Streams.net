/*






 *

 *





 */


























public class ChangeLoggingKeyValueBytesStoreTest {

    private InMemoryKeyValueStore inner = new InMemoryKeyValueStore("kv");
    private ChangeLoggingKeyValueBytesStore store = new ChangeLoggingKeyValueBytesStore(inner);
    private Dictionary<object, object> sent = new HashMap<>();
    private Bytes hi = Bytes.wrap("hi".getBytes());
    private Bytes hello = Bytes.wrap("hello".getBytes());
    private readonly byte[] there = "there".getBytes();
    private readonly byte[] world = "world".getBytes();

    
    public void Before() {
        NoOpRecordCollector collector = new NoOpRecordCollector() {
            
            public void send<K, V>(string topic,
                                    K key,
                                    V value,
                                    Headers headers,
                                    int partition,
                                    long timestamp,
                                    Serializer<K> keySerializer,
                                    Serializer<V> valueSerializer) {
                sent.put(key, value);
            }
        };
        InternalMockProcessorContext context = new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.Long(),
            collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
        context.setTime(0);
        store.init(context, store);
    }

    
    public void After() {
        store.close();
    }

    [Xunit.Fact]
    public void ShouldWriteKeyValueBytesToInnerStoreOnPut() {
        store.put(hi, there);
        Assert.Equal(inner.get(hi), (there));
    }

    [Xunit.Fact]
    public void ShouldLogChangeOnPut() {
        store.put(hi, there);
        Assert.Equal(sent.get(hi), (there));
    }

    [Xunit.Fact]
    public void ShouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Array.asList(KeyValuePair.Create(hi, there),
                                   KeyValuePair.Create(hello, world)));
        Assert.Equal(inner.get(hi), (there));
        Assert.Equal(inner.get(hello), (world));
    }

    [Xunit.Fact]
    public void ShouldLogChangesOnPutAll() {
        store.putAll(Array.asList(KeyValuePair.Create(hi, there),
                                   KeyValuePair.Create(hello, world)));
        Assert.Equal(sent.get(hi), (there));
        Assert.Equal(sent.get(hello), (world));
    }

    [Xunit.Fact]
    public void ShouldPropagateDelete() {
        store.put(hi, there);
        store.delete(hi);
        Assert.Equal(inner.approximateNumEntries(), (0L));
        Assert.Equal(inner.get(hi), nullValue());
    }

    [Xunit.Fact]
    public void ShouldReturnOldValueOnDelete() {
        store.put(hi, there);
        Assert.Equal(store.delete(hi), (there));
    }

    [Xunit.Fact]
    public void ShouldLogKeyNullOnDelete() {
        store.put(hi, there);
        store.delete(hi);
        Assert.Equal(sent.containsKey(hi), (true));
        Assert.Equal(sent.get(hi), nullValue());
    }

    [Xunit.Fact]
    public void ShouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, there);
        Assert.Equal(inner.get(hi), (there));
    }

    [Xunit.Fact]
    public void ShouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, there);
        store.putIfAbsent(hi, world);
        Assert.Equal(inner.get(hi), (there));
    }

    [Xunit.Fact]
    public void ShouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, there);
        Assert.Equal(sent.get(hi), (there));
    }

    [Xunit.Fact]
    public void ShouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, there);
        store.putIfAbsent(hi, world);
        Assert.Equal(sent.get(hi), (there));
    }

    [Xunit.Fact]
    public void ShouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, there);
        Assert.Equal(store.putIfAbsent(hi, world), (there));
    }

    [Xunit.Fact]
    public void ShouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        Assert.Equal(store.putIfAbsent(hi, there), (nullValue()));
    }

    [Xunit.Fact]
    public void ShouldReturnValueOnGetWhenExists() {
        store.put(hello, world);
        Assert.Equal(store.get(hello), (world));
    }

    [Xunit.Fact]
    public void ShouldReturnNullOnGetWhenDoesntExist() {
        Assert.Equal(store.get(hello), (nullValue()));
    }
}
