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
    private byte[] there = "there".getBytes();
    private byte[] world = "world".getBytes();

    
    public void before() {
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

    
    public void after() {
        store.close();
    }

    [Xunit.Fact]
    public void shouldWriteKeyValueBytesToInnerStoreOnPut() {
        store.put(hi, there);
        Assert.Equal(inner.get(hi), (there));
    }

    [Xunit.Fact]
    public void shouldLogChangeOnPut() {
        store.put(hi, there);
        Assert.Equal(sent.get(hi), (there));
    }

    [Xunit.Fact]
    public void shouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Array.asList(KeyValuePair.Create(hi, there),
                                   KeyValuePair.Create(hello, world)));
        Assert.Equal(inner.get(hi), (there));
        Assert.Equal(inner.get(hello), (world));
    }

    [Xunit.Fact]
    public void shouldLogChangesOnPutAll() {
        store.putAll(Array.asList(KeyValuePair.Create(hi, there),
                                   KeyValuePair.Create(hello, world)));
        Assert.Equal(sent.get(hi), (there));
        Assert.Equal(sent.get(hello), (world));
    }

    [Xunit.Fact]
    public void shouldPropagateDelete() {
        store.put(hi, there);
        store.delete(hi);
        Assert.Equal(inner.approximateNumEntries(), (0L));
        Assert.Equal(inner.get(hi), nullValue());
    }

    [Xunit.Fact]
    public void shouldReturnOldValueOnDelete() {
        store.put(hi, there);
        Assert.Equal(store.delete(hi), (there));
    }

    [Xunit.Fact]
    public void shouldLogKeyNullOnDelete() {
        store.put(hi, there);
        store.delete(hi);
        Assert.Equal(sent.containsKey(hi), is(true));
        Assert.Equal(sent.get(hi), nullValue());
    }

    [Xunit.Fact]
    public void shouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, there);
        Assert.Equal(inner.get(hi), (there));
    }

    [Xunit.Fact]
    public void shouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, there);
        store.putIfAbsent(hi, world);
        Assert.Equal(inner.get(hi), (there));
    }

    [Xunit.Fact]
    public void shouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, there);
        Assert.Equal(sent.get(hi), (there));
    }

    [Xunit.Fact]
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, there);
        store.putIfAbsent(hi, world);
        Assert.Equal(sent.get(hi), (there));
    }

    [Xunit.Fact]
    public void shouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, there);
        Assert.Equal(store.putIfAbsent(hi, world), (there));
    }

    [Xunit.Fact]
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        Assert.Equal(store.putIfAbsent(hi, there), is(nullValue()));
    }

    [Xunit.Fact]
    public void shouldReturnValueOnGetWhenExists() {
        store.put(hello, world);
        Assert.Equal(store.get(hello), (world));
    }

    [Xunit.Fact]
    public void shouldReturnNullOnGetWhenDoesntExist() {
        Assert.Equal(store.get(hello), is(nullValue()));
    }
}
