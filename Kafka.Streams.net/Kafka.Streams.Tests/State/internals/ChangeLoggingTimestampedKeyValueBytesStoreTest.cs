/*






 *

 *





 */



























public class ChangeLoggingTimestampedKeyValueBytesStoreTest {

    private InMemoryKeyValueStore root = new InMemoryKeyValueStore("kv");
    private ChangeLoggingTimestampedKeyValueBytesStore store = new ChangeLoggingTimestampedKeyValueBytesStore(root);
    private Dictionary<object, ValueAndTimestamp<byte[]>> sent = new HashMap<>();
    private Bytes hi = Bytes.wrap("hi".getBytes());
    private Bytes hello = Bytes.wrap("hello".getBytes());
    private ValueAndTimestamp<byte[]> there = ValueAndTimestamp.make("there".getBytes(), 97L);
    // timestamp is 97 what is ASCII of 'a'
    private readonly byte[] rawThere = "\0\0\0\0\0\0\0athere".getBytes();
    private ValueAndTimestamp<byte[]> world = ValueAndTimestamp.make("world".getBytes(), 98L);
    // timestamp is 98 what is ASCII of 'b'
    private readonly byte[] rawWorld = "\0\0\0\0\0\0\0bworld".getBytes();

    
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
                sent.put(key, ValueAndTimestamp.make((byte[]) value, timestamp));
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
        store.put(hi, rawThere);
        Assert.Equal(root.get(hi), (rawThere));
    }

    [Xunit.Fact]
    public void ShouldLogChangeOnPut() {
        store.put(hi, rawThere);
        ValueAndTimestamp<byte[]> logged = sent.get(hi);
        Assert.Equal(logged.Value, (there.Value));
        Assert.Equal(logged.Timestamp, (there.Timestamp));
    }

    [Xunit.Fact]
    public void ShouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Array.asList(KeyValuePair.Create(hi, rawThere),
                                   KeyValuePair.Create(hello, rawWorld)));
        Assert.Equal(root.get(hi), (rawThere));
        Assert.Equal(root.get(hello), (rawWorld));
    }

    [Xunit.Fact]
    public void ShouldLogChangesOnPutAll() {
        store.putAll(Array.asList(KeyValuePair.Create(hi, rawThere),
                                   KeyValuePair.Create(hello, rawWorld)));
        ValueAndTimestamp<byte[]> logged = sent.get(hi);
        Assert.Equal(logged.Value, (there.Value));
        Assert.Equal(logged.Timestamp, (there.Timestamp));
        ValueAndTimestamp<byte[]> logged2 = sent.get(hello);
        Assert.Equal(logged2.Value, (world.Value));
        Assert.Equal(logged2.Timestamp, (world.Timestamp));
    }

    [Xunit.Fact]
    public void ShouldPropagateDelete() {
        store.put(hi, rawThere);
        store.delete(hi);
        Assert.Equal(root.approximateNumEntries(), (0L));
        Assert.Equal(root.get(hi), nullValue());
    }

    [Xunit.Fact]
    public void ShouldReturnOldValueOnDelete() {
        store.put(hi, rawThere);
        Assert.Equal(store.delete(hi), (rawThere));
    }

    [Xunit.Fact]
    public void ShouldLogKeyNullOnDelete() {
        store.put(hi, rawThere);
        store.delete(hi);
        Assert.Equal(sent.containsKey(hi), (true));
        Assert.Equal(sent.get(hi), nullValue());
    }

    [Xunit.Fact]
    public void ShouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);
        Assert.Equal(root.get(hi), (rawThere));
    }

    [Xunit.Fact]
    public void ShouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);
        Assert.Equal(root.get(hi), (rawThere));
    }

    [Xunit.Fact]
    public void ShouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);
        ValueAndTimestamp<byte[]> logged = sent.get(hi);
        Assert.Equal(logged.Value, (there.Value));
        Assert.Equal(logged.Timestamp, (there.Timestamp));
    }

    [Xunit.Fact]
    public void ShouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);
        ValueAndTimestamp<byte[]> logged = sent.get(hi);
        Assert.Equal(logged.Value, (there.Value));
        Assert.Equal(logged.Timestamp, (there.Timestamp));
    }

    [Xunit.Fact]
    public void ShouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, rawThere);
        Assert.Equal(store.putIfAbsent(hi, rawWorld), (rawThere));
    }

    [Xunit.Fact]
    public void ShouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        Assert.Equal(store.putIfAbsent(hi, rawThere), (nullValue()));
    }

    [Xunit.Fact]
    public void ShouldReturnValueOnGetWhenExists() {
        store.put(hello, rawWorld);
        Assert.Equal(store.get(hello), (rawWorld));
    }

    [Xunit.Fact]
    public void ShouldReturnNullOnGetWhenDoesntExist() {
        Assert.Equal(store.get(hello), (nullValue()));
    }
}
