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
    private byte[] rawThere = "\0\0\0\0\0\0\0athere".getBytes();
    private ValueAndTimestamp<byte[]> world = ValueAndTimestamp.make("world".getBytes(), 98L);
    // timestamp is 98 what is ASCII of 'b'
    private byte[] rawWorld = "\0\0\0\0\0\0\0bworld".getBytes();

    
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

    
    public void after() {
        store.close();
    }

    [Xunit.Fact]
    public void shouldWriteKeyValueBytesToInnerStoreOnPut() {
        store.put(hi, rawThere);
        Assert.Equal(root.get(hi), (rawThere));
    }

    [Xunit.Fact]
    public void shouldLogChangeOnPut() {
        store.put(hi, rawThere);
        ValueAndTimestamp<byte[]> logged = sent.get(hi);
        Assert.Equal(logged.Value, (there.Value));
        Assert.Equal(logged.Timestamp, (there.Timestamp));
    }

    [Xunit.Fact]
    public void shouldWriteAllKeyValueToInnerStoreOnPutAll() {
        store.putAll(Array.asList(KeyValuePair.Create(hi, rawThere),
                                   KeyValuePair.Create(hello, rawWorld)));
        Assert.Equal(root.get(hi), (rawThere));
        Assert.Equal(root.get(hello), (rawWorld));
    }

    [Xunit.Fact]
    public void shouldLogChangesOnPutAll() {
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
    public void shouldPropagateDelete() {
        store.put(hi, rawThere);
        store.delete(hi);
        Assert.Equal(root.approximateNumEntries(), (0L));
        Assert.Equal(root.get(hi), nullValue());
    }

    [Xunit.Fact]
    public void shouldReturnOldValueOnDelete() {
        store.put(hi, rawThere);
        Assert.Equal(store.delete(hi), (rawThere));
    }

    [Xunit.Fact]
    public void shouldLogKeyNullOnDelete() {
        store.put(hi, rawThere);
        store.delete(hi);
        Assert.Equal(sent.containsKey(hi), is(true));
        Assert.Equal(sent.get(hi), nullValue());
    }

    [Xunit.Fact]
    public void shouldWriteToInnerOnPutIfAbsentNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);
        Assert.Equal(root.get(hi), (rawThere));
    }

    [Xunit.Fact]
    public void shouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);
        Assert.Equal(root.get(hi), (rawThere));
    }

    [Xunit.Fact]
    public void shouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() {
        store.putIfAbsent(hi, rawThere);
        ValueAndTimestamp<byte[]> logged = sent.get(hi);
        Assert.Equal(logged.Value, (there.Value));
        Assert.Equal(logged.Timestamp, (there.Timestamp));
    }

    [Xunit.Fact]
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() {
        store.put(hi, rawThere);
        store.putIfAbsent(hi, rawWorld);
        ValueAndTimestamp<byte[]> logged = sent.get(hi);
        Assert.Equal(logged.Value, (there.Value));
        Assert.Equal(logged.Timestamp, (there.Timestamp));
    }

    [Xunit.Fact]
    public void shouldReturnCurrentValueOnPutIfAbsent() {
        store.put(hi, rawThere);
        Assert.Equal(store.putIfAbsent(hi, rawWorld), (rawThere));
    }

    [Xunit.Fact]
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValue() {
        Assert.Equal(store.putIfAbsent(hi, rawThere), is(nullValue()));
    }

    [Xunit.Fact]
    public void shouldReturnValueOnGetWhenExists() {
        store.put(hello, rawWorld);
        Assert.Equal(store.get(hello), (rawWorld));
    }

    [Xunit.Fact]
    public void shouldReturnNullOnGetWhenDoesntExist() {
        Assert.Equal(store.get(hello), is(nullValue()));
    }
}
