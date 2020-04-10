//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */



























//    public class ChangeLoggingTimestampedKeyValueBytesStoreTest
//    {

//        private InMemoryKeyValueStore root = new InMemoryKeyValueStore("kv");
//        private ChangeLoggingTimestampedKeyValueBytesStore store = new ChangeLoggingTimestampedKeyValueBytesStore(root);
//        private Dictionary<object, ValueAndTimestamp<byte[]>> sent = new HashMap<>();
//        private Bytes hi = Bytes.Wrap("hi".getBytes());
//        private Bytes hello = Bytes.Wrap("hello".getBytes());
//        private ValueAndTimestamp<byte[]> there = ValueAndTimestamp.Make("there".getBytes(), 97L);
//        // timestamp is 97 what is ASCII of 'a'
//        private readonly byte[] rawThere = "\0\0\0\0\0\0\0athere".getBytes();
//        private ValueAndTimestamp<byte[]> world = ValueAndTimestamp.Make("world".getBytes(), 98L);
//        // timestamp is 98 what is ASCII of 'b'
//        private readonly byte[] rawWorld = "\0\0\0\0\0\0\0bworld".getBytes();


//        public void Before()
//        {
//            NoOpRecordCollector collector = new NoOpRecordCollector()
//            {


//            public void send<K, V>(string topic,
//                                    K key,
//                                    V value,
//                                    Headers headers,
//                                    int partition,
//                                    long timestamp,
//                                    Serializer<K> keySerializer,
//                                    Serializer<V> valueSerializer)
//            {
//                sent.Put(key, ValueAndTimestamp.Make((byte[])value, timestamp));
//            }
//        };
//        InternalMockProcessorContext context = new InternalMockProcessorContext(
//            TestUtils.GetTempDirectory(),
//            Serdes.String(),
//            Serdes.Long(),
//            collector,
//            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
//        context.setTime(0);
//        store.Init(context, store);
//    }


//    public void After()
//    {
//        store.Close();
//    }

//    [Fact]
//    public void ShouldWriteKeyValueBytesToInnerStoreOnPut()
//    {
//        store.Put(hi, rawThere);
//        Assert.Equal(root.Get(hi), (rawThere));
//    }

//    [Fact]
//    public void ShouldLogChangeOnPut()
//    {
//        store.Put(hi, rawThere);
//        ValueAndTimestamp<byte[]> logged = sent.Get(hi);
//        Assert.Equal(logged.Value, (there.Value));
//        Assert.Equal(logged.Timestamp, (there.Timestamp));
//    }

//    [Fact]
//    public void ShouldWriteAllKeyValueToInnerStoreOnPutAll()
//    {
//        store.putAll(Array.asList(KeyValuePair.Create(hi, rawThere),
//                                   KeyValuePair.Create(hello, rawWorld)));
//        Assert.Equal(root.Get(hi), (rawThere));
//        Assert.Equal(root.Get(hello), (rawWorld));
//    }

//    [Fact]
//    public void ShouldLogChangesOnPutAll()
//    {
//        store.putAll(Array.asList(KeyValuePair.Create(hi, rawThere),
//                                   KeyValuePair.Create(hello, rawWorld)));
//        ValueAndTimestamp<byte[]> logged = sent.Get(hi);
//        Assert.Equal(logged.Value, (there.Value));
//        Assert.Equal(logged.Timestamp, (there.Timestamp));
//        ValueAndTimestamp<byte[]> logged2 = sent.Get(hello);
//        Assert.Equal(logged2.Value, (world.Value));
//        Assert.Equal(logged2.Timestamp, (world.Timestamp));
//    }

//    [Fact]
//    public void ShouldPropagateDelete()
//    {
//        store.Put(hi, rawThere);
//        store.delete(hi);
//        Assert.Equal(root.approximateNumEntries, (0L));
//        Assert.Equal(root.Get(hi), nullValue());
//    }

//    [Fact]
//    public void ShouldReturnOldValueOnDelete()
//    {
//        store.Put(hi, rawThere);
//        Assert.Equal(store.delete(hi), (rawThere));
//    }

//    [Fact]
//    public void ShouldLogKeyNullOnDelete()
//    {
//        store.Put(hi, rawThere);
//        store.delete(hi);
//        Assert.Equal(sent.containsKey(hi), (true));
//        Assert.Equal(sent.Get(hi), nullValue());
//    }

//    [Fact]
//    public void ShouldWriteToInnerOnPutIfAbsentNoPreviousValue()
//    {
//        store.putIfAbsent(hi, rawThere);
//        Assert.Equal(root.Get(hi), (rawThere));
//    }

//    [Fact]
//    public void ShouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists()
//    {
//        store.Put(hi, rawThere);
//        store.putIfAbsent(hi, rawWorld);
//        Assert.Equal(root.Get(hi), (rawThere));
//    }

//    [Fact]
//    public void ShouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue()
//    {
//        store.putIfAbsent(hi, rawThere);
//        ValueAndTimestamp<byte[]> logged = sent.Get(hi);
//        Assert.Equal(logged.Value, (there.Value));
//        Assert.Equal(logged.Timestamp, (there.Timestamp));
//    }

//    [Fact]
//    public void ShouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists()
//    {
//        store.Put(hi, rawThere);
//        store.putIfAbsent(hi, rawWorld);
//        ValueAndTimestamp<byte[]> logged = sent.Get(hi);
//        Assert.Equal(logged.Value, (there.Value));
//        Assert.Equal(logged.Timestamp, (there.Timestamp));
//    }

//    [Fact]
//    public void ShouldReturnCurrentValueOnPutIfAbsent()
//    {
//        store.Put(hi, rawThere);
//        Assert.Equal(store.putIfAbsent(hi, rawWorld), (rawThere));
//    }

//    [Fact]
//    public void ShouldReturnNullOnPutIfAbsentWhenNoPreviousValue()
//    {
//        Assert.Equal(store.putIfAbsent(hi, rawThere), (nullValue()));
//    }

//    [Fact]
//    public void ShouldReturnValueOnGetWhenExists()
//    {
//        store.Put(hello, rawWorld);
//        Assert.Equal(store.Get(hello), (rawWorld));
//    }

//    [Fact]
//    public void ShouldReturnNullOnGetWhenDoesntExist()
//    {
//        Assert.Equal(store.Get(hello), (nullValue()));
//    }
//}
//}
///*






//*

//*





//*/












































