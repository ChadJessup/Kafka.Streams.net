//using Confluent.Kafka;
//using Kafka.Streams.KStream;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class ChangeLoggingKeyValueBytesStoreTest
//    {

//        private InMemoryKeyValueStore inner = new InMemoryKeyValueStore("kv");
//        private ChangeLoggingKeyValueBytesStore store = new ChangeLoggingKeyValueBytesStore(inner);
//        private Dictionary<object, object> sent = new HashMap<>();
//        private Bytes hi = Bytes.Wrap("hi".getBytes());
//        private Bytes hello = Bytes.Wrap("hello".getBytes());
//        private readonly byte[] there = "there".getBytes();
//        private readonly byte[] world = "world".getBytes();


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
//                                    ISerializer<K> keySerializer,
//                                    ISerializer<V> valueSerializer)
//            {
//                sent.put(key, value);
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
//        store.close();
//    }

//    [Fact]
//    public void ShouldWriteKeyValueBytesToInnerStoreOnPut()
//    {
//        store.put(hi, there);
//        Assert.Equal(inner.Get(hi), (there));
//    }

//    [Fact]
//    public void ShouldLogChangeOnPut()
//    {
//        store.put(hi, there);
//        Assert.Equal(sent.Get(hi), (there));
//    }

//    [Fact]
//    public void ShouldWriteAllKeyValueToInnerStoreOnPutAll()
//    {
//        store.putAll(Array.asList(KeyValuePair.Create(hi, there),
//                                   KeyValuePair.Create(hello, world)));
//        Assert.Equal(inner.Get(hi), (there));
//        Assert.Equal(inner.Get(hello), (world));
//    }

//    [Fact]
//    public void ShouldLogChangesOnPutAll()
//    {
//        store.putAll(Array.asList(KeyValuePair.Create(hi, there),
//                                   KeyValuePair.Create(hello, world)));
//        Assert.Equal(sent.Get(hi), (there));
//        Assert.Equal(sent.Get(hello), (world));
//    }

//    [Fact]
//    public void ShouldPropagateDelete()
//    {
//        store.put(hi, there);
//        store.delete(hi);
//        Assert.Equal(inner.approximateNumEntries, (0L));
//        Assert.Equal(inner.Get(hi), nullValue());
//    }

//    [Fact]
//    public void ShouldReturnOldValueOnDelete()
//    {
//        store.put(hi, there);
//        Assert.Equal(store.delete(hi), (there));
//    }

//    [Fact]
//    public void ShouldLogKeyNullOnDelete()
//    {
//        store.put(hi, there);
//        store.delete(hi);
//        Assert.Equal(sent.containsKey(hi), (true));
//        Assert.Equal(sent.Get(hi), nullValue());
//    }

//    [Fact]
//    public void ShouldWriteToInnerOnPutIfAbsentNoPreviousValue()
//    {
//        store.putIfAbsent(hi, there);
//        Assert.Equal(inner.Get(hi), (there));
//    }

//    [Fact]
//    public void ShouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists()
//    {
//        store.put(hi, there);
//        store.putIfAbsent(hi, world);
//        Assert.Equal(inner.Get(hi), (there));
//    }

//    [Fact]
//    public void ShouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue()
//    {
//        store.putIfAbsent(hi, there);
//        Assert.Equal(sent.Get(hi), (there));
//    }

//    [Fact]
//    public void ShouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists()
//    {
//        store.put(hi, there);
//        store.putIfAbsent(hi, world);
//        Assert.Equal(sent.Get(hi), (there));
//    }

//    [Fact]
//    public void ShouldReturnCurrentValueOnPutIfAbsent()
//    {
//        store.put(hi, there);
//        Assert.Equal(store.putIfAbsent(hi, world), (there));
//    }

//    [Fact]
//    public void ShouldReturnNullOnPutIfAbsentWhenNoPreviousValue()
//    {
//        Assert.Equal(store.putIfAbsent(hi, there), (nullValue()));
//    }

//    [Fact]
//    public void ShouldReturnValueOnGetWhenExists()
//    {
//        store.put(hello, world);
//        Assert.Equal(store.Get(hello), (world));
//    }

//    [Fact]
//    public void ShouldReturnNullOnGetWhenDoesntExist()
//    {
//        Assert.Equal(store.Get(hello), (nullValue()));
//    }
//}
//}
