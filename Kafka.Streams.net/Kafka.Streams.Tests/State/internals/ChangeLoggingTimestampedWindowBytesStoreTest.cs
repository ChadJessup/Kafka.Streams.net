//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */



























//    public class ChangeLoggingTimestampedWindowBytesStoreTest
//    {

//        private TaskId taskId = new TaskId(0, 0);
//        private Dictionary<object, IValueAndTimestamp<object>> sent = new HashMap<>();
//        private NoOpRecordCollector collector = new NoOpRecordCollector()
//        {


//        public void Send<K, V>(string topic,
//                                K key,
//                                V value,
//                                Headers headers,
//                                int partition,
//                                long timestamp,
//                                Serializer<K> keySerializer,
//                                Serializer<V> valueSerializer)
//        {
//            sent.Put(key, ValueAndTimestamp.Make(value, timestamp));
//        }
//    };

//    private byte[] value = { 0 };
//    private byte[] valueAndTimestamp = { 0, 0, 0, 0, 0, 0, 0, 42, 0 };
//    private Bytes bytesKey = Bytes.Wrap(value);

//    (type = MockType.NICE)
//    private IWindowStore<Bytes, byte[]> inner;
//    (type = MockType.NICE)
//    private ProcessorContextImpl context;
//    private ChangeLoggingTimestampedWindowBytesStore store;



//    public void SetUp()
//    {
//        store = new ChangeLoggingTimestampedWindowBytesStore(inner, false);
//    }

//    private void Init()
//    {
//        EasyMock.expect(context.taskId()).andReturn(taskId);
//        EasyMock.expect(context.recordCollector()).andReturn(collector);
//        inner.Init(context, store);
//        EasyMock.expectLastCall();
//        EasyMock.replay(inner, context);

//        store.Init(context, store);
//    }

//    [Fact]
//    public void ShouldLogPuts()
//    {
//        inner.Put(bytesKey, valueAndTimestamp, 0);
//        EasyMock.expectLastCall();

//        Init();

//        store.Put(bytesKey, valueAndTimestamp);

//        assertArrayEquals(
//            value,
//            (byte[])sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0)).Value);
//        Assert.Equal(
//            42L,
//            sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0)).Timestamp);
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFetching()
//    {
//        EasyMock
//            .expect(inner.Fetch(bytesKey, 0, 10))
//            .andReturn(KeyValueIterators.emptyWindowStoreIterator());

//        Init();

//        store.Fetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10));
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFetchingRange()
//    {
//        EasyMock
//            .expect(inner.Fetch(bytesKey, bytesKey, 0, 1))
//            .andReturn(KeyValueIterators.emptyIterator());

//        Init();

//        store.Fetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1));
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldRetainDuplicatesWhenSet()
//    {
//        store = new ChangeLoggingTimestampedWindowBytesStore(inner, true);
//        inner.Put(bytesKey, valueAndTimestamp, 0);
//        EasyMock.expectLastCall().times(2);

//        Init();
//        store.Put(bytesKey, valueAndTimestamp);
//        store.Put(bytesKey, valueAndTimestamp);

//        assertArrayEquals(
//            value,
//            (byte[])sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1)).Value);
//        Assert.Equal(
//            42L,
//            sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1)).Timestamp);
//        assertArrayEquals(
//            value,
//            (byte[])sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2)).Value);
//        Assert.Equal(
//            42L,
//            sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2)).Timestamp);

//        EasyMock.verify(inner);
//    }

//}
//}
///*






//*

//*





//*/





































