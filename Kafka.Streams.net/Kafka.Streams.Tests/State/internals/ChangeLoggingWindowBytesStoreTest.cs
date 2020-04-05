//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class ChangeLoggingWindowBytesStoreTest
//    {

//        private TaskId taskId = new TaskId(0, 0);
//        private Dictionary<object, object> sent = new HashMap<>();
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
//            sent.put(key, value);
//        }
//    };

//    private byte[] value = { 0 };
//    private Bytes bytesKey = Bytes.Wrap(value);

//    (type = MockType.NICE)
//    private IWindowStore<Bytes, byte[]> inner;
//    (type = MockType.NICE)
//    private ProcessorContextImpl context;
//    private ChangeLoggingWindowBytesStore store;



//    public void SetUp()
//    {
//        store = new ChangeLoggingWindowBytesStore(inner, false);
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
//        inner.put(bytesKey, value, 0);
//        EasyMock.expectLastCall();

//        init();

//        store.put(bytesKey, value);

//        assertArrayEquals(
//            value,
//            (byte[])sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0)));
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFetching()
//    {
//        EasyMock
//            .expect(inner.Fetch(bytesKey, 0, 10))
//            .andReturn(KeyValueIterators.emptyWindowStoreIterator());

//        init();

//        store.Fetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10));
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFetchingRange()
//    {
//        EasyMock
//            .expect(inner.Fetch(bytesKey, bytesKey, 0, 1))
//            .andReturn(KeyValueIterators.emptyIterator());

//        init();

//        store.Fetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1));
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldRetainDuplicatesWhenSet()
//    {
//        store = new ChangeLoggingWindowBytesStore(inner, true);
//        inner.put(bytesKey, value, 0);
//        EasyMock.expectLastCall().times(2);

//        init();
//        store.put(bytesKey, value);
//        store.put(bytesKey, value);

//        assertArrayEquals(value, (byte[])sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1)));
//        assertArrayEquals(value, (byte[])sent.Get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2)));

//        EasyMock.verify(inner);
//    }

//}
//}
///*






//*

//*





//*/



































