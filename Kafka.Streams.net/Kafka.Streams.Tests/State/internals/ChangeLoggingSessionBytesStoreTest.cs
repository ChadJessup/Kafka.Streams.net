//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */



























//    public class ChangeLoggingSessionBytesStoreTest
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

//    (type = MockType.NICE)
//    private ISessionStore<Bytes, byte[]> inner;
//    (type = MockType.NICE)
//    private ProcessorContextImpl context;

//    private ChangeLoggingSessionBytesStore store;
//    private byte[] value1 = { 0 };
//    private Bytes bytesKey = Bytes.Wrap(value1);
//    private Windowed<Bytes> key1 = new Windowed<>(bytesKey, new SessionWindow(0, 0));


//    public void SetUp()
//    {
//        store = new ChangeLoggingSessionBytesStore(inner);
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

//    [Xunit.Fact]
//    public void ShouldLogPuts()
//    {
//        inner.put(key1, value1);
//        EasyMock.expectLastCall();

//        init();

//        store.put(key1, value1);

//        assertArrayEquals(value1, (byte[])sent.Get(SessionKeySchema.toBinary(key1)));
//        EasyMock.verify(inner);
//    }

//    [Xunit.Fact]
//    public void ShouldLogRemoves()
//    {
//        inner.remove(key1);
//        EasyMock.expectLastCall();

//        init();
//        store.remove(key1);

//        Bytes binaryKey = SessionKeySchema.toBinary(key1);
//        Assert.True(sent.containsKey(binaryKey));
//        Assert.Null(sent.Get(binaryKey));
//        EasyMock.verify(inner);
//    }

//    [Xunit.Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFetching()
//    {
//        EasyMock.expect(inner.Fetch(bytesKey)).andReturn(KeyValueIterators.< Windowed<Bytes>, byte[] > emptyIterator());

//        init();

//        store.Fetch(bytesKey);
//        EasyMock.verify(inner);
//    }

//    [Xunit.Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFetchingRange()
//    {
//        EasyMock.expect(inner.Fetch(bytesKey, bytesKey)).andReturn(KeyValueIterators.< Windowed<Bytes>, byte[] > emptyIterator());

//        init();

//        store.Fetch(bytesKey, bytesKey);
//        EasyMock.verify(inner);
//    }

//    [Xunit.Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFindingSessions()
//    {
//        EasyMock.expect(inner.findSessions(bytesKey, 0, 1)).andReturn(KeyValueIterators.< Windowed<Bytes>, byte[] > emptyIterator());

//        init();

//        store.findSessions(bytesKey, 0, 1);
//        EasyMock.verify(inner);
//    }

//    [Xunit.Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFindingSessionRange()
//    {
//        EasyMock.expect(inner.findSessions(bytesKey, bytesKey, 0, 1)).andReturn(KeyValueIterators.< Windowed<Bytes>, byte[] > emptyIterator());

//        init();

//        store.findSessions(bytesKey, bytesKey, 0, 1);
//        EasyMock.verify(inner);
//    }

//    [Xunit.Fact]
//    public void ShouldFlushUnderlyingStore()
//    {
//        inner.flush();
//        EasyMock.expectLastCall();

//        init();

//        store.flush();
//        EasyMock.verify(inner);
//    }

//    [Xunit.Fact]
//    public void ShouldCloseUnderlyingStore()
//    {
//        inner.close();
//        EasyMock.expectLastCall();

//        init();

//        store.close();
//        EasyMock.verify(inner);
//    }


//}}
///*






//*

//*





//*/








































