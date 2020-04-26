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
//            sent.Put(key, value);
//        }
//    };

//    
//    private ISessionStore<Bytes, byte[]> inner;
//    
//    private ProcessorContextImpl context;

//    private ChangeLoggingSessionBytesStore store;
//    private byte[] value1 = { 0 };
//    private Bytes bytesKey = Bytes.Wrap(value1);
//    private IWindowed<Bytes> key1 = new Windowed<>(bytesKey, new SessionWindow(0, 0));


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

//    [Fact]
//    public void ShouldLogPuts()
//    {
//        inner.Put(key1, value1);
//        EasyMock.expectLastCall();

//        Init();

//        store.Put(key1, value1);

//        assertArrayEquals(value1, (byte[])sent.Get(SessionKeySchema.toBinary(key1)));
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldLogRemoves()
//    {
//        inner.remove(key1);
//        EasyMock.expectLastCall();

//        Init();
//        store.remove(key1);

//        Bytes binaryKey = SessionKeySchema.toBinary(key1);
//        Assert.True(sent.ContainsKey(binaryKey));
//        Assert.Null(sent.Get(binaryKey));
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFetching()
//    {
//        EasyMock.expect(inner.Fetch(bytesKey)).andReturn(KeyValueIterators.< IWindowed<Bytes>, byte[] > emptyIterator());

//        Init();

//        store.Fetch(bytesKey);
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFetchingRange()
//    {
//        EasyMock.expect(inner.Fetch(bytesKey, bytesKey)).andReturn(KeyValueIterators.< IWindowed<Bytes>, byte[] > emptyIterator());

//        Init();

//        store.Fetch(bytesKey, bytesKey);
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFindingSessions()
//    {
//        EasyMock.expect(inner.findSessions(bytesKey, 0, 1)).andReturn(KeyValueIterators.< IWindowed<Bytes>, byte[] > emptyIterator());

//        Init();

//        store.findSessions(bytesKey, 0, 1);
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldDelegateToUnderlyingStoreWhenFindingSessionRange()
//    {
//        EasyMock.expect(inner.findSessions(bytesKey, bytesKey, 0, 1)).andReturn(KeyValueIterators.< IWindowed<Bytes>, byte[] > emptyIterator());

//        Init();

//        store.findSessions(bytesKey, bytesKey, 0, 1);
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldFlushUnderlyingStore()
//    {
//        inner.Flush();
//        EasyMock.expectLastCall();

//        Init();

//        store.Flush();
//        EasyMock.verify(inner);
//    }

//    [Fact]
//    public void ShouldCloseUnderlyingStore()
//    {
//        inner.Close();
//        EasyMock.expectLastCall();

//        Init();

//        store.Close();
//        EasyMock.verify(inner);
//    }


//}}
///*






//*

//*





//*/








































