/*






 *

 *





 */



























public class ChangeLoggingSessionBytesStoreTest {

    private TaskId taskId = new TaskId(0, 0);
    private Dictionary<object, object> sent = new HashMap<>();
    private NoOpRecordCollector collector = new NoOpRecordCollector() {
        
        public void Send<K, V>(string topic,
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

    @Mock(type = MockType.NICE)
    private SessionStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContextImpl context;

    private ChangeLoggingSessionBytesStore store;
    private byte[] value1 = {0};
    private Bytes bytesKey = Bytes.wrap(value1);
    private Windowed<Bytes> key1 = new Windowed<>(bytesKey, new SessionWindow(0, 0));

    
    public void SetUp() {
        store = new ChangeLoggingSessionBytesStore(inner);
    }

    private void Init() {
        EasyMock.expect(context.taskId()).andReturn(taskId);
        EasyMock.expect(context.recordCollector()).andReturn(collector);
        inner.init(context, store);
        EasyMock.expectLastCall();
        EasyMock.replay(inner, context);

        store.init(context, store);
    }

    [Xunit.Fact]
    public void ShouldLogPuts() {
        inner.put(key1, value1);
        EasyMock.expectLastCall();

        init();

        store.put(key1, value1);

        assertArrayEquals(value1, (byte[]) sent.get(SessionKeySchema.toBinary(key1)));
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldLogRemoves() {
        inner.remove(key1);
        EasyMock.expectLastCall();

        init();
        store.remove(key1);

        Bytes binaryKey = SessionKeySchema.toBinary(key1);
        Assert.True(sent.containsKey(binaryKey));
        assertNull(sent.get(binaryKey));
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldDelegateToUnderlyingStoreWhenFetching() {
        EasyMock.expect(inner.fetch(bytesKey)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());

        init();

        store.fetch(bytesKey);
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldDelegateToUnderlyingStoreWhenFetchingRange() {
        EasyMock.expect(inner.fetch(bytesKey, bytesKey)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());

        init();

        store.fetch(bytesKey, bytesKey);
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldDelegateToUnderlyingStoreWhenFindingSessions() {
        EasyMock.expect(inner.findSessions(bytesKey, 0, 1)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());

        init();

        store.findSessions(bytesKey, 0, 1);
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldDelegateToUnderlyingStoreWhenFindingSessionRange() {
        EasyMock.expect(inner.findSessions(bytesKey, bytesKey, 0, 1)).andReturn(KeyValueIterators.<Windowed<Bytes>, byte[]>emptyIterator());

        init();

        store.findSessions(bytesKey, bytesKey, 0, 1);
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldFlushUnderlyingStore() {
        inner.flush();
        EasyMock.expectLastCall();

        init();

        store.flush();
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldCloseUnderlyingStore() {
        inner.close();
        EasyMock.expectLastCall();

        init();

        store.close();
        EasyMock.verify(inner);
    }


}