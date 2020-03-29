/*






 *

 *





 */

























public class ChangeLoggingWindowBytesStoreTest {

    private TaskId taskId = new TaskId(0, 0);
    private Dictionary<object, object> sent = new HashMap<>();
    private NoOpRecordCollector collector = new NoOpRecordCollector() {
        
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

    private byte[] value = {0};
    private Bytes bytesKey = Bytes.wrap(value);

    @Mock(type = MockType.NICE)
    private WindowStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContextImpl context;
    private ChangeLoggingWindowBytesStore store;


    
    public void setUp() {
        store = new ChangeLoggingWindowBytesStore(inner, false);
    }

    private void init() {
        EasyMock.expect(context.taskId()).andReturn(taskId);
        EasyMock.expect(context.recordCollector()).andReturn(collector);
        inner.init(context, store);
        EasyMock.expectLastCall();
        EasyMock.replay(inner, context);

        store.init(context, store);
    }

    [Xunit.Fact]
    public void shouldLogPuts() {
        inner.put(bytesKey, value, 0);
        EasyMock.expectLastCall();

        init();

        store.put(bytesKey, value);

        assertArrayEquals(
            value,
            (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0)));
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void shouldDelegateToUnderlyingStoreWhenFetching() {
        EasyMock
            .expect(inner.fetch(bytesKey, 0, 10))
            .andReturn(KeyValueIterators.emptyWindowStoreIterator());

        init();

        store.fetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10));
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void shouldDelegateToUnderlyingStoreWhenFetchingRange() {
        EasyMock
            .expect(inner.fetch(bytesKey, bytesKey, 0, 1))
            .andReturn(KeyValueIterators.emptyIterator());

        init();

        store.fetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1));
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void shouldRetainDuplicatesWhenSet() {
        store = new ChangeLoggingWindowBytesStore(inner, true);
        inner.put(bytesKey, value, 0);
        EasyMock.expectLastCall().times(2);

        init();
        store.put(bytesKey, value);
        store.put(bytesKey, value);

        assertArrayEquals(value, (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1)));
        assertArrayEquals(value, (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2)));

        EasyMock.verify(inner);
    }

}
