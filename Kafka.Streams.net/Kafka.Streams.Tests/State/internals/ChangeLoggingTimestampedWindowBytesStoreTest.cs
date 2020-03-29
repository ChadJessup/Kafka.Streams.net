/*






 *

 *





 */



























public class ChangeLoggingTimestampedWindowBytesStoreTest {

    private TaskId taskId = new TaskId(0, 0);
    private Dictionary<object, ValueAndTimestamp<object>> sent = new HashMap<>();
    private NoOpRecordCollector collector = new NoOpRecordCollector() {
        
        public void Send<K, V>(string topic,
                                K key,
                                V value,
                                Headers headers,
                                int partition,
                                long timestamp,
                                Serializer<K> keySerializer,
                                Serializer<V> valueSerializer) {
            sent.put(key, ValueAndTimestamp.make(value, timestamp));
        }
    };

    private byte[] value = {0};
    private byte[] valueAndTimestamp = {0, 0, 0, 0, 0, 0, 0, 42, 0};
    private Bytes bytesKey = Bytes.wrap(value);

    @Mock(type = MockType.NICE)
    private WindowStore<Bytes, byte[]> inner;
    @Mock(type = MockType.NICE)
    private ProcessorContextImpl context;
    private ChangeLoggingTimestampedWindowBytesStore store;


    
    public void SetUp() {
        store = new ChangeLoggingTimestampedWindowBytesStore(inner, false);
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
        inner.put(bytesKey, valueAndTimestamp, 0);
        EasyMock.expectLastCall();

        init();

        store.put(bytesKey, valueAndTimestamp);

        assertArrayEquals(
            value,
            (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0)).Value);
        Assert.Equal(
            42L,
            sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 0)).Timestamp);
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldDelegateToUnderlyingStoreWhenFetching() {
        EasyMock
            .expect(inner.fetch(bytesKey, 0, 10))
            .andReturn(KeyValueIterators.emptyWindowStoreIterator());

        init();

        store.fetch(bytesKey, ofEpochMilli(0), ofEpochMilli(10));
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldDelegateToUnderlyingStoreWhenFetchingRange() {
        EasyMock
            .expect(inner.fetch(bytesKey, bytesKey, 0, 1))
            .andReturn(KeyValueIterators.emptyIterator());

        init();

        store.fetch(bytesKey, bytesKey, ofEpochMilli(0), ofEpochMilli(1));
        EasyMock.verify(inner);
    }

    [Xunit.Fact]
    public void ShouldRetainDuplicatesWhenSet() {
        store = new ChangeLoggingTimestampedWindowBytesStore(inner, true);
        inner.put(bytesKey, valueAndTimestamp, 0);
        EasyMock.expectLastCall().times(2);

        init();
        store.put(bytesKey, valueAndTimestamp);
        store.put(bytesKey, valueAndTimestamp);

        assertArrayEquals(
            value,
            (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1)).Value);
        Assert.Equal(
            42L,
            sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 1)).Timestamp);
        assertArrayEquals(
            value,
            (byte[]) sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2)).Value);
        Assert.Equal(
            42L,
            sent.get(WindowKeySchema.toStoreKeyBinary(bytesKey, 0, 2)).Timestamp);

        EasyMock.verify(inner);
    }

}
