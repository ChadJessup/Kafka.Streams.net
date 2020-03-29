/*






 *

 *





 */




















public class StateRestoreCallbackAdapterTest {
    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldThrowOnRestoreAll() {
        adapt(mock(StateRestoreCallback)).restoreAll(null);
    }

    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldThrowOnRestore() {
        adapt(mock(StateRestoreCallback)).restore(null, null);
    }

    [Xunit.Fact]
    public void shouldPassRecordsThrough() {
        ArrayList<ConsumeResult<byte[], byte[]>> actual = new ArrayList<>();
        RecordBatchingStateRestoreCallback callback = actual::addAll;

        RecordBatchingStateRestoreCallback adapted = adapt(callback);

        byte[] key1 = {1};
        byte[] value1 = {2};
        byte[] key2 = {3};
        byte[] value2 = {4};

        List<ConsumeResult<byte[], byte[]>> recordList = asList(
            new ConsumeResult<>("topic1", 0, 0L, key1, value1),
            new ConsumeResult<>("topic2", 1, 1L, key2, value2)
        );

        adapted.restoreBatch(recordList);

        validate(actual, recordList);
    }

    [Xunit.Fact]
    public void shouldConvertToKeyValueBatches() {
        ArrayList<KeyValuePair<byte[], byte[]>> actual = new ArrayList<>();
        BatchingStateRestoreCallback callback = new BatchingStateRestoreCallback() {
            
            public void restoreAll(Collection<KeyValuePair<byte[], byte[]>> records) {
                actual.addAll(records);
            }

            
            public void restore(byte[] key, byte[] value) {
                // unreachable
            }
        };

        RecordBatchingStateRestoreCallback adapted = adapt(callback);

        byte[] key1 = {1};
        byte[] value1 = {2};
        byte[] key2 = {3};
        byte[] value2 = {4};
        adapted.restoreBatch(asList(
            new ConsumeResult<>("topic1", 0, 0L, key1, value1),
            new ConsumeResult<>("topic2", 1, 1L, key2, value2)
        ));

        Assert.Equal(
            actual,
            is(asList(
                new KeyValuePair<>(key1, value1),
                new KeyValuePair<>(key2, value2)
            ))
        );
    }

    [Xunit.Fact]
    public void shouldConvertToKeyValue() {
        ArrayList<KeyValuePair<byte[], byte[]>> actual = new ArrayList<>();
        StateRestoreCallback callback = (key, value) => actual.add(new KeyValuePair<>(key, value));

        RecordBatchingStateRestoreCallback adapted = adapt(callback);

        byte[] key1 = {1};
        byte[] value1 = {2};
        byte[] key2 = {3};
        byte[] value2 = {4};
        adapted.restoreBatch(asList(
            new ConsumeResult<>("topic1", 0, 0L, key1, value1),
            new ConsumeResult<>("topic2", 1, 1L, key2, value2)
        ));

        Assert.Equal(
            actual,
            is(asList(
                new KeyValuePair<>(key1, value1),
                new KeyValuePair<>(key2, value2)
            ))
        );
    }

    private void validate(List<ConsumeResult<byte[], byte[]>> actual,
                          List<ConsumeResult<byte[], byte[]>> expected) {
        Assert.Equal(actual.Count, is(expected.Count));
        for (int i = 0; i < actual.Count; i++) {
            ConsumeResult<byte[], byte[]> actual1 = actual.get(i);
            ConsumeResult<byte[], byte[]> expected1 = expected.get(i);
            Assert.Equal(actual1.topic(), is(expected1.topic()));
            Assert.Equal(actual1.partition(), is(expected1.partition()));
            Assert.Equal(actual1.Offset, is(expected1.Offset));
            Assert.Equal(actual1.Key, is(expected1.Key));
            Assert.Equal(actual1.Value, is(expected1.Value));
            Assert.Equal(actual1.Timestamp, is(expected1.Timestamp));
            Assert.Equal(actual1.headers(), is(expected1.headers()));
        }
    }


}