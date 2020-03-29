/*






 *

 *





 */
























public class CompositeRestoreListenerTest {

    private MockStateRestoreCallback stateRestoreCallback = new MockStateRestoreCallback();
    private MockBatchingStateRestoreListener batchingStateRestoreCallback = new MockBatchingStateRestoreListener();
    private MockNoListenBatchingStateRestoreCallback
        noListenBatchingStateRestoreCallback =
        new MockNoListenBatchingStateRestoreCallback();
    private MockStateRestoreListener reportingStoreListener = new MockStateRestoreListener();
    private byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    private byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    private Collection<KeyValuePair<byte[], byte[]>> records = Collections.singletonList(KeyValuePair.Create(key, value));
    private Collection<ConsumeResult<byte[], byte[]>> consumerRecords = Collections.singletonList(
        new ConsumeResult<>("", 0, 0L, key, value)
    );
    private string storeName = "test_store";
    private long startOffset = 0L;
    private long endOffset = 1L;
    private long batchOffset = 1L;
    private long numberRestored = 1L;
    private TopicPartition topicPartition = new TopicPartition("testTopic", 1);

    private CompositeRestoreListener compositeRestoreListener;


    [Xunit.Fact]
    public void shouldRestoreInNonBatchMode() {
        setUpCompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.restoreBatch(consumerRecords);
        Assert.Equal(stateRestoreCallback.restoredKey, is(key));
        Assert.Equal(stateRestoreCallback.restoredValue, is(value));
    }

    [Xunit.Fact]
    public void shouldRestoreInBatchMode() {
        setUpCompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.restoreBatch(consumerRecords);
        Assert.Equal(batchingStateRestoreCallback.getRestoredRecords(), is(records));
    }

    [Xunit.Fact]
    public void shouldNotifyRestoreStartNonBatchMode() {
        setUpCompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
        assertStateRestoreListenerOnStartNotification(stateRestoreCallback);
        assertStateRestoreListenerOnStartNotification(reportingStoreListener);
    }

    [Xunit.Fact]
    public void shouldNotifyRestoreStartBatchMode() {
        setUpCompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
        assertStateRestoreListenerOnStartNotification(batchingStateRestoreCallback);
        assertStateRestoreListenerOnStartNotification(reportingStoreListener);
    }

    [Xunit.Fact]
    public void shouldNotifyRestoreProgressNonBatchMode() {
        setUpCompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.onBatchRestored(topicPartition, storeName, endOffset, numberRestored);
        assertStateRestoreListenerOnBatchCompleteNotification(stateRestoreCallback);
        assertStateRestoreListenerOnBatchCompleteNotification(reportingStoreListener);
    }

    [Xunit.Fact]
    public void shouldNotifyRestoreProgressBatchMode() {
        setUpCompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.onBatchRestored(topicPartition, storeName, endOffset, numberRestored);
        assertStateRestoreListenerOnBatchCompleteNotification(batchingStateRestoreCallback);
        assertStateRestoreListenerOnBatchCompleteNotification(reportingStoreListener);
    }

    [Xunit.Fact]
    public void shouldNotifyRestoreEndInNonBatchMode() {
        setUpCompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);
        assertStateRestoreOnEndNotification(stateRestoreCallback);
        assertStateRestoreOnEndNotification(reportingStoreListener);
    }

    [Xunit.Fact]
    public void shouldNotifyRestoreEndInBatchMode() {
        setUpCompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);
        assertStateRestoreOnEndNotification(batchingStateRestoreCallback);
        assertStateRestoreOnEndNotification(reportingStoreListener);
    }

    [Xunit.Fact]
    public void shouldHandleNullReportStoreListener() {
        compositeRestoreListener = new CompositeRestoreListener(batchingStateRestoreCallback);
        compositeRestoreListener.setUserRestoreListener(null);

        compositeRestoreListener.restoreBatch(consumerRecords);
        compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
        compositeRestoreListener.onBatchRestored(topicPartition, storeName, batchOffset, numberRestored);
        compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);

        Assert.Equal(batchingStateRestoreCallback.getRestoredRecords(), is(records));
        assertStateRestoreOnEndNotification(batchingStateRestoreCallback);
    }

    [Xunit.Fact]
    public void shouldHandleNoRestoreListener() {
        compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
        compositeRestoreListener.setUserRestoreListener(null);

        compositeRestoreListener.restoreBatch(consumerRecords);
        compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
        compositeRestoreListener.onBatchRestored(topicPartition, storeName, batchOffset, numberRestored);
        compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);

        Assert.Equal(noListenBatchingStateRestoreCallback.restoredRecords, is(records));
    }

    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldThrowExceptionWhenSinglePutDirectlyCalled() {
        compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
        compositeRestoreListener.restore(key, value);
    }

    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldThrowExceptionWhenRestoreAllDirectlyCalled() {
        compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
        compositeRestoreListener.restoreAll(Collections.emptyList());
    }

    private void assertStateRestoreListenerOnStartNotification(MockStateRestoreListener restoreListener) {
        Assert.True(restoreListener.storeNameCalledStates.containsKey(RESTORE_START));
        Assert.Equal(restoreListener.restoreTopicPartition, is(topicPartition));
        Assert.Equal(restoreListener.restoreStartOffset, is(startOffset));
        Assert.Equal(restoreListener.restoreEndOffset, is(endOffset));
    }

    private void assertStateRestoreListenerOnBatchCompleteNotification(MockStateRestoreListener restoreListener) {
        Assert.True(restoreListener.storeNameCalledStates.containsKey(RESTORE_BATCH));
        Assert.Equal(restoreListener.restoreTopicPartition, is(topicPartition));
        Assert.Equal(restoreListener.restoredBatchOffset, is(batchOffset));
        Assert.Equal(restoreListener.numBatchRestored, is(numberRestored));
    }

    private void assertStateRestoreOnEndNotification(MockStateRestoreListener restoreListener) {
        Assert.True(restoreListener.storeNameCalledStates.containsKey(RESTORE_END));
        Assert.Equal(restoreListener.restoreTopicPartition, is(topicPartition));
        Assert.Equal(restoreListener.totalNumRestored, is(numberRestored));
    }


    private void setUpCompositeRestoreListener(StateRestoreCallback stateRestoreCallback) {
        compositeRestoreListener = new CompositeRestoreListener(stateRestoreCallback);
        compositeRestoreListener.setUserRestoreListener(reportingStoreListener);
    }


    private static class MockStateRestoreCallback : MockStateRestoreListener : StateRestoreCallback {

        byte[] restoredKey;
        byte[] restoredValue;

        
        public void restore(byte[] key, byte[] value) {
            restoredKey = key;
            restoredValue = value;
        }
    }

    private static class MockNoListenBatchingStateRestoreCallback : BatchingStateRestoreCallback {

        Collection<KeyValuePair<byte[], byte[]>> restoredRecords;

        
        public void restoreAll(Collection<KeyValuePair<byte[], byte[]>> records) {
            restoredRecords = records;
        }

        
        public void restore(byte[] key, byte[] value) {
            throw new IllegalStateException("Should not be called");

        }
    }

}
