using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class CompositeRestoreListenerTest
    {

        private readonly MockStateRestoreCallback stateRestoreCallback = new MockStateRestoreCallback();
        private MockBatchingStateRestoreListener batchingStateRestoreCallback = new MockBatchingStateRestoreListener();
        private readonly MockNoListenBatchingStateRestoreCallback
            noListenBatchingStateRestoreCallback =
            new MockNoListenBatchingStateRestoreCallback();
        private MockStateRestoreListener reportingStoreListener = new MockStateRestoreListener();
        private readonly byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        private readonly byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        private Collection<KeyValuePair<byte[], byte[]>> records = Collections.singletonList(KeyValuePair.Create(key, value));
        private Collection<ConsumeResult<byte[], byte[]>> consumerRecords = Collections.singletonList(
            new ConsumeResult<byte[], byte[]>("", 0, 0L, key, value)
        );
        private readonly string storeName = "test_store";
        private readonly long startOffset = 0L;
        private readonly long endOffset = 1L;
        private readonly long batchOffset = 1L;
        private readonly long numberRestored = 1L;
        private TopicPartition topicPartition = new TopicPartition("testTopic", 1);

        private CompositeRestoreListener compositeRestoreListener;


        [Fact]
        public void ShouldRestoreInNonBatchMode()
        {
            setUpCompositeRestoreListener(stateRestoreCallback);
            compositeRestoreListener.restoreBatch(consumerRecords);
            Assert.Equal(stateRestoreCallback.restoredKey, key);
            Assert.Equal(stateRestoreCallback.restoredValue, value);
        }

        [Fact]
        public void ShouldRestoreInBatchMode()
        {
            setUpCompositeRestoreListener(batchingStateRestoreCallback);
            compositeRestoreListener.restoreBatch(consumerRecords);
            Assert.Equal(batchingStateRestoreCallback.getRestoredRecords(), records);
        }

        [Fact]
        public void ShouldNotifyRestoreStartNonBatchMode()
        {
            setUpCompositeRestoreListener(stateRestoreCallback);
            compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
            assertStateRestoreListenerOnStartNotification(stateRestoreCallback);
            AssertStateRestoreListenerOnStartNotification(reportingStoreListener);
        }

        [Fact]
        public void ShouldNotifyRestoreStartBatchMode()
        {
            setUpCompositeRestoreListener(batchingStateRestoreCallback);
            compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
            assertStateRestoreListenerOnStartNotification(batchingStateRestoreCallback);
            AssertStateRestoreListenerOnStartNotification(reportingStoreListener);
        }

        [Fact]
        public void ShouldNotifyRestoreProgressNonBatchMode()
        {
            setUpCompositeRestoreListener(stateRestoreCallback);
            compositeRestoreListener.onBatchRestored(topicPartition, storeName, endOffset, numberRestored);
            assertStateRestoreListenerOnBatchCompleteNotification(stateRestoreCallback);
            AssertStateRestoreListenerOnBatchCompleteNotification(reportingStoreListener);
        }

        [Fact]
        public void ShouldNotifyRestoreProgressBatchMode()
        {
            setUpCompositeRestoreListener(batchingStateRestoreCallback);
            compositeRestoreListener.onBatchRestored(topicPartition, storeName, endOffset, numberRestored);
            assertStateRestoreListenerOnBatchCompleteNotification(batchingStateRestoreCallback);
            AssertStateRestoreListenerOnBatchCompleteNotification(reportingStoreListener);
        }

        [Fact]
        public void ShouldNotifyRestoreEndInNonBatchMode()
        {
            setUpCompositeRestoreListener(stateRestoreCallback);
            compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);
            assertStateRestoreOnEndNotification(stateRestoreCallback);
            AssertStateRestoreOnEndNotification(reportingStoreListener);
        }

        [Fact]
        public void ShouldNotifyRestoreEndInBatchMode()
        {
            setUpCompositeRestoreListener(batchingStateRestoreCallback);
            compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);
            assertStateRestoreOnEndNotification(batchingStateRestoreCallback);
            AssertStateRestoreOnEndNotification(reportingStoreListener);
        }

        [Fact]
        public void ShouldHandleNullReportStoreListener()
        {
            compositeRestoreListener = new CompositeRestoreListener(batchingStateRestoreCallback);
            compositeRestoreListener.SetUserRestoreListener(null);

            compositeRestoreListener.restoreBatch(consumerRecords);
            compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
            compositeRestoreListener.onBatchRestored(topicPartition, storeName, batchOffset, numberRestored);
            compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);

            Assert.Equal(batchingStateRestoreCallback.getRestoredRecords(), records);
            assertStateRestoreOnEndNotification(batchingStateRestoreCallback);
        }

        [Fact]
        public void ShouldHandleNoRestoreListener()
        {
            compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
            compositeRestoreListener.SetUserRestoreListener(null);

            compositeRestoreListener.restoreBatch(consumerRecords);
            compositeRestoreListener.onRestoreStart(topicPartition, storeName, startOffset, endOffset);
            compositeRestoreListener.onBatchRestored(topicPartition, storeName, batchOffset, numberRestored);
            compositeRestoreListener.onRestoreEnd(topicPartition, storeName, numberRestored);

            Assert.Equal(noListenBatchingStateRestoreCallback.restoredRecords, records);
        }

        [Fact]// (expected = UnsupportedOperationException)
        public void ShouldThrowExceptionWhenSinglePutDirectlyCalled()
        {
            compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
            compositeRestoreListener.Restore(key, value);
        }

        [Fact]// (expected = UnsupportedOperationException)
        public void ShouldThrowExceptionWhenRestoreAllDirectlyCalled()
        {
            compositeRestoreListener = new CompositeRestoreListener(noListenBatchingStateRestoreCallback);
            compositeRestoreListener.restoreAll(Collections.emptyList());
        }

        private void AssertStateRestoreListenerOnStartNotification(MockStateRestoreListener restoreListener)
        {
            Assert.True(restoreListener.storeNameCalledStates.ContainsKey(RESTORE_START));
            Assert.Equal(restoreListener.restoreTopicPartition, topicPartition);
            Assert.Equal(restoreListener.restoreStartOffset, startOffset);
            Assert.Equal(restoreListener.restoreEndOffset, endOffset);
        }

        private void AssertStateRestoreListenerOnBatchCompleteNotification(MockStateRestoreListener restoreListener)
        {
            Assert.True(restoreListener.storeNameCalledStates.ContainsKey(RESTORE_BATCH));
            Assert.Equal(restoreListener.restoreTopicPartition, topicPartition);
            Assert.Equal(restoreListener.restoredBatchOffset, batchOffset);
            Assert.Equal(restoreListener.numBatchRestored, numberRestored);
        }

        private void AssertStateRestoreOnEndNotification(MockStateRestoreListener restoreListener)
        {
            Assert.True(restoreListener.storeNameCalledStates.ContainsKey(RESTORE_END));
            Assert.Equal(restoreListener.restoreTopicPartition, topicPartition);
            Assert.Equal(restoreListener.totalNumRestored, numberRestored);
        }

        private void SetUpCompositeRestoreListener(IStateRestoreCallback stateRestoreCallback)
        {
            compositeRestoreListener = new CompositeRestoreListener(stateRestoreCallback);
            compositeRestoreListener.SetUserRestoreListener(reportingStoreListener);
        }


        private class MockStateRestoreCallback : MockStateRestoreListener, IStateRestoreCallback
        {

            byte[] restoredKey;
            byte[] restoredValue;


            public void Restore(byte[] key, byte[] value)
            {
                restoredKey = key;
                restoredValue = value;
            }
        }

        private class MockNoListenBatchingStateRestoreCallback : BatchingStateRestoreCallback
        {

            Collection<KeyValuePair<byte[], byte[]>> restoredRecords;


            public void RestoreAll(Collection<KeyValuePair<byte[], byte[]>> records)
            {
                restoredRecords = records;
            }

            public void Restore(byte[] key, byte[] value)
            {
                throw new IllegalStateException("Should not be called");

            }
        }
    }
}
