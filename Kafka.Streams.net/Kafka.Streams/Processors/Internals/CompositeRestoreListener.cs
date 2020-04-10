using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class CompositeRestoreListener : IRecordBatchingStateRestoreCallback, IStateRestoreListener
    {
        public static NoOpStateRestoreListener NO_OP_STATE_RESTORE_LISTENER { get; } = new NoOpStateRestoreListener();
        private readonly IRecordBatchingStateRestoreCallback internalBatchingRestoreCallback;
        private readonly IStateRestoreListener storeRestoreListener;
        private IStateRestoreListener userRestoreListener = NO_OP_STATE_RESTORE_LISTENER;

        public CompositeRestoreListener(IStateRestoreCallback stateRestoreCallback)
        {

            if (stateRestoreCallback is IStateRestoreListener)
            {
                this.storeRestoreListener = (IStateRestoreListener)stateRestoreCallback;
            }
            else
            {

                this.storeRestoreListener = NO_OP_STATE_RESTORE_LISTENER;
            }

            this.internalBatchingRestoreCallback = StateRestoreCallbackAdapter.Adapt(stateRestoreCallback);
        }

        /**
         * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
         * {@link StateRestoreListener#onRestoreStart(TopicPartition, string, long, long)}
         */
        public void OnRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset)
        {
            this.userRestoreListener.OnRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
            this.storeRestoreListener.OnRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
        }

        /**
         * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
         * {@link StateRestoreListener#onBatchRestored(TopicPartition, string, long, long)}
         */
        public void OnBatchRestored(
            TopicPartition topicPartition,
            string storeName,
            long batchEndOffset,
            long numRestored)
        {
            this.userRestoreListener.OnBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
            this.storeRestoreListener.OnBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
        }

        /**
         * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
         * {@link StateRestoreListener#onRestoreEnd(TopicPartition, string, long)}
         */
        public void OnRestoreEnd(
            TopicPartition topicPartition,
            string storeName,
            long totalRestored)
        {
            this.userRestoreListener.OnRestoreEnd(topicPartition, storeName, totalRestored);
            this.storeRestoreListener.OnRestoreEnd(topicPartition, storeName, totalRestored);
        }


        public void RestoreBatch(List<ConsumeResult<byte[], byte[]>> records)
        {
            this.internalBatchingRestoreCallback.RestoreBatch(records);
        }

        public void SetUserRestoreListener(IStateRestoreListener userRestoreListener)
        {
            if (userRestoreListener != null)
            {
                this.userRestoreListener = userRestoreListener;
            }
        }

        public void RestoreAll(List<KeyValuePair<byte[], byte[]>> records)
        {
            throw new InvalidOperationException();
        }

        public void Restore(byte[] key,
                            byte[] value)
        {
            throw new InvalidOperationException("Single restore functionality shouldn't be called directly but "
                                                        + "through the delegated StateRestoreCallback instance");
        }
    }
}
