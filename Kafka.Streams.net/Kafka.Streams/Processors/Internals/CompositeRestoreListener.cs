using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class CompositeRestoreListener : IRecordBatchingStateRestoreCallback, IStateRestoreListener
    {
        public static NoOpStateRestoreListener NO_OP_STATE_RESTORE_LISTENER = new NoOpStateRestoreListener();
        private readonly IRecordBatchingStateRestoreCallback internalBatchingRestoreCallback;
        private readonly IStateRestoreListener storeRestoreListener;
        private IStateRestoreListener userRestoreListener = NO_OP_STATE_RESTORE_LISTENER;

        public CompositeRestoreListener(IStateRestoreCallback stateRestoreCallback)
        {

            if (stateRestoreCallback is IStateRestoreListener)
            {
                storeRestoreListener = (IStateRestoreListener)stateRestoreCallback;
            }
            else
            {

                storeRestoreListener = NO_OP_STATE_RESTORE_LISTENER;
            }

            internalBatchingRestoreCallback = StateRestoreCallbackAdapter.adapt(stateRestoreCallback);
        }

        /**
         * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
         * {@link StateRestoreListener#onRestoreStart(TopicPartition, string, long, long)}
         */
        public void onRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset)
        {
            userRestoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
            storeRestoreListener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset);
        }

        /**
         * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
         * {@link StateRestoreListener#onBatchRestored(TopicPartition, string, long, long)}
         */
        public void onBatchRestored(
            TopicPartition topicPartition,
            string storeName,
            long batchEndOffset,
            long numRestored)
        {
            userRestoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
            storeRestoreListener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored);
        }

        /**
         * @throws StreamsException if user provided {@link StateRestoreListener} raises an exception in
         * {@link StateRestoreListener#onRestoreEnd(TopicPartition, string, long)}
         */
        public void onRestoreEnd(
            TopicPartition topicPartition,
            string storeName,
            long totalRestored)
        {
            userRestoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);
            storeRestoreListener.onRestoreEnd(topicPartition, storeName, totalRestored);
        }


        public void restoreBatch(List<ConsumeResult<byte[], byte[]>> records)
        {
            internalBatchingRestoreCallback.restoreBatch(records);
        }

        public void setUserRestoreListener(IStateRestoreListener userRestoreListener)
        {
            if (userRestoreListener != null)
            {
                this.userRestoreListener = userRestoreListener;
            }
        }

        public void restoreAll(List<KeyValuePair<byte[], byte[]>> records)
        {
            throw new InvalidOperationException();
        }

        public void restore(byte[] key,
                            byte[] value)
        {
            throw new InvalidOperationException("Single restore functionality shouldn't be called directly but "
                                                        + "through the delegated StateRestoreCallback instance");
        }
    }
}