using Confluent.Kafka;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors
{
    /**
     * Abstract implementation of the  {@link BatchingStateRestoreCallback} used for batch restoration operations.
     *
     * Includes default no-op methods of the {@link StateRestoreListener} {@link StateRestoreListener#onRestoreStart(TopicPartition, string, long, long)},
     * {@link StateRestoreListener#onBatchRestored(TopicPartition, string, long, long)}, and {@link StateRestoreListener#onRestoreEnd(TopicPartition, string, long)}.
     */
    public abstract class AbstractNotifyingBatchingRestoreCallback :
        IBatchingStateRestoreCallback, IStateRestoreListener
    {
        /**
         * Single put restore operations not supported, please use {@link AbstractNotifyingRestoreCallback}
         * or {@link StateRestoreCallback} instead for single action restores.
         */

        public virtual void restore(byte[] key, byte[] value)
        {
            throw new InvalidOperationException("Single restore not supported");
        }

        /**
         * @see StateRestoreListener#onRestoreStart(TopicPartition, string, long, long)
         *
         * This method does nothing by default; if desired, sues should override it with custom functionality.
         *
         */
        public virtual void onRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset)
        {
        }

        /**
         * @see StateRestoreListener#onBatchRestored(TopicPartition, string, long, long)
         *
         * This method does nothing by default; if desired, sues should override it with custom functionality.
         *
         */
        public virtual void onBatchRestored(
            TopicPartition topicPartition,
            string storeName,
            long batchEndOffset,
            long numRestored)
        {
        }

        /**
         * @see StateRestoreListener#onRestoreEnd(TopicPartition, string, long)
         *
         * This method does nothing by default; if desired, sues should override it with custom functionality.
         *
         */
        public virtual void onRestoreEnd(
            TopicPartition topicPartition,
            string storeName,
            long totalRestored)
        {

        }

        public virtual void restoreAll(List<KeyValuePair<byte[], byte[]>> records)
        {
        }
    }
}