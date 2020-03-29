using Confluent.Kafka;

namespace Kafka.Streams.State.Interfaces
{
    /**
     * Class for listening to various states of the restoration process of a IStateStore.
     *
     * When calling {@link org.apache.kafka.streams.KafkaStreams#setGlobalStateRestoreListener(StateRestoreListener)}
     * the passed instance is expected to be stateless since the {@code StateRestoreListener} is shared
     * across all {@link org.apache.kafka.streams.processor.Internals.KafkaStreamThread} instances.
     *
     * Users desiring stateful operations will need to provide synchronization internally in
     * the {@code StateRestorerListener} implementation.
     *
     * When used for monitoring a single {@link IStateStore} using either {@link AbstractNotifyingRestoreCallback} or
     * {@link AbstractNotifyingBatchingRestoreCallback} no synchronization is necessary
     * as each KafkaStreamThread has its own IStateStore instance.
     *
     * Incremental updates are exposed so users can estimate how much progress has been made.
     */
    public interface IStateRestoreListener
    {
        /**
         * Method called at the very beginning of {@link IStateStore} restoration.
         *
         * @param topicPartition the TopicPartition containing the values to restore
         * @param storeName      the name of the store undergoing restoration
         * @param startingOffset the starting offset of the entire restoration process for this TopicPartition
         * @param endingOffset   the exclusive ending offset of the entire restoration process for this TopicPartition
         */
        void OnRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset);

        /**
         * Method called after restoring a batch of records.  In this case the maximum size of the batch is whatever
         * the value of the MAX_POLL_RECORDS is set to.
         *
         * This method is called after restoring each batch and it is advised to keep processing to a minimum.
         * Any heavy processing will hold up recovering the next batch, hence slowing down the restore process as a
         * whole.
         *
         * If you need to do any extended processing or connecting to an external service consider doing so asynchronously.
         *
         * @param topicPartition the TopicPartition containing the values to restore
         * @param storeName the name of the store undergoing restoration
         * @param batchEndOffset the inclusive ending offset for the current restored batch for this TopicPartition
         * @param numRestored the total number of records restored in this batch for this TopicPartition
         */
        void OnBatchRestored(
            TopicPartition topicPartition,
            string storeName,
            long batchEndOffset,
            long numRestored);

        /**
         * Method called when restoring the {@link IStateStore} is complete.
         *
         * @param topicPartition the TopicPartition containing the values to restore
         * @param storeName the name of the store just restored
         * @param totalRestored the total number of records restored for this TopicPartition
         */
        void OnRestoreEnd(
            TopicPartition topicPartition,
            string storeName,
            long totalRestored);
    }
}