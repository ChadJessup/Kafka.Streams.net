using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Interfaces
{
    /**
     * A callback interface that the user can implement to trigger custom actions when the set of partitions assigned to the
     * consumer changes.
     * <p>
     * This is applicable when the consumer is having Kafka auto-manage group membership. If the consumer directly assigns partitions,
     * those partitions will never be reassigned and this callback is not applicable.
     * <p>
     * When Kafka is managing the group membership, a partition re-assignment will be triggered any time the members of the group change or the subscription
     * of the members changes. This can occur when processes die, new process instances are added or old instances come back to life after failure.
     * Rebalances can also be triggered by changes affecting the subscribed topics (e.g. when the number of partitions is
     * administratively adjusted).
     * <p>
     * There are many uses for this functionality. One common use is saving offsets in a custom store. By saving offsets in
     * the {@link #onPartitionsRevoked(Collection)} call we can ensure that any time partition assignment changes
     * the offset gets saved.
     * <p>
     * Another use is flushing out any kind of cache of intermediate results the consumer may be keeping. For example,
     * consider a case where the consumer is subscribed to a topic containing user page views, and the goal is to count the
     * number of page views per user for each five minute window. Let's say the topic is partitioned by the user id so that
     * All events for a particular user go to a single consumer instance. The consumer can keep in memory a running
     * tally of actions per user and only Flush these out to a remote data store when its cache gets too big. However if a
     * partition is reassigned it may want to automatically trigger a Flush of this cache, before the new owner takes over
     * consumption.
     * <p>
     * This callback will only execute in the user thread as part of the {@link Consumer#poll(java.time.TimeSpan) poll(long)} call
     * whenever partition assignment changes.
     * <p>
     * It is guaranteed that All consumer processes will invoke {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} prior to
     * any process invoking {@link #OnPartitionsAssigned(Collection) OnPartitionsAssigned}. So if offsets or other state is saved in the
     * {@link #onPartitionsRevoked(Collection) onPartitionsRevoked} call it is guaranteed to be saved by the time the process taking over that
     * partition has their {@link #OnPartitionsAssigned(Collection) OnPartitionsAssigned} callback called to load the state.
     * <p>
     * Here is pseudo-code for a callback implementation for saving offsets:
     * <pre>
     * {@code
     *   public class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
     *       private Consumer<?,?> consumer;
     *
     *       public SaveOffsetsOnRebalance(Consumer<?,?> consumer) {
     *           this.consumer = consumer;
     *       }
     *
     *       public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
     *           // save the offsets in an external store using some custom code not described here
     *           for(TopicPartition partition: partitions)
     *              saveOffsetInExternalStore(consumer.position(partition));
     *       }
     *
     *       public void OnPartitionsAssigned(Collection<TopicPartition> partitions) {
     *           // read the offsets from an external store using some custom code not described here
     *           for(TopicPartition partition: partitions)
     *              consumer.seek(partition, readOffsetFromExternalStore(partition));
     *       }
     *   }
     * }
     * </pre>
     */
    public interface IConsumerRebalanceListener
    {
        /**
         * A callback method the user can implement to provide handling of offset commits to a customized store on the start
         * of a rebalance operation. This method will be called before a rebalance operation starts and after the consumer
         * stops fetching data. It is recommended that offsets should be committed in this callback to either Kafka or a
         * custom offset store to prevent duplicate data.
         * <p>
         * For examples on usage of this API, see Usage Examples section of {@link KafkaConsumer KafkaConsumer}
         * <p>
         * <b>NOTE:</b> This method is only called before rebalances. It is not called prior to {@link KafkaConsumer#Close()}.
         * <p>
         * It is common for the revocation callback to use the consumer instance in order to commit offsets. It is possible
         * for a {@link org.apache.kafka.common.errors.WakeupException} or {@link org.apache.kafka.common.errors.InterruptException}
         * to be raised from one these nested invocations. In this case, the exception will be propagated to the current
         * invocation of {@link KafkaConsumer#poll(java.time.TimeSpan)} in which this callback is being executed. This means it is not
         * necessary to catch these exceptions and re-attempt to wakeup or interrupt the consumer thread.
         *
         * @param partitions The list of partitions that were assigned to the consumer on the last rebalance
         * @throws org.apache.kafka.common.errors.WakeupException If raised from a nested call to {@link KafkaConsumer}
         * @throws org.apache.kafka.common.errors.InterruptException If raised from a nested call to {@link KafkaConsumer}
         */
        void OnPartitionsRevoked(IConsumer<byte[], byte[]>? consumer, List<TopicPartitionOffset> revokedPartitions);

        void OnPartitionsAssigned(IConsumer<byte[], byte[]>? consumer, List<TopicPartition> assignedPartitions);
    }
}
