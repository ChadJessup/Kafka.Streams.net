using Confluent.Kafka;
using Kafka.Streams.Tasks;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * Performs bulk read operations from a set of partitions. Used to
     * restore  {@link org.apache.kafka.streams.processor.IStateStore}s from their
     * change logs
     */
    public interface IChangelogReader
    {
        /**
         * Register a state store and it's partition for later restoration.
         * @param restorer the state restorer to register
         */
        void register(StateRestorer restorer);

        /**
         * Restore all registered state stores by reading from their changelogs.
         * @return all topic partitions that have been restored
         */
        List<TopicPartition> restore(IRestoringTasks active);

        /**
         * @return the restored offsets for all persistent stores.
         */
        Dictionary<TopicPartition, long> RestoredOffsets { get; }

        void reset();
    }
}