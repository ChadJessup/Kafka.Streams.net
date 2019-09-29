using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using System.Collections.Generic;

namespace Kafka.Streams.Tasks
{
    public interface ITask
    {
        /**
         * Initialize the task and return {@code true} if the task is ready to run, i.e, it has not state stores
         * @return true if this task has no state stores that may need restoring.
         * @throws IllegalStateException If store gets registered after initialized is already finished
         * @throws StreamsException if the store's change log does not contain the partition
         */
        bool initializeStateStores();

        bool commitNeeded { get; }

        void initializeTopology();

        void commit();

        void suspend();

        void resume();

        void closeSuspended(
            bool clean,
            bool isZombie,
            RuntimeException e);

        void close(bool clean, bool isZombie);

        IStateStore getStore(string name);

        string applicationId { get; }

        ProcessorTopology topology { get; }

        TaskId id { get; }

        HashSet<TopicPartition> partitions { get; }

        /**
         * @return any changelog partitions associated with this task
         */
        IEnumerable<TopicPartition> changelogPartitions { get; }
        IProcessorContext context { get; }

        bool hasStateStores();

        string ToString(string indent);
    }
}