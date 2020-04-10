﻿using Confluent.Kafka;
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
        bool InitializeStateStores();
        void InitializeIfNeeded();
        void InitializeTopology();
        bool commitNeeded { get; }

        void Commit();

        void Suspend();

        void Resume();

        void CloseSuspended(
            bool clean,
            bool isZombie,
            RuntimeException e);

        void Close(bool clean, bool isZombie);

        IStateStore GetStore(string Name);

        string applicationId { get; }

        ProcessorTopology topology { get; }

        TaskId id { get; }

        HashSet<TopicPartition> partitions { get; }

        /**
         * @return any changelog partitions associated with this task
         */
        IEnumerable<TopicPartition> changelogPartitions { get; }
        IProcessorContext context { get; }
        void CompleteRestoration();
        bool HasStateStores();

        string ToString(string indent);
    }
}
