//using Confluent.Kafka;
//using Kafka.Streams.Processors.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.Tasks
//{
//    public interface ITask
//    {
//        /**
//         * Initialize the task and return {@code true} if the task is ready to run, i.e, it has not state stores
//         * @return true if this task has no state stores that may need restoring.
//         * @throws InvalidOperationException If store gets registered after initialized is already finished
//         * @throws StreamsException if the store's change log does not contain the partition
//         */
//        bool initializeStateStores();

//        bool commitNeeded();

//        void initializeTopology();

//        void commit();

//        void suspend();

//        void resume();

//        void closeSuspended(bool clean,
//                            bool isZombie,
//                            RuntimeException e);

//        void close(bool clean,
//                   bool isZombie);

//        IStateStore getStore(string name);

//        string applicationId { get; }

//        ProcessorTopology topology { get; }

//        IProcessorContext context;

//        TaskId id();

//        HashSet<TopicPartition> partitions();

//        /**
//         * @return any changelog partitions associated with this task
//         */
//        List<TopicPartition> changelogPartitions();

//        bool hasStateStores();

//        string ToString(string indent);
//    }
//}