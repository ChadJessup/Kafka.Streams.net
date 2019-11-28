using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.State.Interfaces
{
    public interface IStateManager : ICheckpointable
    {
        DirectoryInfo baseDir { get; }

        /**
         * @throws ArgumentException if the store name has already been registered or if it is not a valid name
         * (e.g., when it conflicts with the names of internal topics, like the checkpoint file name)
         * @throws StreamsException if the store's change log does not contain the partition
         */
        void Register(IStateStore store, IStateRestoreCallback stateRestoreCallback);

        void Flush();

        void ReinitializeStateStoresForPartitions(
            List<TopicPartition> partitions,
            IInternalProcessorContext processorContext);

        void Close(bool clean);

        IStateStore? GetGlobalStore(string name);

        IStateStore? GetStore(string name);
    }
}