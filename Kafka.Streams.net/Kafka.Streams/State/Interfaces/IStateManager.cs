using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.State.Interfaces
{
    public interface IStateManager : ICheckpointable
    {
        DirectoryInfo BaseDir { get; }

        /**
         * @throws ArgumentException if the store Name has already been registered or if it is not a valid Name
         * (e.g., when it conflicts with the names of internal topics, like the checkpoint file Name)
         * @throws StreamsException if the store's change log does not contain the partition
         */
        void Register(IStateStore store, IStateRestoreCallback stateRestoreCallback);

        void Flush();

        void ReinitializeStateStoresForPartitions(
            List<TopicPartition> partitions,
            IInternalProcessorContext processorContext);

        void Close(bool clean);

        IStateStore? GetGlobalStore(string Name);

        IStateStore? GetStore(string Name);
    }
}