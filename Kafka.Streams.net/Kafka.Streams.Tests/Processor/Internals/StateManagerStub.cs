using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class StateManagerStub : IStateManager
    {
        public DirectoryInfo BaseDir { get; }

        public void Register(IStateStore store, IStateRestoreCallback stateRestoreCallback)
        { }

        public void ReinitializeStateStoresForPartitions(
            List<TopicPartition> partitions,
            IInternalProcessorContext processorContext)
        { }

        public void Flush() { }

        public void Close(bool clean)
        { //throws IOException}

        }

        public IStateStore GetGlobalStore(string Name)
        {
            return null;
        }


        public IStateStore GetStore(string Name)
        {
            return null;
        }


        public Dictionary<TopicPartition, long?> Checkpointed()
        {
            return null;
        }

        public void Checkpoint(Dictionary<TopicPartition, long> offsets) { }
    }
}
