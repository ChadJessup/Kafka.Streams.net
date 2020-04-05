using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class ProcessorTopologyFactories
    {
        private ProcessorTopologyFactories() { }

        public static ProcessorTopology With(
            List<ProcessorNode> processorNodes,
            Dictionary<string, ISourceNode> sourcesByTopic,
            List<IStateStore> stateStoresByName,
            Dictionary<string, string> storeToChangelogTopic)
        {
            return new ProcessorTopology(
                processorNodes,
                sourcesByTopic,
                new Dictionary<string, ISinkNode>(),
                stateStoresByName,
                new List<IStateStore>(),
                storeToChangelogTopic,
                new HashSet<string>());
        }

        static ProcessorTopology WithLocalStores(List<IStateStore> stateStores,
                                                 Dictionary<string, string> storeToChangelogTopic)
        {
            return new ProcessorTopology(
                new List<IProcessorNode>(),
                new Dictionary<string, ISourceNode>(),
                new Dictionary<string, ISinkNode>(),
                stateStores,
                new List<IStateStore>(),
                storeToChangelogTopic,
                new HashSet<string>());
        }
    }
}
