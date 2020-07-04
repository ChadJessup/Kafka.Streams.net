using Kafka.Streams.Processors;
using Kafka.Streams.Tests.Mocks;
using Kafka.Streams.Topologies;
using System;
using System.Text.RegularExpressions;

namespace Kafka.Streams.Tests.Integration
{
    internal class TopologyWrapper : Topology
    {
        public TopologyWrapper(KafkaStreamsContext context)
            :base(context)
        {
        }

        internal void AddSource(string v, Regex regex)
        {
            throw new NotImplementedException();
        }

        internal void AddProcessor(string v1, IProcessorSupplier<string, string> processorSupplier, string v2)
        {
            throw new NotImplementedException();
        }

        internal void AddStateStore(MockKeyValueStoreBuilder storeBuilder, string v)
        {
            throw new NotImplementedException();
        }

        internal static InternalTopologyBuilder getInternalTopologyBuilder(Topology topology)
        {
            return topology.internalTopologyBuilder;
        }
    }
}