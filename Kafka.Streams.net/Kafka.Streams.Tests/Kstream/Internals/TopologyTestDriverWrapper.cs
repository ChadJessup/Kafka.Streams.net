using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    internal class TopologyTestDriverWrapper
    {
        private readonly Topology topology;
        private readonly StreamsConfig props;

        public TopologyTestDriverWrapper(Topology topology, StreamsConfig props)
        {
            this.topology = topology;
            this.props = props;
        }

        internal IProcessorContext SetCurrentNodeForProcessorContext(string name)
        {
            throw new NotImplementedException();
        }

        internal void PipeInput(ConsumeResult<byte[], byte[]> consumeResult)
        {
            throw new NotImplementedException();
        }
    }
}