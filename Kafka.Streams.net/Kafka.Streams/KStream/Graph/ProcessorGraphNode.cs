using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Used to represent any type of stateless operation:
     *
     * map, mapValues, flatMap, flatMapValues, filter, filterNot, branch
     */
    public class ProcessorGraphNode<K, V> : StreamsGraphNode
    {
        public ProcessorParameters<K, V> processorParameters { get; }

        public ProcessorGraphNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters)
            : base(nodeName)
        {
            this.processorParameters = processorParameters;
        }

        public override string ToString()
        {
            return "ProcessorNode{" +
                   "processorParameters=" + processorParameters +
                   "} " + base.ToString();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            topologyBuilder.AddProcessor(processorParameters.ProcessorName, processorParameters.ProcessorSupplier, ParentNodeNames());
        }
    }
}
