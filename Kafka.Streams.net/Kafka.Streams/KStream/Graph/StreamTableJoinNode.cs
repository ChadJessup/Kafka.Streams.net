using Kafka.Streams.Processors;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    /**
     * Represents a join between a KStream and a KTable or GlobalKTable
     */
    public class StreamTableJoinNode<K, V> : StreamsGraphNode
    {
        private readonly string[] storeNames;
        private readonly ProcessorParameters<K, V> processorParameters;
        private readonly string otherJoinSideNodeName;

        public StreamTableJoinNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters,
            string[] storeNames,
            string otherJoinSideNodeName)
            : base(nodeName)
        {
            // in the case of Stream-Table join the state stores associated with the KTable
            this.storeNames = storeNames;
            this.processorParameters = processorParameters;
            this.otherJoinSideNodeName = otherJoinSideNodeName;
        }

        public override string ToString()
        {
            return "StreamTableJoinNode{" +
                   "storeNames=" + Arrays.ToString(storeNames) +
                   ", processorParameters=" + processorParameters +
                   ", otherJoinSideNodeName='" + otherJoinSideNodeName + '\'' +
                   "} " + base.ToString();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            string processorName = processorParameters.processorName;
            IProcessorSupplier<K, V> IProcessorSupplier = processorParameters.ProcessorSupplier;

            // Stream - Table join (Global or KTable)
            topologyBuilder.AddProcessor(processorName, IProcessorSupplier, ParentNodeNames());

            // Steam - KTable join only
            if (otherJoinSideNodeName != null)
            {
                topologyBuilder.connectProcessorAndStateStores(processorName, storeNames);
            }

        }
    }
}
