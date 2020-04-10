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
                   $"storeNames=[{string.Join(',', this.storeNames)}]" +
                   $", processorParameters={this.processorParameters}" +
                   $", otherJoinSideNodeName='{this.otherJoinSideNodeName}'" +
                   "} " + base.ToString();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            var processorName = this.processorParameters.ProcessorName;
            IProcessorSupplier<K, V> IProcessorSupplier = this.processorParameters.ProcessorSupplier;

            // Stream - Table join (Global or KTable)
            topologyBuilder.AddProcessor<K, V>(processorName, IProcessorSupplier, this.ParentNodeNames());

            // Steam - KTable join only
            if (this.otherJoinSideNodeName != null)
            {
                topologyBuilder.ConnectProcessorAndStateStores(processorName, this.storeNames);
            }

        }
    }
}
