using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StatefulProcessorNode<K, V> : ProcessorGraphNode<K, V>
    {
        private readonly string[] storeNames;
        private readonly IStoreBuilder<IStateStore> storeBuilder;


        /**
         * Create a node representing a stateful processor, where the named store has already been registered.
         */
        public StatefulProcessorNode(string nodeName,
                                      ProcessorParameters<K, V> processorParameters,
                                      string[] storeNames)
            : base(nodeName, processorParameters)
        {

            this.storeNames = storeNames;
            this.storeBuilder = null;
        }


        /**
         * Create a node representing a stateful processor,
         * where the store needs to be built and registered as part of building this node.
         */
        public StatefulProcessorNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters,
            IStoreBuilder<IStateStore> materializedKTableStoreBuilder)
            : base(nodeName, processorParameters)
        {

            this.storeNames = Array.Empty<string>();
            this.storeBuilder = materializedKTableStoreBuilder;
        }


        public override string ToString()
        {
            return "StatefulProcessorNode{" +
                $"storeNames=[{string.Join(',', this.storeNames)}]" +
                $", storeBuilder={this.storeBuilder}" +
                "} " + base.ToString();
        }


        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            var processorName = this.processorParameters.ProcessorName;
            var IProcessorSupplier = this.processorParameters.ProcessorSupplier;

            topologyBuilder.AddProcessor<K, V>(processorName, IProcessorSupplier, this.ParentNodeNames());

            if (this.storeNames != null && this.storeNames.Length > 0)
            {
                topologyBuilder.ConnectProcessorAndStateStores(processorName, this.storeNames);
            }

            if (this.storeBuilder != null)
            {
                //topologyBuilder.addStateStore(storeBuilder, processorName);
            }
        }
    }
}
