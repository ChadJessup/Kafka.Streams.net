using Kafka.Common;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{

    public class StatefulProcessorNode<K, V, TStateStore> : StatefulProcessorNode<K, V>
        where TStateStore : IStateStore
    {
        private readonly IStoreBuilder<TStateStore>? storeBuilder;

        /**
          * Create a node representing a stateful processor,
          * where the store needs to be built and registered as part of building this node.
          */
        public StatefulProcessorNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters,
            IStoreBuilder<TStateStore> materializedKTableStoreBuilder)
            : base(nodeName, processorParameters, Array.Empty<string>())
        {
            this.storeBuilder = materializedKTableStoreBuilder;
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            if (topologyBuilder is null)
            {
                throw new ArgumentNullException(nameof(topologyBuilder));
            }

            base.WriteToTopology(topologyBuilder);

            if (this.storeBuilder != null)
            {
                topologyBuilder.AddStateStore<K, V, TStateStore>(
                    this.storeBuilder,
                    new[] { this.processorParameters.ProcessorName });
            }
        }

        public override string ToString()
        {
            return "StatefulProcessorNode{" +
                $"storeNames=[{string.Join(',', this.storeNames)}]" +
                $", storeBuilder={this.storeBuilder}" +
                "} " + base.ToString();
        }
    }

    public class StatefulProcessorNode<K, V> : ProcessorGraphNode<K, V>
    {
        protected string[] storeNames { get; set; }

        /**
         * Create a node representing a stateful processor, where the named store has already been registered.
         */
        public StatefulProcessorNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters,
            string[] storeNames)
            : base(nodeName, processorParameters)
        {
            this.storeNames = storeNames;
        }

        public override string ToString()
        {
            return "StatefulProcessorNode{" +
                $"storeNames=[{string.Join(',', this.storeNames)}]" +
                $", storeBuilder=" +
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
        }
    }
}
