using Kafka.Streams.Extensions;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class TableProcessorNode<K, V, T> : StreamsGraphNode
        where T : IStateStore
    {
        private readonly IProcessorParameters processorParameters;
        private readonly IStoreBuilder<T>? storeBuilder;
        private readonly string[] storeNames;

        public TableProcessorNode(
            string nodeName,
            IProcessorParameters processorParameters,
            IStoreBuilder<T>? storeBuilder)
            : this(nodeName, processorParameters, storeBuilder, null)
        {
        }

        public TableProcessorNode(
            string nodeName,
            IProcessorParameters processorParameters,
            IStoreBuilder<T>? storeBuilder,
            string[]? storeNames)
            : base(nodeName)
        {
            this.processorParameters = processorParameters;
            this.storeBuilder = storeBuilder;
            this.storeNames = storeNames ?? Array.Empty<string>();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            var processorName = processorParameters.ProcessorName;
            topologyBuilder.AddProcessor<K, V>(processorName, processorParameters.ProcessorSupplier, ParentNodeNames());

            if (storeNames.Length > 0)
            {
                topologyBuilder.ConnectProcessorAndStateStores(processorName, storeNames);
            }

            // TODO: we are enforcing this as a keyvalue store, but it should go beyond any type of stores
            if (storeBuilder != null)
            {
                topologyBuilder.AddStateStore<K, V, T>(storeBuilder, new[] { processorName });
            }
        }

        public override string ToString()
        {
            return "TableProcessorNode{" +
                ", processorParameters=" + processorParameters +
                ", storeBuilder=" + (storeBuilder == null ? "null" : storeBuilder.name) +
                ", storeNames=" + storeNames.ToJoinedString() +
                "} " + base.ToString();
        }
    }
}
