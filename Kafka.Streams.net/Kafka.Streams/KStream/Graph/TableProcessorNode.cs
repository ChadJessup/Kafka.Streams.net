using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;

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

            var processorName = this.processorParameters.ProcessorName;
            topologyBuilder.AddProcessor<K, V>(processorName, this.processorParameters.ProcessorSupplier, this.ParentNodeNames());

            if (this.storeNames.Length > 0)
            {
                topologyBuilder.ConnectProcessorAndStateStores(processorName, this.storeNames);
            }

            // TODO: we are enforcing this as a keyvalue store, but it should go beyond any type of stores
            if (this.storeBuilder != null)
            {
                topologyBuilder.AddStateStore<K, V, T>(this.storeBuilder, new[] { processorName });
            }
        }

        public override string ToString()
        {
            return "TableProcessorNode{" +
                ", processorParameters=" + this.processorParameters +
                ", storeBuilder=" + (this.storeBuilder == null ? "null" : this.storeBuilder.Name) +
                ", storeNames=" + this.storeNames.ToJoinedString() +
                "} " + base.ToString();
        }
    }
}
