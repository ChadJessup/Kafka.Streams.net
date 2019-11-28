using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class TableProcessorNode<K, V> : StreamsGraphNode
    {
        private readonly ProcessorParameters<K, V> processorParameters;
        private readonly IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder;
        private readonly string[] storeNames;

        public TableProcessorNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters,
            IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder)
            : this(nodeName, processorParameters, storeBuilder, null)
        {
        }

        public TableProcessorNode(
            string nodeName,
            ProcessorParameters<K, V> processorParameters,
            IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder,
            string[] storeNames)
            : base(nodeName)
        {
            this.processorParameters = processorParameters;
            this.storeBuilder = storeBuilder;
            this.storeNames = storeNames != null ? storeNames : new string[] { };
        }


        //public string ToString()
        //{
        //    return "TableProcessorNode{" +
        //        ", processorParameters=" + processorParameters +
        //        ", storeBuilder=" + (storeBuilder == null ? "null" : storeBuilder.name) +
        //        ", storeNames=" + Arrays.ToString(storeNames) +
        //        "} " + base.ToString();
        //}

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            string processorName = processorParameters.ProcessorName;
            topologyBuilder.AddProcessor(processorName, processorParameters.ProcessorSupplier, ParentNodeNames());

            if (storeNames.Length > 0)
            {
                topologyBuilder.ConnectProcessorAndStateStores(processorName, storeNames);
            }

            // TODO: we are enforcing this as a keyvalue store, but it should go beyond any type of stores
            //if (storeBuilder != null)
            //{
            //    topologyBuilder.addStateStore<K, V, T>(storeBuilder, processorName);
            //}
        }
    }
}
