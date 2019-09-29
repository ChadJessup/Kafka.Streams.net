using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class GlobalStoreNode<K, V, T> : StateStoreNode<T>
        where T : IStateStore
    {
        private readonly string sourceName;
        private readonly string topic;
        private readonly ConsumedInternal<K, V> consumed;
        private readonly string processorName;
        private readonly IProcessorSupplier<K, V> stateUpdateSupplier;

        public GlobalStoreNode(
            IStoreBuilder<T> storeBuilder,
            string sourceName,
            string topic,
            ConsumedInternal<K, V> consumed,
            string processorName,
            IProcessorSupplier<K, V> stateUpdateSupplier)
            : base(storeBuilder)
        {
            this.sourceName = sourceName;
            this.topic = topic;
            this.consumed = consumed;
            this.processorName = processorName;
            this.stateUpdateSupplier = stateUpdateSupplier;
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            // storeBuilder.withLoggingDisabled();

            topologyBuilder.addGlobalStore<K, V, T>(
                storeBuilder,
                sourceName,
                consumed.timestampExtractor,
                consumed.keyDeserializer(),
                consumed.valueDeserializer(),
                topic,
                processorName,
                stateUpdateSupplier);
        }

        public override string ToString()
        {
            return "GlobalStoreNode{" +
                   "sourceName='" + sourceName + '\'' +
                   ", topic='" + topic + '\'' +
                   ", processorName='" + processorName + '\'' +
                   "} ";
        }
    }
}
