using Kafka.Streams.Processors;
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
            // storeBuilder.WithLoggingDisabled();

            topologyBuilder.AddGlobalStore<K, V, T>(
                this.storeBuilder,
                this.sourceName,
                this.consumed.TimestampExtractor,
                this.consumed.KeyDeserializer(),
                this.consumed.ValueDeserializer(),
                this.topic,
                this.processorName,
                this.stateUpdateSupplier);
        }

        public override string ToString()
        {
            return "GlobalStoreNode{" +
                   "sourceName='" + this.sourceName + '\'' +
                   ", topic='" + this.topic + '\'' +
                   ", processorName='" + this.processorName + '\'' +
                   "} ";
        }
    }
}
