using Kafka.Streams.Interfaces;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class GroupedTableOperationRepartitionNodeBuilder<K, V>
    {
        private ISerde<K> keySerde;
        private ISerde<V> valueSerde;
        private string sinkName;
        private string nodeName;
        private string sourceName;
        private string repartitionTopic;
        private ProcessorParameters<K, V> processorParameters;

        public GroupedTableOperationRepartitionNodeBuilder()
        {
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> WithKeySerde(ISerde<K> keySerde)
        {
            this.keySerde = keySerde;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> WithValueSerde(ISerde<V> valueSerde)
        {
            this.valueSerde = valueSerde;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> WithSinkName(string sinkName)
        {
            this.sinkName = sinkName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> WithNodeName(string nodeName)
        {
            this.nodeName = nodeName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> WithSourceName(string sourceName)
        {
            this.sourceName = sourceName;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> WithRepartitionTopic(string repartitionTopic)
        {
            this.repartitionTopic = repartitionTopic;
            return this;
        }

        public GroupedTableOperationRepartitionNodeBuilder<K, V> WithProcessorParameters(ProcessorParameters<K, V> processorParameters)
        {
            this.processorParameters = processorParameters;
            return this;
        }

        public GroupedTableOperationRepartitionNode<K, V> Build()
        {
            return new GroupedTableOperationRepartitionNode<K, V>(
                this.nodeName,
                this.keySerde,
                this.valueSerde,
                this.sinkName,
                this.sourceName,
                this.repartitionTopic,
                this.processorParameters);
        }
    }
}
