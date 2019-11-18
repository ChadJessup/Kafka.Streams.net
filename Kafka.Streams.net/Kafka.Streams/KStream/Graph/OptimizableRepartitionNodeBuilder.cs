using Kafka.Streams.Interfaces;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class OptimizableRepartitionNodeBuilder<K, V>
    {
        private string nodeName;
        private ProcessorParameters<K, V> processorParameters;
        private ISerde<K> keySerde;
        private ISerde<V> valueSerde;
        private string sinkName;
        private string sourceName;
        private string repartitionTopic;

        public OptimizableRepartitionNodeBuilder<K, V> WithProcessorParameters(
            ProcessorParameters<K, V> processorParameters)
        {
            this.processorParameters = processorParameters;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithKeySerde(ISerde<K> keySerde)
        {
            this.keySerde = keySerde;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithValueSerde(ISerde<V> valueSerde)
        {
            this.valueSerde = valueSerde;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithSinkName(string sinkName)
        {
            this.sinkName = sinkName;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithSourceName(string sourceName)
        {
            this.sourceName = sourceName;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithRepartitionTopic(string repartitionTopic)
        {
            this.repartitionTopic = repartitionTopic;

            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V> WithNodeName(string nodeName)
        {
            this.nodeName = nodeName;

            return this;
        }

        public OptimizableRepartitionNode<K, V> Build()
            => new OptimizableRepartitionNode<K, V>(
                nodeName,
                sourceName,
                processorParameters,
                keySerde,
                valueSerde,
                sinkName,
                repartitionTopic);
    }
}
