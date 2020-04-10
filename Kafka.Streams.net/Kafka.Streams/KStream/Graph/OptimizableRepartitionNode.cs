using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public static class OptimizableRepartitionNode
    {
        public static OptimizableRepartitionNodeBuilder<K, V> GetOptimizableRepartitionNodeBuilder<K, V>()
        {
            return new OptimizableRepartitionNodeBuilder<K, V>();
        }
    }

    public class OptimizableRepartitionNode<K, V> : BaseRepartitionNode<K, V>, IOptimizableRepartitionNode
    {
        public OptimizableRepartitionNode(
            string nodeName,
            string sourceName,
            ProcessorParameters<K, V> processorParameters,
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
            string sinkName,
            string repartitionTopic)
            : base(
                nodeName,
                sourceName,
                processorParameters,
                keySerde,
                valueSerde,
                sinkName,
                repartitionTopic)
        {
        }

        public override ISerializer<V>? GetValueSerializer()
            => this.ValueSerde?.Serializer;

        public override IDeserializer<V>? GetValueDeserializer()
            => this.ValueSerde?.Deserializer;

        public override string ToString()
            => $"OptimizableRepartitionNode{{{base.ToString()}}}";

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            if (topologyBuilder is null)
            {
                throw new System.ArgumentNullException(nameof(topologyBuilder));
            }

            ISerializer<K>? keySerializer = this.KeySerde?.Serializer;
            IDeserializer<K>? keyDeserializer = this.KeySerde?.Deserializer;

            topologyBuilder.AddInternalTopic(this.RepartitionTopic);

            topologyBuilder.AddProcessor<K, V>(
                this.ProcessorParameters.ProcessorName,
                this.ProcessorParameters.ProcessorSupplier,
                this.ParentNodeNames());

            topologyBuilder.AddSink(
                this.SinkName,
                this.RepartitionTopic,
                keySerializer,
                this.GetValueSerializer(),
                null,
                new[] { this.ProcessorParameters.ProcessorName });

            topologyBuilder.AddSource(
                null,
                this.SourceName,
                new FailOnInvalidTimestamp(logger: null),
                keyDeserializer,
                this.GetValueDeserializer(),
                new[] { this.RepartitionTopic });
        }
    }
}
