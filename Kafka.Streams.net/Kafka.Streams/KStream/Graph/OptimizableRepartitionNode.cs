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
            => valueSerde?.Serializer;

        public override IDeserializer<V>? GetValueDeserializer()
            => valueSerde?.Deserializer;

        public override string ToString()
            => $"OptimizableRepartitionNode{{{base.ToString()}}}";

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            if (topologyBuilder is null)
            {
                throw new System.ArgumentNullException(nameof(topologyBuilder));
            }

            ISerializer<K>? keySerializer = keySerde != null
                ? keySerde.Serializer
                : null;

            IDeserializer<K>? keyDeserializer = keySerde != null
                ? keySerde.Deserializer
                : null;

            topologyBuilder.AddInternalTopic(repartitionTopic);

            topologyBuilder.AddProcessor(
                processorParameters.ProcessorName,
                processorParameters.ProcessorSupplier,
                ParentNodeNames());

            topologyBuilder.AddSink(
                sinkName,
                repartitionTopic,
                keySerializer,
                GetValueSerializer(),
                null,
                new[] { processorParameters.ProcessorName });

            topologyBuilder.AddSource(
                null,
                sourceName,
                new FailOnInvalidTimestamp(logger: null),
                keyDeserializer,
                GetValueDeserializer(),
                new[] { repartitionTopic });
        }
    }
}
