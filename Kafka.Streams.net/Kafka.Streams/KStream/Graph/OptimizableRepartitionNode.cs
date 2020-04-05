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
            => ValueSerde?.Serializer;

        public override IDeserializer<V>? GetValueDeserializer()
            => ValueSerde?.Deserializer;

        public override string ToString()
            => $"OptimizableRepartitionNode{{{base.ToString()}}}";

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            if (topologyBuilder is null)
            {
                throw new System.ArgumentNullException(nameof(topologyBuilder));
            }

            ISerializer<K>? keySerializer = KeySerde?.Serializer;
            IDeserializer<K>? keyDeserializer = KeySerde?.Deserializer;

            topologyBuilder.AddInternalTopic(RepartitionTopic);

            topologyBuilder.AddProcessor(
                ProcessorParameters.ProcessorName,
                ProcessorParameters.ProcessorSupplier,
                ParentNodeNames());

            topologyBuilder.AddSink(
                SinkName,
                RepartitionTopic,
                keySerializer,
                GetValueSerializer(),
                null,
                new[] { ProcessorParameters.ProcessorName });

            topologyBuilder.AddSource(
                null,
                SourceName,
                new FailOnInvalidTimestamp(logger: null),
                keyDeserializer,
                GetValueDeserializer(),
                new[] { RepartitionTopic });
        }
    }
}
