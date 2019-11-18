using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class OptimizableRepartitionNode
    {
        public static OptimizableRepartitionNodeBuilder<K, V> GetOptimizableRepartitionNodeBuilder<K, V>()
        {
            return new OptimizableRepartitionNodeBuilder<K, V>();
        }
    }

    public class OptimizableRepartitionNode<K, V> : BaseRepartitionNode<K, V>
    {
        public OptimizableRepartitionNode(
            string nodeName,
            string sourceName,
            ProcessorParameters<K, V> processorParameters,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
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


        //        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        //        {
        //            ISerializer<K> keySerializer = keySerde != null
        //                ? keySerde.Serializer
        //                : null;

        //            IDeserializer<K> keyDeserializer = keySerde != null
        //                ? keySerde.Deserializer
        //                : null;

        //            topologyBuilder.addInternalTopic(repartitionTopic);

        //            topologyBuilder.addProcessor(
        //                processorParameters.processorName,
        //                processorParameters.IProcessorSupplier,
        //                parentNodeNames());

        //            topologyBuilder.addSink(
        //                sinkName,
        //                repartitionTopic,
        //                keySerializer,
        //                getValueSerializer(),
        //                null,
        //                new[] { processorParameters.processorName });

        //            topologyBuilder.addSource(
        //                AutoOffsetReset.Error,
        //                sourceName,
        //                new FailOnInvalidTimestamp(),
        //                keyDeserializer,
        //                getValueDeserializer(),
        //                new[] { repartitionTopic });
        //        }
        //    }
    }
}