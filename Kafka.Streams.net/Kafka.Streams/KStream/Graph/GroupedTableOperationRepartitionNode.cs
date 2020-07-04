using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class GroupedTableOperationRepartitionNode<K, V> : BaseRepartitionNode<K, V>
    {
        public GroupedTableOperationRepartitionNode(
            string nodeName,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            string sinkName,
            string sourceName,
            string repartitionTopic,
            ProcessorParameters<K, V> processorParameters)
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

        public override ISerializer<V> GetValueSerializer()
        {
            ISerializer<V>? valueSerializer = this.ValueSerde?.Serializer;
            return this.unsafeCastChangedToValueSerializer(valueSerializer);
        }

        private ISerializer<V> unsafeCastChangedToValueSerializer(ISerializer<V> valueSerializer)
        {
            return new ChangedSerializer<V>(valueSerializer);
        }

        public override IDeserializer<V> GetValueDeserializer()
        {
            IDeserializer<V>? valueDeserializer = this.ValueSerde?.Deserializer;
            return this.unsafeCastChangedToValueDeserializer(valueDeserializer);
        }

        private IDeserializer<V> unsafeCastChangedToValueDeserializer(IDeserializer<V> valueDeserializer)
        {
            return (IDeserializer<V>)new ChangedDeserializer<V>(valueDeserializer);
        }

        public override string ToString()
        {
            return "GroupedTableOperationRepartitionNode{} " + base.ToString();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            ISerializer<K>? keySerializer = this.KeySerde != null ? this.KeySerde.Serializer : null;
            IDeserializer<K>? keyDeserializer = this.KeySerde != null ? this.KeySerde.Deserializer : null;

            topologyBuilder.AddInternalTopic(this.RepartitionTopic);

            topologyBuilder.AddSink(
                this.SinkName,
                this.RepartitionTopic,
                keySerializer,
                this.GetValueSerializer(),
                null,
                this.ParentNodeNames());

            topologyBuilder.AddSource(
                null,
                this.SourceName,
                new FailOnInvalidTimestamp(),
                keyDeserializer,
                this.GetValueDeserializer(),
                this.RepartitionTopic);
        }

        public static GroupedTableOperationRepartitionNodeBuilder<K1, V1> groupedTableOperationNodeBuilder<K1, V1>()
        {
            return new GroupedTableOperationRepartitionNodeBuilder<K1, V1>();
        }
    }
}
