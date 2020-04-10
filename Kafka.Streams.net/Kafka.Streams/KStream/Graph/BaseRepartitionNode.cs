
using Confluent.Kafka;
using Kafka.Streams.Interfaces;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public abstract class BaseRepartitionNode<K, V> : StreamsGraphNode
    {
        public ISerde<K>? KeySerde { get; protected set; }
        public ISerde<V>? ValueSerde { get; protected set; }

        protected string SinkName { get; }
        protected string SourceName { get; }
        protected string RepartitionTopic { get; }
        protected ProcessorParameters<K, V> ProcessorParameters { get; }

        public BaseRepartitionNode(
            string nodeName,
            string sourceName,
            ProcessorParameters<K, V> processorParameters,
            ISerde<K>? keySerde,
            ISerde<V>? valueSerde,
            string sinkName,
            string repartitionTopic)
            : base(nodeName)
        {
            this.KeySerde = keySerde;
            this.ValueSerde = valueSerde;
            this.SinkName = sinkName;
            this.SourceName = sourceName;
            this.RepartitionTopic = repartitionTopic;
            this.ProcessorParameters = processorParameters;
        }

        public abstract ISerializer<V>? GetValueSerializer();

        public abstract IDeserializer<V>? GetValueDeserializer();

        public override string ToString()
            => "BaseRepartitionNode{" +
                    $"keySerde={this.KeySerde}" +
                    $", valueSerde={this.ValueSerde}" +
                    $", sinkName='{this.SinkName}'" +
                    $", sourceName='{this.SourceName}'" +
                    $", repartitionTopic='{this.RepartitionTopic}'" +
                    $", processorParameters={this.ProcessorParameters}" +
                    $"}} {base.ToString()}";
    }
}
