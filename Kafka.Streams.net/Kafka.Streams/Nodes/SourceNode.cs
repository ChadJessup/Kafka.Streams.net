using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Nodes
{
    public class SourceNode : ProcessorNode, ISourceNode
    {
        public SourceNode(KafkaStreamsContext context, string Name, HashSet<string>? stateStores)
            : base(context, Name, stateStores, null)
        {
        }
    }

    public class SourceNode<K, V> : ProcessorNode<K, V>, ISourceNode<K, V>
    {
        private readonly List<string> topics;
        private readonly SourceNode sourceNode;

        private IProcessorContext processorContext;
        private readonly IDeserializer<K> keyDeserializer;
        private readonly IDeserializer<V> valDeserializer;

        public SourceNode(
            KafkaStreamsContext context,
            string name,
            List<string> topics,
            ITimestampExtractor? timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer)
            : base(context, name)
        {
            this.sourceNode = new SourceNode(context, name, null);

            this.topics = topics;
            this.TimestampExtractor = timestampExtractor;
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
        }

        public SourceNode(
            KafkaStreamsContext context,
            string Name,
            List<string> topics,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer)
            : this(context, Name, topics, null, keyDeserializer, valDeserializer)
        {
        }

        public static implicit operator SourceNode(SourceNode<K, V> sourceNode)
        {
            return sourceNode.sourceNode;
        }

        public virtual K DeserializeKey(string topic, Headers headers, byte[] data)
        {
            return this.keyDeserializer.Deserialize(data, false, new SerializationContext(MessageComponentType.Key, topic));
        }

        public virtual V DeserializeValue(string topic, Headers headers, byte[] data)
        {
            return this.valDeserializer.Deserialize(data, data == null, new SerializationContext(MessageComponentType.Value, topic));
        }

        public override void Init(IInternalProcessorContext processorContext)
        {
            base.Init(processorContext);
            this.processorContext = processorContext;

            // if deserializers are null, get the default ones from the context
            if (this.keyDeserializer == null)
            {
                //this.keyDeserializer = context.keySerde.Deserializer;
            }

            if (this.valDeserializer == null)
            {
                // this.valDeserializer = context.valueSerde.Deserializer;
            }

            // if value deserializers are for {@code Change} values, set the inner deserializer when necessary
            //if (this.valDeserializer is ChangedDeserializer<V> &&
            //        ((ChangedDeserializer<V>)this.valDeserializer).inner() == null)
            //{
            //    ((ChangedDeserializer<V>)this.valDeserializer).setInner(context.valueSerde.Deserializer);
            //}
        }

        public override void Process(K key, V value)
        {
            this.processorContext.Forward(key, value);
            //sourceNodeForwardSensor.record();
        }

        /**
         * @return a string representation of this node, useful for debugging.
         */
        public override string ToString()
        {
            return this.ToString("");
        }

        /**
         * @return a string representation of this node starting with the given indent, useful for debugging.
         */
        public override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(base.ToString(indent));
            sb.Append(indent).Append("\ttopics:\t\t[");
            foreach (string topic in this.topics)
            {
                sb.Append(topic);
                sb.Append(", ");
            }

            sb.Length -= 2;  // Remove the last comma
            sb.Append("]\n");
            return sb.ToString();
        }
    }

    public interface ISourceNode<K, V> : IProcessorNode<K, V>, ISourceNode
    {
    }

    public interface ISourceNode : IProcessorNode
    {
    }
}
