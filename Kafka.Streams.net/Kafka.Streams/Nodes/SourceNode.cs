using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using NodaTime;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Nodes
{
    public class SourceNode : ProcessorNode, ISourceNode
    {
        public SourceNode(IClock clock, string name, HashSet<string>? stateStores)
            : base(clock, name, stateStores, null)
        {
        }
    }

    public class SourceNode<K, V> : ProcessorNode<K, V>, ISourceNode<K, V>
    {
        private readonly List<string> topics;
        private SourceNode sourceNode;

        private IProcessorContext context;
        private readonly IDeserializer<K> keyDeserializer;
        private readonly IDeserializer<V> valDeserializer;
        public ITimestampExtractor? TimestampExtractor { get; }

        public SourceNode(
            IClock clock,
            string name,
            List<string> topics,
            ITimestampExtractor? timestampExtractor,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer)
            : base(clock, name)
        {
            this.sourceNode = new SourceNode(clock, Name, null);

            this.topics = topics;
            this.TimestampExtractor = timestampExtractor;
            this.keyDeserializer = keyDeserializer;
            this.valDeserializer = valDeserializer;
        }

        public SourceNode(
            IClock clock,
            string name,
            List<string> topics,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valDeserializer)
            : this(clock, name, topics, null, keyDeserializer, valDeserializer)
        {
        }

        public static implicit operator SourceNode(SourceNode<K, V> sourceNode)
        {
            return sourceNode.sourceNode;
        }

        public K deserializeKey(string topic, Headers headers, byte[] data)
        {
            return keyDeserializer.Deserialize(data, false, new SerializationContext(MessageComponentType.Key, topic));
        }

        public V deserializeValue(string topic, Headers headers, byte[] data)
        {
            return valDeserializer.Deserialize(data, data == null, new SerializationContext(MessageComponentType.Value, topic));
        }

        public override void Init(IInternalProcessorContext context)
        {
            base.Init(context);
            this.context = context;

            // if deserializers are null, get the default ones from the context
            if (this.keyDeserializer == null)
            {
                // this.keyDeserializer = context.keySerde.Deserializer;
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
            context.forward(key, value);
            //sourceNodeForwardSensor.record();
        }

        /**
         * @return a string representation of this node, useful for debugging.
         */
        public override string ToString()
        {
            return ToString("");
        }

        /**
         * @return a string representation of this node starting with the given indent, useful for debugging.
         */
        public override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(base.ToString(indent));
            sb.Append(indent).Append("\ttopics:\t\t[");
            foreach (string topic in topics)
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
        void Process<K, V>(K key, V value);
    }
}
