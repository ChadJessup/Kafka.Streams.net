using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;

using System;
using System.Text;

namespace Kafka.Streams.Nodes
{
    public class SinkNode<K, V> : ProcessorNode<K, V>, ISinkNode<K, V>
    {
        private readonly ISerializer<K> keySerializer;
        private readonly ISerializer<V> valSerializer;
        private readonly ITopicNameExtractor topicExtractor;
        private readonly IStreamPartitioner<K, V> partitioner;

        private IInternalProcessorContext context;

        public SinkNode(
            IClock clock,
            string name,
            ITopicNameExtractor topicExtractor,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner)
            : base(clock, name)
        {
            this.topicExtractor = topicExtractor;
            this.keySerializer = keySerializer;
            this.valSerializer = valSerializer;
            this.partitioner = partitioner;
        }

        /**
         * @throws InvalidOperationException if this method.Adds a child to a sink node
         */
        public void AddChild(ProcessorNode<object, object> child)
        {
            throw new InvalidOperationException("sink node does not allow AddChild");
        }

        public override void Init(IInternalProcessorContext context)
        {
            base.Init(context);
            this.context = context;

            // if serializers are null, get the default ones from the context
            // this.keySerializer ??= context.keySerde.Serializer;
            // this.valSerializer ??= context.valueSerde.Serializer;

            // if value serializers are for {@code Change} values, set the inner serializer when necessary
            if (valSerializer is ChangedSerializer<V>
                && ((ChangedSerializer<V>)valSerializer).inner == null)
            {
                // ((ChangedSerializer<V>)valSerializer).setInner(context.valueSerde.Serializer);
            }
        }

        public override void Process(K key, V value)
        {
            IRecordCollector collector = ((ISupplier)context).RecordCollector();

            var timestamp = context.Timestamp;
            if (timestamp < 0)
            {
                throw new StreamsException("Invalid (negative) timestamp of " + timestamp + " for output record <" + key + ":" + value + ">.");
            }

            var topic = topicExtractor.Extract(key, value, this.context.RecordContext);

            try
            {
                collector.Send(topic, key, value, context.Headers, timestamp, keySerializer, valSerializer, partitioner);
            }
            catch (Exception e)
            {
                var keyClass = key == null ? "unknown because key is null" : key.GetType().FullName;
                var valueClass = value == null ? "unknown because value is null" : value.GetType().FullName;

                throw new StreamsException(
                        string.Format("A serializer (key: %s / value: %s) is not compatible to the actual key or value type " +
                                        "(key type: %s / value type: %s). Change the default Serdes in StreamConfig or " +
                                        "provide correct Serdes via method parameters.",
                                        keySerializer.GetType().FullName,
                                        valSerializer.GetType().FullName,
                                        keyClass,
                                        valueClass),
                        e);
            }
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
            var sb = new StringBuilder(base.ToString(indent));

            sb.Append(indent).Append("\ttopic:\t\t");
            sb.Append(topicExtractor);
            sb.Append("\n");

            return sb.ToString();
        }
    }

    public interface ISinkNode<K, V> : ISinkNode
    {
    }

    public interface ISinkNode : IProcessorNode
    {
    }
}