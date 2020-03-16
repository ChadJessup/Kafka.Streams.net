using Confluent.Kafka;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;
using NodaTime;
using System.Linq;

namespace Kafka.Streams.Factories
{
    public class SinkNodeFactory<K, V> : NodeFactory<K, V>, ISinkNodeFactory<K, V>
    {
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly IClock clock;

        public ITopicNameExtractor TopicExtractor { get; }

        private readonly ISerializer<K> keySerializer;
        private readonly ISerializer<V> valueSerializer;
        private readonly IStreamPartitioner<K, V> partitioner;

        public SinkNodeFactory(
            IClock clock,
            string name,
            string[] predecessors,
            ITopicNameExtractor topicExtractor,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner)
            : base(clock, name, predecessors.ToArray())
        {
            this.clock = clock;
            this.TopicExtractor = topicExtractor;
            this.keySerializer = keySerializer;
            this.valueSerializer = valSerializer;
            this.partitioner = partitioner;
        }

        public override IProcessorNode Build()
        {
            if (TopicExtractor is StaticTopicNameExtractor)
            {
                var topic = ((StaticTopicNameExtractor)TopicExtractor).topicName;
                if (this.internalTopologyBuilder.internalTopicNames.Contains(topic))
                {
                    // prefix the internal topic name with the application id
                    return new SinkNode<K, V>(
                        this.clock,
                        this.Name,
                        new StaticTopicNameExtractor(this.internalTopologyBuilder.DecorateTopic(topic)),
                        keySerializer,
                        valueSerializer,
                        partitioner);
                }
                else
                {
                    return new SinkNode<K, V>(
                        this.clock,
                        this.Name,
                        TopicExtractor,
                        keySerializer,
                        valueSerializer,
                        partitioner);
                }
            }
            else
            {
                return new SinkNode<K, V>(
                    this.clock,
                    this.Name,
                    TopicExtractor,
                    keySerializer,
                    valueSerializer,
                    partitioner);
            }
        }

        public override INode Describe()
        {
            return new Sink(this.Name, TopicExtractor);
        }
    }

    public interface ISinkNodeFactory<K, V> : ISinkNodeFactory, INodeFactory<K, V>
    {
    }

    public interface ISinkNodeFactory : INodeFactory
    {
        ITopicNameExtractor TopicExtractor { get; }
    }
}
