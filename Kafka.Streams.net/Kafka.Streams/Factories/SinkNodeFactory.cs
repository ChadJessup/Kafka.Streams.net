﻿using System.Linq;
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.Factories
{
    public class SinkNodeFactory<K, V> : NodeFactory<K, V>, ISinkNodeFactory<K, V>
    {
        private readonly InternalTopologyBuilder internalTopologyBuilder;

        public ITopicNameExtractor TopicExtractor { get; }

        private readonly ISerializer<K>? keySerializer;
        private readonly ISerializer<V>? valueSerializer;
        private readonly IStreamPartitioner<K, V>? partitioner;

        public SinkNodeFactory(
            KafkaStreamsContext context,
            string Name,
            string[] predecessors,
            TopicNameExtractor<K, V> topicExtractor,
            ISerializer<K>? keySerializer,
            ISerializer<V>? valSerializer,
            IStreamPartitioner<K, V>? partitioner,
            InternalTopologyBuilder internalTopologyBuilder)
            : base(context, Name, predecessors.ToArray())
        {
            this.partitioner = partitioner;
            this.keySerializer = keySerializer;
            this.valueSerializer = valSerializer;
            this.TopicExtractor = new WrappedTopicExtractor<K, V>(topicExtractor);
            this.internalTopologyBuilder = internalTopologyBuilder;
        }

        public override IProcessorNode Build()
        {
            if (this.TopicExtractor is StaticTopicNameExtractor extractor)
            {
                var topic = extractor.TopicName;
                if (this.internalTopologyBuilder.internalTopicNames.Contains(topic))
                {
                    // prefix the internal topic Name with the application id
                    return new SinkNode<K, V>(
                        this.Context,
                        this.Name,
                        new StaticTopicNameExtractor(this.internalTopologyBuilder.DecorateTopic(topic)),
                        this.keySerializer,
                        this.valueSerializer,
                        this.partitioner);
                }
                else
                {
                    return new SinkNode<K, V>(
                        this.Context,
                        this.Name,
                        this.TopicExtractor,
                        this.keySerializer,
                        this.valueSerializer,
                        this.partitioner);
                }
            }
            else
            {
                return new SinkNode<K, V>(
                    this.Context,
                    this.Name,
                    this.TopicExtractor,
                    this.keySerializer,
                    this.valueSerializer,
                    this.partitioner);
            }
        }

        public override INode Describe()
        {
            return new Sink(this.Name, this.TopicExtractor);
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
