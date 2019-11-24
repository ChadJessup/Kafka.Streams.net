﻿using Confluent.Kafka;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;
using System.Linq;

namespace Kafka.Streams.Factories
{
    public class SinkNodeFactory<K, V> : NodeFactory<K, V>
    {
        private readonly InternalTopologyBuilder internalTopologyBuilder;

        public ITopicNameExtractor topicExtractor { get; }

        private readonly ISerializer<K> keySerializer;
        private readonly ISerializer<V> valueSerializer;
        private readonly IStreamPartitioner<K, V> partitioner;

        public SinkNodeFactory(
            string name,
            string[] predecessors,
            ITopicNameExtractor topicExtractor,
            ISerializer<K> keySerializer,
            ISerializer<V> valSerializer,
            IStreamPartitioner<K, V> partitioner)
            : base(name, predecessors.ToArray())
        {
            this.topicExtractor = topicExtractor;
            this.keySerializer = keySerializer;
            this.valueSerializer = valSerializer;
            this.partitioner = partitioner;
        }

        public override ProcessorNode<K, V> Build()
        {
            if (topicExtractor is StaticTopicNameExtractor)
            {
                string topic = ((StaticTopicNameExtractor)topicExtractor).topicName;
                if (this.internalTopologyBuilder.internalTopicNames.Contains(topic))
                {
                    // prefix the internal topic name with the application id
                    return new SinkNode<K, V>(
                        this.Name,
                        new StaticTopicNameExtractor(this.internalTopologyBuilder.DecorateTopic(topic)),
                        keySerializer,
                        valueSerializer,
                        partitioner);
                }
                else
                {
                    return new SinkNode<K, V>(
                        this.Name,
                        topicExtractor,
                        keySerializer,
                        valueSerializer,
                        partitioner);
                }
            }
            else
            {
                return new SinkNode<K, V>(
                    this.Name,
                    topicExtractor,
                    keySerializer,
                    valueSerializer,
                    partitioner);
            }
        }

        public override INode Describe()
        {
            return new Sink(this.Name, topicExtractor);
        }
    }
}