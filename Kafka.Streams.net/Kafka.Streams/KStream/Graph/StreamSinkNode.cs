using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StreamSinkNode<K, V> : StreamsGraphNode
    {
        private readonly ITopicNameExtractor topicNameExtractor;
        private readonly ProducedInternal<K, V> producedInternal;

        public StreamSinkNode(
            string nodeName,
            ITopicNameExtractor topicNameExtractor,
            ProducedInternal<K, V> producedInternal)
            : base(nodeName)
        {
            this.topicNameExtractor = topicNameExtractor;
            this.producedInternal = producedInternal;
        }

        public override string ToString()
        {
            return "StreamSinkNode{" +
                   "topicNameExtractor=" + topicNameExtractor +
                   ", producedInternal=" + producedInternal +
                   "} " + base.ToString();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            var keySerializer = producedInternal.KeySerde?.Serializer;

            var valSerializer = producedInternal.ValueSerde?.Serializer;

            IStreamPartitioner<K, V> partitioner = producedInternal.StreamPartitioner();
            var parentNames = ParentNodeNames();

            if (partitioner == null && keySerializer is IWindowedSerializer<K>)
            {
                var windowedPartitioner = (IStreamPartitioner<K, V>)new WindowedStreamPartitioner<K, V>((IWindowedSerializer<K>)keySerializer);
                topologyBuilder.AddSink(NodeName, topicNameExtractor, keySerializer, valSerializer, windowedPartitioner, parentNames);
            }
            else
            {
                topologyBuilder.AddSink(NodeName, topicNameExtractor, keySerializer, valSerializer, partitioner, parentNames);
            }
        }
    }
}
