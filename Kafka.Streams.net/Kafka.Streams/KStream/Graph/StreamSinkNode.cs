using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Topologies;
using System;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StreamSinkNode<K, V> : StreamsGraphNode
    {
        private readonly TopicNameExtractor<K, V> topicNameExtractor;
        private readonly ProducedInternal<K, V> producedInternal;

        public StreamSinkNode(
            string nodeName,
            TopicNameExtractor<K, V> topicNameExtractor,
            ProducedInternal<K, V> producedInternal)
            : base(nodeName)
        {
            this.topicNameExtractor = topicNameExtractor;
            this.producedInternal = producedInternal;
        }

        public override string ToString()
        {
            return "StreamSinkNode{" +
                   "topicNameExtractor=" + this.topicNameExtractor +
                   ", producedInternal=" + this.producedInternal +
                   "} " + base.ToString();
        }

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            topologyBuilder = topologyBuilder ?? throw new ArgumentNullException(nameof(topologyBuilder));

            var keySerializer = this.producedInternal.KeySerde?.Serializer;

            var valSerializer = this.producedInternal.ValueSerde?.Serializer;

            IStreamPartitioner<K, V> partitioner = this.producedInternal.StreamPartitioner();
            var parentNames = this.ParentNodeNames();

            if (partitioner == null && keySerializer is IWindowedSerializer<K>)
            {
                var windowedPartitioner = (IStreamPartitioner<K, V>)new WindowedStreamPartitioner<K, V>((IWindowedSerializer<K>)keySerializer);
                topologyBuilder.AddSink(this.NodeName, this.topicNameExtractor, keySerializer, valSerializer, windowedPartitioner, parentNames);
            }
            else
            {
                topologyBuilder.AddSink(this.NodeName, this.topicNameExtractor, keySerializer, valSerializer, partitioner, parentNames);
            }
        }
    }
}
