using Kafka.Streams.Extensions;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Kafka.Streams.KStream.Graph
{
    public class StreamSourceNode<K, V> : StreamsGraphNode
    {
        private readonly IEnumerable<string> topicNames;
        private readonly Regex topicPattern;
        protected ConsumedInternal<K, V> consumedInternal;

        public StreamSourceNode(
            string nodeName,
            IEnumerable<string> topicNames,
            ConsumedInternal<K, V> consumedInternal)
            : base(nodeName)
        {
            this.topicNames = topicNames;
            this.consumedInternal = consumedInternal;
        }

        public StreamSourceNode(
            string nodeName,
            Regex topicPattern,
            ConsumedInternal<K, V> consumedInternal)
            : base(nodeName)
        {
            this.topicPattern = topicPattern;
            this.consumedInternal = consumedInternal;
        }

        public List<string> GetTopicNames()
        {
            return new List<string>(this.topicNames);
        }

        public override string ToString()
            => "StreamSourceNode{" +
                   $"topicNames={this.topicNames.ToJoinedString()}" +
                   $", topicPattern={this.topicPattern}" +
                   $", consumedInternal={this.consumedInternal.GetType().Name}" +
                   $"}} {base.ToString()}";

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
            if (topologyBuilder is null)
            {
                throw new ArgumentNullException(nameof(topologyBuilder));
            }

            if (this.topicPattern != null)
            {
                topologyBuilder.AddSource(
                    this.consumedInternal.OffsetResetPolicy(),
                    this.NodeName,
                    this.consumedInternal.timestampExtractor,
                    this.consumedInternal.KeyDeserializer(),
                    this.consumedInternal.ValueDeserializer(),
                    this.topicPattern.ToString());
            }
            else
            {
                topologyBuilder.AddSource(
                    this.consumedInternal.OffsetResetPolicy(),
                    this.NodeName,
                    this.consumedInternal.timestampExtractor,
                    this.consumedInternal.KeyDeserializer(),
                    this.consumedInternal.ValueDeserializer(),
                    this.topicNames.ToArray());
            }
        }
    }
}
