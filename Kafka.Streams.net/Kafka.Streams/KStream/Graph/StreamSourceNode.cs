using Kafka.Streams.KStream.Internals;
using Kafka.Streams.KStream.Internals.Graph;
using Kafka.Streams.Topologies;
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
            return new List<string>(topicNames);
        }

        public override string ToString()
            => "StreamSourceNode{" +
                   $"topicNames={topicNames}" +
                   $", topicPattern={topicPattern}" +
                   $", consumedInternal={consumedInternal}" +
                   $"}} {base.ToString()}";

        public override void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {

            if (topicPattern != null)
            {
                //topologyBuilder.addSource(consumedInternal.offsetResetPolicy(),
                //                          nodeName,
                //                          consumedInternal.timestampExtractor,
                //                          consumedInternal.keyDeserializer(),
                //                          consumedInternal.valueDeserializer(),
                //                          topicPattern);
            }
            else
            {

                topologyBuilder.AddSource(
                    consumedInternal.OffsetResetPolicy(),
                    NodeName,
                    consumedInternal.timestampExtractor,
                    consumedInternal.keyDeserializer(),
                    consumedInternal.valueDeserializer(),
                    topicNames.ToArray());

            }
        }
    }
}
