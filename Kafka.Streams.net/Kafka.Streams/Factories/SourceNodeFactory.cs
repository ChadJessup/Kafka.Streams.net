using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Kafka.Streams.Factories
{
    public class SourceNodeFactory<K, V> : NodeFactory<K, V>, ISourceNodeFactory
    {
        private readonly ITimestampExtractor timestampExtractor;
        private readonly IDeserializer<V> valueDeserializer;
        private readonly IDeserializer<K> keyDeserializer;

        public SourceNodeFactory(
            string name,
            string[] topics,
            Regex pattern,
            ITimestampExtractor timestampExtractor,
            Dictionary<string, Regex> topicToPatterns,
            Dictionary<string, List<string>> nodeToSourceTopics,
            IDeserializer<K> keyDeserializer,
            IDeserializer<V> valueDeserializer)
            : base(name, Array.Empty<string>())
        {
            this.Topics = topics != null
                ? topics.ToList()
                : new List<string>();

            this.nodeToSourceTopics = nodeToSourceTopics;
            this.timestampExtractor = timestampExtractor;
            this.valueDeserializer = valueDeserializer;
            this.keyDeserializer = keyDeserializer;
            this.topicToPatterns = topicToPatterns;
            this.Pattern = pattern;
        }

        public List<string> Topics { get; private set; }

        private readonly Dictionary<string, Regex> topicToPatterns;

        private Dictionary<string, List<string>> nodeToSourceTopics;

        public Regex Pattern { get; }

        private readonly InternalTopologyBuilder internalTopologyBuilder;

        public List<string> GetTopics(List<string> subscribedTopics)
        {
            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (!subscribedTopics.Any())
            {
                return new List<string>(new[] { Pattern.ToString() });
            }

            List<string> matchedTopics = new List<string>();
            foreach (string update in subscribedTopics ?? Enumerable.Empty<string>())
            {
                if (Pattern == topicToPatterns[update])
                {
                    matchedTopics.Add(update);
                }
                else if (topicToPatterns.ContainsKey(update) && IsMatch(update))
                {
                    // the same topic cannot be matched to more than one pattern
                    // TODO: we should lift this requirement in the future
                    throw new TopologyException($"Topic {update}" +
                        $" is already matched for another regex pattern {topicToPatterns[update]}" +
                        $" and hence cannot be matched to this regex pattern {Pattern} any more.");
                }
                else if (IsMatch(update))
                {
                    topicToPatterns.Add(update, Pattern);
                    matchedTopics.Add(update);
                }
            }

            return matchedTopics;
        }

        public override ProcessorNode<K, V> Build()
        {
            List<string> sourceTopics = nodeToSourceTopics[Name];

            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (sourceTopics == null)
            {
                return new SourceNode<K, V>(
                    Name,
                    new List<string>(),
                    timestampExtractor,
                    keyDeserializer,
                    valueDeserializer);
            }
            else
            {
                return new SourceNode<K, V>(
                    Name,
                    internalTopologyBuilder.MaybeDecorateInternalSourceTopics(sourceTopics),
                    timestampExtractor,
                    keyDeserializer,
                    valueDeserializer);
            }
        }

        private bool IsMatch(string topic)
        {
            return Pattern.IsMatch(topic);
        }

        public override INode Describe()
        {
            return new Source(Name, Topics.Count == 0
                ? null
                : new HashSet<string>(Topics),
                Pattern);
        }
    }
}
