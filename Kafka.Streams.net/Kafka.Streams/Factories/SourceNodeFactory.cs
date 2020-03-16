using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;
using NodaTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Kafka.Streams.Factories
{
    public class SourceNodeFactory<K, V> : NodeFactory<K, V>, ISourceNodeFactory
    {
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly ITimestampExtractor? timestampExtractor;
        private readonly IDeserializer<V>? valueDeserializer;
        private readonly IDeserializer<K>? keyDeserializer;

        public SourceNodeFactory(
            IClock clock,
            string name,
            string[]? topics,
            Regex? pattern,
            ITimestampExtractor? timestampExtractor,
            Dictionary<string, Regex>? topicToPatterns,
            Dictionary<string, List<string>>? nodeToSourceTopics,
            InternalTopologyBuilder internalTopologyBuilder,
            IDeserializer<K>? keyDeserializer,
            IDeserializer<V>? valueDeserializer)
            : base(clock, name, Array.Empty<string>())
        {
            this.clock = clock;
            this.Topics = topics != null
                ? topics.ToList()
                : new List<string>();

            this.internalTopologyBuilder = internalTopologyBuilder;
            this.nodeToSourceTopics = nodeToSourceTopics;
            this.timestampExtractor = timestampExtractor;
            this.valueDeserializer = valueDeserializer;
            this.keyDeserializer = keyDeserializer;
            this.topicToPatterns = topicToPatterns;
            this.Pattern = pattern;
        }

        private readonly IClock clock;

        public List<string> Topics { get; private set; }

        private readonly Dictionary<string, Regex> topicToPatterns;

        private readonly Dictionary<string, List<string>>? nodeToSourceTopics;

        public Regex? Pattern { get; }

        public List<string> GetTopics(List<string> subscribedTopics)
        {
            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (!subscribedTopics.Any() && Pattern != null)
            {
                return new List<string>(new[] { Pattern.ToString() });
            }

            var matchedTopics = new List<string>();
            foreach (var update in subscribedTopics ?? Enumerable.Empty<string>())
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

        public override IProcessorNode Build()
        {
            List<string> sourceTopics = nodeToSourceTopics[Name];

            // if it is subscribed via patterns, it is possible that the topic metadata has not been updated
            // yet and hence the map from source node to topics is stale, in this case we put the pattern as a place holder;
            // this should only happen for debugging since during runtime this function should always be called after the metadata has updated.
            if (sourceTopics == null)
            {
                return new SourceNode<K, V>(
                    this.clock,
                    Name,
                    new List<string>(),
                    timestampExtractor,
                    keyDeserializer,
                    valueDeserializer);
            }
            else
            {
                return new SourceNode<K, V>(
                    this.clock,
                    Name,
                    internalTopologyBuilder.MaybeDecorateInternalSourceTopics(sourceTopics),
                    timestampExtractor,
                    keyDeserializer,
                    valueDeserializer);
            }
        }

        private bool IsMatch(string topic)
        {
            return Pattern?.IsMatch(topic) ?? false;
        }

        public override INode Describe()
        {
            return new Source(Name,
                Topics.Count == 0
                    ? null
                    : new HashSet<string>(Topics),
                Pattern);
        }
    }
}
