using Kafka.Streams.Topologies;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Kafka.Streams.Processors.Internals
{
    public class Source : AbstractNode, ISource
    {
        private readonly HashSet<string>? topics;
        public Regex? topicPattern { get; }

        public Source(string name, HashSet<string>? topics, Regex? pattern)
            : base(name)
        {
            if (topics == null && pattern == null)
            {
                throw new ArgumentException("Either topics or pattern must be not-null, but both are null.");
            }

            if (topics != null && pattern != null)
            {
                throw new ArgumentException("Either topics or pattern must be null, but both are not null.");
            }

            this.topics = topics;
            this.topicPattern = pattern;
        }

        public HashSet<string>? TopicSet()
        {
            return topics;
        }


        public override void AddPredecessor(INode predecessor)
        {
            throw new InvalidOperationException("Sources don't have predecessors.");
        }

        public override string ToString()
        {
            var topicsString = topics == null
                ? topicPattern?.ToString()
                : topics.ToString();

            return $"Source: {this.Name} (topics: {topicsString })\n      -=> " +
                $"{InternalTopologyBuilder.GetNodeNames(this.Successors)}";
        }


        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var source = (Source)o;
            // omit successor to avoid infinite loops
            return this.Name.Equals(source.Name)
                && (topics?.Equals(source.topics) ?? false)
                && (topicPattern == null
                    ? source.topicPattern == null
                    : topicPattern.ToString().Equals(source.topicPattern?.ToString()));
        }

        public override int GetHashCode()
        {
            // omit successor as it might change and alter the hash code
            return (Name, topics, topicPattern).GetHashCode();
        }
    }
}
