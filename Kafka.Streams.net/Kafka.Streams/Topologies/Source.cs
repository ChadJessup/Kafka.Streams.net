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

        public Source(string Name, HashSet<string>? topics, Regex? pattern)
            : base(Name)
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
            return this.topics;
        }


        public override void AddPredecessor(INode predecessor)
        {
            throw new InvalidOperationException("Sources don't have predecessors.");
        }

        public override string ToString()
        {
            var topicsString = this.topics == null
                ? this.topicPattern?.ToString()
                : this.topics.ToJoinedString();

            return $"Source: {this.Name} (topics: {topicsString })\n      -=> " +
                $"{InternalTopologyBuilder.GetNodeNames(this.Successors)}";
        }


        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var source = (Source)o;
            // omit successor to avoid infinite loops
            if (!this.Name.Equals(source.Name))
            {
                return false;
            }

            if (this.topics == null && source.topics != null)
            {
                return false;
            }

            if (source.topics == null && this.topics != null)
            {
                return false;
            }

            return this.topicPattern == null
                ? source.topicPattern == null
                : this.topicPattern.ToString().Equals(source.topicPattern?.ToString());
        }

        public override int GetHashCode()
        {
            // omit successor as it might change and alter the hash code
            return (this.Name, this.topics, this.topicPattern).GetHashCode();
        }
    }
}
