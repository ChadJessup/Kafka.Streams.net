using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using System;

namespace Kafka.Streams.Topologies
{
    public class Sink : AbstractNode, ISink
    {
        private readonly ITopicNameExtractor topicNameExtractor;

        public Sink(
            string Name,
            ITopicNameExtractor topicNameExtractor)
            : base(Name)
        {
            this.topicNameExtractor = topicNameExtractor;
        }

        public ITopicNameExtractor TopicNameExtractor
            => this.topicNameExtractor;

        public string? Topic
             => this.topicNameExtractor is StaticTopicNameExtractor
                    ? ((StaticTopicNameExtractor)this.topicNameExtractor).topicName
                    : null;


        public override string ToString()
            => this.topicNameExtractor is StaticTopicNameExtractor
                ? $"Sink: {this.Name} (topic: {this.Topic}){Environment.NewLine}      <-- {InternalTopologyBuilder.GetNodeNames(this.Predecessors)}"
                : $"Sink: {this.Name} (extractor: {this.topicNameExtractor}){Environment.NewLine}      <-- {InternalTopologyBuilder.GetNodeNames(this.Predecessors)}";

        public override bool Equals(object other)
        {
            if (this == other)
            {
                return true;
            }

            if (other == null || this.GetType() != other.GetType())
            {
                return false;
            }

            var sink = (Sink)other;

            return this.Name.Equals(sink.Name)
                && this.TopicNameExtractor.Equals(sink.TopicNameExtractor)
                && this.Predecessors.Equals(sink.Predecessors);
        }


        public override int GetHashCode()
            // omit predecessors as it might change and alter the hash code
            => (this.Name, this.topicNameExtractor).GetHashCode();
    }
}