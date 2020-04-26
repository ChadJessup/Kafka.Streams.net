using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * Static topic Name extractor
     */
    public class StaticTopicNameExtractor : ITopicNameExtractor
    {
        public string TopicName { get; }

        public StaticTopicNameExtractor(string topicName)
        {
            this.TopicName = topicName;
        }

        public string Extract(object key, object value, IRecordContext recordContext)
        {
            return this.TopicName;
        }

        public override string ToString()
        {
            return $"StaticTopicNameExtractor({this.TopicName})";
        }
    }
}
