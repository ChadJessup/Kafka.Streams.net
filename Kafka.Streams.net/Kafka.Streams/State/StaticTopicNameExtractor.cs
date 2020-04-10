using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Processors.Internals
{
    /**
     * Static topic Name extractor
     */
    public class StaticTopicNameExtractor : ITopicNameExtractor
    {
        public string topicName { get; }

        public StaticTopicNameExtractor(string topicName)
        {
            this.topicName = topicName;
        }

        public string Extract<K, V>(K key, V value, IRecordContext recordContext)
        {
            return this.topicName;
        }

        public override string ToString()
        {
            return $"StaticTopicNameExtractor({this.topicName})";
        }
    }
}