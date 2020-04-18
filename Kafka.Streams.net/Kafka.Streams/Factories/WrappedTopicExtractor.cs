using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Factories
{
    internal class WrappedTopicExtractor : ITopicNameExtractor
    {
        private TopicNameExtractor<object, object> topicExtractor;

        public WrappedTopicExtractor(TopicNameExtractor<object, object> topicExtractor)
        {
            this.topicExtractor = topicExtractor;
        }

        string ITopicNameExtractor.Extract<K1, V1>(K1 key, V1 value, IRecordContext recordContext)
        {
            return this.topicExtractor(key, value, recordContext);
        }
    }
}
