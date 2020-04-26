using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Factories
{
    internal class WrappedTopicExtractor<K, V> : ITopicNameExtractor<K, V>
    {
        private TopicNameExtractor<K, V> topicExtractor;

        public WrappedTopicExtractor(TopicNameExtractor<K, V> topicExtractor)
        {
            this.topicExtractor = topicExtractor ?? throw new System.ArgumentNullException(nameof(topicExtractor));
        }

        public string Extract(K key, V value, IRecordContext recordContext)
        {
            return this.topicExtractor.Invoke(key, value, recordContext);
        }
    }
}
