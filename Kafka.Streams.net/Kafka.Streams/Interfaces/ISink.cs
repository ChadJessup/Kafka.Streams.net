using Kafka.Streams.IProcessor.Interfaces;

namespace Kafka.Streams.Interfaces
{
    /**
     * A sink node of a topology.
     */
    public interface ISink<K, V> : INode
    {
        /**
         * The topic name this sink node is writing to.
         * Could be {@code null} if the topic name can only be dynamically determined based on {@link TopicNameExtractor}
         * @return a topic name
         */
        string Topic { get; }

        /**
         * The {@link TopicNameExtractor} that this sink node uses to dynamically
         * extract the topic name to write to.
         * Could be {@code null} if the topic name is not dynamically determined.
         * @return the {@link TopicNameExtractor} used get the topic name
         */
        ITopicNameExtractor<K, V> topicNameExtractor();
    }
}
