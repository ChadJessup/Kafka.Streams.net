using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.Interfaces
{
    /// <summary>
    /// A sink node of a topology.
    /// </summary>
    public interface ISink : INode
    {
        /// <summary>
        /// Gets the topic name this sink node is writing to.
        /// Could be null if the topic name can only be dynamically determined based on <seealso cref="ITopicNameExtractor"/>.
        /// </summary>
        string? Topic { get; }

        /// <summary>
        /// The <seealso cref="ITopicNameExtractor"/> that this sink node uses to dynamically
        /// extract the topic name to write to.
        /// Could be null if the topic name is not dynamically determined.
        /// </summary>
        ITopicNameExtractor TopicNameExtractor { get; }
    }
}
