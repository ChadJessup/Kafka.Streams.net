using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Topologies
{
    /// <summary>
    /// A sink node of a topology.
    /// </summary>
    public interface ISink : INode
    {
        /// <summary>
        /// Gets the topic Name this sink node is writing to.
        /// Could be null if the topic Name can only be dynamically determined based on <seealso cref="ITopicNameExtractor"/>.
        /// </summary>
        string? Topic { get; }

        /// <summary>
        /// The <seealso cref="ITopicNameExtractor"/> that this sink node uses to dynamically
        /// extract the topic Name to write to.
        /// Could be null if the topic Name is not dynamically determined.
        /// </summary>
        ITopicNameExtractor TopicNameExtractor { get; }
    }
}
