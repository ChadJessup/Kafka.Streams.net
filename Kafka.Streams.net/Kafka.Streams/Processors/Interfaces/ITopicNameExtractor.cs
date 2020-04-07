
namespace Kafka.Streams.Processors.Interfaces
{
    /// <summary>
    /// An interface that dynamically determines the name of the Kafka topic to send at the sink node of the topology.
    /// </summary>
    public interface ITopicNameExtractor
    {
        /// <summary>
        /// Extracts the topic name to send to.The topic name must already exist, since the Kafka Streams library will not
        /// try to automatically create the topic with the extracted name.
        /// </summary>
        /// <param name="key">The record key.</param>
        /// <param name="value">The record value.</param>
        /// <param name="recordContext">Current context metadata of the record.</param>
        /// <returns>The topic name this record should be sent to.</returns>
        string Extract<K, V>(K key, V value, IRecordContext recordContext);
    }
}
