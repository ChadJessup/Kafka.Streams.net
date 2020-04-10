
namespace Kafka.Streams.Processors.Interfaces
{
    /// <summary>
    /// An interface that dynamically determines the Name of the Kafka topic to send at the sink node of the topology.
    /// </summary>
    public interface ITopicNameExtractor
    {
        /// <summary>
        /// Extracts the topic Name to send to.The topic Name must already exist, since the Kafka Streams library will not
        /// try to automatically create the topic with the extracted Name.
        /// </summary>
        /// <param Name="key">The record key.</param>
        /// <param Name="value">The record value.</param>
        /// <param Name="recordContext">Current context metadata of the record.</param>
        /// <returns>The topic Name this record should be sent to.</returns>
        string Extract<K, V>(K key, V value, IRecordContext recordContext);
    }
}
