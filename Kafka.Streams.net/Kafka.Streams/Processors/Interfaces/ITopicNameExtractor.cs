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
        string Extract(object key, object value, IRecordContext recordContext);
    }
    
    public interface ITopicNameExtractor<in K, in V> : ITopicNameExtractor
    {
        string Extract(K key, V value, IRecordContext recordContext);

        string ITopicNameExtractor.Extract(object key, object value, IRecordContext recordContext)
            => this.Extract((K)key, (V)value, recordContext);
    }

    public delegate string TopicNameExtractor<in TKey, in TValue>(TKey key, TValue value, IRecordContext recordContext);
}
