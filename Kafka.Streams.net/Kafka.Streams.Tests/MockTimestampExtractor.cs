using Confluent.Kafka;
using Kafka.Streams.Interfaces;

namespace Kafka.Streams.Tests
{
    public class MockTimestampExtractor : ITimestampExtractor
    {
        public long Extract<K, V>(ConsumeResult<K, V> record, long partitionTime)
        {
            throw new System.NotImplementedException();
        }
    }
}