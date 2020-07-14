using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using System;

namespace Kafka.Streams.Tests
{
    public class MockTimestampExtractor : ITimestampExtractor
    {
        public DateTime Extract<K, V>(ConsumeResult<K, V> record, DateTime partitionTime)
        {
            return partitionTime;
        }
    }
}