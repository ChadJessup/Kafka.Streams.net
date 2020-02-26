using Confluent.Kafka;
using Kafka.Streams.Clients.Consumers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Tests
{
    public class MockRestoreConsumer : RestoreConsumer
    {
        public MockRestoreConsumer(IConsumer<byte[], byte[]> mockConsumer)
            : base(null, mockConsumer)
        {
        }
    }

    public class MockConsumer<TKey, TValue> : BaseConsumer<TKey, TValue>
    {
        public MockConsumer(IConsumer<TKey, TValue> mockConsumer)
            : base(null, mockConsumer)
        {
        }

        internal void UpdateBeginningOffsets(Dictionary<TopicPartition, long> offsets)
        {
        }
    }
}
