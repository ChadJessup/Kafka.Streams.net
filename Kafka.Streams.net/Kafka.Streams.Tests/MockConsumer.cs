using Confluent.Kafka;
using Kafka.Streams.Clients.Consumers;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

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

        public Dictionary<TopicPartition, long> BeginningPartitionOffsets { get; set; } = new Dictionary<TopicPartition, long>();
        public override List<TopicPartition> Assignment { get; } = new List<TopicPartition>();

        public void UpdateBeginningOffsets(Dictionary<TopicPartition, long> offsets)
        {
            foreach (var tpo in offsets)
            {
                this.BeginningPartitionOffsets[tpo.Key] = tpo.Value;
            }
        }

        public override void Assign(IEnumerable<TopicPartition> partitions)
        {
            this.Assignment.AddRange(partitions);
        }

        public override void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            this.Assignment.AddRange(partitions.Select(p => p.TopicPartition));
        }

        public override void Assign(TopicPartition partition)
        {
            this.Assignment.Add(partition);
        }

        public override void Assign(TopicPartitionOffset partition)
        {
            this.Assignment.Add(partition.TopicPartition);
        }

        public override ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default)
        {
            return base.Consume(cancellationToken);
        }
    }
}
