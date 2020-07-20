using Confluent.Kafka;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Processors.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
        private readonly Dictionary<string, IEnumerable<TopicPartitionOffset>> partitions;
        private readonly List<TopicPartitionOffset> beginningOffsets;
        private readonly List<TopicPartitionOffset> endOffsets;
        private readonly Dictionary<TopicPartition, OffsetAndMetadata> committed;
        private readonly Queue<Task> pollTasks;
        private readonly HashSet<TopicPartition> paused;

        private readonly Dictionary<TopicPartition, List<ConsumeResult<TKey, TValue>>> records;
        private readonly KafkaException pollException;
        private readonly KafkaException offsetsException;
        private readonly bool wakeup;
        private readonly TimeSpan lastPollTimeout;
        private readonly bool closed;
        private readonly bool shouldRebalance;

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

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void UpdatePartitions(string topic, IEnumerable<TopicPartitionOffset> partitions)
        {
            EnsureNotClosed();
            this.partitions.Add(topic, partitions);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void UpdateEndOffsets(IEnumerable<TopicPartitionOffset> newOffsets)
        {
            endOffsets.AddRange(newOffsets);
        }

        public override ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken = default)
        {
            return base.Consume(cancellationToken);
        }

        private void EnsureNotClosed()
        {
            if (this.closed)
            {
                throw new InvalidOperationException("This consumer has already been closed.");
            }
        }
    }
}
