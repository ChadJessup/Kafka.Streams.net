using Confluent.Kafka;
using Kafka.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka
{
    public static class ConsumerExtensions
    {
        public static ConsumerRecords<K, V> Poll<K, V>(this IConsumer<K, V> client, long timeoutMs)
            => Poll(client, TimeSpan.FromMilliseconds(timeoutMs), false);

        public static ConsumerRecords<K, V> Poll<K, V>(this IConsumer<K, V> client, TimeSpan timeout)
            => Poll(client, timeout, true);

        public static ConsumerRecords<K, V> Poll<K, V>(this IConsumer<K, V> consumer, TimeSpan timeout, bool includeMetadataInTimeout)
        {
            if (!consumer.Assignment.Any())
            {
                throw new InvalidOperationException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            using var cts = new CancellationTokenSource(timeout);

            var consumeResults = new List<ConsumeResult<K, V>>();

            // poll for new data until the timeout expires
            while (!cts.IsCancellationRequested)
            {
                consumeResults.Add(consumer.Consume(cts.Token));
            }

            return new ConsumerRecords<K, V>(consumeResults);
        }

        /**
         * Seek to the last offset for each of the given partitions. This function evaluates lazily, seeking to the
         * final offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
         * If no partitions are provided, seek to the final offset for all of the currently assigned partitions.
         * <p>
         * If {@code isolation.level=read_committed}, the end offset will be the Last Stable Offset, i.e., the offset
         * of the first message with an open transaction.
         *
         * @throws IllegalArgumentException if {@code partitions} is {@code null}
         * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
         */
        public static void SeekToEnd<K, V>(this IConsumer<K, V> consumer, IEnumerable<TopicPartition> partitions)
        {
            if (partitions == null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }

            List<TopicPartition> parts = !partitions.Any()
                ? consumer.Assignment
                : partitions.ToList();

            foreach (var part in parts)
            {
                consumer.Seek(new TopicPartitionOffset(part, Offset.End));
            }
        }

        public static void SeekToBeginning<K, V>(this IConsumer<K, V> consumer, IEnumerable<TopicPartition> partitions)
        {
            if (partitions == null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }

            List<TopicPartition> parts = !partitions.Any()
                ? consumer.Assignment
                : partitions.ToList();

            foreach (var part in parts)
            {
                consumer.Seek(new TopicPartitionOffset(part, Offset.Beginning));
            }
        }
    }
}
