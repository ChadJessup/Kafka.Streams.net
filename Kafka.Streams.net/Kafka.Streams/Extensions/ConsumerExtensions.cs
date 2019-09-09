using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Extensions
{
    public static class ConsumerExtensions
    {
        public static void SeekToBeginning<K, V>(this IConsumer<K, V> consumer, IEnumerable<TopicPartition> partitions)
        {
            if (partitions is null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }
        }
    }
}
