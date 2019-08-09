using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Common.Extensions
{
    public static class ConsumerExtensions
    {
        public static ConsumerRecords<K, V> poll<K, V>(this IConsumer<K, V> consumer, TimeSpan timeout)
        {
            return new ConsumerRecords<K, V>();
        }
    }
}
