using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.Utils
{
    public static class ClientUtils
    {
        public static Dictionary<TopicPartition, TopicPartitionOffset> fetchEndOffsetsFuture(
            IEnumerable<TopicPartition> topicPartitions,
            IAdminClient consumer)
        {
            if (topicPartitions is null)
            {
                throw new ArgumentNullException(nameof(topicPartitions));
            }

            if (consumer is null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            var results = new Dictionary<TopicPartition, TopicPartitionOffset>();
            foreach (var topicPartition in topicPartitions)
            {
                // var watermark = consumer.GetWatermarkOffsets(topicPartition);
                // results.Add(topicPartition, new TopicPartitionOffset(topicPartition, watermark.High));
            }

            return results;
        }
    }
}
