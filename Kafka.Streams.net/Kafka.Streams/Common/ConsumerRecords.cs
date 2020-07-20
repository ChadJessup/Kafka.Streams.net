using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Common
{
    public class ConsumerRecord
    {
        public static long NO_TIMESTAMP = -1L;

    }

    public static class ConsumerRecords
    {
        public static ConsumerRecords<K, V> EMPTY<K, V>() => new ConsumerRecords<K, V>();
    }

    public class ConsumerRecords<K, V> : List<ConsumeResult<K, V>>
    {

        private readonly Dictionary<TopicPartition, List<ConsumeResult<K, V>>> records = new Dictionary<TopicPartition, List<ConsumeResult<K, V>>>();

        public ConsumerRecords()
        {
        }

        public ConsumerRecords(Dictionary<TopicPartition, List<ConsumeResult<K, V>>> records)
            => this.records = records;

        public ConsumerRecords(List<ConsumeResult<K, V>> consumeResults)
        {
            this.AddRange(consumeResults);
            this.records = new Dictionary<TopicPartition, List<ConsumeResult<K, V>>>();

            foreach (var result in consumeResults.Where(r => r != null))
            {
                if (this.records.ContainsKey(result.TopicPartition))
                {
                    this.records[result.TopicPartition].Add(result);
                }
                else
                {
                    this.records.Add(result.TopicPartition, new List<ConsumeResult<K, V>>
                    {
                        result
                    });
                }
            }
        }

        public IEnumerable<TopicPartition> Partitions => this.Select(cr => cr.TopicPartition);

        /**
         * Get just the records for the given partition
         * 
         * @param partition The partition to get records for
         */
        public List<ConsumeResult<K, V>> GetRecords(TopicPartition partition)
        {
            List<ConsumeResult<K, V>> recs = this.records[partition];
            if (recs == null)
            {
                return new List<ConsumeResult<K, V>>();
            }
            else
            {
                return recs;
            }
        }

        /**
         * Get just the records for the given topic
         */
        public IEnumerable<ConsumeResult<K, V>> GetRecords(string topic)
        {
            if (topic == null)
            {
                throw new ArgumentException("Topic must be non-null.");
            }

            List<List<ConsumeResult<K, V>>> recs = new List<List<ConsumeResult<K, V>>>();

            foreach (var entry in this.records)
            {
                if (entry.Key.Topic.Equals(topic))
                {
                    recs.Add(entry.Value);
                }
            }

            return recs.SelectMany(r => r);
        }
    }
}