using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class WindowedStreamPartitioner<K, V> : IStreamPartitioner<IWindowed<K>, V>
    {
        private readonly IWindowedSerializer<K> serializer;

        public WindowedStreamPartitioner(IWindowedSerializer<K> serializer)
        {
            this.serializer = serializer;
        }

        /**
         * WindowedStreamPartitioner determines the partition number for a record with the given windowed key and value
         * and the current number of partitions. The partition number id determined by the original key of the windowed key
         * using the same logic as DefaultPartitioner so that the topic is partitioned by the original key.
         *
         * @param topic the topic Name this record is sent to
         * @param windowedKey the key of the record
         * @param value the value of the record
         * @param numPartitions the total number of partitions
         * @return an integer between 0 and {@code numPartitions-1}, or {@code null} if the default partitioning logic should be used
         */

        public int Partition(string topic, IWindowed<K> windowedKey, V value, int numPartitions)
        {
            var keyBytes = this.serializer.SerializeBaseKey(topic, windowedKey);

            // hash the keyBytes to choose a partition
            return 0; // toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }
}
