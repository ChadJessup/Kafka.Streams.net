using Kafka.Streams.Processors;
using RocksDbSharp;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Streams.Errors;

namespace Kafka.Streams.State.Internals
{
    public class RocksDbBatchingRestoreCallback : AbstractNotifyingBatchingRestoreCallback
    {
        private readonly RocksDbStore rocksDBStore;

        public RocksDbBatchingRestoreCallback(RocksDbStore rocksDBStore)
        {
            this.rocksDBStore = rocksDBStore;
        }

        public override void restoreAll(List<KeyValue<byte[], byte[]>> records)
        {
            try
            {
                using (WriteBatch batch = new WriteBatch())
                {
                    rocksDBStore.DbAccessor.prepareBatchForRestore(records, batch);
                    rocksDBStore.write(batch);
                }
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error restoring batch to store " + rocksDBStore.name, e);
            }
        }

        public override void onRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset)
        {
            rocksDBStore.toggleDbForBulkLoading(true);
        }

        public override void onRestoreEnd(
            TopicPartition topicPartition,
            string storeName,
            long totalRestored)
        {
            rocksDBStore.toggleDbForBulkLoading(false);
        }
    }
}