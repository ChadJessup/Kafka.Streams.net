using Kafka.Streams.Processors;
using RocksDbSharp;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Streams.Errors;

namespace Kafka.Streams.RocksDbState
{
    public class RocksDbBatchingRestoreCallback : AbstractNotifyingBatchingRestoreCallback
    {
        private readonly RocksDbStore rocksDBStore;

        public RocksDbBatchingRestoreCallback(RocksDbStore rocksDBStore)
        {
            this.rocksDBStore = rocksDBStore;
        }

        public override void RestoreAll(List<KeyValuePair<byte[], byte[]>> records)
        {
            try
            {
                using var batch = new WriteBatch();

                this.rocksDBStore.DbAccessor.PrepareBatchForRestore(records, batch);
                this.rocksDBStore.Write(batch);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error restoring batch to store " + this.rocksDBStore.Name, e);
            }
        }

        public override void OnRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset)
        {
            this.rocksDBStore.ToggleDbForBulkLoading(true);
        }

        public override void OnRestoreEnd(
            TopicPartition topicPartition,
            string storeName,
            long totalRestored)
        {
            this.rocksDBStore.ToggleDbForBulkLoading(false);
        }
    }
}