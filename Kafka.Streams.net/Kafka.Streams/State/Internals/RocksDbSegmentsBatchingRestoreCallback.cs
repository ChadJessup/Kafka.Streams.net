
//using Confluent.Kafka;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class RocksDbSegmentsBatchingRestoreCallback : AbstractNotifyingBatchingRestoreCallback
//    {
//        public override void restoreAll(List<KeyValue<byte[], byte[]>> records)
//        {
//            restoreAllInternal(records);
//        }

//        public override void onRestoreStart(
//            TopicPartition topicPartition,
//            string storeName,
//            long startingOffset,
//            long endingOffset)
//        {
//            toggleForBulkLoading(true);
//        }

//        public override void onRestoreEnd(
//            TopicPartition topicPartition,
//            string storeName,
//            long totalRestored)
//        {
//            toggleForBulkLoading(false);
//        }
//    }
//}
