
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class RocksDbWindowBytesStoreSupplier : IWindowBytesStoreSupplier
//    {
//        private string name;
//        private long retentionPeriod;
//        private long segmentInterval;
//        private long windowSize;
//        private bool retainDuplicates;
//        private bool returnTimestampedStore;

//        public RocksDbWindowBytesStoreSupplier(
//            string name,
//            long retentionPeriod,
//            long segmentInterval,
//            long windowSize,
//            bool retainDuplicates,
//            bool returnTimestampedStore)
//        {
//            this.name = name;
//            this.retentionPeriod = retentionPeriod;
//            this.segmentInterval = segmentInterval;
//            this.windowSize = windowSize;
//            this.retainDuplicates = retainDuplicates;
//            this.returnTimestampedStore = returnTimestampedStore;
//        }

//        public IWindowStore<Bytes, byte[]> get()
//        {
//            if (!returnTimestampedStore)
//            {
//                return new RocksDbWindowStore(
//                    new RocksDbSegmentedBytesStore(
//                        name,
//                        metricsScope(),
//                        retentionPeriod,
//                        segmentInterval,
//                        new WindowKeySchema()),
//                    retainDuplicates,
//                    windowSize);
//            }
//            else
//            {
//                return new RocksDbTimestampedWindowStore(
//                    new RocksDbTimestampedSegmentedBytesStore(
//                        name,
//                        metricsScope(),
//                        retentionPeriod,
//                        segmentInterval,
//                        new WindowKeySchema()),
//                    retainDuplicates,
//                    windowSize);
//            }
//        }

//        public string metricsScope()
//        {
//            return "rocksdb-window-state";
//        }

//        public long segmentIntervalMs()
//        {
//            return segmentInterval;
//        }
//    }
//}