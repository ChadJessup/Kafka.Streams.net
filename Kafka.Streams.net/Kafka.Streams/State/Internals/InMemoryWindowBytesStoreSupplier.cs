
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;
//using System;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemoryWindowBytesStoreSupplier : IWindowBytesStoreSupplier
//    {
//        private string name;
//        private long retentionPeriod;
//        private long windowSize;
//        private bool retainDuplicates;

//        public InMemoryWindowBytesStoreSupplier(string name,
//                                                long retentionPeriod,
//                                                long windowSize,
//                                                bool retainDuplicates)
//        {
//            this.name = name;
//            this.retentionPeriod = retentionPeriod;
//            this.windowSize = windowSize;
//            this.retainDuplicates = retainDuplicates;
//        }

//        public IWindowStore<Bytes, byte[]> get()
//        {
//            return new InMemoryWindowStore(
//                name,
//                retentionPeriod,
//                windowSize,
//                retainDuplicates,
//                metricsScope());
//        }

//        public string metricsScope()
//        {
//            return "in-memory-window-state";
//        }

//        [System.Obsolete]
//        public int segments()
//        {
//            throw new InvalidOperationException("Segments is deprecated and should not be called");
//        }

//        // In-memory window store is not *really* segmented, so just say size is 1 ms
//        public long segmentIntervalMs()
//        {
//            return 1;
//        }
//    }
//}