
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;
//using System;

//namespace Kafka.Streams.State.Internals
//{
//    public class RocksDbSessionBytesStoreSupplier : ISessionBytesStoreSupplier
//    {
//        private string name;
//        public long retentionPeriod { get; }

//        public RocksDbSessionBytesStoreSupplier(string name,
//                                                long retentionPeriod)
//        {
//            this.name = name;
//            this.retentionPeriod = retentionPeriod;
//        }

//        public ISessionStore<Bytes, byte[]> get()
//        {
//            RocksDbSegmentedBytesStore segmented = new RocksDbSegmentedBytesStore(
//                name,
//                metricsScope(),
//                retentionPeriod,
//                segmentIntervalMs(),
//                new SessionKeySchema());
//            return new RocksDbSessionStore(segmented);
//        }

//        public string metricsScope()
//        {
//            return "rocksdb-session-state";
//        }

//        public long segmentIntervalMs()
//        {
//            // Selected somewhat arbitrarily. Profiling may reveal a different value is preferable.
//            return Math.Max(retentionPeriod / 2, 60_000L);
//        }
//    }
//}