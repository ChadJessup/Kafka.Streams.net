
//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemorySessionBytesStoreSupplier : ISessionBytesStoreSupplier
//    {
//        private string name;
//        private long retentionPeriod;

//        public InMemorySessionBytesStoreSupplier(string name,
//                                                 long retentionPeriod)
//        {
//            this.name = name;
//            this.retentionPeriod = retentionPeriod;
//        }

//        public ISessionStore<Bytes, byte[]> get()
//        {
//            return new InMemorySessionStore(name, retentionPeriod, metricsScope());
//        }

//        public string metricsScope()
//        {
//            return "in-memory-session-state";
//        }

//        // In-memory store is not *really* segmented, so just say it is 1 (for ordering consistency with caching enabled)
//        public override long segmentIntervalMs()
//        {
//            return 1;
//        }
//    }
//}