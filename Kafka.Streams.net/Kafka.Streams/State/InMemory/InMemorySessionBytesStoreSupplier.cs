using Kafka.Streams.State.Sessions;
using System;

namespace Kafka.Streams.State.Internals
{
    public class InMemorySessionBytesStoreSupplier : ISessionBytesStoreSupplier
    {
        public string Name { get; }
        public TimeSpan RetentionPeriod { get; }

        public InMemorySessionBytesStoreSupplier(
            string name,
            TimeSpan retentionPeriod)
        {
            this.Name = name;
            this.RetentionPeriod = retentionPeriod;
        }

        public ISessionStore<Bytes, byte[]> Get()
        {
            return new InMemorySessionStore(Name, RetentionPeriod);
        }

        //public string metricsScope()
        //{
        //    return "in-memory-session-state";
        //}

        // In-memory store is not *really* segmented, so just say it is 1 (for ordering consistency with caching enabled)
        public long SegmentIntervalMs()
        {
            return 1;
        }
    }
}
