using Kafka.Streams.State.Sessions;
using System;

namespace Kafka.Streams.State.Internals
{
    public class InMemorySessionBytesStoreSupplier : ISessionBytesStoreSupplier
    {
        public string Name { get; protected set; }
        public TimeSpan RetentionPeriod { get; }

        public InMemorySessionBytesStoreSupplier(
            string Name,
            TimeSpan retentionPeriod)
        {
            this.Name = Name;
            this.RetentionPeriod = retentionPeriod;
        }

        public ISessionStore<Bytes, byte[]> Get()
        {
            return new InMemorySessionStore(this.Name, this.RetentionPeriod);
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

        public void SetName(string Name)
        {
            throw new NotImplementedException();
        }
    }
}
