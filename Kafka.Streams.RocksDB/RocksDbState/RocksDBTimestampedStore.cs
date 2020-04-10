using Kafka.Streams.State.TimeStamped;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.RocksDbState
{
    /**
     * A Persistent key-(value-timestamp) store based on RocksDb.
     */
    public class RocksDbTimestampedStore : RocksDbStore, ITimestampedBytesStore
    {
        private static readonly ILogger log = new LoggerFactory().CreateLogger<RocksDbTimestampedStore>();

        public RocksDbTimestampedStore(string Name)
            : base(Name)
        {
        }

        public RocksDbTimestampedStore(string Name, string parentDir)
            : base(Name, parentDir)
        {
        }
    }
}