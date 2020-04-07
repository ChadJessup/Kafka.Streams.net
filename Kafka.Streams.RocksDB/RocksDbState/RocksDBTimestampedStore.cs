using Kafka.Streams.State.TimeStamped;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.RocksDbState
{
    /**
     * A persistent key-(value-timestamp) store based on RocksDb.
     */
    public class RocksDbTimestampedStore : RocksDbStore, ITimestampedBytesStore
    {
        private static readonly ILogger log = new LoggerFactory().CreateLogger<RocksDbTimestampedStore>();

        public RocksDbTimestampedStore(string name)
            : base(name)
        {
        }

        public RocksDbTimestampedStore(string name, string parentDir)
            : base(name, parentDir)
        {
        }
    }
}