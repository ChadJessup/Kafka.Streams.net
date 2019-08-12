using Kafka.Common.Utils;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.State.Internals
{
    public class DirtyEntry
    {
        private Bytes key;
        private byte[] newValue;
        private LRUCacheEntry recordContext;

        public DirtyEntry(
            Bytes key,
            byte[] newValue,
            LRUCacheEntry recordContext)
        {
            this.key = key;
            this.newValue = newValue;
            this.recordContext = recordContext;
        }

        public LRUCacheEntry entry()
        {
            return recordContext;
        }
    }
}