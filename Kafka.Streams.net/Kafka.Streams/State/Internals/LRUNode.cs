using Kafka.Common.Utils;

namespace Kafka.Streams.State.Internals
{
    /**
     * A simple wrapper to implement a doubly-linked list around MemoryLRUCacheBytesEntry
     */
    public class LRUNode
    {
        public Bytes key { get; }
        public LRUCacheEntry entry { get; private set; }
        public LRUNode previous { get; set; }
        public LRUNode next { get; set; }

        public LRUNode(Bytes key, LRUCacheEntry entry)
        {
            this.key = key;
            this.entry = entry;
        }

        public long size()
        {
            return key.get().Length +
                8 + // entry
                8 + // previous
                8 + // next
                entry.size();
        }

        public void update(LRUCacheEntry entry)
        {
            this.entry = entry;
        }
    }
}
