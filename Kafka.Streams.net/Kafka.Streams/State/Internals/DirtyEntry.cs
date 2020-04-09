namespace Kafka.Streams.State.Internals
{
    public class DirtyEntry
    {
        public Bytes Key { get; }
        public byte[] NewValue { get; }
        private LRUCacheEntry recordContext;

        public DirtyEntry(
            Bytes key,
            byte[] newValue,
            LRUCacheEntry recordContext)
        {
            this.Key = key;
            this.NewValue = newValue;
            this.recordContext = recordContext;
        }

        public LRUCacheEntry Entry()
        {
            return recordContext;
        }
    }
}
