using Kafka.Common.Utils;
using Kafka.Streams.State.Internals;

static class DirtyEntry
{
    private Bytes key;
    private byte[] newValue;
    private LRUCacheEntry recordContext;

    DirtyEntry(Bytes key, byte[] newValue, LRUCacheEntry recordContext)
    {
        this.key = key;
        this.newValue = newValue;
        this.recordContext = recordContext;
    }

    public Bytes key()
    {
        return key;
    }

    public byte[] newValue()
    {
        return newValue;
    }

    public LRUCacheEntry entry()
    {
        return recordContext;
    }
}
