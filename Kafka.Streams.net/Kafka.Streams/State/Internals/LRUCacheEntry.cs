using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.State.Internals
{
    /**
     * A cache entry
     */
    public class LRUCacheEntry
    {
        private readonly ContextualRecord record;
        private readonly long sizeBytes;
        public bool isDirty { get; private set; }

        public LRUCacheEntry(byte[] value)
            : this(value, null, false, -1, -1, -1, "")
        {
        }

        LRUCacheEntry(byte[] value,
                      Headers headers,
                      bool isDirty,
                      long offset,
                      long timestamp,
                      int partition,
                      string topic)
        {
            var context = new ProcessorRecordContext(
                timestamp,
                offset,
                partition,
                topic,
                headers);

            this.record = new ContextualRecord(
                value,
                context
            );

            this.isDirty = isDirty;
            this.sizeBytes = 1 + // isDirty
                record.residentMemorySizeEstimate();
        }

        public void markClean()
        {
            isDirty = false;
        }

        public long size()
        {
            return sizeBytes;
        }

        public byte[] value()
        {
            return record.value;
        }

        public ProcessorRecordContext context
        {
            get => record.recordContext;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var that = (LRUCacheEntry)o;

            return sizeBytes == that.sizeBytes
                && isDirty == that.isDirty
                && record.Equals(that.record);
        }

        public override int GetHashCode()
        {
            return (record, sizeBytes, isDirty).GetHashCode();
        }
    }
}
