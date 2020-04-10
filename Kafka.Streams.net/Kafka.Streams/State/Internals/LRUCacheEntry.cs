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

        public LRUCacheEntry(byte[] value,
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
                this.record.ResidentMemorySizeEstimate();
        }

        public void MarkClean()
        {
            this.isDirty = false;
        }

        public long Size()
        {
            return this.sizeBytes;
        }

        public byte[] Value()
        {
            return this.record.value;
        }

        public ProcessorRecordContext context
        {
            get => this.record.recordContext;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (LRUCacheEntry)o;

            return this.sizeBytes == that.sizeBytes
                && this.isDirty == that.isDirty
                && this.record.Equals(that.record);
        }

        public override int GetHashCode()
        {
            return (this.record, this.sizeBytes, this.isDirty).GetHashCode();
        }
    }
}
