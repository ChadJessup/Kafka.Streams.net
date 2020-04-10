
using Kafka.Streams.Processors.Internals;
using System;

namespace Kafka.Streams.State.Internals
{
    public class ContextualRecord
    {
        public byte[] value { get; }
        public ProcessorRecordContext recordContext { get; }

        public ContextualRecord(byte[] value, ProcessorRecordContext recordContext)
        {
            this.recordContext = recordContext ?? throw new ArgumentNullException(nameof(recordContext));

            this.value = value;
        }

        public long ResidentMemorySizeEstimate()
        {
            return (this.value == null ? 0 : this.value.Length) + this.recordContext.ResidentMemorySizeEstimate();
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

            var that = (ContextualRecord)o;

            return this.value.Equals(that.value)
                && this.recordContext.Equals(that.recordContext);
        }

        public override int GetHashCode()
        {
            return (this.value, this.recordContext).GetHashCode();
        }

        public override string ToString()
        {
            return "ContextualRecord{" +
                "recordContext=" + this.recordContext +
                ", value=" + this.value +
                '}';
        }
    }
}
