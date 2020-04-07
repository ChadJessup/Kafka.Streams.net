
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
            return (value == null ? 0 : value.Length) + recordContext.ResidentMemorySizeEstimate();
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

            var that = (ContextualRecord)o;

            return value.Equals(that.value)
                && recordContext.Equals(that.recordContext);
        }

        public override int GetHashCode()
        {
            return (value, recordContext).GetHashCode();
        }

        public override string ToString()
        {
            return "ContextualRecord{" +
                "recordContext=" + recordContext +
                ", value=" + value +
                '}';
        }
    }
}
