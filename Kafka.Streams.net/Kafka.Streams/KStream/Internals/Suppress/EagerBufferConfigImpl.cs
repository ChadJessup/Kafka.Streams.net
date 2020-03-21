using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class EagerBufferConfigImpl : BufferConfigInternal<IEagerBufferConfig>, IEagerBufferConfig
    {
        public override long MaxRecords { get; protected set; }
        public override long MaxBytes { get; protected set; }

        public EagerBufferConfigImpl(long MaxRecords, long MaxBytes)
        {
            this.MaxRecords = MaxRecords;
            this.MaxBytes = MaxBytes;
        }

        public override IBufferConfig<IEagerBufferConfig> WithMaxRecords(long recordLimit)
        {
            return new EagerBufferConfigImpl(recordLimit, this.MaxBytes);
        }

        public override IBufferConfig<IEagerBufferConfig> WithMaxBytes(long byteLimit)
        {
            return new EagerBufferConfigImpl(this.MaxRecords, byteLimit);
        }

        public BufferFullStrategy BufferFullStrategy
            => BufferFullStrategy.EMIT;

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

            EagerBufferConfigImpl that = (EagerBufferConfigImpl)o;
            return MaxRecords == that.MaxRecords &&
                MaxBytes == that.MaxBytes;
        }

        public override int GetHashCode()
        {
            return (MaxRecords, MaxBytes).GetHashCode();
        }

        public override string ToString()
        {
            return "EagerBufferConfigImpl{MaxRecords=" + MaxRecords + ", MaxBytes=" + MaxBytes + '}';
        }
    }
}