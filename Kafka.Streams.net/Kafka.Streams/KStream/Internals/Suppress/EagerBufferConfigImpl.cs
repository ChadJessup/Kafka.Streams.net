using Kafka.Streams.KStream.Interfaces;
using System;

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

        public override BufferFullStrategy BufferFullStrategy
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
            return this.MaxRecords == that.MaxRecords &&
                this.MaxBytes == that.MaxBytes;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(this.MaxRecords, this.MaxBytes);
        }

        public override string ToString()
        {
            return "EagerBufferConfigImpl{MaxRecords=" + this.MaxRecords + ", MaxBytes=" + this.MaxBytes + '}';
        }
    }
}