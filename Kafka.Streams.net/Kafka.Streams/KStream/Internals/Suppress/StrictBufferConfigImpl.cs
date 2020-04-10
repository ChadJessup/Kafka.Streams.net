using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class StrictBufferConfigImpl : BufferConfigInternal<IStrictBufferConfig>, IStrictBufferConfig
    {
        public override long MaxRecords { get; protected set; }
        public override long MaxBytes { get; protected set; }

        public StrictBufferConfigImpl(
            long MaxRecords,
            long MaxBytes,
            BufferFullStrategy BufferFullStrategy)
        {
            this.MaxRecords = MaxRecords;
            this.MaxBytes = MaxBytes;
            this.BufferFullStrategy = BufferFullStrategy;
        }

        public StrictBufferConfigImpl()
        {
            this.MaxRecords = long.MaxValue;
            this.MaxBytes = long.MaxValue;
            this.BufferFullStrategy = BufferFullStrategy.SHUT_DOWN;
        }

        public override IBufferConfig<IStrictBufferConfig> WithMaxRecords(long recordLimit)
        {
            return new StrictBufferConfigImpl(recordLimit, this.MaxBytes, this.BufferFullStrategy);
        }

        public override IBufferConfig<IStrictBufferConfig> WithMaxBytes(long byteLimit)
        {
            return new StrictBufferConfigImpl(this.MaxRecords, byteLimit, this.BufferFullStrategy);
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

            StrictBufferConfigImpl that = (StrictBufferConfigImpl)o;
            return this.MaxRecords == that.MaxRecords &&
                this.MaxBytes == that.MaxBytes &&
                this.BufferFullStrategy == that.BufferFullStrategy;
        }

        public override int GetHashCode()
            => HashCode.Combine(
                this.MaxRecords,
                this.MaxBytes,
                this.BufferFullStrategy);

        public override string ToString()
        {
            return "StrictBufferConfigImpl{maxKeys=" + this.MaxRecords +
                ", MaxBytes=" + this.MaxBytes +
                ", BufferFullStrategy=" + this.BufferFullStrategy + '}';
        }
    }
}
