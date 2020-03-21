
using Kafka.Streams.KStream.Interfaces;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public abstract class BufferConfigInternal<BC> : IBufferConfig<BC>
        where BC : IBufferConfig<BC>
    {
        public abstract long MaxRecords { get; protected set; }

        public abstract long MaxBytes { get; protected set; }
        public BufferFullStrategy BufferFullStrategy { get; protected set; }

        public IStrictBufferConfig WithNoBound()
        {
            return new StrictBufferConfigImpl(
                long.MaxValue,
                long.MaxValue,
                BufferFullStrategy.SHUT_DOWN // doesn't matter, given the bounds
            );
        }

        public IStrictBufferConfig ShutDownWhenFull()
        {
            return new StrictBufferConfigImpl(MaxRecords, MaxBytes, BufferFullStrategy.SHUT_DOWN);
        }

        public IEagerBufferConfig EmitEarlyWhenFull()
        {
            return new EagerBufferConfigImpl(MaxRecords, MaxBytes);
        }

        public abstract IBufferConfig<BC> WithMaxRecords(long recordLimit);
        public abstract IBufferConfig<BC> WithMaxBytes(long byteLimit);
    }
}
