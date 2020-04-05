using Kafka.Streams.KStream.Internals.Suppress;

namespace Kafka.Streams.KStream.Interfaces
{
    public interface IBufferConfig
    {
        /**
        * Create a size-constrained buffer in terms of the maximum number of keys it will store.
        */
        public static IEagerBufferConfig MaxRecords(long recordLimit)
        {
            return new EagerBufferConfigImpl(recordLimit, long.MaxValue);
        }

        /**
         * Create a size-constrained buffer in terms of the maximum number of bytes it will use.
         */
        public static IEagerBufferConfig MaxBytes(long byteLimit)
        {
            return new EagerBufferConfigImpl(long.MaxValue, byteLimit);
        }

        /**
         * Create a buffer unconstrained by size (either keys or bytes).
         *
         * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
         *
         * If there isn't enough heap available to meet the demand, the application will encounter an
         * {@link OutOfMemoryError} and shut down (not guaranteed to be a graceful exit). Also, note that
         * JVM processes under extreme memory pressure may exhibit poor GC behavior.
         *
         * This is a convenient option if you doubt that your buffer will be that large, but also don't
         * wish to pick particular constraints, such as in testing.
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or crash.
         * It will never emit early.
         */
        public static IStrictBufferConfig Unbounded()
        {
            return new StrictBufferConfigImpl();
        }

        /**
         * Set the buffer to be unconstrained by size (either keys or bytes).
         *
         * As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
         *
         * If there isn't enough heap available to meet the demand, the application will encounter an
         * {@link OutOfMemoryError} and shut down (not guaranteed to be a graceful exit). Also, note that
         * JVM processes under extreme memory pressure may exhibit poor GC behavior.
         *
         * This is a convenient option if you doubt that your buffer will be that large, but also don't
         * wish to pick particular constraints, such as in testing.
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or crash.
         * It will never emit early.
         */
        IStrictBufferConfig WithNoBound();

        /**
         * Set the buffer to gracefully shut down the application when any of its constraints are violated
         *
         * This buffer is "strict" in the sense that it will enforce the time bound or shut down.
         * It will never emit early.
         */
        IStrictBufferConfig ShutDownWhenFull();

        /**
         * Set the buffer to just emit the oldest records when any of its constraints are violated.
         *
         * This buffer is "not strict" in the sense that it may emit early, so it is suitable for reducing
         * duplicate results downstream, but does not promise to eliminate them.
         */
        IEagerBufferConfig EmitEarlyWhenFull();
        BufferFullStrategy BufferFullStrategy { get; }
    }

    public interface IBufferConfig<BC> : IBufferConfig
        where BC : IBufferConfig<BC>
    {
        /**
         * Set a size constraint on the buffer in terms of the maximum number of keys it will store.
         */
        IBufferConfig<BC> WithMaxRecords(long recordLimit);

        /**
         * Set a size constraint on the buffer, the maximum number of bytes it will use.
         */
        IBufferConfig<BC> WithMaxBytes(long byteLimit);
    }
}
