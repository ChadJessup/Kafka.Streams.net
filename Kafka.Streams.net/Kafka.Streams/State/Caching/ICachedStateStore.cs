using System;

namespace Kafka.Streams.State.Internals
{
    public delegate void FlushListener<in TKey, in TValue>(
        TKey key,
        TValue oldValue,
        TValue newValue,
        DateTime timeStamp);

    public interface ICachedStateStore
    {
    }

    public interface ICachedStateStore<K, V> : ICachedStateStore
    {
        /**
         * Set the {@link CacheFlushListener} to be notified when entries are flushed from the
         * cache to the underlying {@link org.apache.kafka.streams.processor.IStateStore}
         * @param listener
         * @param sendOldValues
         */
        bool SetFlushListener(FlushListener<K, V> listener, bool sendOldValues);
    }
}
