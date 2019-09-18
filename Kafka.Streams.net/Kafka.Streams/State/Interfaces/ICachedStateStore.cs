namespace Kafka.Streams.State.Internals
{
    public interface ICachedStateStore
    {
        /**
         * Set the {@link CacheFlushListener} to be notified when entries are flushed from the
         * cache to the underlying {@link org.apache.kafka.streams.processor.IStateStore}
         * @param listener
         * @param sendOldValues
         */
        bool setFlushListener<K, V>(
            ICacheFlushListener<K, V> listener,
            bool sendOldValues);
    }
}