
namespace Kafka.Streams.State.Internals
{
    /**
     * Listen to cache flush events
     * @param key type
     * @param value type
     */
    public interface ICacheFlushListener<K, V>
    {
        /**
         * Called when records are flushed from the {@link ThreadCache}
         * @param key         key of the entry
         * @param newValue    current value
         * @param oldValue    previous value
         * @param timestamp   timestamp of new value
         */
        void Apply(K key, V newValue, V oldValue, long timestamp);
    }
}
