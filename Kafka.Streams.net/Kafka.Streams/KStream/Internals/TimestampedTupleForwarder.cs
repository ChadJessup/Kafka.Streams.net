using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.KStream.Internals
{
    /**
     * This is used to determine if a processor should forward values to child nodes.
     * Forwarding by this only occurs when caching is not enabled. If caching is enabled,
     * forwarding occurs in the flush listener when the cached store flushes.
     *
     * @param the type of the key
     * @param the type of the value
     */
    public class TimestampedTupleForwarder<K, V>
    {
        private readonly IProcessorContext<K, V> context;
        private readonly bool sendOldValues;
        private readonly bool cachingEnabled;

        public TimestampedTupleForwarder(
            IStateStore store,
            IProcessorContext<K, V> context,
            TimestampedCacheFlushListener<K, V> flushListener,
            bool sendOldValues)
        {
            this.context = context;
            this.sendOldValues = sendOldValues;
            this.cachingEnabled = ((WrappedStateStore)store).setFlushListener(flushListener, sendOldValues);
        }

        public void maybeForward(
            K key,
            V newValue,
            V oldValue)
        {
            if (!cachingEnabled)
            {
                context.forward(key, new Change<V>(newValue, sendOldValues ? oldValue : default));
            }
        }

        public void maybeForward(
            K key,
            V newValue,
            V oldValue,
            long timestamp)
        {
            if (!cachingEnabled)
            {
                context.forward(key, new Change<V>(newValue, sendOldValues ? oldValue : default), To.all().withTimestamp(timestamp));
            }
        }
    }
}