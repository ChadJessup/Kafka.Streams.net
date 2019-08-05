using Kafka.Streams.KStream.Internals;
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
        private IProcessorContext context;
        private bool sendOldValues;
        private bool cachingEnabled;

        TimestampedTupleForwarder(
            IStateStore store,
            IProcessorContext context,
            TimestampedCacheFlushListener<K, V> flushListener,
            bool sendOldValues)
        {
            this.context = context;
            this.sendOldValues = sendOldValues;
            cachingEnabled = ((WrappedStateStore)store).setFlushListener(flushListener, sendOldValues);
        }

        public void maybeForward(
            K key,
            V newValue,
            V oldValue)
        {
            if (!cachingEnabled)
            {
                context.forward(key, new Change<>(newValue, sendOldValues ? oldValue : null));
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
                context.forward(key, new Change<>(newValue, sendOldValues ? oldValue : null), To.all().withTimestamp(timestamp));
            }
        }
    }
}