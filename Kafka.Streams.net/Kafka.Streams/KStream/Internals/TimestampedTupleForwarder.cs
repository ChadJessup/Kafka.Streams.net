using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
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
        private readonly IProcessorContext context;
        private readonly bool sendOldValues;
        private readonly bool cachingEnabled;

        public TimestampedTupleForwarder(
            IStateStore store,
            IProcessorContext context,
            TimestampedCacheFlushListener<K, V> flushListener,
            bool sendOldValues)
        {
            this.context = context;
            this.sendOldValues = sendOldValues;
            this.cachingEnabled = ((WrappedStateStore)store).SetFlushListener(flushListener, sendOldValues);
        }

        public void MaybeForward(
            K key,
            V newValue,
            V oldValue)
        {
            if (!cachingEnabled)
            {
                context.Forward(key, new Change<V>(newValue, sendOldValues ? oldValue : default));
            }
        }

        public void MaybeForward(
            K key,
            V newValue,
            V oldValue,
            long timestamp)
        {
            if (!cachingEnabled)
            {
                context.Forward(key, new Change<V>(newValue, sendOldValues ? oldValue : default), To.All().WithTimestamp(timestamp));
            }
        }
    }
}