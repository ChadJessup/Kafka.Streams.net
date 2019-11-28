using Confluent.Kafka;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregateProcessor<K, V, Agg, W> : AbstractProcessor<K, V>
        where W : Window
    {
        private readonly string storeName;
        private readonly bool sendOldValues;
        private readonly IInitializer<Agg> initializer;
        private readonly IAggregator<K, V, Agg> aggregator;
        private readonly Windows<W> windows;
        private ITimestampedWindowStore<K, Agg> windowStore;
        private TimestampedTupleForwarder<Windowed<K>, Agg> tupleForwarder;
        private IInternalProcessorContext internalProcessorContext;
        private long observedStreamTime = Timestamp.Default.UnixTimestampMs;

        public KStreamWindowAggregateProcessor(
            Windows<W> windows,
            string storeName,
            bool sendOldValues,
            IInitializer<Agg> initializer,
            IAggregator<K, V, Agg> aggregator)
        {
            this.sendOldValues = sendOldValues;
            this.initializer = initializer;
            this.aggregator = aggregator;
            this.storeName = storeName;
            this.windows = windows;
        }

        public override void init(IProcessorContext context)
        {
            context = context ?? throw new ArgumentNullException(nameof(context));

            base.init(context);
            internalProcessorContext = (IInternalProcessorContext)context;

            windowStore = (ITimestampedWindowStore<K, Agg>)context.getStateStore(storeName);

            tupleForwarder = new TimestampedTupleForwarder<Windowed<K>, Agg>(
                windowStore,
                context,
                new TimestampedCacheFlushListener<Windowed<K>, Agg>(context),
                sendOldValues);
        }

        public override void process(K key, V value)
        {
            if (key == null)
            {
                //log.LogWarning(
                //    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                //    value, context.Topic, context.partition, context.offset);
                return;
            }

            // first get the matching windows
            long timestamp = context.timestamp;
            observedStreamTime = Math.Max(observedStreamTime, timestamp);
            long closeTime = observedStreamTime - (long)windows.gracePeriod().TotalMilliseconds;

            Dictionary<long, W> matchedWindows = windows.windowsFor(TimeSpan.FromMilliseconds(timestamp));

            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
            foreach (var entry in matchedWindows)
            {
                long windowStart = entry.Key;
                long windowEnd = entry.Value.End();
                if (windowEnd > closeTime)
                {
                    ValueAndTimestamp<Agg> oldAggAndTimestamp = windowStore.fetch(key, windowStart);
                    Agg oldAgg = ValueAndTimestamp<Agg>.GetValueOrNull(oldAggAndTimestamp);

                    Agg newAgg;
                    long newTimestamp;

                    if (oldAgg == null)
                    {
                        oldAgg = initializer.apply();
                        newTimestamp = context.timestamp;
                    }
                    else
                    {
                        newTimestamp = Math.Max(context.timestamp, oldAggAndTimestamp.timestamp);
                    }

                    newAgg = aggregator.apply(key, value, oldAgg);

                    // update the store with the new value
                    windowStore.put(key, ValueAndTimestamp<Agg>.make(newAgg, newTimestamp), windowStart);

                    tupleForwarder.maybeForward(
                        new Windowed<K>(key, entry.Value),
                        newAgg,
                        sendOldValues ? oldAgg : default,
                        newTimestamp);
                }
                else
                {
                    //log.LogDebug(
                    //    "Skipping record for expired window. " +
                    //        "key=[{}] " +
                    //        "topic=[{}] " +
                    //        "partition=[{}] " +
                    //        "offset=[{}] " +
                    //        "timestamp=[{}] " +
                    //        "window=[{},{}] " +
                    //        "expiration=[{}] " +
                    //        "streamTime=[{}]",
                    //    key,
                    //    context.Topic,
                    //    context.partition,
                    //    context.offset,
                    //    context.timestamp,
                    //    windowStart, windowEnd,
                    //    closeTime,
                    //    observedStreamTime);
                }
            }
        }
    }
}
