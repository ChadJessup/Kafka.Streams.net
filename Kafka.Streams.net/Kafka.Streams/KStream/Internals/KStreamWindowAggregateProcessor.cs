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
        private readonly KafkaStreamsContext context;
        private readonly bool sendOldValues;
        private readonly IInitializer<Agg> initializer;
        private readonly IAggregator<K, V, Agg> aggregator;
        private readonly Windows<W> windows;
        private ITimestampedWindowStore<K, Agg> windowStore;
        private TimestampedTupleForwarder<IWindowed<K>, Agg> tupleForwarder;
        private IInternalProcessorContext internalProcessorContext;
        private long observedStreamTime = Timestamp.Default.UnixTimestampMs;

        public KStreamWindowAggregateProcessor(
            KafkaStreamsContext context,
            Windows<W> windows,
            string storeName,
            bool sendOldValues,
            IInitializer<Agg> initializer,
            IAggregator<K, V, Agg> aggregator)
        {
            this.context = context;
            this.sendOldValues = sendOldValues;
            this.initializer = initializer;
            this.aggregator = aggregator;
            this.storeName = storeName;
            this.windows = windows;
        }

        public override void Init(IProcessorContext context)
        {
            context = context ?? throw new ArgumentNullException(nameof(context));

            base.Init(context);
            this.internalProcessorContext = (IInternalProcessorContext)context;

            this.windowStore = (ITimestampedWindowStore<K, Agg>)context.GetStateStore(this.storeName);

            this.tupleForwarder = new TimestampedTupleForwarder<IWindowed<K>, Agg>(
                this.windowStore,
                context,
                new TimestampedCacheFlushListener<IWindowed<K>, Agg>(context),
                this.sendOldValues);
        }

        public override void Process(K key, V value)
        {
            if (key == null)
            {
                //log.LogWarning(
                //    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                //    value, context.Topic, context.partition, context.offset);
                return;
            }

            // first get the matching windows
            var timestamp = this.Context.Timestamp;
            this.observedStreamTime = Math.Max(this.observedStreamTime, timestamp);
            var closeTime = this.observedStreamTime - (long)this.windows.GracePeriod().TotalMilliseconds;

            Dictionary<long, W> matchedWindows = this.windows.WindowsFor(TimeSpan.FromMilliseconds(timestamp));

            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
            foreach (var entry in matchedWindows)
            {
                var windowStart = entry.Key;
                var windowEnd = entry.Value.End();
                if (windowEnd > closeTime)
                {
                    IValueAndTimestamp<Agg> oldAggAndTimestamp = this.windowStore.Fetch(key, windowStart);
                    Agg oldAgg = ValueAndTimestamp.GetValueOrNull(oldAggAndTimestamp);

                    Agg newAgg;
                    long newTimestamp;

                    if (oldAgg == null)
                    {
                        oldAgg = this.initializer.Apply();
                        newTimestamp = this.Context.Timestamp;
                    }
                    else
                    {
                        newTimestamp = Math.Max(this.Context.Timestamp, oldAggAndTimestamp.Timestamp);
                    }

                    newAgg = this.aggregator.Apply(key, value, oldAgg);

                    // update the store with the new value
                    this.windowStore.Put(key, ValueAndTimestamp.Make(newAgg, newTimestamp), windowStart);

                    this.tupleForwarder.MaybeForward(
                        new Windowed2<K>(key, entry.Value),
                        newAgg,
                        this.sendOldValues ? oldAgg : default,
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
