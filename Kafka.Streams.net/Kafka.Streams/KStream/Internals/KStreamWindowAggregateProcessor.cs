using Confluent.Kafka;
using Kafka.Common.Extensions;
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
        private readonly Initializer<Agg> initializer;
        private readonly Aggregator<K, V, Agg> aggregator;
        private readonly Windows<W> windows;
        private ITimestampedWindowStore<K, Agg> windowStore;
        private TimestampedTupleForwarder<IWindowed<K>, Agg> tupleForwarder;
        private IInternalProcessorContext internalProcessorContext;
        private DateTime observedStreamTime;

        public KStreamWindowAggregateProcessor(
            KafkaStreamsContext context,
            Windows<W> windows,
            string storeName,
            bool sendOldValues,
            Initializer<Agg> initializer,
            Aggregator<K, V, Agg> aggregator)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
            this.observedStreamTime = this.context.Clock.UtcNow;
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
            this.observedStreamTime = this.observedStreamTime.GetNewest(timestamp);
            var closeTime = this.observedStreamTime.Subtract(this.windows.GracePeriod());

            var matchedWindows = this.windows.WindowsFor(timestamp);

            // try update the window, and create the new window for the rest of unmatched window that do not exist yet
            foreach (var entry in matchedWindows)
            {
                var windowStart = entry.Key;
                var windowEnd = entry.Value.EndTime;
                if (windowEnd.ToEpochMilliseconds() > closeTime.TotalMilliseconds)
                {
                    IValueAndTimestamp<Agg> oldAggAndTimestamp = this.windowStore.Fetch(key, windowStart);
                    Agg oldAgg = ValueAndTimestamp.GetValueOrNull(oldAggAndTimestamp);

                    Agg newAgg;
                    DateTime newTimestamp;

                    if (oldAgg == null)
                    {
                        oldAgg = this.initializer();
                        newTimestamp = this.Context.Timestamp;
                    }
                    else
                    {
                        newTimestamp = this.Context.Timestamp.GetNewest(oldAggAndTimestamp.Timestamp);
                    }

                    newAgg = this.aggregator(key, value, oldAgg);

                    // update the store with the new value
                    this.windowStore.Put(key, ValueAndTimestamp.Make(newAgg, newTimestamp), windowStart);

                    this.tupleForwarder.MaybeForward(
                        new Windowed<K>(key, entry.Value),
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
