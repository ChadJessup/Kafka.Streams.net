﻿using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamAggregateProcessor<K, V, T> : AbstractProcessor<K, V>
    {
        private TimestampedTupleForwarder<K, T> tupleForwarder;
        private ITimestampedKeyValueStore<K, T> store;
        private readonly IInitializer<T> initializer;
        private readonly IAggregator<K, V, T> aggregator;
        private readonly bool sendOldValues;
        private readonly string storeName;

        public KStreamAggregateProcessor(
            string storeName,
            bool sendOldValues,
            IInitializer<T> initializer,
            IAggregator<K, V, T> aggregator)
        {
            this.storeName = storeName;
            this.sendOldValues = sendOldValues;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public override void Init(IProcessorContext context)
        {
            if (context is null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            base.Init(context);
            store = (ITimestampedKeyValueStore<K, T>)context.GetStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<K, T>(
                store,
                context,
                new TimestampedCacheFlushListener<K, T>(context),
                this.sendOldValues);
        }

        public override void Process(K key, V value)
        {
            // If the key or value is null we don't need to proceed
            if (key == null || value == null)
            {
                //LOG.LogWarning(
                //    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                //    key, value, context.Topic, context.partition, context.offset);

                return;
            }

            ValueAndTimestamp<T> oldAggAndTimestamp = store.Get(key);
            T oldAgg = ValueAndTimestamp.GetValueOrNull(oldAggAndTimestamp);

            T newAgg;
            long newTimestamp;

            if (oldAgg == null)
            {
                oldAgg = initializer.Apply();
                newTimestamp = Context.Timestamp;
            }
            else
            {
                oldAgg = oldAggAndTimestamp.Value;
                newTimestamp = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);
            }

            newAgg = aggregator.Apply(key, value, oldAgg);

            store.Add(key, ValueAndTimestamp.Make(newAgg, newTimestamp));
            tupleForwarder.MaybeForward(key, newAgg, sendOldValues
                ? oldAgg
                : default,
                newTimestamp);
        }
    }
}