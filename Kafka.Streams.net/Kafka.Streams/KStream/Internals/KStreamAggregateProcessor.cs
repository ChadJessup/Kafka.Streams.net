using Kafka.Streams.State;
using Microsoft.Extensions.Logging;
using Kafka.Streams.IProcessor;
using Kafka.Streams.IProcessor.Internals.Metrics;
using Kafka.Common.Metrics;
using Kafka.Streams.IProcessor.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamAggregateProcessor<K, T> : AbstractProcessor<K, T>
    {
        private ITimestampedKeyValueStore<K, T> store;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;
        private TimestampedTupleForwarder<K, T> tupleForwarder;

        public override void init(IProcessorContext<K, T> context)
        {
            base.init(context);
            metrics = (StreamsMetricsImpl)context.metrics;
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
            store = (ITimestampedKeyValueStore<K, T>)context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<K, T>(
                store,
                context,
                new TimestampedCacheFlushListener<K, T>(context),
                sendOldValues);
        }


        public override void process(K key, T value)
        {
            // If the key or value is null we don't need to proceed
            if (key == null || value == null)
            {
                LOG.LogWarning(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context.Topic, context.partition(), context.offset()
                );
                skippedRecordsSensor.record();
                return;
            }

            ValueAndTimestamp<T> oldAggAndTimestamp = store[key];
            T oldAgg = getValueOrNull(oldAggAndTimestamp);

            T newAgg;
            long newTimestamp;

            if (oldAgg == null)
            {
                oldAgg = initializer.apply();
                newTimestamp = context.timestamp();
            }
            else
            {
                oldAgg = oldAggAndTimestamp.value();
                newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
            }

            newAgg = aggregator.apply(key, value, oldAgg);

            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }
    }
}