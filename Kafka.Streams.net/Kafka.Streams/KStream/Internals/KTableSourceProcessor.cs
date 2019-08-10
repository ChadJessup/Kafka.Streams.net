using Kafka.Common.Metrics;
using Kafka.Streams.IProcessor;
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.IProcessor.Internals.metrics;
using Kafka.Streams.IProcessor.Internals.Metrics;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSourceProcessor<K, V> : AbstractProcessor<K, V>
    {

        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;

        public void init(IProcessorContext<K, V> context)
        {
            base.init(context);
            metrics = (StreamsMetricsImpl)context.metrics();
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
            if (queryableName != null)
            {
                store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<>(context),
                    sendOldValues);
            }
        }


        public void process(K key, V value)
        {
            // if the key is null, then ignore the record
            if (key == null)
            {
                LOG.LogWarning(
                    "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
                    context.Topic, context.partition(), context.offset()
                );
                skippedRecordsSensor.record();
                return;
            }

            if (queryableName != null)
            {
                ValueAndTimestamp<V> oldValueAndTimestamp = store[key];
                V oldValue;
                if (oldValueAndTimestamp != null)
                {
                    oldValue = oldValueAndTimestamp.value;
                    if (context.timestamp() < oldValueAndTimestamp.timestamp)
                    {
                        LOG.LogWarning("Detected out-of-order KTable update for {} at offset {}, partition {}.",
                            store.name, context.offset(), context.partition());
                    }
                }
                else
                {

                    oldValue = null;
                }
                store.Add(key, ValueAndTimestamp.make(value, context.timestamp()));
                tupleForwarder.maybeForward(key, value, oldValue);
            }
            else
            {

                context.forward(key, new Change<>(value, null));
            }
        }
    }
}
