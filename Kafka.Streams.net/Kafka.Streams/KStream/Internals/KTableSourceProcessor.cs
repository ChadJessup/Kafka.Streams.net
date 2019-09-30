using Kafka.Common.Metrics;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals.Metrics;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSourceProcessor<K, V> : AbstractProcessor<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private readonly Sensor skippedRecordsSensor;
        private readonly string queryableName;
        private readonly ILogger<KTableSourceProcessor<K, V>> LOG;
        private readonly bool sendOldValues;

        public KTableSourceProcessor(
            ILogger<KTableSourceProcessor<K, V>> logger,
            string queryableName,
            bool sendOldValues)
        {
            this.LOG = logger;
            this.queryableName = queryableName;
            this.sendOldValues = sendOldValues;
        }

        public override void init(IProcessorContext context)
        {
            base.init(context);
            metrics = (StreamsMetricsImpl)context.metrics;
            //skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);

            if (queryableName != null)
            {
                store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<K, V>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V>(context),
                    sendOldValues);
            }
        }


        public override void process(K key, V value)
        {
            // if the key is null, then ignore the record
            if (key == null)
            {
                LOG.LogWarning(
                    "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
                    context.Topic, context.partition, context.offset);

                skippedRecordsSensor.record();
                return;
            }

            if (queryableName != null)
            {
                ValueAndTimestamp<V> oldValueAndTimestamp = store.get(key);

                V oldValue;
                if (oldValueAndTimestamp != null)
                {
                    oldValue = oldValueAndTimestamp.value;
                    
                    if (context.timestamp < oldValueAndTimestamp.timestamp)
                    {
                        LOG.LogWarning("Detected out-of-order KTable update for {} at offset {}, partition {}.",
                            store.name, context.offset, context.partition);
                    }
                }
                else
                {
                    oldValue = default;
                }

                store.Add(key, ValueAndTimestamp<V>.make(value, context.timestamp));

                tupleForwarder.maybeForward(key, value, oldValue);
            }
            else
            {

                context.forward(key, new Change<V>(value, default));
            }
        }
    }
}
