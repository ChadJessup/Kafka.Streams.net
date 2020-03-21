//using Kafka.Common.Metrics;
//using Kafka.Streams.State;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals.Metrics;
//using Microsoft.Extensions.Logging;
//using Kafka.Streams.Processors;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamReduceProcessor : AbstractProcessor<K, V>
//    {
//        private ITimestampedKeyValueStore<K, V> store;
//        private TimestampedTupleForwarder<K, V> tupleForwarder;
//        private StreamsMetricsImpl metrics;
//        private Sensor skippedRecordsSensor;

//        public void init(IProcessorContext context)
//        {
//            base.init(context);
//            metrics = (StreamsMetricsImpl)context.metrics;
//            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
//            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
//            tupleForwarder = new TimestampedTupleForwarder<K, V>(
//                store,
//                context,
//                new TimestampedCacheFlushListener<K, V>(context),
//                sendOldValues);
//        }


//        public void process(K key, V value)
//        {
//            // If the key or value is null we don't need to proceed
//            if (key == null || value == null)
//            {
//                LOG.LogWarning(
//                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
//                    key, value, context.Topic, context.partition(), context.offset()
//                );
//                skippedRecordsSensor.record();
//                return;
//            }

//            ValueAndTimestamp<V> oldAggAndTimestamp = store[key];
//            V oldAgg = ValueAndTimestamp.GetValueOrNull(oldAggAndTimestamp);

//            V newAgg;
//            long newTimestamp;

//            if (oldAgg == null)
//            {
//                newAgg = value;
//                newTimestamp = context.timestamp();
//            }
//            else
//            {

//                newAgg = reducer.apply(oldAgg, value);
//                newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
//            }

//            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
//            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
//        }
//    }
//}
