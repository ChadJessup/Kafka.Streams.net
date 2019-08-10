using Kafka.Common.Metrics;
using Kafka.Streams.kstream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.IProcessor.Internals.metrics;
using Kafka.Streams.State;
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.IProcessor.Internals.Metrics;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamReduce<K, V> : KStreamAggIProcessorSupplier<K, K, V, V>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KStreamReduce>();

        private string storeName;
        private IReducer<V> reducer;

        private bool sendOldValues = false;

        KStreamReduce(string storeName, IReducer<V> reducer)
        {
            this.storeName = storeName;
            this.reducer = reducer;
        }


        public IProcessor<K, V> get()
        {
            return new KStreamReduceProcessor();
        }


        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }


        private KStreamReduceProcessor : AbstractProcessor<K, V> {
        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;

        public void init(IProcessorContext<K, V> context)
        {
            base.init(context);
            metrics = (StreamsMetricsImpl)context.metrics();
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<K, V>(
                store,
                context,
                new TimestampedCacheFlushListener<K, V>(context),
                sendOldValues);
        }


        public void process(K key, V value)
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

            ValueAndTimestamp<V> oldAggAndTimestamp = store[key];
            V oldAgg = getValueOrNull(oldAggAndTimestamp);

            V newAgg;
            long newTimestamp;

            if (oldAgg == null)
            {
                newAgg = value;
                newTimestamp = context.timestamp();
            }
            else
            {

                newAgg = reducer.apply(oldAgg, value);
                newTimestamp = Math.Max(context.timestamp(), oldAggAndTimestamp.timestamp());
            }

            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }
    }


    public IKTableValueGetterSupplier<K, V> view()
    {
        //    return new KTableValueGetterSupplier<K, V>()
        //    {

        //        public KTableValueGetter<K, V> get()
        //    {
        //        return new KStreamReduceValueGetter();
        //    }


        //    public string[] storeNames()
        //    {
        //        return new string[] { storeName };
        //    }
        //};
    }


    private class KStreamReduceValueGetter : IKTableValueGetter<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store;



        public void init(IProcessorContext<K, V> context)
        {
            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
        }


        public ValueAndTimestamp<V> get(K key)
        {
            return store[key];
        }


        public void close() { }
    }
}
