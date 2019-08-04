using Kafka.streams.state;
using Kafka.Streams.KStream.Internals;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamAggregate<K, V, T> : KStreamAggProcessorSupplier<K, K, V, T>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KStreamAggregate<K, V, T>>();
        private string storeName;
        private Initializer<T> initializer;
        private Aggregator<K, V, T> aggregator;

        private bool sendOldValues = false;

        KStreamAggregate(string storeName, Initializer<T> initializer, Aggregator<K, V, T> aggregator)
        {
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }


        public Processor<K, V> get()
        {
            return new KStreamAggregateProcessor();
        }


        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }


        private KStreamAggregateProcessor : AbstractProcessor<K, V>
        {
        private TimestampedKeyValueStore<K, T> store;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;
        private TimestampedTupleForwarder<K, T> tupleForwarder;



        public void init(IProcessorContext context)
        {
            base.init(context);
            metrics = (StreamsMetricsImpl)context.metrics();
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
            store = (TimestampedKeyValueStore<K, T>)context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<>(
                store,
                context,
                new TimestampedCacheFlushListener<>(context),
                sendOldValues);
        }


        public void process(K key, V value)
        {
            // If the key or value is null we don't need to proceed
            if (key == null || value == null)
            {
                LOG.LogWarning(
                    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    key, value, context().topic(), context().partition(), context().offset()
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
                newTimestamp = context().timestamp();
            }
            else
            {
                oldAgg = oldAggAndTimestamp.value();
                newTimestamp = Math.Max(context().timestamp(), oldAggAndTimestamp.timestamp());
            }

            newAgg = aggregator.apply(key, value, oldAgg);

            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }
    }


    public KTableValueGetterSupplier<K, T> view()
    {
        return new KTableValueGetterSupplier<K, T>()
        {

            public KTableValueGetter<K, T> get()
        {
            return new KStreamAggregateValueGetter();
        }


        public string[] storeNames()
        {
            return new string[] { storeName };
        }
    };
}


private class KStreamAggregateValueGetter : KTableValueGetter<K, T> {
        private TimestampedKeyValueStore<K, T> store;



public void init(IProcessorContext context)
{
    store = (TimestampedKeyValueStore<K, T>)context.getStateStore(storeName);
}


public ValueAndTimestamp<T> get(K key)
{
    return store[key];
}


public void close() { }
    }
}