using Kafka.Common.Metrics;
using Kafka.streams.kstream;
using Kafka.streams.kstream.internals;
using Kafka.Streams.Processor.Internals.metrics;
using Kafka.streams.state;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
public class KStreamReduce<K, V> : KStreamAggProcessorSupplier<K, K, V, V>
    {
    private static  ILogger LOG = new LoggerFactory().CreateLogger<KStreamReduce>();

    private  string storeName;
    private  Reducer<V> reducer;

    private bool sendOldValues = false;

    KStreamReduce( string storeName,  Reducer<V> reducer)
{
        this.storeName = storeName;
        this.reducer = reducer;
    }


    public Processor<K, V> get()
{
        return new KStreamReduceProcessor();
    }


    public void enableSendingOldValues()
{
        sendOldValues = true;
    }


    private class KStreamReduceProcessor : AbstractProcessor<K, V> {
        private TimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private Sensor skippedRecordsSensor;

        public void init( IProcessorContext context)
{
            base.init(context);
            metrics = (StreamsMetricsImpl) context.metrics();
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
            store = (TimestampedKeyValueStore<K, V>) context.getStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<K, V>(
                store,
                context,
                new TimestampedCacheFlushListener<K, V>(context),
                sendOldValues);
        }


        public void process( K key,  V value)
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

             ValueAndTimestamp<V> oldAggAndTimestamp = store[key];
             V oldAgg = getValueOrNull(oldAggAndTimestamp);

             V newAgg;
             long newTimestamp;

            if (oldAgg == null)
{
                newAgg = value;
                newTimestamp = context().timestamp();
            } else {
                newAgg = reducer.apply(oldAgg, value);
                newTimestamp = Math.Max(context().timestamp(), oldAggAndTimestamp.timestamp());
            }

            store.Add(key, ValueAndTimestamp.make(newAgg, newTimestamp));
            tupleForwarder.maybeForward(key, newAgg, sendOldValues ? oldAgg : null, newTimestamp);
        }
    }


    public KTableValueGetterSupplier<K, V> view()
{
        return new KTableValueGetterSupplier<K, V>()
{

            public KTableValueGetter<K, V> get()
{
                return new KStreamReduceValueGetter();
            }


            public string[] storeNames()
{
                return new string[]{storeName};
            }
        };
    }


    private class KStreamReduceValueGetter : KTableValueGetter<K, V>
    {
        private TimestampedKeyValueStore<K, V> store;



        public void init( IProcessorContext context)
{
            store = (TimestampedKeyValueStore<K, V>) context.getStateStore(storeName);
        }


        public ValueAndTimestamp<V> get( K key)
{
            return store[key];
        }


        public void close() {}
    }
}
