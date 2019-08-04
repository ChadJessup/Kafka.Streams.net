using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
public KTableSource<K, V> : ProcessorSupplier<K, V>
    {
    private static ILogger LOG = new LoggerFactory().CreateLogger<KTableSource<K, V>>();

    private string storeName;
    private string queryableName;
    private bool sendOldValues;

    public KTableSource(string storeName, string queryableName)
    {
        storeName = storeName ?? throw new System.ArgumentNullException("storeName can't be null", nameof(storeName));

        this.storeName = storeName;
        this.queryableName = queryableName;
        this.sendOldValues = false;
    }

    public string queryableName()
    {
        return queryableName;
    }

    public Processor<K, V> get()
    {
        return new KTableSourceProcessor();
    }

    // when source ktable requires sending old values, we just
    // need to set the queryable name as the store name to enforce materialization
    public void enableSendingOldValues()
    {
        this.sendOldValues = true;
        this.queryableName = storeName;
    }

    // when the source ktable requires materialization from downstream, we just
    // need to set the queryable name as the store name to enforce materialization
    public void materialize()
    {
        this.queryableName = storeName;
    }

    private KTableSourceProcessor : AbstractProcessor<K, V>
        {

        private TimestampedKeyValueStore<K, V> store;
    private TimestampedTupleForwarder<K, V> tupleForwarder;
    private StreamsMetricsImpl metrics;
    private Sensor skippedRecordsSensor;

    public void init(IProcessorContext context)
    {
        base.init(context);
        metrics = (StreamsMetricsImpl)context.metrics();
        skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);
        if (queryableName != null)
        {
            store = (TimestampedKeyValueStore<K, V>)context.getStateStore(queryableName);
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
                context().topic(), context().partition(), context().offset()
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
                oldValue = oldValueAndTimestamp.value();
                if (context().timestamp() < oldValueAndTimestamp.timestamp())
                {
                    LOG.LogWarning("Detected out-of-order KTable update for {} at offset {}, partition {}.",
                        store.name(), context().offset(), context().partition());
                }
            } else {
                oldValue = null;
            }
            store.Add(key, ValueAndTimestamp.make(value, context().timestamp()));
            tupleForwarder.maybeForward(key, value, oldValue);
        } else {
            context().forward(key, new Change<>(value, null));
        }
    }
}