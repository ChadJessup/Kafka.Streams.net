using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamSessionWindowAggregate<K, V, Agg> : KStreamAggIProcessorSupplier<K, Windowed<K>, V, Agg>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KStreamSessionWindowAggregate>();

        private string storeName;
        private SessionWindows windows;
        private IInitializer<Agg> initializer;
        private IAggregator<K, V, Agg> aggregator;
        private IMerger<K, Agg> sessionMerger;

        private bool sendOldValues = false;

        public KStreamSessionWindowAggregate(SessionWindows windows,
                                              string storeName,
                                              IInitializer<Agg> initializer,
                                              IAggregator<K, V, Agg> aggregator,
                                              IMerger<K, Agg> sessionMerger)
        {
            this.windows = windows;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
            this.sessionMerger = sessionMerger;
        }


        public IProcessor<K, V> get()
        {
            return new KStreamSessionWindowAggregateProcessor();
        }

        public SessionWindows windows()
        {
            return windows;
        }


        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }

        private KStreamSessionWindowAggregateProcessor : AbstractProcessor<K, V> {

        private ISessionStore<K, Agg> store;
        private SessionTupleForwarder<K, Agg> tupleForwarder;
        private StreamsMetricsImpl metrics;
        private IInternalProcessorContext<K, V>  internalProcessorContext;
        private Sensor lateRecordDropSensor;
        private Sensor skippedRecordsSensor;
        private long observedStreamTime = ConsumeResult.NO_TIMESTAMP;



        public void init(IProcessorContext<K, V> context)
        {
            base.init(context);
            internalProcessorContext = (IInternalProcessorContext)context;
            metrics = (StreamsMetricsImpl)context.metrics();
            lateRecordDropSensor = Sensors.lateRecordDropSensor(internalProcessorContext);
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);

            store = (ISessionStore<K, Agg>)context.getStateStore(storeName);
            tupleForwarder = new SessionTupleForwarder<>(store, context, new SessionCacheFlushListener<>(context), sendOldValues);
        }


        public void process(K key, V value)
        {
            // if the key is null, we do not need proceed aggregating
            // the record with the table
            if (key == null)
            {
                LOG.LogWarning(
                    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                    value, context.Topic, context.partition(), context.offset()
                );
                skippedRecordsSensor.record();
                return;
            }

            long timestamp = context.timestamp();
            observedStreamTime = Math.Max(observedStreamTime, timestamp);
            long closeTime = observedStreamTime - windows.gracePeriodMs();

            List<KeyValue<Windowed<K>, Agg>> merged = new List<>();
            SessionWindow newSessionWindow = new SessionWindow(timestamp, timestamp);
            SessionWindow mergedWindow = newSessionWindow;
            Agg agg = initializer.apply();

            using (
                 IKeyValueIterator<Windowed<K>, Agg> iterator = store.findSessions(
                    key,
                    timestamp - windows.inactivityGap(),
                    timestamp + windows.inactivityGap()
                )
            )
            {
                while (iterator.hasNext())
                {
                    KeyValue<Windowed<K>, Agg> next = iterator.next();
                    merged.Add(next);
                    agg = sessionMerger.apply(key, agg, next.value);
                    mergedWindow = mergeSessionWindow(mergedWindow, next.key.window);
                }
            }

            if (mergedWindow.end() < closeTime)
            {
                LOG.LogDebug(
                    "Skipping record for expired window. " +
                        "key=[{}] " +
                        "topic=[{}] " +
                        "partition=[{}] " +
                        "offset=[{}] " +
                        "timestamp=[{}] " +
                        "window=[{},{}] " +
                        "expiration=[{}] " +
                        "streamTime=[{}]",
                    key,
                    context.Topic,
                    context.partition(),
                    context.offset(),
                    timestamp,
                    mergedWindow.start(),
                    mergedWindow.end(),
                    closeTime,
                    observedStreamTime
                );
                lateRecordDropSensor.record();
            }
            else
            {

                if (!mergedWindow.Equals(newSessionWindow))
                {
                    foreach (KeyValue<Windowed<K>, Agg> session in merged)
                    {
                        store.Remove(session.key);
                        tupleForwarder.maybeForward(session.key, null, sendOldValues ? session.value : null);
                    }
                }

                agg = aggregator.apply(key, value, agg);
                Windowed<K> sessionKey = new Windowed<K>(key, mergedWindow);
                store.Add(sessionKey, agg);
                tupleForwarder.maybeForward(sessionKey, agg, null);
            }
        }
    }

    private SessionWindow mergeSessionWindow(SessionWindow one, SessionWindow two)
    {
        long start = one.start() < two.start() ? one.start() : two.start();
        long end = one.end() > two.end() ? one.end() : two.end();
        return new SessionWindow(start, end);
    }


    public IKTableValueGetterSupplier<Windowed<K>, Agg> view()
    {
        //    return new IKTableValueGetterSupplier<Windowed<K>, Agg>()
        //    {

        //        public IKTableValueGetter<Windowed<K>, Agg> get()
        //    {
        //        return new KTableSessionWindowValueGetter();
        //    }


        //    public string[] storeNames()
        //    {
        //        return new string[] { storeName };
        //    }
        //};
    }
}