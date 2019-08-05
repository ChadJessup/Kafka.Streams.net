using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamSessionWindowAggregate<K, V, Agg> : KStreamAggProcessorSupplier<K, Windowed<K>, V, Agg> {
        private static ILogger LOG = new LoggerFactory().CreateLogger<KStreamSessionWindowAggregate>();

        private string storeName;
        private SessionWindows windows;
        private Initializer<Agg> initializer;
        private Aggregator<K, V, Agg> aggregator;
        private Merger<K, Agg> sessionMerger;

        private bool sendOldValues = false;

        public KStreamSessionWindowAggregate(SessionWindows windows,
                                              string storeName,
                                              Initializer<Agg> initializer,
                                              Aggregator<K, V, Agg> aggregator,
                                              Merger<K, Agg> sessionMerger)
        {
            this.windows = windows;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
            this.sessionMerger = sessionMerger;
        }


        public Processor<K, V> get()
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
        private InternalProcessorContext internalProcessorContext;
        private Sensor lateRecordDropSensor;
        private Sensor skippedRecordsSensor;
        private long observedStreamTime = ConsumeResult.NO_TIMESTAMP;



        public void init(IProcessorContext context)
        {
            base.init(context);
            internalProcessorContext = (InternalProcessorContext)context;
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
                    value, context().topic(), context().partition(), context().offset()
                );
                skippedRecordsSensor.record();
                return;
            }

            long timestamp = context().timestamp();
            observedStreamTime = Math.Max(observedStreamTime, timestamp);
            long closeTime = observedStreamTime - windows.gracePeriodMs();

            List<KeyValue<Windowed<K>, Agg>> merged = new List<>();
            SessionWindow newSessionWindow = new SessionWindow(timestamp, timestamp);
            SessionWindow mergedWindow = newSessionWindow;
            Agg agg = initializer.apply();

            try (
                 KeyValueIterator<Windowed<K>, Agg> iterator = store.findSessions(
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
                    mergedWindow = mergeSessionWindow(mergedWindow, (SessionWindow)next.key.window());
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
                    context().topic(),
                    context().partition(),
                    context().offset(),
                    timestamp,
                    mergedWindow.start(),
                    mergedWindow.end(),
                    closeTime,
                    observedStreamTime
                );
                lateRecordDropSensor.record();
            } else
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
                Windowed<K> sessionKey = new Windowed<>(key, mergedWindow);
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


        public KTableValueGetterSupplier<Windowed<K>, Agg> view()
        {
            return new KTableValueGetterSupplier<Windowed<K>, Agg>()
            {

            public KTableValueGetter<Windowed<K>, Agg> get()
            {
                return new KTableSessionWindowValueGetter();
            }


            public string[] storeNames()
            {
                return new string[] { storeName };
            }
        };
    }

    private class KTableSessionWindowValueGetter : KTableValueGetter<Windowed<K>, Agg> {
        private ISessionStore<K, Agg> store;



        public void init(IProcessorContext context)
        {
            store = (ISessionStore<K, Agg>)context.getStateStore(storeName);
        }


        public ValueAndTimestamp<Agg> get(Windowed<K> key)
        {
            return ValueAndTimestamp.make(
                store.fetchSession(key.key(), key.window().start(), key.window().end()),
                key.window().end());
        }


        public void close()
        {
        }
    }
}