//using Kafka.Common.Metrics;
//using Kafka.Streams.KStream.Internals.Metrics;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals.Metrics;
//using Kafka.Streams.State.Interfaces;
//using Microsoft.Extensions.Logging;
//using System;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamSessionWindowAggregateProcessor<K, V, Agg> : AbstractProcessor<K, V>
//    {

//        private ISessionStore<K, Agg> store;
//        private SessionTupleForwarder<K, Agg> tupleForwarder;
//        private StreamsMetricsImpl metrics;
//        private IInternalProcessorContext<K, V> internalProcessorContext;
//        private Sensor lateRecordDropSensor;
//        private Sensor skippedRecordsSensor;
//        private long observedStreamTime = ConsumeResult.NO_TIMESTAMP;



//        public void init(IProcessorContext context)
//        {
//            base.Init(context);
//            internalProcessorContext = (IInternalProcessorContext)context;
//            metrics = (StreamsMetricsImpl)context.metrics;
//            lateRecordDropSensor = Sensors.lateRecordDropSensor(internalProcessorContext);
//            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(metrics);

//            store = (ISessionStore<K, Agg>)context.getStateStore(storeName);
//            tupleForwarder = new SessionTupleForwarder<>(store, context, new SessionCacheFlushListener<>(context), sendOldValues);
//        }


//        public void process(K key, V value)
//        {
//            // if the key is null, we do not need proceed aggregating
//            // the record with the table
//            if (key == null)
//            {
//                LOG.LogWarning(
//                    "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
//                    value, context.Topic, context.Partition, context.offset()
//                );
//                skippedRecordsSensor.record();
//                return;
//            }

//            long timestamp = context.timestamp();
//            observedStreamTime = Math.Max(observedStreamTime, timestamp);
//            long closeTime = observedStreamTime - windows.gracePeriodMs();

//            List<KeyValuePair<Windowed<K>, Agg>> merged = new List<>();
//            SessionWindow newSessionWindow = new SessionWindow(timestamp, timestamp);
//            SessionWindow mergedWindow = newSessionWindow;
//            Agg agg = initializer.apply();

//            using (
//                 IKeyValueIterator<Windowed<K>, Agg> iterator = store.findSessions(
//                    key,
//                    timestamp - windows.inactivityGap(),
//                    timestamp + windows.inactivityGap()
//                )
//            )
//            {
//                while (iterator.HasNext())
//                {
//                    KeyValuePair<Windowed<K>, Agg> next = iterator.MoveNext();
//                    merged.Add(next);
//                    agg = sessionMerger.apply(key, agg, next.value);
//                    mergedWindow = mergeSessionWindow(mergedWindow, next.key.window);
//                }
//            }

//            if (mergedWindow.end() < closeTime)
//            {
//                LOG.LogDebug(
//                    "Skipping record for expired window. " +
//                        "key=[{}] " +
//                        "topic=[{}] " +
//                        "partition=[{}] " +
//                        "offset=[{}] " +
//                        "timestamp=[{}] " +
//                        "window=[{},{}] " +
//                        "expiration=[{}] " +
//                        "streamTime=[{}]",
//                    key,
//                    context.Topic,
//                    context.Partition,
//                    context.offset(),
//                    timestamp,
//                    mergedWindow.start(),
//                    mergedWindow.end(),
//                    closeTime,
//                    observedStreamTime
//                );
//                lateRecordDropSensor.record();
//            }
//            else
//            {

//                if (!mergedWindow.Equals(newSessionWindow))
//                {
//                    foreach (KeyValuePair<Windowed<K>, Agg> session in merged)
//                    {
//                        store.Remove(session.key);
//                        tupleForwarder.maybeForward(session.key, null, sendOldValues ? session.value : null);
//                    }
//                }

//                agg = aggregator.apply(key, value, agg);
//                Windowed<K> sessionKey = new Windowed<K>(key, mergedWindow);
//                store.Add(sessionKey, agg);
//                tupleForwarder.maybeForward(sessionKey, agg, null);
//            }
//        }
//    }
//}