
//using Kafka.Streams.State.Interfaces;
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Interfaces;
//using Kafka.Common.Utils.Interfaces;
//using Kafka.Streams.Processors.Internals.Metrics;
//using Kafka.Common.Metrics;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using System.Collections.Generic;
//using Kafka.Streams.Errors;

//namespace Kafka.Streams.State.Metered
//{
//    public class MeteredSessionStore<K, V>
//        : WrappedStateStore<ISessionStore<Bytes, byte[]>, Windowed<K>, V>,
//        ISessionStore<K, V>
//    {
//        private string metricScope;
//        private ISerde<K> keySerde;
//        private ISerde<V> valueSerde;
//        private ITime time;
//        private StateSerdes<K, V> serdes;
//        private StreamsMetricsImpl metrics;
//        private Sensor putTime;
//        private Sensor fetchTime;
//        private Sensor flushTime;
//        private Sensor removeTime;
//        private string taskName;

//        public MeteredSessionStore(
//            ISessionStore<Bytes, byte[]> inner,
//            string metricScope,
//            ISerde<K> keySerde,
//            ISerde<V> valueSerde,
//            ITime time)
//            : base(inner)
//        {
//            this.metricScope = metricScope;
//            this.keySerde = keySerde;
//            this.valueSerde = valueSerde;
//            this.time = time;
//        }


//        public override void init(
//            IProcessorContext<K, V> context,
//            IStateStore root)
//        {
//            //noinspection unchecked
//            serdes = new StateSerdes<K, V>(
//                ProcessorStateManager.storeChangelogTopic(context.applicationId(), name),
//                keySerde == null ? (ISerde<K>)context.keySerde : keySerde,
//                valueSerde == null ? (ISerde<V>)context.valueSerde : valueSerde);
//            metrics = (StreamsMetricsImpl)context.metrics;

//            taskName = context.taskId().ToString();
//            string metricsGroup = "stream-" + metricScope + "-metrics";
//            Dictionary<string, string> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
//            Dictionary<string, string> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name);

//            putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            fetchTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "fetch", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            removeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Remove", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name, taskTags, storeTags);

//            // register and possibly restore the state from the logs
//            long startNs = time.nanoseconds();
//            try
//            {
//                base.init(context, root);
//            }
//            finally
//            {
//                metrics.recordLatency(
//                    restoreTime,
//                    startNs,
//                    time.nanoseconds()
//                );
//            }
//        }


//        public override bool setFlushListener(
//            ICacheFlushListener<Windowed<K>, V> listener,
//            bool sendOldValues)
//        {
//            ISessionStore<Bytes, byte[]> wrapped = wrapped;
//            if (wrapped is CachedStateStore<K, V>)
//            {
//                return ((CachedStateStore<byte[], byte[]>)wrapped).setFlushListener(
//                   (key, newValue, oldValue, timestamp) => listener.apply(
//                       SessionKeySchema.from(key, serdes.keyDeserializer(), serdes.Topic),
//                       newValue != null ? serdes.valueFrom(newValue) : null,
//                       oldValue != null ? serdes.valueFrom(oldValue) : null,
//                       timestamp),
//                   sendOldValues);
//            }

//            return false;
//        }

//        public override void put(
//            Windowed<K> sessionKey,
//            V aggregate)
//        {
//            sessionKey = sessionKey ?? throw new ArgumentNullException(nameof(sessionKey));
//            long startNs = time.nanoseconds();
//            try
//            {
//                Bytes key = keyBytes(sessionKey.key);
//                wrapped.Add(new Windowed<K>(key, sessionKey.window), serdes.rawValue(aggregate));
//            }
//            catch (ProcessorStateException e)
//            {
//                string message = string.Format(e.Message, sessionKey.key, aggregate);
//                throw new ProcessorStateException(message, e);
//            }
//            finally
//            {
//                metrics.recordLatency(putTime, startNs, time.nanoseconds());
//            }
//        }

//        public override void Remove(Windowed<K> sessionKey)
//        {
//            sessionKey = sessionKey ?? throw new ArgumentNullException(nameof(sessionKey));
//            long startNs = time.nanoseconds();
//            try
//            {
//                Bytes key = keyBytes(sessionKey.key());
//                wrapped.Remove(new Windowed<>(key, sessionKey.window()));
//            }
//            catch (ProcessorStateException e)
//            {
//                string message = string.Format(e.getMessage(), sessionKey.key());
//                throw new ProcessorStateException(message, e);
//            }
//            finally
//            {
//                metrics.recordLatency(removeTime, startNs, time.nanoseconds());
//            }
//        }

//        public override V fetchSession(K key, long startTime, long endTime)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            Bytes bytesKey = keyBytes(key);
//            long startNs = time.nanoseconds();
//            try
//            {
//                byte[] result = wrapped.fetchSession(bytesKey, startTime, endTime);
//                if (result == null)
//                {
//                    return default;
//                }
//                return serdes.valueFrom(result);
//            }
//            finally
//            {
//                metrics.recordLatency(flushTime, startNs, time.nanoseconds());
//            }
//        }

//        public override IKeyValueIterator<Windowed<K>, V> fetch(K key)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            return new MeteredWindowedKeyValueIterator<>(
//                wrapped.fetch(keyBytes(key)),
//                fetchTime,
//                metrics,
//                serdes,
//                time);
//        }

//        public override IKeyValueIterator<Windowed<K>, V> fetch(K from,
//                                                      K to)
//        {
//            from = from ?? throw new ArgumentNullException(nameof(from));
//            to = to ?? throw new ArgumentNullException(nameof(to));
//            return new MeteredWindowedKeyValueIterator<>(
//                wrapped.fetch(keyBytes(from), keyBytes(to)),
//                fetchTime,
//                metrics,
//                serdes,
//                time);
//        }

//        public override IKeyValueIterator<Windowed<K>, V> findSessions(K key,
//                                                             long earliestSessionEndTime,
//                                                             long latestSessionStartTime)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            Bytes bytesKey = keyBytes(key);
//            return new MeteredWindowedKeyValueIterator<>(
//                wrapped.findSessions(
//                    bytesKey,
//                    earliestSessionEndTime,
//                    latestSessionStartTime),
//                fetchTime,
//                metrics,
//                serdes,
//                time);
//        }

//        public override IKeyValueIterator<Windowed<K>, V> findSessions(K keyFrom,
//                                                             K keyTo,
//                                                             long earliestSessionEndTime,
//                                                             long latestSessionStartTime)
//        {
//            keyFrom = keyFrom ?? throw new ArgumentNullException(nameof(keyFrom));
//            keyTo = keyTo ?? throw new ArgumentNullException(nameof(keyTo));
//            Bytes bytesKeyFrom = keyBytes(keyFrom);
//            Bytes bytesKeyTo = keyBytes(keyTo);
//            return new MeteredWindowedKeyValueIterator<>(
//                wrapped.findSessions(
//                    bytesKeyFrom,
//                    bytesKeyTo,
//                    earliestSessionEndTime,
//                    latestSessionStartTime),
//                fetchTime,
//                metrics,
//                serdes,
//                time);
//        }

//        public override void flush()
//        {
//            long startNs = time.nanoseconds();
//            try
//            {
//                base.flush();
//            }
//            finally
//            {
//                metrics.recordLatency(flushTime, startNs, time.nanoseconds());
//            }
//        }

//        public override void close()
//        {
//            base.close();
//            metrics.removeAllStoreLevelSensors(taskName, name);
//        }

//        private Bytes keyBytes(K key)
//        {
//            return Bytes.wrap(serdes.rawKey(key));
//        }
//    }
//}