using Kafka.Common.Metrics;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * A Metered {@link KeyValueStore} wrapper that is used for recording operation metrics, and hence its
     * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
     * The inner {@link KeyValueStore} of this is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
     * to convert from &lt;K,V&gt; to &lt;Bytes,byte[]&gt;
     * @param <K>
     * @param <V>
     */
    public class MeteredKeyValueStore<K, V>
        : WrappedStateStore<IKeyValueStore<Bytes, byte[]>, K, V>, IKeyValueStore<K, V>
    {
        ISerde<K> keySerde;
        ISerde<V> valueSerde;
        StateSerdes<K, V> serdes;

        private string metricScope;
        protected ITime time;
        private Sensor putTime;
        private Sensor putIfAbsentTime;
        private Sensor getTime;
        private Sensor deleteTime;
        private Sensor putAllTime;
        private Sensor allTime;
        private Sensor rangeTime;
        private Sensor flushTime;
        private StreamsMetricsImpl metrics;
        private string taskName;

        MeteredKeyValueStore(IKeyValueStore<Bytes, byte[]> inner,
                             string metricScope,
                             ITime time,
                             ISerde<K> keySerde,
                             ISerde<V> valueSerde)
            : base(inner)
        {
            this.metricScope = metricScope;
            this.time = time != null ? time : Time.SYSTEM;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        public override void init(IProcessorContext<K, V> context,
                         IStateStore root)
        {
            metrics = (StreamsMetricsImpl)context.metrics();

            taskName = context.taskId().ToString();
            string metricsGroup = "stream-" + metricScope + "-metrics";
            Dictionary<string, string> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
            Dictionary<string, string> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name());

            initStoreSerde(context);

            putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            putIfAbsentTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put-if-absent", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            putAllTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put-all", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            getTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "get", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            allTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "all", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            rangeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "range", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            deleteTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "delete", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name(), taskTags, storeTags);

            // register and possibly restore the state from the logs
            if (restoreTime.shouldRecord())
            {
                measureLatency(
                    ()=>
    {
                    base.init(context, root);
                    return null;
                },
                restoreTime);
            }
            else
            {
                base.init(context, root);
            }
        }


        void initStoreSerde(IProcessorContext<K, V> context)
        {
            serdes = new StateSerdes<>(
                ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
                keySerde == null ? (ISerde<K>)context.keySerde : keySerde,
                valueSerde == null ? (ISerde<V>)context.valueSerde : valueSerde);
        }


        public override bool setFlushListener(CacheFlushListener<K, V> listener,
                                        bool sendOldValues)
        {
            IKeyValueStore<Bytes, byte[]> wrapped = wrapped;
            if (wrapped is CachedStateStore)
            {
                return ((CachedStateStore<byte[], byte[]>)wrapped].setFlushListener(
                   (rawKey, rawNewValue, rawOldValue, timestamp)=>listener.apply(
                       serdes.keyFrom(rawKey),
                       rawNewValue != null ? serdes.valueFrom(rawNewValue) : null,
                       rawOldValue != null ? serdes.valueFrom(rawOldValue) : null,
                       timestamp
                   ),
                   sendOldValues);
            }
            return false;
        }

        public override V get(K key)
        {
            try
            {
                if (getTime.shouldRecord())
                {
                    return measureLatency(()=>outerValue(wrapped[keyBytes(key)]), getTime);
                }
                else
                {
                    return outerValue(wrapped[keyBytes(key)]);
                }
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.getMessage(), key);
                throw new ProcessorStateException(message, e);
            }
        }

        public override void put(K key,
                        V value)
        {
            try
            {
                if (putTime.shouldRecord())
                {
                    measureLatency(()=>
    {
                        wrapped.Add(keyBytes(key), serdes.rawValue(value));
                        return null;
                    }, putTime);
                }
                else
                {
                    wrapped.Add(keyBytes(key), serdes.rawValue(value));
                }
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.getMessage(), key, value);
                throw new ProcessorStateException(message, e);
            }
        }

        public override V putIfAbsent(K key,
                             V value)
        {
            if (putIfAbsentTime.shouldRecord())
            {
                return measureLatency(
                    ()=>outerValue(wrapped.putIfAbsent(keyBytes(key), serdes.rawValue(value))),
                    putIfAbsentTime);
            }
            else
            {
                return outerValue(wrapped.putIfAbsent(keyBytes(key), serdes.rawValue(value)));
            }
        }

        public override void putAll(List<KeyValue<K, V>> entries)
        {
            if (putAllTime.shouldRecord())
            {
                measureLatency(
                    ()=>
    {
                    wrapped.putAll(innerEntries(entries));
                    return null;
                },
                putAllTime);
            }
            else
            {
                wrapped.putAll(innerEntries(entries));
            }
        }

        public override V delete(K key)
        {
            try
            {
                if (deleteTime.shouldRecord())
                {
                    return measureLatency(()=>outerValue(wrapped.delete(keyBytes(key))), deleteTime);
                }
                else
                {
                    return outerValue(wrapped.delete(keyBytes(key)));
                }
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.getMessage(), key);
                throw new ProcessorStateException(message, e);
            }
        }

        public override IKeyValueIterator<K, V> range(K from,
                                            K to)
        {
            return new MeteredKeyValueIterator(
                wrapped.range(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to))),
                rangeTime);
        }

        public override IKeyValueIterator<K, V> all()
        {
            return new MeteredKeyValueIterator(wrapped.all(), allTime);
        }

        public override void flush()
        {
            if (flushTime.shouldRecord())
            {
                measureLatency(
                    ()=>
    {
                    base.flush();
                    return null;
                },
                flushTime);
            }
            else
            {
                base.flush();
            }
        }

        public override long approximateNumEntries()
        {
            return wrapped.approximateNumEntries();
        }

        public override void close()
        {
            base.close();
            metrics.removeAllStoreLevelSensors(taskName, name());
        }

        private interface Action<V>
        {
            V execute();
        }

        private V measureLatency(Action<V> action,
                                 Sensor sensor)
        {
            long startNs = time.nanoseconds();
            try
            {
                return action.execute();
            }
            finally
            {
                metrics.recordLatency(sensor, startNs, time.nanoseconds());
            }
        }

        private V outerValue(byte[] value)
        {
            return value != null ? serdes.valueFrom(value) : null;
        }

        private Bytes keyBytes(K key)
        {
            return Bytes.wrap(serdes.rawKey(key));
        }

        private List<KeyValue<Bytes, byte[]>> innerEntries(List<KeyValue<K, V>> from)
        {
            List<KeyValue<Bytes, byte[]>> byteEntries = new List<>();
            foreach (KeyValue<K, V> entry in from)
            {
                byteEntries.Add(KeyValue.pair(Bytes.wrap(serdes.rawKey(entry.key)), serdes.rawValue(entry.value)));
            }
            return byteEntries;
        }

        private class MeteredKeyValueIterator : IKeyValueIterator<K, V>
        {

            private IKeyValueIterator<Bytes, byte[]> iter;
            private Sensor sensor;
            private long startNs;

            private MeteredKeyValueIterator(IKeyValueIterator<Bytes, byte[]> iter,
                                            Sensor sensor)
            {
                this.iter = iter;
                this.sensor = sensor;
                this.startNs = time.nanoseconds();
            }


            public bool hasNext()
            {
                return iter.hasNext();
            }


            public KeyValue<K, V> next()
            {
                KeyValue<Bytes, byte[]> keyValue = iter.next();
                return KeyValue.pair(
                    serdes.keyFrom(keyValue.key()],
                    outerValue(keyValue.value));
            }


            public void Remove()
            {
                iter.Remove();
            }


            public void close()
            {
                try
                {
                    iter.close();
                }
                finally
                {
                    metrics.recordLatency(sensor, startNs, time.nanoseconds());
                }
            }


            public K peekNextKey()
            {
                return serdes.keyFrom(iter.peekNextKey()());
            }
        }
    }
