//using Kafka.Common.Utils;
//using Kafka.Common.Utils.Interfaces;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.Interfaces;
//using NodaTime;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Metered
//{
//    /**
//     * A Metered {@link KeyValueStore} wrapper that is used for recording operation metrics, and hence its
//     * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
//     * The inner {@link KeyValueStore} of this is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
//     * to convert from &lt;K,V&gt; to &lt;Bytes,byte[]&gt;
//     * @param <K>
//     * @param <V>
//     */
//    public class MeteredKeyValueStore<K, V>
//        : WrappedStateStore<IKeyValueStore<Bytes, byte[]>>, IKeyValueStore<K, V>
//    {
//        ISerde<K> keySerde;
//        ISerde<V> valueSerde;
//        StateSerdes<K, V> serdes;

//        protected IClock clock;
//        private string taskName;

//        public MeteredKeyValueStore(
//            IKeyValueStore<Bytes, byte[]> inner,
//            string metricScope,
//            IClock clock,
//            ISerde<K> keySerde,
//            ISerde<V> valueSerde)
//            : base(inner)
//        {
//            this.clock = clock;
//            this.keySerde = keySerde;
//            this.valueSerde = valueSerde;
//        }

//        public void init(
//            IProcessorContext context,
//            IStateStore root)
//        {
//            taskName = context.taskId.ToString();

//            initStoreSerde(context);

//            //putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            //putIfAbsentTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put-if-absent", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            //putAllTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put-all", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            //getTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "get", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            //allTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "all", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            //rangeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "range", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            //flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            //deleteTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "delete", metrics, metricsGroup, taskName, name, taskTags, storeTags);
//            //Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name, taskTags, storeTags);

//            // register and possibly restore the state from the logs
//            if (restoreTime.shouldRecord())
//            {
//                //            measureLatency(
//                //                () =>
//                //{
//                //    base.init(context, root);
//                //    return null;
//                //},
//                //            restoreTime);
//            }
//            else
//            {
//                base.init(context, root);
//            }
//        }


//        void initStoreSerde(IProcessorContext context)
//        {
//            serdes = new StateSerdes<K, V>(
//                ProcessorStateManager.storeChangelogTopic(context.applicationId, name),
//                keySerde == null ? (ISerde<K>)context.Serializer.keySerde : keySerde,
//                valueSerde == null ? (ISerde<V>)context.valueSerde : valueSerde);
//        }


//        public bool setFlushListener(ICacheFlushListener<K, V> listener,
//                                        bool sendOldValues)
//        {
//            IKeyValueStore<Bytes, byte[]> wrapped = wrapped;
//            if (wrapped is CachedStateStore)
//            {
//                return null;
//                //return ((CachedStateStore<byte[], byte[]>)wrapped].setFlushListener(
//                //   (rawKey, rawNewValue, rawOldValue, timestamp) => listener.apply(
//                //       serdes.keyFrom(rawKey),
//                //       rawNewValue != null ? serdes.valueFrom(rawNewValue) : null,
//                //       rawOldValue != null ? serdes.valueFrom(rawOldValue) : null,
//                //       timestamp
//                //   ),
//                //   sendOldValues);
//            }
//            return false;
//        }

//        public V get(K key)
//        {
//            try
//            {
//                if (getTime.shouldRecord())
//                {
//                    return measureLatency(() => outerValue(wrapped[keyBytes(key)]), getTime);
//                }
//                else
//                {
//                    return outerValue(wrapped[keyBytes(key)]);
//                }
//            }
//            catch (ProcessorStateException e)
//            {
//                string message = string.Format(e.getMessage(), key);
//                throw new ProcessorStateException(message, e);
//            }
//        }

//        public void put(K key, V value)
//        {
//            try
//            {
//                if (putTime.shouldRecord())
//                {
//                    //                measureLatency(() =>
//                    //{
//                    //    wrapped.Add(keyBytes(key), serdes.rawValue(value));
//                    //    return null;
//                    //}, putTime);
//                }
//                else
//                {
//                    wrapped.Add(keyBytes(key), serdes.rawValue(value));
//                }
//            }
//            catch (ProcessorStateException e)
//            {
//                string message = string.Format(e.getMessage(), key, value);
//                throw new ProcessorStateException(message, e);
//            }
//        }

//        public V putIfAbsent(K key, V value)
//        {
//            if (putIfAbsentTime.shouldRecord())
//            {
//                return measureLatency(
//                    () => outerValue(wrapped.putIfAbsent(keyBytes(key), serdes.rawValue(value))),
//                    putIfAbsentTime);
//            }
//            else
//            {
//                return outerValue(wrapped.putIfAbsent(keyBytes(key), serdes.rawValue(value)));
//            }
//        }

//        public void putAll(List<KeyValuePair<K, V>> entries)
//        {
//            if (putAllTime.shouldRecord())
//            {
//                //            measureLatency(
//                //                () =>
//                //{
//                //    wrapped.putAll(innerEntries(entries));
//                //    return null;
//                //},
//                //            putAllTime);
//            }
//            else
//            {
//                wrapped.putAll(innerEntries(entries));
//            }
//        }

//        public V delete(K key)
//        {
//            try
//            {
//                if (deleteTime.shouldRecord())
//                {
//                    return measureLatency(() => outerValue(wrapped.delete(keyBytes(key))), deleteTime);
//                }
//                else
//                {
//                    return outerValue(wrapped.delete(keyBytes(key)));
//                }
//            }
//            catch (ProcessorStateException e)
//            {
//                string message = string.Format(e.getMessage(), key);
//                throw new ProcessorStateException(message, e);
//            }
//        }

//        public IKeyValueIterator<K, V> range(K from, K to)
//        {
//            return new MeteredKeyValueIterator(
//                wrapped.range(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to))),
//                rangeTime);
//        }

//        public IKeyValueIterator<K, V> all()
//        {
//            return new MeteredKeyValueIterator(wrapped.all(), allTime);
//        }

//        public void flush()
//        {
//            if (flushTime.shouldRecord())
//            {
//                //            measureLatency(
//                //                () =>
//                //{
//                //    base.flush();
//                //    return null;
//                //},
//                //            flushTime);
//            }
//            else
//            {
//                base.flush();
//            }
//        }

//        public long approximateNumEntries
//         => wrapped.approximateNumEntries;

//        public void close()
//        {
//            base.close();
//            //metrics.removeAllStoreLevelSensors(taskName, name);
//        }

//        private interface Action<V>
//        {
//            V execute();
//        }

//        private V measureLatency(Action<V> action)
//        {
//            long startNs = clock.nanoseconds();
//            try
//            {
//                return action.execute();
//            }
//            finally
//            {
//                metrics.recordLatency(sensor, startNs, clock.nanoseconds());
//            }
//        }

//        private V outerValue(byte[] value)
//        {
//            return value != null ? serdes.valueFrom(value) : null;
//        }

//        private Bytes keyBytes(K key)
//        {
//            return Bytes.wrap(serdes.rawKey(key));
//        }

//        private List<KeyValuePair<Bytes, byte[]>> innerEntries(List<KeyValuePair<K, V>> from)
//        {
//            List<KeyValuePair<Bytes, byte[]>> byteEntries = new List<KeyValuePair<Bytes, byte[]>>();
//            foreach (KeyValuePair<K, V> entry in from)
//            {
//                byteEntries.Add(KeyValuePair<K, V>.Pair(Bytes.wrap(serdes.rawKey(entry.Key)), serdes.rawValue(entry.Value)));
//            }
//            return byteEntries;
//        }
//    }
//}
