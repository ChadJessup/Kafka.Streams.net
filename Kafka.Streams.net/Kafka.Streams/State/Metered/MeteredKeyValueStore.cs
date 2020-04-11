using System;
using System.Collections.Generic;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Metered
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
        : WrappedStateStore<IKeyValueStore<Bytes, byte[]>, K, V>,
        IKeyValueStore<K, V>
    {
        protected KafkaStreamsContext Context { get; }
        protected ISerde<K> KeySerde { get; }
        protected ISerde<V> ValueSerde { get; }
        protected IStateSerdes<K, V> Serdes { get; set; }

        private string taskName;

        public MeteredKeyValueStore(
            KafkaStreamsContext context,
            IKeyValueStore<Bytes, byte[]> inner,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            : base(context, inner)
        {
            this.Context = context;
            this.KeySerde = keySerde;
            this.ValueSerde = valueSerde;
        }

        public override void Init(IProcessorContext context, IStateStore root)
        {
            if (context is null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            if (root is null)
            {
                throw new ArgumentNullException(nameof(root));
            }

            this.taskName = context.TaskId.ToString();

            this.InitStoreSerde(context);

            base.Init(context, root);

            //putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Put", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //putIfAbsentTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Put-if-absent", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //putAllTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Put-All", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //getTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "get", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //allTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "All", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //rangeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "range", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Flush", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //deleteTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "delete", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, Name, taskTags, storeTags);

            // register and possibly restore the state from the logs
            //if (restoreTime.shouldRecord())
            //{
            //    //            measureLatency(
            //    //                () =>
            //    //{
            //    //    base.Init(context, root);
            //    //    return null;
            //    //},
            //    //            restoreTime);
            //}
            //else
            //{
            //    base.Init(context, root);
            //}
        }

        protected virtual void InitStoreSerde(IProcessorContext context)
        {
            var ks = this.KeySerde ?? (ISerde<K>)context.KeySerde;
            var vs = this.ValueSerde ?? (ISerde<V>)context.ValueSerde;

            this.Serdes = new StateSerdes<K, V>(
                ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, this.Name),
                ks,
                vs);
        }


        public bool? SetFlushListener(ICacheFlushListener<K, V> listener, bool sendOldValues)
        {
            IKeyValueStore<Bytes, byte[]> wrapped = this.Wrapped;
            if (wrapped is ICachedStateStore)
            {
                return null;
                //return ((CachedStateStore<byte[], byte[]>)wrapped].setFlushListener(
                //   (rawKey, rawNewValue, rawOldValue, timestamp) => listener.apply(
                //       serdes.keyFrom(rawKey),
                //       rawNewValue != null ? serdes.valueFrom(rawNewValue) : null,
                //       rawOldValue != null ? serdes.valueFrom(rawOldValue) : null,
                //       timestamp
                //   ),
                //   sendOldValues);
            }

            return false;
        }

        public V Get(K key)
        {
            return default;
            //try
            //{
            //    if (getTime.shouldRecord())
            //    {
            //        return measureLatency(() => outerValue(wrapped[KeyBytes(key)]), getTime);
            //    }
            //    else
            //    {
            //        return outerValue(wrapped[KeyBytes(key)]);
            //    }
            //}
            //catch (ProcessorStateException e)
            //{
            //    string message = string.Format(e.ToString(), key);
            //    throw new ProcessorStateException(message, e);
            //}
        }

        public void Put(K key, V value)
        {
            //try
            //{
            //    if (putTime.shouldRecord())
            //    {
            //        //                measureLatency(() =>
            //        //{
            //        //    wrapped.Add(keyBytes(key), serdes.RawValue(value));
            //        //    return null;
            //        //}, putTime);
            //    }
            //    else
            //    {
            //        wrapped.Add(KeyBytes(key), serdes.RawValue(value));
            //    }
            //}
            //catch (ProcessorStateException e)
            //{
            //    string message = string.Format(e.ToString(), key, value);
            //    throw new ProcessorStateException(message, e);
            //}
        }

        public V PutIfAbsent(K key, V value)
        {
            return default;
            //if (PutIfAbsentTime.shouldRecord())
            //{
            //    return measureLatency(
            //        () => outerValue(wrapped.putIfAbsent(KeyBytes(key), serdes.RawValue(value))),
            //        PutIfAbsentTime);
            //}
            //else
            //{
            //    return outerValue(wrapped.PutIfAbsent(KeyBytes(key), serdes.RawValue(value)));
            //}
        }

        public void PutAll(List<KeyValuePair<K, V>> entries)
        {
            //if (PutAllTime.shouldRecord())
            //{
            //    //            measureLatency(
            //    //                () =>
            //    //{
            //    //    wrapped.putAll(innerEntries(entries));
            //    //    return null;
            //    //},
            //    //            putAllTime);
            //}
            //else
            //{
            //    wrapped.PutAll(InnerEntries(entries));
            //}
        }

        public V Delete(K key)
        {
            return default;
            //try
            //{
            //    if (DeleteTime.shouldRecord())
            //    {
            //        return default; // measureLatency(() => outerValue(wrapped.delete(KeyBytes(key))), deleteTime);
            //    }
            //    else
            //    {
            //        return outerValue(wrapped.Delete(KeyBytes(key)));
            //    }
            //}
            //catch (ProcessorStateException e)
            //{
            //    string message = string.Format(e.ToString(), key);
            //    throw new ProcessorStateException(message, e);
            //}
        }

        public IKeyValueIterator<K, V> Range(K from, K to)
            => null;//new MeteredKeyValueIterator(
                    // wrapped.Range(Bytes.Wrap(serdes.RawKey(from)), Bytes.Wrap(serdes.RawKey(to))),
                    // rangeTime);

        public IKeyValueIterator<K, V> All()
        {
            return new MeteredKeyValueIterator<K, V>(this.Context, this.Wrapped.All());//, allTime);
        }

        public override void Flush()
        {
            //if (flushTime.shouldRecord())
            //{
            //    //            measureLatency(
            //    //                () =>
            //    //{
            //    //    base.Flush();
            //    //    return null;
            //    //},
            //    //            flushTime);
            //}
            //else
            //{
            //    base.Flush();
            //}
        }

        public long approximateNumEntries => this.Wrapped.approximateNumEntries;

        public override void Close()
        {
            base.Close();
            //metrics.removeAllStoreLevelSensors(taskName, Name);
        }

        //private interface Action<V>
        //{
        //    V execute();
        //}

        // private V measureLatency(Action<V> action)
        // {
        //     long startNs = clock.nanoseconds();
        //     try
        //     {
        //         return action.execute();
        //     }
        //     finally
        //     {
        //         metrics.recordLatency(sensor, startNs, clock.nanoseconds());
        //     }
        // }

        private V outerValue(byte[] value)
        {
            return default; // value != null ? serdes.ValueFrom(value) : null;
        }

        private Bytes KeyBytes(K key)
        {
            return Bytes.Wrap(this.Serdes.RawKey(key));
        }

        private List<KeyValuePair<Bytes, byte[]>> InnerEntries(List<KeyValuePair<K, V>> from)
        {
            List<KeyValuePair<Bytes, byte[]>> byteEntries = new List<KeyValuePair<Bytes, byte[]>>();
            foreach (KeyValuePair<K, V> entry in from)
            {
                byteEntries.Add(KeyValuePair.Create<Bytes, byte[]>(
                    Bytes.Wrap(this.Serdes.RawKey(entry.Key)),
                        this.Serdes.RawValue(entry.Value)));
            }

            return byteEntries;
        }

        public void Add(K key, V value)
        {
        }
    }
}
