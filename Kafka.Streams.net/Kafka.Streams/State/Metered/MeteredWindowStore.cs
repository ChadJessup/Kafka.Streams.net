using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.State.Windowed;
using System;

namespace Kafka.Streams.State.Metered
{
    public class MeteredWindowStore<K, V>
        : WrappedStateStore<IWindowStore<Bytes, byte[]>, IWindowed<K>, V>,
        IWindowStore<K, V>
    {
        protected ISerde<K> KeySerde { get; set; }
        protected ISerde<V> ValueSerde { get; set; }
        protected IStateSerdes<K, V> serdes { get; set; }
        private IProcessorContext context;
        private readonly TimeSpan windowSize;
        private string taskName;

        public MeteredWindowStore(
            KafkaStreamsContext context,
            IWindowStore<Bytes, byte[]> inner,
            TimeSpan windowSizeMs,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            : base(context, inner)
        {
            this.windowSize = windowSizeMs;
            this.ValueSerde = valueSerde;
            this.KeySerde = keySerde;
        }

        public override void Init(IProcessorContext context, IStateStore root)
        {
            this.context = context;
            this.InitStoreSerde(context);

            this.taskName = context.TaskId.ToString();
            //string metricsGroup = "stream-" + metricScope + "-metrics";
            //Dictionary<string, string> taskTags = metrics.tagMap("task-id", this.taskName, metricScope + "-id", "All");
            //Dictionary<string, string> storeTags = metrics.tagMap("task-id", this.taskName, metricScope + "-id", this.Name);

            //putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Put", metrics, metricsGroup, this.taskName, this.Name, taskTags, storeTags);
            //fetchTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Fetch", metrics, metricsGroup, this.taskName, this.Name, taskTags, storeTags);
            //flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Flush", metrics, metricsGroup, this.taskName, this.Name, taskTags, storeTags);
            //Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, this.taskName, this.Name, taskTags, storeTags);

            // register and possibly restore the state from the logs
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                base.Init(context, root);
            }
            finally
            {
                //metrics.recordLatency(
                //    restoreTime,
                //    startNs,
                //    this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        protected virtual void InitStoreSerde(IProcessorContext context)
        {
            this.serdes = new StateSerdes<K, V>(
                ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, this.Name),
                this.KeySerde ?? (ISerde<K>)context.KeySerde,
                this.ValueSerde ?? (ISerde<V>)context.ValueSerde);
        }

        public override bool SetFlushListener(FlushListener<IWindowed<K>, V> listener, bool sendOldValues)
        {
            IWindowStore<Bytes, byte[]> wrapped = this.Wrapped;
            if (wrapped is ICachedStateStore)
            {
                return ((ICachedStateStore<byte[], byte[]>)wrapped)
                    .SetFlushListener((key, newValue, oldValue, timestamp) =>
                    {
                        V nv = this.serdes.ValueFrom(newValue);
                        V ov = this.serdes.ValueFrom(oldValue);

                        var windowed = WindowKeySchema.FromStoreKey(
                            key,
                            this.windowSize,
                            this.serdes.KeyDeserializer(),
                            this.serdes.Topic);

                        listener?.Invoke(
                            windowed,
                            nv,
                            ov,
                            timestamp);
                    },
                    sendOldValues);
            }

            return false;
        }

        public void Put(K key, V value)
        {
            this.Put(key, value, this.context.Timestamp);
        }

        public void Put(K key, V value, DateTime windowStartTimestamp)
        {
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                this.Wrapped.Put(this.KeyBytes(key), this.serdes.RawValue(value), windowStartTimestamp);
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.ToString(), key, value);

                throw new ProcessorStateException(message, e);
            }
            finally
            {
                //metrics.recordLatency(putTime, startNs, this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        public V Fetch(K key, DateTime timestamp)
        {
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                byte[] result = this.Wrapped.Fetch(this.KeyBytes(key), timestamp);
                if (result == null)
                {
                    return default;
                }

                return this.serdes.ValueFrom(result);
            }
            finally
            {
                //metrics.recordLatency(fetchTime, startNs, this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        public IWindowStoreIterator<V> Fetch(K key, DateTime timeFrom, DateTime timeTo)
        {
            var iter = this.Wrapped.Fetch(this.KeyBytes(key), timeFrom, timeTo);
            return new MeteredWindowStoreIterator<V>(
                this.Context,
                iter,
                (IStateSerdes<long, V>)this.serdes);
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(
            K from,
            K to,
            DateTime timeFrom,
            DateTime timeTo)
        {
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                this.Wrapped.Fetch(this.KeyBytes(from), this.KeyBytes(to), timeFrom, timeTo),
                this.serdes);
        }


        public IKeyValueIterator<IWindowed<K>, V> FetchAll(DateTime timeFrom, DateTime timeTo)
        {
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                this.Wrapped.FetchAll(timeFrom, timeTo),
                this.serdes);
        }

        public IKeyValueIterator<IWindowed<K>, V> All()
        {
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                this.Wrapped.All(),
                this.serdes);
        }

        public override void Flush()
        {
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;

            try
            {
                base.Flush();
            }
            finally
            {
                // metrics.recordLatency(flushTime, startNs, this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        public override void Close()
        {
            base.Close();
            // metrics.removeAllStoreLevelSensors(this.taskName, this.Name);
        }

        private Bytes KeyBytes(K key)
        {
            return Bytes.Wrap(this.serdes.RawKey(key));
        }

        public void Add(K key, V value)
        {
        }
    }
}
