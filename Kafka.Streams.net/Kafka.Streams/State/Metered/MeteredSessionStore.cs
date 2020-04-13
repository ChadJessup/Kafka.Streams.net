using System;
using System.Collections.Generic;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.State.Metered
{
    public class MeteredSessionStore<K, V>
        : WrappedStateStore<ISessionStore<Bytes, byte[]>, K, V>,
        ISessionStore<K, V>
    {
        //private string metricScope;
        private ISerde<K> keySerde;
        private ISerde<V> valueSerde;
        private IStateSerdes<K, V> serdes;
        private string taskName;

        public MeteredSessionStore(
            KafkaStreamsContext context,
            ISessionStore<Bytes, byte[]> inner,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
            : base(context, inner)
        {
            //this.metricScope = metricScope;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        public override void Init(
            IProcessorContext context,
            IStateStore root)
        {
            //noinspection unchecked
            this.serdes = new StateSerdes<K, V>(
                ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, this.Name),
                this.keySerde ?? (ISerde<K>)context.KeySerde,
                this.valueSerde ?? (ISerde<V>)context.ValueSerde);

            this.taskName = context.TaskId.ToString();
            //string metricsGroup = "stream-" + metricScope + "-metrics";
            //Dictionary<string, string> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "All");
            //Dictionary<string, string> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", Name);

            //putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Put", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //fetchTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Fetch", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Flush", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //removeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Remove", metrics, metricsGroup, taskName, Name, taskTags, storeTags);
            //Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, Name, taskTags, storeTags);

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
                //    this.Context.Clock.NowAsEpochNanoseconds;
                //);
            }
        }

        public override bool SetFlushListener(
            FlushListener<K, V> listener,
            bool sendOldValues)
        {
            ISessionStore<Bytes, byte[]> wrapped = this.Wrapped;
            if (wrapped is ICachedStateStore)
            {
                return ((ICachedStateStore<byte[], byte[]>)wrapped)
                   .SetFlushListener((key, newValue, oldValue, timestamp) =>
                   {
                       var nv = this.serdes.ValueFrom(newValue);
                       var ov = this.serdes.ValueFrom(oldValue);

                       var windowed = SessionKeySchema.From(
                           key,
                           this.serdes.KeyDeserializer(),
                           this.serdes.Topic);

                       listener?.Invoke(
                           default, //windowed,
                           nv,
                           ov,
                           timestamp);
                   },
                   sendOldValues);
            }

            return false;
        }

        public void Put(IWindowed<K> sessionKey, V aggregate)
        {
            sessionKey = sessionKey ?? throw new ArgumentNullException(nameof(sessionKey));
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                Bytes key = this.KeyBytes(sessionKey.Key);
                this.Wrapped.Put(new Windowed<Bytes>(key, sessionKey.Window), this.serdes.RawValue(aggregate));
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.Message, sessionKey.Key, aggregate);
                throw new ProcessorStateException(message, e);
            }
            finally
            {
                //metrics.recordLatency(putTime, startNs, this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        public void Remove(IWindowed<K> sessionKey)
        {
            sessionKey = sessionKey ?? throw new ArgumentNullException(nameof(sessionKey));
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                Bytes key = this.KeyBytes(sessionKey.Key);
                this.Wrapped.Remove(new Windowed<Bytes>(key, sessionKey.Window));
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.ToString(), sessionKey.Key);
                throw new ProcessorStateException(message, e);
            }
            finally
            {
                //metrics.recordLatency(removeTime, startNs, this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        public V FetchSession(K key, DateTime startTime, DateTime endTime)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            Bytes bytesKey = this.KeyBytes(key);
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                byte[] result = this.Wrapped.FetchSession(
                    bytesKey,
                    startTime,
                    endTime);

                if (result == null)
                {
                    return default;
                }
                return this.serdes.ValueFrom(result);
            }
            finally
            {
                //metrics.recordLatency(flushTime, startNs, this.Context.Clock.NowAsEpochNanoseconds;);
            }
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(K key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                this.Wrapped.Fetch(this.KeyBytes(key)),
                this.serdes);
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(K from, K to)
        {
            from = from ?? throw new ArgumentNullException(nameof(from));
            to = to ?? throw new ArgumentNullException(nameof(to));

            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                this.Wrapped.Fetch(this.KeyBytes(from), this.KeyBytes(to)),
                this.serdes);
        }

        public IKeyValueIterator<IWindowed<K>, V> FindSessions(
            K key,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            Bytes bytesKey = this.KeyBytes(key);
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                this.Wrapped.FindSessions(
                    bytesKey,
                    earliestSessionEndTime,
                    latestSessionStartTime),
                this.serdes);
        }

        public IKeyValueIterator<IWindowed<K>, V> FindSessions(
            K keyFrom,
            K keyTo,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            keyFrom = keyFrom ?? throw new ArgumentNullException(nameof(keyFrom));
            keyTo = keyTo ?? throw new ArgumentNullException(nameof(keyTo));
            Bytes bytesKeyFrom = this.KeyBytes(keyFrom);
            Bytes bytesKeyTo = this.KeyBytes(keyTo);
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                this.Wrapped.FindSessions(
                    bytesKeyFrom,
                    bytesKeyTo,
                    earliestSessionEndTime,
                    latestSessionStartTime),
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
            //metrics.removeAllStoreLevelSensors(taskName, Name);
        }

        private Bytes KeyBytes(K key)
        {
            return Bytes.Wrap(this.serdes.RawKey(key));
        }
    }
}
