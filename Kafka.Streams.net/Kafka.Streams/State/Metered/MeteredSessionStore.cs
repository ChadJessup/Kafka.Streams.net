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
                ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, Name),
                keySerde ?? (ISerde<K>)context.KeySerde,
                valueSerde ?? (ISerde<V>)context.ValueSerde);

            taskName = context.TaskId.ToString();
            //string metricsGroup = "stream-" + metricScope + "-metrics";
            //Dictionary<string, string> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
            //Dictionary<string, string> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name);

            //putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name, taskTags, storeTags);
            //fetchTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "fetch", metrics, metricsGroup, taskName, name, taskTags, storeTags);
            //flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name, taskTags, storeTags);
            //removeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Remove", metrics, metricsGroup, taskName, name, taskTags, storeTags);
            //Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name, taskTags, storeTags);

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
                //    time.nanoseconds()
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
                       var nv = serdes.ValueFrom(newValue);
                       var ov = serdes.ValueFrom(oldValue);

                       var windowed = SessionKeySchema.From(
                           key,
                           serdes.KeyDeserializer(),
                           serdes.Topic);

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

        public void Put(Windowed<K> sessionKey, V aggregate)
        {
            sessionKey = sessionKey ?? throw new ArgumentNullException(nameof(sessionKey));
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                Bytes key = KeyBytes(sessionKey.Key);
                Wrapped.Put(new Windowed<Bytes>(key, sessionKey.window), serdes.RawValue(aggregate));
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.Message, sessionKey.Key, aggregate);
                throw new ProcessorStateException(message, e);
            }
            finally
            {
                //metrics.recordLatency(putTime, startNs, time.nanoseconds());
            }
        }

        public void Remove(Windowed<K> sessionKey)
        {
            sessionKey = sessionKey ?? throw new ArgumentNullException(nameof(sessionKey));
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                Bytes key = KeyBytes(sessionKey.Key);
                Wrapped.Remove(new Windowed<Bytes>(key, sessionKey.window));
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.ToString(), sessionKey.Key);
                throw new ProcessorStateException(message, e);
            }
            finally
            {
                //metrics.recordLatency(removeTime, startNs, time.nanoseconds());
            }
        }

        public V FetchSession(K key, long startTime, long endTime)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            Bytes bytesKey = KeyBytes(key);
            long startNs = this.Context.Clock.NowAsEpochNanoseconds;
            try
            {
                byte[] result = Wrapped.FetchSession(bytesKey, startTime, endTime);
                if (result == null)
                {
                    return default;
                }
                return serdes.ValueFrom(result);
            }
            finally
            {
                //metrics.recordLatency(flushTime, startNs, time.nanoseconds());
            }
        }

        public IKeyValueIterator<Windowed<K>, V> Fetch(K key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                Wrapped.Fetch(KeyBytes(key)),
                serdes);
        }

        public IKeyValueIterator<Windowed<K>, V> Fetch(K from, K to)
        {
            from = from ?? throw new ArgumentNullException(nameof(from));
            to = to ?? throw new ArgumentNullException(nameof(to));

            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                Wrapped.Fetch(KeyBytes(from), KeyBytes(to)),
                serdes);
        }

        public IKeyValueIterator<Windowed<K>, V> FindSessions(
            K key,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            Bytes bytesKey = KeyBytes(key);
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                Wrapped.FindSessions(
                    bytesKey,
                    earliestSessionEndTime,
                    latestSessionStartTime),
                serdes);
        }

        public IKeyValueIterator<Windowed<K>, V> FindSessions(
            K keyFrom,
            K keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            keyFrom = keyFrom ?? throw new ArgumentNullException(nameof(keyFrom));
            keyTo = keyTo ?? throw new ArgumentNullException(nameof(keyTo));
            Bytes bytesKeyFrom = KeyBytes(keyFrom);
            Bytes bytesKeyTo = KeyBytes(keyTo);
            return new MeteredWindowedKeyValueIterator<K, V>(
                this.Context,
                Wrapped.FindSessions(
                    bytesKeyFrom,
                    bytesKeyTo,
                    earliestSessionEndTime,
                    latestSessionStartTime),
                serdes);
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
                // metrics.recordLatency(flushTime, startNs, time.nanoseconds());
            }
        }

        public override void Close()
        {
            base.Close();
            //metrics.removeAllStoreLevelSensors(taskName, name);
        }

        private Bytes KeyBytes(K key)
        {
            return Bytes.Wrap(serdes.RawKey(key));
        }
    }
}
