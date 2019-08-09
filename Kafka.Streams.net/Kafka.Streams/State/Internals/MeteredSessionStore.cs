/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.Streams.State.Internals;

using Kafka.Common.metrics.Sensor;
using Kafka.Common.serialization.Serde;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.Errors.ProcessorStateException;
using Kafka.Streams.KStream.Windowed;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.Internals.ProcessorStateManager;
using Kafka.Streams.Processor.Internals.metrics.StreamsMetricsImpl;
using Kafka.Streams.State.IKeyValueIterator;
using Kafka.Streams.State.ISessionStore;
using Kafka.Streams.State.StateSerdes;







public class MeteredSessionStore<K, V>
    : WrappedStateStore<ISessionStore<Bytes, byte[]>, Windowed<K>, V>
    : ISessionStore<K, V>
{

    private string metricScope;
    private ISerde<K> keySerde;
    private ISerde<V> valueSerde;
    private ITime time;
    private StateSerdes<K, V> serdes;
    private StreamsMetricsImpl metrics;
    private Sensor putTime;
    private Sensor fetchTime;
    private Sensor flushTime;
    private Sensor removeTime;
    private string taskName;

    MeteredSessionStore(ISessionStore<Bytes, byte[]> inner,
                        string metricScope,
                        ISerde<K> keySerde,
                        ISerde<V> valueSerde,
                        ITime time)
{
        base(inner);
        this.metricScope = metricScope;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
    }


    public override void init(IProcessorContext context,
                     IStateStore root)
{
        //noinspection unchecked
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (ISerde<K>) context.keySerde : keySerde,
            valueSerde == null ? (ISerde<V>) context.valueSerde : valueSerde);
        metrics = (StreamsMetricsImpl) context.metrics();

        taskName = context.taskId().ToString();
        string metricsGroup = "stream-" + metricScope + "-metrics";
        Dictionary<string, string> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
        Dictionary<string, string> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name());

        putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        fetchTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "fetch", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        removeTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "Remove", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
        Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name(), taskTags, storeTags);

        // register and possibly restore the state from the logs
        long startNs = time.nanoseconds();
        try
{
            base.init(context, root);
        } finally
{
            metrics.recordLatency(
                restoreTime,
                startNs,
                time.nanoseconds()
            );
        }
    }


    public override bool setFlushListener(CacheFlushListener<Windowed<K>, V> listener,
                                    bool sendOldValues)
{
        ISessionStore<Bytes, byte[]> wrapped = wrapped;
        if (wrapped is CachedStateStore)
{
            return ((CachedStateStore<byte[], byte[]>) wrapped].setFlushListener(
                (key, newValue, oldValue, timestamp) => listener.apply(
                    SessionKeySchema.from(key, serdes.keyDeserializer(), serdes.Topic),
                    newValue != null ? serdes.valueFrom(newValue) : null,
                    oldValue != null ? serdes.valueFrom(oldValue) : null,
                    timestamp
                ),
                sendOldValues);
        }
        return false;
    }

    public override void put(Windowed<K> sessionKey,
                    V aggregate)
{
        sessionKey = sessionKey ?? throw new System.ArgumentNullException("sessionKey can't be null", nameof(sessionKey));
        long startNs = time.nanoseconds();
        try
{
            Bytes key = keyBytes(sessionKey.key());
            wrapped.Add(new Windowed<>(key, sessionKey.window()), serdes.rawValue(aggregate));
        } catch (ProcessorStateException e)
{
            string message = string.Format(e.getMessage(), sessionKey.key(), aggregate);
            throw new ProcessorStateException(message, e);
        } finally
{
            metrics.recordLatency(putTime, startNs, time.nanoseconds());
        }
    }

    public override void Remove(Windowed<K> sessionKey)
{
        sessionKey = sessionKey ?? throw new System.ArgumentNullException("sessionKey can't be null", nameof(sessionKey));
        long startNs = time.nanoseconds();
        try
{
            Bytes key = keyBytes(sessionKey.key());
            wrapped.Remove(new Windowed<>(key, sessionKey.window()));
        } catch (ProcessorStateException e)
{
            string message = string.Format(e.getMessage(), sessionKey.key());
            throw new ProcessorStateException(message, e);
        } finally
{
            metrics.recordLatency(removeTime, startNs, time.nanoseconds());
        }
    }

    public override V fetchSession(K key, long startTime, long endTime)
{
        key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
        Bytes bytesKey = keyBytes(key);
        long startNs = time.nanoseconds();
        try
{
            byte[] result = wrapped.fetchSession(bytesKey, startTime, endTime);
            if (result == null)
{
                return default;
            }
            return serdes.valueFrom(result);
        } finally
{
            metrics.recordLatency(flushTime, startNs, time.nanoseconds());
        }
    }

    public override IKeyValueIterator<Windowed<K>, V> fetch(K key)
{
        key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
        return new MeteredWindowedKeyValueIterator<>(
            wrapped.fetch(keyBytes(key)),
            fetchTime,
            metrics,
            serdes,
            time);
    }

    public override IKeyValueIterator<Windowed<K>, V> fetch(K from,
                                                  K to)
{
        from = from ?? throw new System.ArgumentNullException("from cannot be null", nameof(from));
        to = to ?? throw new System.ArgumentNullException("to cannot be null", nameof(to));
        return new MeteredWindowedKeyValueIterator<>(
            wrapped.fetch(keyBytes(from), keyBytes(to)),
            fetchTime,
            metrics,
            serdes,
            time);
    }

    public override IKeyValueIterator<Windowed<K>, V> findSessions(K key,
                                                         long earliestSessionEndTime,
                                                         long latestSessionStartTime)
{
        key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
        Bytes bytesKey = keyBytes(key);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped.findSessions(
                bytesKey,
                earliestSessionEndTime,
                latestSessionStartTime),
            fetchTime,
            metrics,
            serdes,
            time);
    }

    public override IKeyValueIterator<Windowed<K>, V> findSessions(K keyFrom,
                                                         K keyTo,
                                                         long earliestSessionEndTime,
                                                         long latestSessionStartTime)
{
        keyFrom = keyFrom ?? throw new System.ArgumentNullException("keyFrom cannot be null", nameof(keyFrom));
        keyTo = keyTo ?? throw new System.ArgumentNullException("keyTo cannot be null", nameof(keyTo));
        Bytes bytesKeyFrom = keyBytes(keyFrom);
        Bytes bytesKeyTo = keyBytes(keyTo);
        return new MeteredWindowedKeyValueIterator<>(
            wrapped.findSessions(
                bytesKeyFrom,
                bytesKeyTo,
                earliestSessionEndTime,
                latestSessionStartTime),
            fetchTime,
            metrics,
            serdes,
            time);
    }

    public override void flush()
{
        long startNs = time.nanoseconds();
        try
{
            base.flush();
        } finally
{
            metrics.recordLatency(flushTime, startNs, time.nanoseconds());
        }
    }

    public override void close()
{
        base.close();
        metrics.removeAllStoreLevelSensors(taskName, name());
    }

    private Bytes keyBytes(K key)
{
        return Bytes.wrap(serdes.rawKey(key));
    }
}
