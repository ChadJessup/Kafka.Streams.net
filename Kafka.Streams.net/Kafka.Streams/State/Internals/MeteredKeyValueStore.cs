/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package org.apache.kafka.streams.state.internals;

using Kafka.Common.metrics.Sensor;
using Kafka.Common.serialization.Serde;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Errors.ProcessorStateException;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.internals.ProcessorStateManager;
using Kafka.Streams.Processor.internals.metrics.StreamsMetricsImpl;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.KeyValueStore;
using Kafka.Streams.State.StateSerdes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.metrics.Sensor.RecordingLevel.DEBUG;
import static org.apache.kafka.streams.state.internals.metrics.Sensors.createTaskAndStoreLatencyAndThroughputSensors;

/**
 * A Metered {@link KeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,V&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueStore<K, V>
    : WrappedStateStore<KeyValueStore<Bytes, byte[]>, K, V>
    : KeyValueStore<K, V>
{

    ISerde<K> keySerde;
    ISerde<V> valueSerde;
    StateSerdes<K, V> serdes;

    private string metricScope;
    protected Time time;
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

    MeteredKeyValueStore(KeyValueStore<Bytes, byte[]> inner,
                         string metricScope,
                         Time time,
                         ISerde<K> keySerde,
                         ISerde<V> valueSerde)
{
        super(inner);
        this.metricScope = metricScope;
        this.time = time != null ? time : Time.SYSTEM;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public override void init(IProcessorContext context,
                     IStateStore root)
{
        metrics = (StreamsMetricsImpl) context.metrics();

        taskName = context.taskId().toString();
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
                () ->
{
                    super.init(context, root);
                    return null;
                },
                restoreTime);
        } else
{
            super.init(context, root);
        }
    }

    @SuppressWarnings("unchecked")
    void initStoreSerde(IProcessorContext context)
{
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (ISerde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (ISerde<V>) context.valueSerde() : valueSerde);
    }

    @SuppressWarnings("unchecked")
    public override bool setFlushListener(CacheFlushListener<K, V> listener,
                                    bool sendOldValues)
{
        KeyValueStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped is CachedStateStore)
{
            return ((CachedStateStore<byte[], byte[]>) wrapped).setFlushListener(
                (rawKey, rawNewValue, rawOldValue, timestamp) -> listener.apply(
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
                return measureLatency(() -> outerValue(wrapped().get(keyBytes(key))), getTime);
            } else
{
                return outerValue(wrapped().get(keyBytes(key)));
            }
        } catch (ProcessorStateException e)
{
            string message = string.format(e.getMessage(), key);
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
                measureLatency(() ->
{
                    wrapped().put(keyBytes(key), serdes.rawValue(value));
                    return null;
                }, putTime);
            } else
{
                wrapped().put(keyBytes(key), serdes.rawValue(value));
            }
        } catch (ProcessorStateException e)
{
            string message = string.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    public override V putIfAbsent(K key,
                         V value)
{
        if (putIfAbsentTime.shouldRecord())
{
            return measureLatency(
                () -> outerValue(wrapped().putIfAbsent(keyBytes(key), serdes.rawValue(value))),
                putIfAbsentTime);
        } else
{
            return outerValue(wrapped().putIfAbsent(keyBytes(key), serdes.rawValue(value)));
        }
    }

    public override void putAll(List<KeyValue<K, V>> entries)
{
        if (putAllTime.shouldRecord())
{
            measureLatency(
                () ->
{
                    wrapped().putAll(innerEntries(entries));
                    return null;
                },
                putAllTime);
        } else
{
            wrapped().putAll(innerEntries(entries));
        }
    }

    public override V delete(K key)
{
        try
{
            if (deleteTime.shouldRecord())
{
                return measureLatency(() -> outerValue(wrapped().delete(keyBytes(key))), deleteTime);
            } else
{
                return outerValue(wrapped().delete(keyBytes(key)));
            }
        } catch (ProcessorStateException e)
{
            string message = string.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    public override KeyValueIterator<K, V> range(K from,
                                        K to)
{
        return new MeteredKeyValueIterator(
            wrapped().range(Bytes.wrap(serdes.rawKey(from)), Bytes.wrap(serdes.rawKey(to))),
            rangeTime);
    }

    public override KeyValueIterator<K, V> all()
{
        return new MeteredKeyValueIterator(wrapped().all(), allTime);
    }

    public override void flush()
{
        if (flushTime.shouldRecord())
{
            measureLatency(
                () ->
{
                    super.flush();
                    return null;
                },
                flushTime);
        } else
{
            super.flush();
        }
    }

    public override long approximateNumEntries()
{
        return wrapped().approximateNumEntries();
    }

    public override void close()
{
        super.close();
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
        } finally
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
        List<KeyValue<Bytes, byte[]>> byteEntries = new ArrayList<>();
        for (KeyValue<K, V> entry : from)
{
            byteEntries.add(KeyValue.pair(Bytes.wrap(serdes.rawKey(entry.key)), serdes.rawValue(entry.value)));
        }
        return byteEntries;
    }

    private class MeteredKeyValueIterator : KeyValueIterator<K, V>
{

        private KeyValueIterator<Bytes, byte[]> iter;
        private Sensor sensor;
        private long startNs;

        private MeteredKeyValueIterator(KeyValueIterator<Bytes, byte[]> iter,
                                        Sensor sensor)
{
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public bool hasNext()
{
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, V> next()
{
            KeyValue<Bytes, byte[]> keyValue = iter.next();
            return KeyValue.pair(
                serdes.keyFrom(keyValue.key.get()),
                outerValue(keyValue.value));
        }

        @Override
        public void remove()
{
            iter.remove();
        }

        @Override
        public void close()
{
            try
{
                iter.close();
            } finally
{
                metrics.recordLatency(sensor, startNs, time.nanoseconds());
            }
        }

        @Override
        public K peekNextKey()
{
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }
}
