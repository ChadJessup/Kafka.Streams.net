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
using Kafka.Common.Metrics;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class MeteredWindowStore<K, V>
        : WrappedStateStore<IWindowStore<Bytes, byte[]>, Windowed<K>, V>, IWindowStore<K, V>
    {

        private long windowSizeMs;
        private string metricScope;
        private ITime time;
        ISerde<K> keySerde;
        ISerde<V> valueSerde;
        StateSerdes<K, V> serdes;
        private StreamsMetricsImpl metrics;
        private Sensor putTime;
        private Sensor fetchTime;
        private Sensor flushTime;
        private IProcessorContext context;
        private string taskName;

        MeteredWindowStore(IWindowStore<Bytes, byte[]> inner,
                           long windowSizeMs,
                           string metricScope,
                           ITime time,
                           ISerde<K> keySerde,
                           ISerde<V> valueSerde)
            : base(inner)
        {
            this.windowSizeMs = windowSizeMs;
            this.metricScope = metricScope;
            this.time = time;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        public override void init(IProcessorContext context,
                         IStateStore root)
        {
            this.context = context;
            initStoreSerde(context);
            metrics = (StreamsMetricsImpl)context.metrics();

            taskName = context.taskId().ToString();
            string metricsGroup = "stream-" + metricScope + "-metrics";
            Dictionary<string, string> taskTags = metrics.tagMap("task-id", taskName, metricScope + "-id", "all");
            Dictionary<string, string> storeTags = metrics.tagMap("task-id", taskName, metricScope + "-id", name());

            putTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "put", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            fetchTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "fetch", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            flushTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "flush", metrics, metricsGroup, taskName, name(), taskTags, storeTags);
            Sensor restoreTime = createTaskAndStoreLatencyAndThroughputSensors(DEBUG, "restore", metrics, metricsGroup, taskName, name(), taskTags, storeTags);

            // register and possibly restore the state from the logs
            long startNs = time.nanoseconds();
            try
            {
                base.init(context, root);
            }
            finally
            {
                metrics.recordLatency(
                    restoreTime,
                    startNs,
                    time.nanoseconds()
                );
            }
        }


        void initStoreSerde(IProcessorContext context)
        {
            serdes = new StateSerdes<>(
                ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
                keySerde == null ? (ISerde<K>)context.keySerde : keySerde,
                valueSerde == null ? (ISerde<V>)context.valueSerde : valueSerde);
        }


        public override bool setFlushListener(CacheFlushListener<Windowed<K>, V> listener,
                                        bool sendOldValues)
        {
            IWindowStore<Bytes, byte[]> wrapped = wrapped;
            if (wrapped is CachedStateStore)
            {
                return ((CachedStateStore<byte[], byte[]>)wrapped].setFlushListener(
                   (key, newValue, oldValue, timestamp)=>listener.apply(
                       WindowKeySchema.fromStoreKey(key, windowSizeMs, serdes.keyDeserializer(), serdes.Topic),
                       newValue != null ? serdes.valueFrom(newValue) : null,
                       oldValue != null ? serdes.valueFrom(oldValue) : null,
                       timestamp
                   ),
                   sendOldValues);
            }
            return false;
        }

        public override void put(K key,
                        V value)
        {
            put(key, value, context.timestamp());
        }

        public override void put(K key,
                        V value,
                        long windowStartTimestamp)
        {
            long startNs = time.nanoseconds();
            try
            {
                wrapped.Add(keyBytes(key), serdes.rawValue(value), windowStartTimestamp);
            }
            catch (ProcessorStateException e)
            {
                string message = string.Format(e.getMessage(), key, value);
                throw new ProcessorStateException(message, e);
            }
            finally
            {
                metrics.recordLatency(putTime, startNs, time.nanoseconds());
            }
        }

        public override V fetch(K key,
                       long timestamp)
        {
            long startNs = time.nanoseconds();
            try
            {
                byte[] result = wrapped.fetch(keyBytes(key), timestamp);
                if (result == null)
                {
                    return default;
                }
                return serdes.valueFrom(result);
            }
            finally
            {
                metrics.recordLatency(fetchTime, startNs, time.nanoseconds());
            }
        }


        public override WindowStoreIterator<V> fetch(K key,
                                            long timeFrom,
                                            long timeTo)
        {
            return new MeteredWindowStoreIterator<>(wrapped.fetch(keyBytes(key), timeFrom, timeTo),
                                                    fetchTime,
                                                    metrics,
                                                    serdes,
                                                    time);
        }


        public override IKeyValueIterator<Windowed<K>, V> fetch(K from,
                                                      K to,
                                                      long timeFrom,
                                                      long timeTo)
        {
            return new MeteredWindowedKeyValueIterator<>(
                wrapped.fetch(keyBytes(from), keyBytes(to), timeFrom, timeTo),
                fetchTime,
                metrics,
                serdes,
                time);
        }


        public override IKeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom,
                                                         long timeTo)
        {
            return new MeteredWindowedKeyValueIterator<>(
                wrapped.fetchAll(timeFrom, timeTo),
                fetchTime,
                metrics,
                serdes,
                time);
        }

        public override IKeyValueIterator<Windowed<K>, V> all()
        {
            return new MeteredWindowedKeyValueIterator<>(wrapped.all(), fetchTime, metrics, serdes, time);
        }

        public override void flush()
        {
            long startNs = time.nanoseconds();
            try
            {
                base.flush();
            }
            finally
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
}