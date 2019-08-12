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
using Confluent.Kafka;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;

namespace Kafka.Streams.State.Internals
{
    /**
     * Note that the use of array-typed keys is discouraged because they result in incorrect caching behavior.
     * If you intend to work on byte arrays as key, for example, you may want to wrap them with the {@code Bytes},
     * i.e. use {@code RocksDbStore<Bytes, ...>} rather than {@code RocksDbStore<byte[], ...>}.
     *
     * @param <K>
     * @param <V>
     */
    public class StoreChangeLogger<K, V>
    {
        private string topic;
        private int partition;
        private IProcessorContext<K, V> context;
        private IRecordCollector collector;
        private ISerializer<K> keySerializer;
        private ISerializer<V> valueSerializer;

        StoreChangeLogger(string storeName,
                          IProcessorContext<K, V> context,
                          StateSerdes<K, V> serialization)
            : this(storeName, context, context.taskId().partition, serialization)
        {
        }

        private StoreChangeLogger(string storeName,
                                  IProcessorContext<K, V> context,
                                  int partition,
                                  StateSerdes<K, V> serialization)
        {
            topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName);
            this.context = context;
            this.partition = partition;
            this.collector = ((IRecordCollector.ISupplier)context).recordCollector();
            keySerializer = serialization.keySerializer();
            valueSerializer = serialization.valueSerializer();
        }

        void logChange(K key,
                       V value)
        {
            logChange(key, value, context.timestamp());
        }

        void logChange(K key,
                       V value,
                       long timestamp)
        {
            // Sending null headers to changelog topics (KIP-244)
            collector.send(topic, key, value, null, partition, timestamp, keySerializer, valueSerializer);
        }
    }
}