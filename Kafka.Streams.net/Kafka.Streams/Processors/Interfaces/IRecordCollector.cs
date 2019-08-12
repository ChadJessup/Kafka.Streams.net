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
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Interfaces
{
    public interface IRecordCollector<K, V> : AutoCloseable
    {
        void send(
            string topic,
            K key,
            V value,
            Headers headers,
            int partition,
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer);

        void send(
            string topic,
            K key,
            V value,
            Headers headers,
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IStreamPartitioner<K, V> partitioner);

        /**
         * Initialize the collector with a producer.
         * @param producer the producer that should be used by this collector
         */
        void init(IProducer<byte[], byte[]> producer);

        /**
         * Flush the internal {@link Producer}.
         */
        void flush();

        /**
         * Close the internal {@link Producer}.
         */
        void close();

        /**
         * The last acked offsets from the internal {@link Producer}.
         *
         * @return the map from TopicPartition to offset
         */
        Dictionary<TopicPartition, long> offsets();
    }
}
