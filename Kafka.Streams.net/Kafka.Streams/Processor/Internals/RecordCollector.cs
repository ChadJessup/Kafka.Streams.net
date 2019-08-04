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
namespace Kafka.streams.processor.internals;

import org.apache.kafka.clients.producer.Producer;
using Kafka.Common.TopicPartition;
using Kafka.Common.header.Headers;
using Kafka.Common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Map;

public interface RecordCollector : AutoCloseable {

    <K, V> void send(string topic,
                     K key,
                     V value,
                     Headers headers,
                     Integer partition,
                     Long timestamp,
                     Serializer<K> keySerializer,
                     Serializer<V> valueSerializer);

    <K, V> void send(string topic,
                     K key,
                     V value,
                     Headers headers,
                     Long timestamp,
                     Serializer<K> keySerializer,
                     Serializer<V> valueSerializer,
                     StreamPartitioner<? super K, ? super V> partitioner);

    /**
     * Initialize the collector with a producer.
     * @param producer the producer that should be used by this collector
     */
    void init(Producer<byte[], byte[]> producer];

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
    Dictionary<TopicPartition, Long> offsets();

    /**
     * A supplier of a {@link RecordCollectorImpl} instance.
     */
    interface Supplier {
        /**
         * Get the record collector.
         * @return the record collector
         */
        RecordCollector recordCollector();
    }
}
