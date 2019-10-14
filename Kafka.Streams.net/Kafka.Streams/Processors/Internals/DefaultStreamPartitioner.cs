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
using Kafka.Common;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.Processors.Internals
{
    public class DefaultStreamPartitioner<K, V> : IStreamPartitioner<K, V>
    {
        private readonly Cluster cluster;
        private readonly ISerializer<K> keySerializer;
        private readonly DefaultPartitioner defaultPartitioner;

        public DefaultStreamPartitioner(
            ISerializer<K> keySerializer,
            Cluster cluster)
        {
            this.cluster = cluster;
            this.keySerializer = keySerializer;
            this.defaultPartitioner = new DefaultPartitioner();
        }

        public int partition(string topic, K key, V value, int numPartitions)
        {
            byte[] keyBytes = keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic));
            return defaultPartitioner.partition(topic, key, keyBytes, value, null, cluster);
        }
    }
}