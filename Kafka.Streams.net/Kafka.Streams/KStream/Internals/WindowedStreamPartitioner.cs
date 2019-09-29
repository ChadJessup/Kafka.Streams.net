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
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Interfaces;

namespace Kafka.Streams.KStream.Internals
{
    public class WindowedStreamPartitioner<K, V> : IStreamPartitioner<Windowed<K>, V>
    {
        private readonly IWindowedSerializer<K> serializer;

        public WindowedStreamPartitioner(IWindowedSerializer<K> serializer)
        {
            this.serializer = serializer;
        }

        /**
         * WindowedStreamPartitioner determines the partition number for a record with the given windowed key and value
         * and the current number of partitions. The partition number id determined by the original key of the windowed key
         * using the same logic as DefaultPartitioner so that the topic is partitioned by the original key.
         *
         * @param topic the topic name this record is sent to
         * @param windowedKey the key of the record
         * @param value the value of the record
         * @param numPartitions the total number of partitions
         * @return an integer between 0 and {@code numPartitions-1}, or {@code null} if the default partitioning logic should be used
         */

        public int partition(string topic, Windowed<K> windowedKey, V value, int numPartitions)
        {
            byte[] keyBytes = serializer.serializeBaseKey(topic, windowedKey);

            // hash the keyBytes to choose a partition
            return 0; // toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }
}