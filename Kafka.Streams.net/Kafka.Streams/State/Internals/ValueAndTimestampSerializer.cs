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
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{

    public class ValueAndTimestampSerializer<V> : ISerializer<ValueAndTimestamp<V>>
    {
        public ISerializer<V> valueSerializer;
        private ISerializer<long> timestampSerializer;

        ValueAndTimestampSerializer(ISerializer<V> valueSerializer)
        {
            Objects.requireNonNull(valueSerializer);
            this.valueSerializer = valueSerializer;
            timestampSerializer = new LongSerializer();
        }

        public override void configure(Dictionary<string, ?> configs,
                              bool isKey)
        {
            valueSerializer.configure(configs, isKey);
            timestampSerializer.configure(configs, isKey);
        }

        public override byte[] serialize(string topic,
                                ValueAndTimestamp<V> data)
        {
            if (data == null)
            {
                return null;
            }
            return serialize(topic, data.value(), data.timestamp());
        }

        public byte[] serialize(string topic,
                                V data,
                                long timestamp)
        {
            if (data == null)
            {
                return null;
            }
            byte[] rawValue = valueSerializer.serialize(topic, data);
            byte[] rawTimestamp = timestampSerializer.serialize(topic, timestamp);
            return ByteBuffer
                .allocate(rawTimestamp.Length + rawValue.Length)
                .Add(rawTimestamp)
                .Add(rawValue)
                .array();
        }

        public override void close()
        {
            valueSerializer.close();
            timestampSerializer.close();
        }
    }
}