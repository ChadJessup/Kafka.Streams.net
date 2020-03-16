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
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class ValueAndTimestampDeserializer<V> : IDeserializer<ValueAndTimestamp<V>>
    {
        private static readonly IDeserializer<long> LONG_DESERIALIZER = Serdes.Long().Deserializer;

        public IDeserializer<V> valueDeserializer;
        private readonly IDeserializer<long> timestampDeserializer;

        public ValueAndTimestampDeserializer(IDeserializer<V> valueDeserializer)
        {
            valueDeserializer = valueDeserializer ?? throw new ArgumentNullException(nameof(valueDeserializer));

            this.valueDeserializer = valueDeserializer;
            timestampDeserializer = Serdes.Long().Deserializer;
        }

        public void Configure(
            IDictionary<string, string> configs,
            bool isKey)
        {
            // valueDeserializer.Configure(configs, isKey);
            // timestampDeserializer.Configure(configs, isKey);
        }

        public ValueAndTimestamp<V> Deserialize(
            string topic,
            byte[] valueAndTimestamp)
        {
            if (valueAndTimestamp == null)
            {
                return null;
            }

            var timestamp = timestampDeserializer.Deserialize(rawTimestamp(valueAndTimestamp), false, new SerializationContext(MessageComponentType.Value, topic));
            V value = valueDeserializer.Deserialize(rawValue(valueAndTimestamp), false, new SerializationContext(MessageComponentType.Value, topic));

            return ValueAndTimestamp<V>.make(value, timestamp);
        }

        public void Close()
        {
            // valueDeserializer.close();
            // timestampDeserializer.close();
        }

        static byte[] rawValue(byte[] rawValueAndTimestamp)
        {
            var rawValueLength = rawValueAndTimestamp.Length - 8;

            return new ByteBuffer()
                .allocate(rawValueLength)
                .Add(rawValueAndTimestamp)//, 8, rawValueLength)
                .array();
        }

        private static byte[] rawTimestamp(byte[] rawValueAndTimestamp)
        {
            return new ByteBuffer()
                .allocate(8)
                .add(rawValueAndTimestamp)//.UnionWith(, 0, 8 })
                .array();
        }

        public ValueAndTimestamp<V> Deserialize(
            ReadOnlySpan<byte> data,
            bool isNull,
            SerializationContext context)
        {
            return Deserialize(context.Topic, data.ToArray());
        }
    }
}