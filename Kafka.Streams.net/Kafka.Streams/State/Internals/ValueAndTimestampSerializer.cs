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

    public class ValueAndTimestampSerializer<V> : ISerializer<ValueAndTimestamp<V>>
    {
        public ISerializer<V> valueSerializer;
        private readonly ISerializer<long> timestampSerializer;

        public ValueAndTimestampSerializer(ISerializer<V> valueSerializer)
        {
            valueSerializer = valueSerializer ?? throw new ArgumentNullException(nameof(valueSerializer));

            this.valueSerializer = valueSerializer;
            timestampSerializer = Serdes.Long().Serializer;
        }

        public void Configure(
            Dictionary<string, string> configs,
            bool isKey)
        {
            //valueSerializer.Configure(configs, isKey);
            //timestampSerializer.Configure(configs, isKey);
        }

        public byte[] Serialize(
            ValueAndTimestamp<V> data,
            SerializationContext context)
        {
            if (data == null)
            {
                return null;
            }

            return Serialize(context.Topic, data.value, data.timestamp);
        }

        public byte[] Serialize(
            string topic,
            V data,
            long timestamp)
        {
            if (data == null)
            {
                return null;
            }

            byte[] rawValue = valueSerializer.Serialize(data, new SerializationContext(MessageComponentType.Value, topic));
            byte[] rawTimestamp = timestampSerializer.Serialize(timestamp, new SerializationContext(MessageComponentType.Value, topic));

            return new ByteBuffer()
                .allocate(rawTimestamp.Length + rawValue.Length)
                .Add(rawTimestamp)
                .Add(rawValue)
                .array();
        }

        public void Close()
        {
            // valueSerializer.close();
            // timestampSerializer.close();
        }
    }
}