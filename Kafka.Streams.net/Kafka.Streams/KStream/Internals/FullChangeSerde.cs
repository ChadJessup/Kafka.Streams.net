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
using Kafka.Streams.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class FullChangeSerde<T>
    {
        private readonly ISerde<T> inner;

        public static FullChangeSerde<T> Wrap(ISerde<T> serde)
        {
            if (serde == null)
            {
                return null;
            }
            else
            {
                return new FullChangeSerde<T>(serde);
            }
        }

        private FullChangeSerde(ISerde<T> inner)
        {
            this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public ISerde<T> InnerSerde()
        {
            return inner;
        }

        public Change<byte[]> SerializeParts(string topic, Change<T> data)
        {
            if (data == null)
            {
                return null;
            }

            ISerializer<T> innerSerializer = InnerSerde().Serializer;

            var oldBytes = data.oldValue == null ? null : innerSerializer.Serialize(data.oldValue, new SerializationContext(MessageComponentType.Key, topic));
            var newBytes = data.newValue == null ? null : innerSerializer.Serialize(data.newValue, new SerializationContext(MessageComponentType.Key, topic));

            return new Change<byte[]>(newBytes, oldBytes);
        }

        public Change<T> DeserializeParts(string topic, Change<byte[]> serialChange)
        {
            if (serialChange == null)
            {
                return null;
            }

            IDeserializer<T> innerDeserializer = InnerSerde().Deserializer;

            var oldValue = innerDeserializer.Deserialize(serialChange.oldValue, isNull: serialChange.oldValue == null, new SerializationContext(MessageComponentType.Key, topic));
            var newValue = innerDeserializer.Deserialize(serialChange.newValue, isNull: serialChange.newValue == null, new SerializationContext(MessageComponentType.Key, topic));

            return new Change<T>(newValue, oldValue);
        }

        /**
         * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still keep this logic here
         * so that we can produce the legacy string.Format to test that we can still deserialize it.
         */
        public static byte[] MergeChangeArraysIntoSingleLegacyFormattedArray(Change<byte[]> serialChange)
        {
            if (serialChange == null)
            {
                return null;
            }

            var oldSize = serialChange.oldValue == null ? -1 : serialChange.oldValue.Length;
            var newSize = serialChange.newValue == null ? -1 : serialChange.newValue.Length;

            ByteBuffer buffer = new ByteBuffer().Allocate(sizeof(int) * 2 + Math.Max(0, oldSize) + Math.Max(0, newSize));

            buffer.PutInt(oldSize);

            if (serialChange.oldValue != null)
            {
                buffer.Add(serialChange.oldValue);
            }

            buffer.PutInt(newSize);
            if (serialChange.newValue != null)
            {
                buffer.Add(serialChange.newValue);
            }
            return buffer.Array();
        }

        /**
         * We used to serialize a Change into a single byte[]. Now, we don't anymore, but we still
         * need to be able to read it (so that we can load the state store from previously-written changelog records).
         */
        public static Change<byte[]> DecomposeLegacyFormattedArrayIntoChangeArrays(byte[] data)
        {
            if (data == null)
            {
                return null;
            }

            ByteBuffer buffer = new ByteBuffer().Wrap(data);

            var oldSize = buffer.GetInt();
            var oldBytes = oldSize == -1
                ? null
                : new byte[oldSize];

            if (oldBytes != null)
            {
                buffer.Get(oldBytes);
            }

            var newSize = buffer.GetInt();
            var newBytes = newSize == -1 ? null : new byte[newSize];
            if (newBytes != null)
            {
                buffer.Get(newBytes);
            }

            return new Change<byte[]>(newBytes, oldBytes);
        }
    }
}
