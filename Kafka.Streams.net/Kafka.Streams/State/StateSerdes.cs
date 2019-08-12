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
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.State
{
    /**
     * Factory for creating serializers / deserializers for state stores in Kafka Streams.
     *
     * @param key type of serde
     * @param value type of serde
     */
    public class StateSerdes<K, V>
    {
        /**
         * Create a new instance of {@link StateSerdes} for the given state name and key-/value-typees.
         *
         * @param topic      the topic name
         * @param keyClass   the of the key type
         * @param valueClass the of the value type
         * @param        the key type
         * @param        the value type
         * @return a new instance of {@link StateSerdes}
         */
        public static StateSerdes<K, V> withBuiltinTypes(
            string topic,
            Type keyClass,
            Type valueClass)
        {
            return new StateSerdes<>(topic, Serdes.serdeFrom(keyClass), Serdes.serdeFrom(valueClass));
        }

        private string topic;
        private ISerde<K> keySerde;
        private ISerde<V> valueSerde;

        /**
         * Create a context for serialization using the specified serializers and deserializers which
         * <em>must</em> match the key and value types used as parameters for this object; the state changelog topic
         * is provided to bind this serde factory to, so that future calls for serialize / deserialize do not
         * need to provide the topic name any more.
         *
         * @param topic         the topic name
         * @param keySerde      the serde for keys; cannot be null
         * @param valueSerde    the serde for values; cannot be null
         * @throws ArgumentException if key or value serde is null
         */
        public StateSerdes(
            string topic,
            ISerde<K> keySerde,
            ISerde<V> valueSerde)
        {
            topic = topic ?? throw new System.ArgumentNullException("topic cannot be null", nameof(topic));
            keySerde = keySerde ?? throw new System.ArgumentNullException("key serde cannot be null", nameof(keySerde));
            valueSerde = valueSerde ?? throw new System.ArgumentNullException("value serde cannot be null", nameof(valueSerde));

            this.topic = topic;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        /**
         * Return the key deserializer.
         *
         * @return the key deserializer
         */
        public IDeserializer<K> keyDeserializer()
        {
            return keySerde.Deserializer;
        }

        /**
         * Return the key serializer.
         *
         * @return the key serializer
         */
        public ISerializer<K> keySerializer()
        {
            return keySerde.Serializer;
        }

        /**
         * Return the value deserializer.
         *
         * @return the value deserializer
         */
        public IDeserializer<V> valueDeserializer()
        {
            return valueSerde.Deserializer;
        }

        /**
         * Return the value serializer.
         *
         * @return the value serializer
         */
        public ISerializer<V> valueSerializer()
        {
            return valueSerde.Serializer;
        }

        /**
         * Return the topic.
         *
         * @return the topic
         */
        public string Topic { get; private set; }

        /**
         * Deserialize the key from raw bytes.
         *
         * @param rawKey  the key as raw bytes
         * @return        the key as typed object
         */
        public K keyFrom(byte[] rawKey)
        {
            return keySerde.Deserializer.Deserialize(topic, rawKey);
        }

        /**
         * Deserialize the value from raw bytes.
         *
         * @param rawValue  the value as raw bytes
         * @return          the value as typed object
         */
        public V valueFrom(byte[] rawValue)
        {
            return valueSerde.Deserializer.Deserialize(topic, rawValue);
        }

        /**
         * Serialize the given key.
         *
         * @param key  the key to be serialized
         * @return     the serialized key
         */
        public byte[] rawKey(K key)
        {
            try
            {
                return keySerde.Serializer.Serialize(topic, key);
            }
            catch (InvalidCastException e)
            {
                string keyClass = key == null ? "unknown because key is null" : key.GetType().FullName;
                throw new StreamsException(
                        string.Format("A serializer (%s) is not compatible to the actual key type " +
                                        "(key type: %s). Change the default Serdes in StreamConfig or " +
                                        "provide correct Serdes via method parameters.",
                                keySerializer().GetType().FullName,
                                keyClass),
                        e);
            }
        }

        /**
         * Serialize the given value.
         *
         * @param value  the value to be serialized
         * @return       the serialized value
         */
        public byte[] rawValue(V value)
        {
            try
            {
                return valueSerde.Serializer.Serialize(topic, value);
            }
            catch (InvalidCastException e)
            {
                string valueClass;
                Type serializerClass;
                if (valueSerializer() is ValueAndTimestampSerializer)
                {
                    serializerClass = ((ValueAndTimestampSerializer)valueSerializer()).valueSerializer.GetType();
                    valueClass = value == null ? "unknown because value is null" : ((ValueAndTimestamp)value).value().GetType().getName();
                }
                else
                {
                    serializerClass = valueSerializer().GetType();
                    valueClass = value == null ? "unknown because value is null" : value.GetType().FullName;
                }
                throw new StreamsException(
                        string.Format("A serializer (%s) is not compatible to the actual value type " +
                                        "(value type: %s). Change the default Serdes in StreamConfig or " +
                                        "provide correct Serdes via method parameters.",
                                serializerClass.FullName,
                                valueClass),
                        e);
            }
        }
    }
}