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
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;

namespace Kafka.Streams.KStream
{
    /**
     * The that is used to capture the key and value {@link Serde}s used when performing
     * {@link KStream#groupBy(KeyValueMapper, Serialized)} and {@link KStream#groupByKey(Serialized)} operations.
     *
     * @param the key type
     * @param the value type
     *
     *  @deprecated since 2.1. Use {@link  org.apache.kafka.streams.kstream.Grouped} instead
     */
    [System.Obsolete]
    public class Serialized<K, V> : ISerialized<K, V>
    {
        public ISerde<K> keySerde { get; }
        public ISerde<V> valueSerde { get; }

        private Serialized(ISerde<K> keySerde,
                            ISerde<V> valueSerde)
        {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        protected Serialized(ISerialized<K, V> serialized)
            : this(serialized.keySerde, serialized.valueSerde)
        {
        }

        /**
         * Construct a {@code Serialized} instance with the provided key and value {@link Serde}s.
         * If the {@link Serde} params are {@code null} the default serdes defined in the configs will be used.
         *
         * @param keySerde   keySerde that will be used to materialize a stream
         *                   if not specified the default serdes defined in the configs will be used
         * @param valueSerde valueSerde that will be used to materialize a stream
         *                   if not specified the default serdes defined in the configs will be used
         * @param        the key type
         * @param        the value type
         * @return a new instance of {@link Serialized} configured with the provided serdes
         */
        public static Serialized<K, V> with(ISerde<K> keySerde,
                                                    ISerde<V> valueSerde)
        {
            return new Serialized<K, V>(keySerde, valueSerde);
        }

        /**
         * Construct a {@code Serialized} instance with the provided key {@link Serde}.
         * If the {@link Serde} params are null the default serdes defined in the configs will be used.
         *
         * @param keySerde keySerde that will be used to materialize a stream
         *                 if not specified the default serdes defined in the configs will be used
         * @return a new instance of {@link Serialized} configured with the provided key serde
         */
        public Serialized<K, V> withKeySerde(ISerde<K> keySerde)
        {
            return new Serialized<K, V>(keySerde, null);
        }

        /**
         * Construct a {@code Serialized} instance with the provided value {@link Serde}.
         * If the {@link Serde} params are null the default serdes defined in the configs will be used.
         *
         * @param valueSerde valueSerde that will be used to materialize a stream
         *                   if not specified the default serdes defined in the configs will be used
         * @return a new instance of {@link Serialized} configured with the provided key serde
         */
        public Serialized<K, V> withValueSerde(ISerde<V> valueSerde)
        {
            return new Serialized<K, V>(null, valueSerde);
        }
    }
}