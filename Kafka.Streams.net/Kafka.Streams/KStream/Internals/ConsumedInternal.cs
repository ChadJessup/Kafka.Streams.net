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
using Kafka.Streams.Processor;

namespace Kafka.Streams.KStream.Internals
{








    public class ConsumedInternal<K, V> : Consumed<K, V>
    {

        public ConsumedInternal(Consumed<K, V> consumed)
            : base(consumed)
        {
        }

        public ConsumedInternal(
            ISerde<K> keySerde,
            ISerde<V> valSerde,
            TimestampExtractor timestampExtractor,
            Topology.AutoOffsetReset offsetReset)
        {
            this(Consumed.with(keySerde, valSerde, timestampExtractor, offsetReset));
        }

        public ConsumedInternal()
            : this(with(null, null))
        {
        }

        public ISerde<K> keySerde()
        {
            return keySerde;
        }

        public IDeserializer<K> keyDeserializer()
        {
            return keySerde == null ? null : keySerde.Deserializer();
        }

        public ISerde<V> valueSerde()
        {
            return valueSerde;
        }

        public IDeserializer<V> valueDeserializer()
        {
            return valueSerde == null ? null : valueSerde.Deserializer();
        }

        public TimestampExtractor timestampExtractor()
        {
            return timestampExtractor;
        }

        public Topology.AutoOffsetReset offsetResetPolicy()
        {
            return resetPolicy;
        }

        public string name()
        {
            return processorName;
        }
    }
}
