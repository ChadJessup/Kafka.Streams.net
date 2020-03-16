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
using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class ValueAndTimestampSerde<V> : Serde<ValueAndTimestamp<V>>
    {
        private readonly ValueAndTimestampSerializer<V> valueAndTimestampSerializer;
        private readonly ValueAndTimestampDeserializer<V> valueAndTimestampDeserializer;

        public ValueAndTimestampSerde(ISerde<V> valueSerde)
            : base(
                 serializer: new ValueAndTimestampSerializer<V>(valueSerde.Serializer),
                 deserializer: new ValueAndTimestampDeserializer<V>(valueSerde.Deserializer))
        {
            valueSerde = valueSerde ?? throw new ArgumentNullException(nameof(valueSerde));
        }

        public override void Configure(
            IDictionary<string, string> configs,
            bool isKey)
        {
            //this.Serializer.Configure(configs, isKey);
            valueAndTimestampSerializer.Configure(configs, isKey);
            valueAndTimestampDeserializer.Configure(configs, isKey);
        }

        public override void Close()
        {
            valueAndTimestampSerializer.Close();
            valueAndTimestampDeserializer.Close();
        }
    }
}