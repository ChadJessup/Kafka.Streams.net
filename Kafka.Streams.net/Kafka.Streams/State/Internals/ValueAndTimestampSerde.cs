/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.state.internals;

using Kafka.Common.serialization.Deserializer;
using Kafka.Common.serialization.Serde;
using Kafka.Common.serialization.Serializer;
using Kafka.Streams.State.ValueAndTimestamp;

import java.util.Map;
import java.util.Objects;

public class ValueAndTimestampSerde<V> : ISerde<ValueAndTimestamp<V>>
{
    private ValueAndTimestampSerializer<V> valueAndTimestampSerializer;
    private ValueAndTimestampDeserializer<V> valueAndTimestampDeserializer;

    public ValueAndTimestampSerde(ISerde<V> valueSerde)
{
        Objects.requireNonNull(valueSerde);
        valueAndTimestampSerializer = new ValueAndTimestampSerializer<>(valueSerde.serializer());
        valueAndTimestampDeserializer = new ValueAndTimestampDeserializer<>(valueSerde.deserializer());
    }

    public override void configure(Dictionary<string, ?> configs,
                          bool isKey)
{
        valueAndTimestampSerializer.configure(configs, isKey);
        valueAndTimestampDeserializer.configure(configs, isKey);
    }

    public override void close()
{
        valueAndTimestampSerializer.close();
        valueAndTimestampDeserializer.close();
    }

    public override Serializer<ValueAndTimestamp<V>> serializer()
{
        return valueAndTimestampSerializer;
    }

    public override Deserializer<ValueAndTimestamp<V>> deserializer()
{
        return valueAndTimestampDeserializer;
    }
}