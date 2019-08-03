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
package org.apache.kafka.streams.state.internals;

using Kafka.Common.serialization.Deserializer;
using Kafka.Common.serialization.LongDeserializer;
using Kafka.Streams.State.ValueAndTimestamp;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

class ValueAndTimestampDeserializer<V> : Deserializer<ValueAndTimestamp<V>>
{
    private static LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

    public Deserializer<V> valueDeserializer;
    private Deserializer<Long> timestampDeserializer;

    ValueAndTimestampDeserializer(Deserializer<V> valueDeserializer)
{
        Objects.requireNonNull(valueDeserializer);
        this.valueDeserializer = valueDeserializer;
        timestampDeserializer = new LongDeserializer();
    }

    public override void configure(Dictionary<string, ?> configs,
                          bool isKey)
{
        valueDeserializer.configure(configs, isKey);
        timestampDeserializer.configure(configs, isKey);
    }

    public override ValueAndTimestamp<V> deserialize(string topic,
                                            byte[] valueAndTimestamp)
{
        if (valueAndTimestamp == null)
{
            return null;
        }

        long timestamp = timestampDeserializer.deserialize(topic, rawTimestamp(valueAndTimestamp));
        V value = valueDeserializer.deserialize(topic, rawValue(valueAndTimestamp));
        return ValueAndTimestamp.make(value, timestamp);
    }

    public override void close()
{
        valueDeserializer.close();
        timestampDeserializer.close();
    }

    static byte[] rawValue(byte[] rawValueAndTimestamp)
{
        int rawValueLength = rawValueAndTimestamp.length - 8;

        return ByteBuffer
            .allocate(rawValueLength)
            .put(rawValueAndTimestamp, 8, rawValueLength)
            .array();
    }

    private static byte[] rawTimestamp(byte[] rawValueAndTimestamp)
{
        return ByteBuffer
            .allocate(8)
            .put(rawValueAndTimestamp, 0, 8)
            .array();
    }

    static long timestamp(byte[] rawValueAndTimestamp)
{
        return LONG_DESERIALIZER.deserialize(null, rawTimestamp(rawValueAndTimestamp));
    }

}