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
namespace Kafka.Streams.State;

using Kafka.Streams.KeyValue;



/**
 * Combines a value from a {@link KeyValue} with a timestamp.
 *
 * @param <V>
 */
public class ValueAndTimestamp<V>
{
    private V value;
    private long timestamp;

    private ValueAndTimestamp(V value,
                              long timestamp)
{
        Objects.requireNonNull(value);
        this.value = value;
        this.timestamp = timestamp;
    }

    /**
     * Create a new {@link ValueAndTimestamp} instance if the provide {@code value} is not {@code null}.
     *
     * @param value      the value
     * @param timestamp  the timestamp
     * @param the type of the value
     * @return a new {@link ValueAndTimestamp} instance if the provide {@code value} is not {@code null};
     *         otherwise {@code null} is returned
     */
    public static ValueAndTimestamp<V> make(V value,
                                                long timestamp)
{
        return value == null ? null : new ValueAndTimestamp<>(value, timestamp);
    }

    /**
     * Return the wrapped {@code value} of the given {@code valueAndTimestamp} parameter
     * if the parameter is not {@code null}.
     *
     * @param valueAndTimestamp a {@link ValueAndTimestamp} instance; can be {@code null}
     * @param the type of the value
     * @return the wrapped {@code value} of {@code valueAndTimestamp} if not {@code null}; otherwise {@code null}
     */
    public static V getValueOrNull(ValueAndTimestamp<V> valueAndTimestamp)
{
        return valueAndTimestamp == null ? null : valueAndTimestamp.value();
    }

    public V value()
{
        return value;
    }

    public long timestamp()
{
        return timestamp;
    }

    public override string ToString()
{
        return "<" + value + "," + timestamp + ">";
    }

    public override bool Equals(object o)
{
        if (this == o)
{
            return true;
        }
        if (o == null || GetType() != o.GetType())
{
            return false;
        }
        ValueAndTimestamp<?> that = (ValueAndTimestamp<?>) o;
        return timestamp == that.timestamp &&
            Objects.Equals(value, that.value);
    }

    public override int GetHashCode()
{
        return Objects.hash(value, timestamp);
    }
}
