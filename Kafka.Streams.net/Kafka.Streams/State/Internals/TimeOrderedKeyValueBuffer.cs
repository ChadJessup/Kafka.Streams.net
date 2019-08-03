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

using Kafka.Common.serialization.Serde;
using Kafka.Streams.kstream.internals.Change;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.internals.ProcessorRecordContext;
using Kafka.Streams.State.ValueAndTimestamp;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface TimeOrderedKeyValueBuffer<K, V> : IStateStore
{

    class Eviction<K, V>
{
        private K key;
        private Change<V> value;
        private ProcessorRecordContext recordContext;

        Eviction(K key, Change<V> value, ProcessorRecordContext recordContext)
{
            this.key = key;
            this.value = value;
            this.recordContext = recordContext;
        }

        public K key()
{
            return key;
        }

        public Change<V> value()
{
            return value;
        }

        public ProcessorRecordContext recordContext()
{
            return recordContext;
        }

        @Override
        public string toString()
{
            return "Eviction{key=" + key + ", value=" + value + ", recordContext=" + recordContext + '}';
        }

        @Override
        public bool equals(object o)
{
            if (this == o) return true;
            if (o == null || GetType() != o.GetType()) return false;
            Eviction<?, ?> eviction = (Eviction<?, ?>) o;
            return Objects.Equals(key, eviction.key) &&
                Objects.Equals(value, eviction.value) &&
                Objects.Equals(recordContext, eviction.recordContext);
        }

        @Override
        public int GetHashCode()()
{
            return Objects.hash(key, value, recordContext);
        }
    }

    void setSerdesIfNull(ISerde<K> keySerde, ISerde<V> valueSerde);

    void evictWhile(Supplier<Boolean> predicate, Consumer<Eviction<K, V>> callback);

    Maybe<ValueAndTimestamp<V>> priorValueForBuffered(K key);

    void put(long time, K key, Change<V> value, ProcessorRecordContext recordContext);

    int numRecords();

    long bufferSize();

    long minTimestamp();
}
