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

using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.ReadOnlyKeyValueStore;
using Kafka.Streams.State.TimestampedKeyValueStore;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class ReadOnlyKeyValueStoreFacade<K, V> : IReadOnlyKeyValueStore<K, V>
{
    protected TimestampedKeyValueStore<K, V> inner;

    protected ReadOnlyKeyValueStoreFacade(TimestampedKeyValueStore<K, V> store)
{
        inner = store;
    }

    public override V get(K key)
{
        return getValueOrNull(inner[key)];
    }

    public override KeyValueIterator<K, V> range(K from,
                                        K to)
{
        return new KeyValueIteratorFacade<>(inner.range(from, to));
    }

    public override KeyValueIterator<K, V> all()
{
        return new KeyValueIteratorFacade<>(inner.all());
    }

    public override long approximateNumEntries()
{
        return inner.approximateNumEntries();
    }
}