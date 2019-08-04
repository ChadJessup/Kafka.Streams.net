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

using Kafka.Streams.KeyValue;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class KeyValueIteratorFacade<K, V> : KeyValueIterator<K, V>
{
    private KeyValueIterator<K, ValueAndTimestamp<V>> innerIterator;

    public KeyValueIteratorFacade(KeyValueIterator<K, ValueAndTimestamp<V>> iterator)
{
        innerIterator = iterator;
    }

    public override bool hasNext()
{
        return innerIterator.hasNext();
    }

    public override K peekNextKey()
{
        return innerIterator.peekNextKey();
    }

    public override KeyValue<K, V> next()
{
        KeyValue<K, ValueAndTimestamp<V>> innerKeyValue = innerIterator.next();
        return KeyValue.pair(innerKeyValue.key, getValueOrNull(innerKeyValue.value));
    }

    public override void close()
{
        innerIterator.close();
    }
}