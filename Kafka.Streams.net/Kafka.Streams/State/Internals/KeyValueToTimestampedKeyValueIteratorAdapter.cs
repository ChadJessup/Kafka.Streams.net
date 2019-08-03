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

using Kafka.Streams.KeyValue;
using Kafka.Streams.State.KeyValueIterator;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;

/**
 * This class is used to ensure backward compatibility at DSL level between
 * {@link org.apache.kafka.streams.state.TimestampedKeyValueStore} and
 * {@link org.apache.kafka.streams.state.KeyValueStore}.
 *
 * @see KeyValueToTimestampedKeyValueByteStoreAdapter
 */
class KeyValueToTimestampedKeyValueIteratorAdapter<K> : KeyValueIterator<K, byte[]>
{
    private KeyValueIterator<K, byte[]> innerIterator;

    KeyValueToTimestampedKeyValueIteratorAdapter(KeyValueIterator<K, byte[]> innerIterator)
{
        this.innerIterator = innerIterator;
    }

    public override void close()
{
        innerIterator.close();
    }

    public override K peekNextKey()
{
        return innerIterator.peekNextKey();
    }

    public override bool hasNext()
{
        return innerIterator.hasNext();
    }

    public override KeyValue<K, byte[]> next()
{
        KeyValue<K, byte[]> plainKeyValue = innerIterator.next();
        return KeyValue.pair(plainKeyValue.key, convertToTimestampedFormat(plainKeyValue.value));
    }
}
