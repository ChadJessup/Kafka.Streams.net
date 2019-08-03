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

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.KeyValueBytesStoreSupplier;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.KeyValueStore;

import java.util.List;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;
import static org.apache.kafka.streams.state.internals.ValueAndTimestampDeserializer.rawValue;

/**
 * This class is used to ensure backward compatibility at DSL level between
 * {@link org.apache.kafka.streams.state.TimestampedKeyValueStore} and {@link KeyValueStore}.
 * <p>
 * If a user provides a supplier for plain {@code KeyValueStores} via
 * {@link org.apache.kafka.streams.kstream.Materialized#as(KeyValueBytesStoreSupplier)} this adapter is used to
 * translate between old a new {@code byte[]} format of the value.
 *
 * @see KeyValueToTimestampedKeyValueIteratorAdapter
 */
public class KeyValueToTimestampedKeyValueByteStoreAdapter : KeyValueStore<Bytes, byte[]>
{
    KeyValueStore<Bytes, byte[]> store;

    KeyValueToTimestampedKeyValueByteStoreAdapter(KeyValueStore<Bytes, byte[]> store)
{
        if (!store.persistent())
{
            throw new IllegalArgumentException("Provided store must be a persistent store, but it is not.");
        }
        this.store = store;
    }

    public override void put(Bytes key,
                    byte[] valueWithTimestamp)
{
        store.put(key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp));
    }

    public override byte[] putIfAbsent(Bytes key,
                              byte[] valueWithTimestamp)
{
        return convertToTimestampedFormat(store.putIfAbsent(
            key,
            valueWithTimestamp == null ? null : rawValue(valueWithTimestamp)));
    }

    public override void putAll(List<KeyValue<Bytes, byte[]>> entries)
{
        for (KeyValue<Bytes, byte[]> entry : entries)
{
            byte[] valueWithTimestamp = entry.value;
            store.put(entry.key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp));
        }
    }

    public override byte[] delete(Bytes key)
{
        return convertToTimestampedFormat(store.delete(key));
    }

    public override string name()
{
        return store.name();
    }

    public override void init(IProcessorContext context,
                     IStateStore root)
{
        store.init(context, root);
    }

    public override void flush()
{
        store.flush();
    }

    public override void close()
{
        store.close();
    }

    public override bool persistent()
{
        return true;
    }

    public override bool isOpen()
{
        return store.isOpen();
    }

    public override byte[] get(Bytes key)
{
        return convertToTimestampedFormat(store.get(key));
    }

    public override KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                 Bytes to)
{
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.range(from, to));
    }

    public override KeyValueIterator<Bytes, byte[]> all()
{
        return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.all());
    }

    public override long approximateNumEntries()
{
        return store.approximateNumEntries();
    }

}