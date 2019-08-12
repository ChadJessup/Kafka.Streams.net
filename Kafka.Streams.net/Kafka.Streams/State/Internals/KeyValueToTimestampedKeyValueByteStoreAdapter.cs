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

using Kafka.Common.Utils;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * This is used to ensure backward compatibility at DSL level between
     * {@link org.apache.kafka.streams.state.TimestampedKeyValueStore} and {@link KeyValueStore}.
     * <p>
     * If a user provides a supplier for plain {@code KeyValueStores} via
     * {@link org.apache.kafka.streams.kstream.Materialized#As(KeyValueBytesStoreSupplier)} this adapter is used to
     * translate between old a new {@code byte[]} string.Format of the value.
     *
     * @see KeyValueToTimestampedKeyValueIteratorAdapter
     */
    public class KeyValueToTimestampedKeyValueByteStoreAdapter : IKeyValueStore<Bytes, byte[]>
    {
        IKeyValueStore<Bytes, byte[]> store;

        KeyValueToTimestampedKeyValueByteStoreAdapter(IKeyValueStore<Bytes, byte[]> store)
        {
            if (!store.persistent())
            {
                throw new System.ArgumentException("Provided store must be a persistent store, but it is not.");
            }
            this.store = store;
        }

        public void put(Bytes key,
                        byte[] valueWithTimestamp)
        {
            store.Add(key, valueWithTimestamp == null
                ? null
                : rawValue(valueWithTimestamp));
        }

        public byte[] putIfAbsent(Bytes key,
                                  byte[] valueWithTimestamp)
        {
            return convertToTimestampedFormat(store.putIfAbsent(
                key,
                valueWithTimestamp == null
                ? null
                : rawValue(valueWithTimestamp)));
        }

        public void putAll(List<KeyValue<Bytes, byte[]>> entries)
        {
            foreach (KeyValue<Bytes, byte[]> entry in entries)
            {
                byte[] valueWithTimestamp = entry.value;
                store.Add(entry.key, valueWithTimestamp == null ? null : rawValue(valueWithTimestamp));
            }
        }

        public byte[] delete(Bytes key)
        {
            return convertToTimestampedFormat(store.delete(key));
        }

        public void init(IProcessorContext<Bytes, byte[]> context,
                         IStateStore root)
        {
            store.init(context, root);
        }

        public void flush()
        {
            store.flush();
        }

        public void close()
        {
            store.close();
        }

        public bool persistent()
        {
            return true;
        }

        public bool isOpen()
        {
            return store.isOpen();
        }

        public byte[] get(Bytes key)
        {
            return convertToTimestampedFormat(store[key]);
        }

        public IKeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                     Bytes to)
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<>(store.range(from, to));
        }

        public IKeyValueIterator<Bytes, byte[]> all()
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<Bytes, byte[]>(store.all());
        }

        public override long approximateNumEntries()
        {
            return store.approximateNumEntries();
        }

    }
}