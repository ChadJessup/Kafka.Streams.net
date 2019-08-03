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
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.KeyValueBytesStoreSupplier;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.KeyValueStore;
using Kafka.Streams.State.TimestampedBytesStore;
using Kafka.Streams.State.TimestampedKeyValueStore;
using Kafka.Streams.State.ValueAndTimestamp;

import java.util.List;
import java.util.Objects;

public class TimestampedKeyValueStoreBuilder<K, V>
    : AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>
{

    private KeyValueBytesStoreSupplier storeSupplier;

    public TimestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier storeSupplier,
                                           ISerde<K> keySerde,
                                           ISerde<V> valueSerde,
                                           Time time)
{
        super(
            storeSupplier.name(),
            keySerde,
            valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde),
            time);
        Objects.requireNonNull(storeSupplier, "bytesStoreSupplier can't be null");
        this.storeSupplier = storeSupplier;
    }

    public override TimestampedKeyValueStore<K, V> build()
{
        KeyValueStore<Bytes, byte[]> store = storeSupplier.get();
        if (!(store is TimestampedBytesStore))
{
            if (store.persistent())
{
                store = new KeyValueToTimestampedKeyValueByteStoreAdapter(store);
            } else
{
                store = new InMemoryTimestampedKeyValueStoreMarker(store);
            }
        }
        return new MeteredTimestampedKeyValueStore<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapCaching(KeyValueStore<Bytes, byte[]> inner)
{
        if (!enableCaching)
{
            return inner;
        }
        return new CachingKeyValueStore(inner);
    }

    private KeyValueStore<Bytes, byte[]> maybeWrapLogging(KeyValueStore<Bytes, byte[]> inner)
{
        if (!enableLogging)
{
            return inner;
        }
        return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
    }

    private static class InMemoryTimestampedKeyValueStoreMarker
        : KeyValueStore<Bytes, byte[]>, TimestampedBytesStore
{

        KeyValueStore<Bytes, byte[]> wrapped;

        private InMemoryTimestampedKeyValueStoreMarker(KeyValueStore<Bytes, byte[]> wrapped)
{
            if (wrapped.persistent())
{
                throw new IllegalArgumentException("Provided store must not be a persistent store, but it is.");
            }
            this.wrapped = wrapped;
        }

        @Override
        public void init(IProcessorContext context,
                         IStateStore root)
{
            wrapped.init(context, root);
        }

        @Override
        public void put(Bytes key,
                        byte[] value)
{
            wrapped.put(key, value);
        }

        @Override
        public byte[] putIfAbsent(Bytes key,
                                  byte[] value)
{
            return wrapped.putIfAbsent(key, value);
        }

        @Override
        public void putAll(List<KeyValue<Bytes, byte[]>> entries)
{
            wrapped.putAll(entries);
        }

        @Override
        public byte[] delete(Bytes key)
{
            return wrapped.delete(key);
        }

        @Override
        public byte[] get(Bytes key)
{
            return wrapped.get(key);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                     Bytes to)
{
            return wrapped.range(from, to);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all()
{
            return wrapped.all();
        }

        @Override
        public long approximateNumEntries()
{
            return wrapped.approximateNumEntries();
        }

        @Override
        public void flush()
{
            wrapped.flush();
        }

        @Override
        public void close()
{
            wrapped.close();
        }

        @Override
        public bool isOpen()
{
            return wrapped.isOpen();
        }

        @Override
        public string name()
{
            return wrapped.name();
        }

        @Override
        public bool persistent()
{
            return false;
        }
    }
}