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
namespace Kafka.Streams.State.Internals
{
    public class TimestampedKeyValueStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>>
    {

        private IKeyValueBytesStoreSupplier storeSupplier;

        public TimestampedKeyValueStoreBuilder(
            IKeyValueBytesStoreSupplier storeSupplier,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            ITime time)
            : base(
                storeSupplier.name(),
                keySerde,
                valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde),
                time)
        {
            storeSupplier = storeSupplier ?? throw new System.ArgumentNullException("bytesStoreSupplier can't be null", nameof(storeSupplier));
            this.storeSupplier = storeSupplier;
        }

        public override TimestampedKeyValueStore<K, V> build()
        {
            IKeyValueStore<Bytes, byte[]> store = storeSupplier[];
            if (!(store is ITimestampedBytesStore))
            {
                if (store.persistent())
                {
                    store = new KeyValueToTimestampedKeyValueByteStoreAdapter(store);
                }
                else
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

        private IKeyValueStore<Bytes, byte[]> maybeWrapCaching(IKeyValueStore<Bytes, byte[]> inner)
        {
            if (!enableCaching)
            {
                return inner;
            }
            return new CachingKeyValueStore(inner);
        }

        private IKeyValueStore<Bytes, byte[]> maybeWrapLogging(IKeyValueStore<Bytes, byte[]> inner)
        {
            if (!enableLogging)
            {
                return inner;
            }
            return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
        }

        private static class InMemoryTimestampedKeyValueStoreMarker
        : IKeyValueStore<Bytes, byte[]>, ITimestampedBytesStore
        {

            IKeyValueStore<Bytes, byte[]> wrapped;

            private InMemoryTimestampedKeyValueStoreMarker(IKeyValueStore<Bytes, byte[]> wrapped)
            {
                if (wrapped.persistent())
                {
                    throw new System.ArgumentException("Provided store must not be a persistent store, but it is.");
                }
                this.wrapped = wrapped;
            }


            public void init(IProcessorContext context,
                             IStateStore root)
            {
                wrapped.init(context, root);
            }


            public void put(Bytes key,
                            byte[] value)
            {
                wrapped.Add(key, value);
            }


            public byte[] putIfAbsent(Bytes key,
                                      byte[] value)
            {
                return wrapped.putIfAbsent(key, value);
            }


            public void putAll(List<KeyValue<Bytes, byte[]>> entries)
            {
                wrapped.putAll(entries);
            }


            public byte[] delete(Bytes key)
            {
                return wrapped.delete(key);
            }


            public byte[] get(Bytes key)
            {
                return wrapped[key];
            }


            public IKeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                         Bytes to)
            {
                return wrapped.range(from, to);
            }


            public IKeyValueIterator<Bytes, byte[]> all()
            {
                return wrapped.all();
            }


            public long approximateNumEntries()
            {
                return wrapped.approximateNumEntries();
            }


            public void flush()
            {
                wrapped.flush();
            }


            public void close()
            {
                wrapped.close();
            }


            public bool isOpen()
            {
                return wrapped.isOpen();
            }


            public string name()
            {
                return wrapped.name();
            }


            public bool persistent()
            {
                return false;
            }
        }
    }
}