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
namespace Kafka.Streams.State.Internals;

using Kafka.Common.serialization.Serde;
using Kafka.Common.Utils.Bytes;
using Kafka.Common.Utils.Time;
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.TimestampedBytesStore;
using Kafka.Streams.State.TimestampedWindowStore;
using Kafka.Streams.State.ValueAndTimestamp;
using Kafka.Streams.State.WindowBytesStoreSupplier;
using Kafka.Streams.State.WindowStore;
using Kafka.Streams.State.WindowStoreIterator;



public TimestampedWindowStoreBuilder<K, V>
    : AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedWindowStore<K, V>>
{

    private WindowBytesStoreSupplier storeSupplier;

    public TimestampedWindowStoreBuilder(WindowBytesStoreSupplier storeSupplier,
                                         ISerde<K> keySerde,
                                         ISerde<V> valueSerde,
                                         ITime time)
{
        base(storeSupplier.name(), keySerde, valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde), time);
        storeSupplier = storeSupplier ?? throw new System.ArgumentNullException("bytesStoreSupplier can't be null", nameof(storeSupplier));
        this.storeSupplier = storeSupplier;
    }

    public override TimestampedWindowStore<K, V> build()
{
        WindowStore<Bytes, byte[]> store = storeSupplier[];
        if (!(store is TimestampedBytesStore))
{
            if (store.persistent())
{
                store = new WindowToTimestampedWindowByteStoreAdapter(store);
            } else
{
                store = new InMemoryTimestampedWindowStoreMarker(store);
            }
        }
        return new MeteredTimestampedWindowStore<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.windowSize(),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private WindowStore<Bytes, byte[]> maybeWrapCaching(WindowStore<Bytes, byte[]> inner)
{
        if (!enableCaching)
{
            return inner;
        }
        return new CachingWindowStore(
            inner,
            storeSupplier.windowSize(),
            storeSupplier.segmentIntervalMs());
    }

    private WindowStore<Bytes, byte[]> maybeWrapLogging(WindowStore<Bytes, byte[]> inner)
{
        if (!enableLogging)
{
            return inner;
        }
        return new ChangeLoggingTimestampedWindowBytesStore(inner, storeSupplier.retainDuplicates());
    }

    public long retentionPeriod()
{
        return storeSupplier.retentionPeriod();
    }


    private static InMemoryTimestampedWindowStoreMarker
        : WindowStore<Bytes, byte[]>, TimestampedBytesStore
{

        private WindowStore<Bytes, byte[]> wrapped;

        private InMemoryTimestampedWindowStoreMarker(WindowStore<Bytes, byte[]> wrapped)
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

        
        public void put(Bytes key,
                        byte[] value,
                        long windowStartTimestamp)
{
            wrapped.Add(key, value, windowStartTimestamp);
        }

        
        public byte[] fetch(Bytes key,
                            long time)
{
            return wrapped.fetch(key, time);
        }

        
        
        public WindowStoreIterator<byte[]> fetch(Bytes key,
                                                 long timeFrom,
                                                 long timeTo)
{
            return wrapped.fetch(key, timeFrom, timeTo);
        }

        
        
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
                                                               Bytes to,
                                                               long timeFrom,
                                                               long timeTo)
{
            return wrapped.fetch(from, to, timeFrom, timeTo);
        }

        
        
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
                                                                  long timeTo)
{
            return wrapped.fetchAll(timeFrom, timeTo);
        }

        
        public KeyValueIterator<Windowed<Bytes>, byte[]> all()
{
            return wrapped.all();
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
