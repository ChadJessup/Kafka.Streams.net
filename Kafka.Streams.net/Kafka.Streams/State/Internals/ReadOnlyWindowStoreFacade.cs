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
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.ReadOnlyWindowStore;
using Kafka.Streams.State.TimestampedWindowStore;
using Kafka.Streams.State.ValueAndTimestamp;
using Kafka.Streams.State.WindowStoreIterator;

import java.time.Instant;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

public class ReadOnlyWindowStoreFacade<K, V> : ReadOnlyWindowStore<K, V>
{
    protected TimestampedWindowStore<K, V> inner;

    protected ReadOnlyWindowStoreFacade(TimestampedWindowStore<K, V> store)
{
        inner = store;
    }

    public override V fetch(K key,
                   long time)
{
        return getValueOrNull(inner.fetch(key, time));
    }

    
    @SuppressWarnings("deprecation")
    public WindowStoreIterator<V> fetch(K key,
                                        long timeFrom,
                                        long timeTo)
{
        return new WindowStoreIteratorFacade<>(inner.fetch(key, timeFrom, timeTo));
    }

    public override WindowStoreIterator<V> fetch(K key,
                                        Instant from,
                                        Instant to) throws ArgumentException
{
        return new WindowStoreIteratorFacade<>(inner.fetch(key, from, to));
    }

    
    @SuppressWarnings("deprecation")
    public KeyValueIterator<Windowed<K>, V> fetch(K from,
                                                  K to,
                                                  long timeFrom,
                                                  long timeTo)
{
        return new KeyValueIteratorFacade<>(inner.fetch(from, to, timeFrom, timeTo));
    }

    public override KeyValueIterator<Windowed<K>, V> fetch(K from,
                                                  K to,
                                                  Instant fromTime,
                                                  Instant toTime) throws ArgumentException
{
        return new KeyValueIteratorFacade<>(inner.fetch(from, to, fromTime, toTime));
    }

    
    @SuppressWarnings("deprecation")
    public KeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom,
                                                     long timeTo)
{
        return new KeyValueIteratorFacade<>(inner.fetchAll(timeFrom, timeTo));
    }

    public override KeyValueIterator<Windowed<K>, V> fetchAll(Instant from,
                                                     Instant to) throws ArgumentException
{
        KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.fetchAll(from, to);
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    public override KeyValueIterator<Windowed<K>, V> all()
{
        KeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.all();
        return new KeyValueIteratorFacade<>(innerIterator);
    }

    private static class WindowStoreIteratorFacade<V> : WindowStoreIterator<V>
{
        KeyValueIterator<Long, ValueAndTimestamp<V>> innerIterator;

        WindowStoreIteratorFacade(KeyValueIterator<Long, ValueAndTimestamp<V>> iterator)
{
            innerIterator = iterator;
        }

        
        public void close()
{
            innerIterator.close();
        }

        
        public Long peekNextKey()
{
            return innerIterator.peekNextKey();
        }

        
        public bool hasNext()
{
            return innerIterator.hasNext();
        }

        
        public KeyValue<Long, V> next()
{
            KeyValue<Long, ValueAndTimestamp<V>> innerKeyValue = innerIterator.next();
            return KeyValue.pair(innerKeyValue.key, getValueOrNull(innerKeyValue.value));
        }
    }
}