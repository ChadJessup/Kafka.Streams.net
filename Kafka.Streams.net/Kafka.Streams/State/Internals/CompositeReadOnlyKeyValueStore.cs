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

using Kafka.Streams.Errors.InvalidStateStoreException;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.QueryableStoreType;
using Kafka.Streams.State.ReadOnlyKeyValueStore;

import java.util.List;
import java.util.Objects;

/**
 * A wrapper over the underlying {@link ReadOnlyKeyValueStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CompositeReadOnlyKeyValueStore<K, V> : IReadOnlyKeyValueStore<K, V>
{

    private StateStoreProvider storeProvider;
    private QueryableStoreType<IReadOnlyKeyValueStore<K, V>> storeType;
    private string storeName;

    public CompositeReadOnlyKeyValueStore(StateStoreProvider storeProvider,
                                          QueryableStoreType<IReadOnlyKeyValueStore<K, V>> storeType,
                                          string storeName)
{
        this.storeProvider = storeProvider;
        this.storeType = storeType;
        this.storeName = storeName;
    }


    public override V get(K key)
{
        Objects.requireNonNull(key);
        List<IReadOnlyKeyValueStore<K, V>> stores = storeProvider.stores(storeName, storeType);
        foreach (IReadOnlyKeyValueStore<K, V> store in stores)
{
            try
{
                V result = store[key];
                if (result != null)
{
                    return result;
                }
            } catch (InvalidStateStoreException e)
{
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

    public override KeyValueIterator<K, V> range(K from, K to)
{
        Objects.requireNonNull(from);
        Objects.requireNonNull(to);
        NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>> nextIteratorFunction = new NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>>()
{
            
            public KeyValueIterator<K, V> apply(IReadOnlyKeyValueStore<K, V> store)
{
                try
{
                    return store.range(from, to);
                } catch (InvalidStateStoreException e)
{
                    throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
                }
            }
        };
        List<IReadOnlyKeyValueStore<K, V>> stores = storeProvider.stores(storeName, storeType);
        return new DelegatingPeekingKeyValueIterator<>(storeName, new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
    }

    public override KeyValueIterator<K, V> all()
{
        NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>> nextIteratorFunction = new NextIteratorFunction<K, V, IReadOnlyKeyValueStore<K, V>>()
{
            
            public KeyValueIterator<K, V> apply(IReadOnlyKeyValueStore<K, V> store)
{
                try
{
                    return store.all();
                } catch (InvalidStateStoreException e)
{
                    throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
                }
            }
        };
        List<IReadOnlyKeyValueStore<K, V>> stores = storeProvider.stores(storeName, storeType);
        return new DelegatingPeekingKeyValueIterator<>(storeName, new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
    }

    public override long approximateNumEntries()
{
        List<IReadOnlyKeyValueStore<K, V>> stores = storeProvider.stores(storeName, storeType);
        long total = 0;
        foreach (IReadOnlyKeyValueStore<K, V> store in stores)
{
            total += store.approximateNumEntries();
            if (total < 0)
{
                return Long.MAX_VALUE;
            }
        }
        return total;
    }


}

