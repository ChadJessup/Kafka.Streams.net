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
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.QueryableStoreType;
using Kafka.Streams.State.ReadOnlySessionStore;

import java.util.List;
import java.util.Objects;

/**
 * Wrapper over the underlying {@link ReadOnlySessionStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 */
public class CompositeReadOnlySessionStore<K, V> : ReadOnlySessionStore<K, V>
{
    private StateStoreProvider storeProvider;
    private QueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType;
    private string storeName;

    public CompositeReadOnlySessionStore(StateStoreProvider storeProvider,
                                         QueryableStoreType<ReadOnlySessionStore<K, V>> queryableStoreType,
                                         string storeName)
{
        this.storeProvider = storeProvider;
        this.queryableStoreType = queryableStoreType;
        this.storeName = storeName;
    }

    public override KeyValueIterator<Windowed<K>, V> fetch(K key)
{
        Objects.requireNonNull(key, "key can't be null");
        List<ReadOnlySessionStore<K, V>> stores = storeProvider.stores(storeName, queryableStoreType);
        foreach (ReadOnlySessionStore<K, V> store in stores)
{
            try
{
                KeyValueIterator<Windowed<K>, V> result = store.fetch(key);
                if (!result.hasNext())
{
                    result.close();
                } else
{
                    return result;
                }
            } catch (InvalidStateStoreException ise)
{
                throw new InvalidStateStoreException("State store  [" + storeName + "] is not available anymore" +
                                                             " and may have been migrated to another instance; " +
                                                             "please re-discover its location from the state metadata. " +
                                                             "Original error message: " + ise.ToString());
            }
        }
        return KeyValueIterators.emptyIterator();
    }

    public override KeyValueIterator<Windowed<K>, V> fetch(K from, K to)
{
        Objects.requireNonNull(from, "from can't be null");
        Objects.requireNonNull(to, "to can't be null");
        NextIteratorFunction<Windowed<K>, V, ReadOnlySessionStore<K, V>> nextIteratorFunction = store -> store.fetch(from, to);
        return new DelegatingPeekingKeyValueIterator<>(storeName,
                                                       new CompositeKeyValueIterator<>(
                                                               storeProvider.stores(storeName, queryableStoreType).iterator(),
                                                               nextIteratorFunction));
    }
}
