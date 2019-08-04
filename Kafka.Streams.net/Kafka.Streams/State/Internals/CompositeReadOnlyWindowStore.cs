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

using Kafka.Streams.Errors.InvalidStateStoreException;
using Kafka.Streams.internals.ApiUtils;
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.QueryableStoreType;
using Kafka.Streams.State.ReadOnlyWindowStore;
using Kafka.Streams.State.WindowStoreIterator;







/**
 * Wrapper over the underlying {@link ReadOnlyWindowStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 */
public CompositeReadOnlyWindowStore<K, V> : ReadOnlyWindowStore<K, V>
{

    private QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStoreType;
    private string storeName;
    private StateStoreProvider provider;

    public CompositeReadOnlyWindowStore(StateStoreProvider provider,
                                        QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStoreType,
                                        string storeName)
{
        this.provider = provider;
        this.windowStoreType = windowStoreType;
        this.storeName = storeName;
    }

    public override V fetch(K key, long time)
{
        key = key ?? throw new System.ArgumentNullException("key can't be null", nameof(key));
        List<ReadOnlyWindowStore<K, V>> stores = provider.stores(storeName, windowStoreType);
        foreach (ReadOnlyWindowStore<K, V> windowStore in stores)
{
            try
{
                V result = windowStore.fetch(key, time);
                if (result != null)
{
                    return result;
                }
            } catch (InvalidStateStoreException e)
{
                throw new InvalidStateStoreException(
                        "State store is not available anymore and may have been migrated to another instance; " +
                                "please re-discover its location from the state metadata.");
            }
        }
        return null;
    }

    
    [System.Obsolete]
    public WindowStoreIterator<V> fetch(K key,
                                        long timeFrom,
                                        long timeTo)
{
        key = key ?? throw new System.ArgumentNullException("key can't be null", nameof(key));
        List<ReadOnlyWindowStore<K, V>> stores = provider.stores(storeName, windowStoreType);
        foreach (ReadOnlyWindowStore<K, V> windowStore in stores)
{
            try
{
                WindowStoreIterator<V> result = windowStore.fetch(key, timeFrom, timeTo);
                if (!result.hasNext())
{
                    result.close();
                } else
{
                    return result;
                }
            } catch (InvalidStateStoreException e)
{
                throw new InvalidStateStoreException(
                        "State store is not available anymore and may have been migrated to another instance; " +
                                "please re-discover its location from the state metadata.");
            }
        }
        return KeyValueIterators.emptyWindowStoreIterator();
    }

    @SuppressWarnings("deprecation") // removing fetch(K from, long from, long to) will fix this
    public override WindowStoreIterator<V> fetch(K key,
                                        Instant from,
                                        Instant to) throws ArgumentException
{
        return fetch(
            key,
            ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from")),
            ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to")));
    }

    @SuppressWarnings("deprecation") // removing fetch(K from, K to, long from, long to) will fix this
    public override KeyValueIterator<Windowed<K>, V> fetch(K from,
                                                  K to,
                                                  long timeFrom,
                                                  long timeTo)
{
        from = from ?? throw new System.ArgumentNullException("from can't be null", nameof(from));
        to = to ?? throw new System.ArgumentNullException("to can't be null", nameof(to));
        NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.fetch(from, to, timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }

    public override KeyValueIterator<Windowed<K>, V> fetch(K from,
                                                  K to,
                                                  Instant fromTime,
                                                  Instant toTime) throws ArgumentException
{
        return fetch(
            from,
            to,
            ApiUtils.validateMillisecondInstant(fromTime, prepareMillisCheckFailMsgPrefix(fromTime, "fromTime")),
            ApiUtils.validateMillisecondInstant(toTime, prepareMillisCheckFailMsgPrefix(toTime, "toTime")));
    }

    public override KeyValueIterator<Windowed<K>, V> all()
{
        NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            ReadOnlyWindowStore::all;
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }

    
    [System.Obsolete]
    public KeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom,
                                                     long timeTo)
{
        NextIteratorFunction<Windowed<K>, V, ReadOnlyWindowStore<K, V>> nextIteratorFunction =
            store -> store.fetchAll(timeFrom, timeTo);
        return new DelegatingPeekingKeyValueIterator<>(
            storeName,
            new CompositeKeyValueIterator<>(
                provider.stores(storeName, windowStoreType).iterator(),
                nextIteratorFunction));
    }

    @SuppressWarnings("deprecation") // removing fetchAll(long from, long to) will fix this
    public override KeyValueIterator<Windowed<K>, V> fetchAll(Instant from,
                                                     Instant to) throws ArgumentException
{
        return fetchAll(
            ApiUtils.validateMillisecondInstant(from, prepareMillisCheckFailMsgPrefix(from, "from")),
            ApiUtils.validateMillisecondInstant(to, prepareMillisCheckFailMsgPrefix(to, "to")));
    }
}
