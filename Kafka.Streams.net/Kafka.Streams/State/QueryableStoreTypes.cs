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
namespace Kafka.streams.state;

using Kafka.Streams.KafkaStreams;
using Kafka.Streams.Topology;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.State.internals.CompositeReadOnlyKeyValueStore;
using Kafka.Streams.State.internals.CompositeReadOnlySessionStore;
using Kafka.Streams.State.internals.CompositeReadOnlyWindowStore;
using Kafka.Streams.State.internals.StateStoreProvider;






/**
 * Provides access to the {@link QueryableStoreType}s provided with {@link KafkaStreams}.
 * These can be used with {@link KafkaStreams#store(string, QueryableStoreType)}.
 * To access and query the {@link IStateStore}s that are part of a {@link Topology}.
 */
public class QueryableStoreTypes
{

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyKeyValueStore}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.KeyValueStoreType}
     */
    public staticQueryableStoreType<IReadOnlyKeyValueStore<K, V>> keyValueStore()
{
        return new KeyValueStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyKeyValueStore ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.TimestampedKeyValueStoreType}
     */
    public staticQueryableStoreType<IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> timestampedKeyValueStore()
{
        return new TimestampedKeyValueStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.WindowStoreType}
     */
    public staticQueryableStoreType<ReadOnlyWindowStore<K, V>> windowStore()
{
        return new WindowStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore ReadOnlyWindowStore<K, ValueAndTimestamp<V>>}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.TimestampedWindowStoreType}
     */
    public staticQueryableStoreType<ReadOnlyWindowStore<K, ValueAndTimestamp<V>>> timestampedWindowStore()
{
        return new TimestampedWindowStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlySessionStore}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableStoreTypes.SessionStoreType}
     */
    public staticQueryableStoreType<ReadOnlySessionStore<K, V>> sessionStore()
{
        return new SessionStoreType<>();
    }

    private static abstract class QueryableStoreTypeMatcher<T> : QueryableStoreType<T>
{

        private HashSet<Class> matchTo;

        QueryableStoreTypeMatcher(Set<Class> matchTo)
{
            this.matchTo = matchTo;
        }

        
        
        public bool accepts(IStateStore stateStore)
{
            foreach (Class matchToClass in matchTo)
{
                if (!matchToClass.isAssignableFrom(stateStore.GetType()))
{
                    return false;
                }
            }
            return true;
        }
    }

    public static class KeyValueStoreType<K, V> : QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, V>>
{

        KeyValueStoreType()
{
            super(Collections.singleton(ReadOnlyKeyValueStore.class));
        }

        
        public IReadOnlyKeyValueStore<K, V> create(StateStoreProvider storeProvider,
                                                  string storeName)
{
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }

    }

    private static class TimestampedKeyValueStoreType<K, V>
        : QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>>
{

        TimestampedKeyValueStoreType()
{
            super(new HashSet<>(Arrays.asList(
                TimestampedKeyValueStore.class,
                IReadOnlyKeyValueStore.class)));
        }

        
        public IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> create(StateStoreProvider storeProvider,
                                                                     string storeName)
{
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }
    }

    public static class WindowStoreType<K, V> : QueryableStoreTypeMatcher<ReadOnlyWindowStore<K, V>>
{

        WindowStoreType()
{
            super(Collections.singleton(ReadOnlyWindowStore.class));
        }

        
        public ReadOnlyWindowStore<K, V> create(StateStoreProvider storeProvider,
                                                string storeName)
{
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }

    private static class TimestampedWindowStoreType<K, V>
        : QueryableStoreTypeMatcher<ReadOnlyWindowStore<K, ValueAndTimestamp<V>>>
{

        TimestampedWindowStoreType()
{
            super(new HashSet<>(Arrays.asList(
                TimestampedWindowStore.class,
                ReadOnlyWindowStore.class)));
        }

        
        public ReadOnlyWindowStore<K, ValueAndTimestamp<V>> create(StateStoreProvider storeProvider,
                                                                   string storeName)
{
            return new CompositeReadOnlyWindowStore<>(storeProvider, this, storeName);
        }
    }

    public static class SessionStoreType<K, V> : QueryableStoreTypeMatcher<ReadOnlySessionStore<K, V>>
{

        SessionStoreType()
{
            super(Collections.singleton(ReadOnlySessionStore.class));
        }

        
        public ReadOnlySessionStore<K, V> create(StateStoreProvider storeProvider,
                                                 string storeName)
{
            return new CompositeReadOnlySessionStore<>(storeProvider, this, storeName);
        }
    }

}