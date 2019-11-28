using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Window;
using System;

namespace Kafka.Streams.State.Queryable
{
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
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.KeyValueStoreType}
         */
        public static IQueryableStoreType<IReadOnlyKeyValueStore<K, V>> keyValueStore<K, V>()
        {
            return new KeyValueStoreType<K, V>();
        }

        /**
         * A {@link QueryableStoreType} that accepts {@link ReadOnlyKeyValueStore ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>}.
         *
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.TimestampedKeyValueStoreType}
         */
        public static IQueryableStoreType<IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> timestampedKeyValueStore<K, V>()
        {
            return new TimestampedKeyValueStoreType<K, V>();
        }

        /**
         * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore}.
         *
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.WindowStoreType}
         */
        public static IQueryableStoreType<IReadOnlyWindowStore<K, V>> windowStore<K, V>()
        {
            return new WindowStoreType<K, V>();
        }

        /**
         * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore ReadOnlyWindowStore<K, ValueAndTimestamp<V>>}.
         *
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.TimestampedWindowStoreType}
         */
        public static IQueryableStoreType<IReadOnlyWindowStore<K, ValueAndTimestamp<V>>> timestampedWindowStore<K, V>()
        {
            return new TimestampedWindowStoreType<K, V>();
        }

        /**
         * A {@link QueryableStoreType} that accepts {@link ReadOnlySessionStore}.
         *
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.SessionStoreType}
         */
        public static IQueryableStoreType<IReadOnlySessionStore<K, V>> sessionStore<K, V>()
        {
            return new SessionStoreType<K, V>();
        }
    }
}