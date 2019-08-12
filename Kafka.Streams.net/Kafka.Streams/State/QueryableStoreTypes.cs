





















using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State
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
        public static IQueryableStoreType<IReadOnlyKeyValueStore<K, V>> keyValueStore()
        {
            return new KeyValueStoreType<>();
        }

        /**
         * A {@link QueryableStoreType} that accepts {@link ReadOnlyKeyValueStore ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>}.
         *
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.TimestampedKeyValueStoreType}
         */
        public static IQueryableStoreType<IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> timestampedKeyValueStore()
        {
            return new TimestampedKeyValueStoreType<>();
        }

        /**
         * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore}.
         *
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.WindowStoreType}
         */
        public static IQueryableStoreType<ReadOnlyWindowStore<K, V>> windowStore()
        {
            return new WindowStoreType<>();
        }

        /**
         * A {@link QueryableStoreType} that accepts {@link ReadOnlyWindowStore ReadOnlyWindowStore<K, ValueAndTimestamp<V>>}.
         *
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.TimestampedWindowStoreType}
         */
        public static IQueryableStoreType<ReadOnlyWindowStore<K, ValueAndTimestamp<V>>> timestampedWindowStore()
        {
            return new TimestampedWindowStoreType<>();
        }

        /**
         * A {@link QueryableStoreType} that accepts {@link ReadOnlySessionStore}.
         *
         * @param key type of the store
         * @param value type of the store
         * @return {@link QueryableStoreTypes.SessionStoreType}
         */
        public static IQueryableStoreType<ReadOnlySessionStore<K, V>> sessionStore()
        {
            return new SessionStoreType<>();
        }
    }
}