using Kafka.Streams.State.ReadOnly;
using System.Collections.Generic;

namespace Kafka.Streams.State.KeyValues
{
    /**
     * A key-value store that supports Put/get/delete and range queries.
     *
     * @param The key type
     * @param The value type
     */
    public interface IKeyValueStore<K, V> : IKeyValueStore, IStateStore, IReadOnlyKeyValueStore<K, V>
    {
        /**
         * Update the value associated with this key.
         *
         * @param key The key to associate the value to
         * @param value The value to update, it can be {@code null};
         *              if the serialized bytes are also {@code null} it is interpreted as deletes
         * @throws ArgumentNullException If {@code null} is used for key.
         */
        void Add(K key, V value);

        /**
         * Update the value associated with this key, unless a value is already associated with the key.
         *
         * @param key The key to associate the value to
         * @param value The value to update, it can be {@code null};
         *              if the serialized bytes are also {@code null} it is interpreted as deletes
         * @return The old value or {@code null} if there is no such key.
         * @throws ArgumentNullException If {@code null} is used for key.
         */
        V PutIfAbsent(K key, V value);

        /**
         * Update All the given key/value pairs.
         *
         * @param entries A list of entries to Put into the store;
         *                if the serialized bytes are also {@code null} it is interpreted as deletes
         * @throws ArgumentNullException If {@code null} is used for key.
         */
        void PutAll(List<KeyValuePair<K, V>> entries);

        /**
         * Delete the value from the store (if there is one).
         *
         * @param key The key
         * @return The old value or {@code null} if there is no such key.
         * @throws ArgumentNullException If {@code null} is used for key.
         */
        V Delete(K key);
    }

    public interface IKeyValueStore
    {
    }
}
