﻿using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State
{
    /**
     * A key-value store that only supports read operations.
     * Implementations should be thread-safe as concurrent reads and writes are expected.
     *
     * Please note that this contract defines the thread-safe read functionality only; it does not
     * guarantee anything about whether the actual instance is writable by another thread, or
     * whether it uses some locking mechanism under the hood. For this reason, making dependencies
     * between the read and write operations on different StateStore instances can cause concurrency
     * problems like deadlock.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public interface IReadOnlyKeyValueStore<K, V>
    {
        /**
         * Get the value corresponding to this key.
         *
         * @param key The key to fetch
         * @return The value or null if no value is found.
         * @throws NullPointerException If null is used for key.
         * @throws InvalidStateStoreException if the store is not initialized
         */
        V get(K key);

        /**
         * Get an iterator over a given range of keys. This iterator must be closed after use.
         * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
         * and must not return null values. No ordering guarantees are provided.
         * @param from The first key that could be in the range
         * @param to The last key that could be in the range
         * @return The iterator for this range.
         * @throws NullPointerException If null is used for from or to.
         * @throws InvalidStateStoreException if the store is not initialized
         */
        IKeyValueIterator<K, V> range(K from, K to);

        /**
         * Return an iterator over all keys in this store. This iterator must be closed after use.
         * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
         * and must not return null values. No ordering guarantees are provided.
         * @return An iterator of all key/value pairs in the store.
         * @throws InvalidStateStoreException if the store is not initialized
         */
        IKeyValueIterator<K, V> all();

        /**
         * Return an approximate count of key-value mappings in this store.
         *
         * The count is not guaranteed to be exact in order to accommodate stores
         * where an exact count is expensive to calculate.
         *
         * @return an approximate count of key-value mappings in the store.
         * @throws InvalidStateStoreException if the store is not initialized
         */
        long approximateNumEntries { get; }
    }
}