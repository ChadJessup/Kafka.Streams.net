using Kafka.Streams.State.KeyValues;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class KeyValueStoreReadOnlyDecorator<K, V>
        : StateStoreReadOnlyDecorator<IKeyValueStore<K, V>>
        , IKeyValueStore<K, V>
    {
        public KeyValueStoreReadOnlyDecorator(IKeyValueStore<K, V> inner)
            : base(inner)
        {
        }

        public V get(K key)
        {
            return wrapped.get(key);
        }

        public IKeyValueIterator<K, V> range(K from, K to)
        {
            return wrapped.range(from, to);
        }

        public IKeyValueIterator<K, V> all()
        {
            return wrapped.all();
        }

        public long approximateNumEntries
            => wrapped.approximateNumEntries;

        public void put(K key, V value)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public V putIfAbsent(K key, V value)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public void putAll(List<KeyValuePair<K, V>> entries)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public V delete(K key)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
