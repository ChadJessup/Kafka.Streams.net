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

        public V Get(K key)
        {
            return wrapped.Get(key);
        }

        public IKeyValueIterator<K, V> Range(K from, K to)
        {
            return wrapped.Range(from, to);
        }

        public IKeyValueIterator<K, V> All()
        {
            return wrapped.All();
        }

        public long approximateNumEntries
            => wrapped.approximateNumEntries;

        public void put(K key, V value)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public V PutIfAbsent(K key, V value)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public void PutAll(List<KeyValuePair<K, V>> entries)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public V Delete(K key)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
