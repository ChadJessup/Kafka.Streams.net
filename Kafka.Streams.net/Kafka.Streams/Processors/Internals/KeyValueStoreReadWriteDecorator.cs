using System.Collections.Generic;
using System;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.Processors.Internals
{
    public class KeyValueStoreReadWriteDecorator<K, V>
        : StateStoreReadWriteDecorator<IKeyValueStore<K, V>>
        , IKeyValueStore<K, V>
    {

        public KeyValueStoreReadWriteDecorator(IKeyValueStore<K, V> inner)
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
            wrapped.Add(key, value);
        }

        public V PutIfAbsent(K key, V value)
        {
            return wrapped.PutIfAbsent(key, value);
        }

        public void PutAll(List<KeyValuePair<K, V>> entries)
        {
            wrapped.PutAll(entries);
        }

        public V Delete(K key)
        {
            return wrapped.Delete(key);
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
