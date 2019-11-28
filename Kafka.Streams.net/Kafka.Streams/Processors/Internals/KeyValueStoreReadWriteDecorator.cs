using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using System.Collections.Generic;
using System;
using Kafka.Streams.State.KeyValue;

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
            wrapped.Add(key, value);
        }

        public V putIfAbsent(K key, V value)
        {
            return wrapped.putIfAbsent(key, value);
        }

        public void putAll(List<KeyValue<K, V>> entries)
        {
            wrapped.putAll(entries);
        }

        public V delete(K key)
        {
            return wrapped.delete(key);
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
