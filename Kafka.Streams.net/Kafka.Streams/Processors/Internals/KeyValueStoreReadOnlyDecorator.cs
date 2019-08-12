using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class KeyValueStoreReadOnlyDecorator<K, V>
        : StateStoreReadOnlyDecorator<IKeyValueStore<K, V>, K, V>
        , IKeyValueStore<K, V>
    {
        public KeyValueStoreReadOnlyDecorator(IKeyValueStore<K, V> inner)
        : base(inner)
        {
        }


        public V get(K key)
        {
            return wrapped[key];
        }


        public IKeyValueIterator<K, V> range(K from,
                                            K to)
        {
            return wrapped.range(from, to);
        }


        public IKeyValueIterator<K, V> all()
        {
            return wrapped.all();
        }


        public long approximateNumEntries()
        {
            return wrapped.approximateNumEntries();
        }


        public void put(K key,
                        V value)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }


        public V putIfAbsent(K key,
                             V value)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }


        public void putAll(List entries)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }


        public V delete(K key)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }
    }
}
