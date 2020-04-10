using System.Collections.Generic;
using System;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.Processors.Internals
{
    public class KeyValueStoreReadWriteDecorator<K, V>
        : StateStoreReadWriteDecorator<IKeyValueStore<K, V>, K, V>
        , IKeyValueStore<K, V>
    {

        public KeyValueStoreReadWriteDecorator(
            KafkaStreamsContext context,
            IKeyValueStore<K, V> inner)
            : base(context, inner)
        {
        }

        public V Get(K key)
        {
            return this.Wrapped.Get(key);
        }

        public IKeyValueIterator<K, V> Range(K from, K to)
        {
            return this.Wrapped.Range(from, to);
        }

        public IKeyValueIterator<K, V> All()
        {
            return this.Wrapped.All();
        }

        public long approximateNumEntries
            => this.Wrapped.approximateNumEntries;

        public void Put(K key, V value)
        {
            this.Wrapped.Add(key, value);
        }

        public V PutIfAbsent(K key, V value)
        {
            return this.Wrapped.PutIfAbsent(key, value);
        }

        public void PutAll(List<KeyValuePair<K, V>> entries)
        {
            this.Wrapped.PutAll(entries);
        }

        public V Delete(K key)
        {
            return this.Wrapped.Delete(key);
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
