using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class WindowStoreReadWriteDecorator<K, V>
        : StateStoreReadWriteDecorator<IWindowStore<K, V>, K, V>
        , IWindowStore<K, V>
    {
        public WindowStoreReadWriteDecorator(KafkaStreamsContext context, IWindowStore<K, V> inner)
            : base(context, inner)
        {
        }

        public void Put(K key, V value)
        {
            this.Wrapped.Add(key, value);
        }

        public void Put(
            K key,
            V value,
            DateTime windowStartTimestamp)
        {
            this.Wrapped.Put(key, value, windowStartTimestamp);
        }

        public V Fetch(K key, DateTime time)
        {
            return this.Wrapped.Fetch(key, time);
        }

        public IWindowStoreIterator<V> Fetch(
            K key,
            DateTime timeFrom,
            DateTime timeTo)
        {
            return this.Wrapped.Fetch(key, timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(
            K from,
            K to,
            DateTime timeFrom,
            DateTime timeTo)
        {
            return this.Wrapped.Fetch(from, to, timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<K>, V> FetchAll(
            DateTime timeFrom, 
            DateTime timeTo)
        {
            return this.Wrapped.FetchAll(timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<K>, V> All()
        {
            return this.Wrapped.All();
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
