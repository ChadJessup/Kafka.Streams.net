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
            long windowStartTimestamp)
        {
            this.Wrapped.Put(key, value, windowStartTimestamp);
        }

        public V Fetch(K key, long time)
        {
            return this.Wrapped.Fetch(key, time);
        }

        public IWindowStoreIterator<V> Fetch(
            K key,
            long timeFrom,
            long timeTo)
        {
            return this.Wrapped.Fetch(key, timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(
            K from,
            K to,
            long timeFrom,
            long timeTo)
        {
            return this.Wrapped.Fetch(from, to, timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<K>, V> FetchAll(long timeFrom, long timeTo)
        {
            return this.Wrapped.FetchAll(timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<K>, V> All()
        {
            return this.Wrapped.All();
        }

        public IWindowStoreIterator<V> Fetch(K key, DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(K from, K to, DateTime fromTime, DateTime toTime)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<IWindowed<K>, V> FetchAll(DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
