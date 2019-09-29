using Kafka.Streams.KStream;
using Kafka.Streams.State.Interfaces;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class WindowStoreReadOnlyDecorator<K, V>
            : StateStoreReadOnlyDecorator<IWindowStore<K, V>>
            , IWindowStore<K, V>
    {
        public WindowStoreReadOnlyDecorator(IWindowStore<K, V> inner)
            : base(inner)
        {
        }

        public void put(K key, V value)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public void put(K key, V value, long windowStartTimestamp)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public V fetch(K key, long time)
        {
            return wrapped.fetch(key, time);
        }

        [Obsolete]
        public IWindowStoreIterator<V> fetch(
            K key,
            long timeFrom,
            long timeTo)
        {
            return wrapped.fetch(key, timeFrom, timeTo);
        }

        public IKeyValueIterator<Windowed<K>, V> all()
        {
            return wrapped.all();
        }

        public IKeyValueIterator<Windowed<K>, V> fetch(K from, K to, long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }

        public IWindowStoreIterator<V> fetch(K key, DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<Windowed<K>, V> fetch(K from, K to, DateTime fromTime, DateTime toTime)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<Windowed<K>, V> fetchAll(DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
