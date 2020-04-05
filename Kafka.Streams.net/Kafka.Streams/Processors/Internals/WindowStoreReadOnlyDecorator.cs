using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Window;
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

        public void Put(K key, V value)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public void Put(K key, V value, long windowStartTimestamp)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public V Fetch(K key, long time)
        {
            return wrapped.Fetch(key, time);
        }

        [Obsolete]
        public IWindowStoreIterator<V> Fetch(
            K key,
            long timeFrom,
            long timeTo)
        {
            return wrapped.Fetch(key, timeFrom, timeTo);
        }

        public IKeyValueIterator<Windowed<K>, V> All()
        {
            return wrapped.All();
        }

        public IKeyValueIterator<Windowed<K>, V> Fetch(K from, K to, long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<Windowed<K>, V> FetchAll(long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }

        public IWindowStoreIterator<V> Fetch(K key, DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<Windowed<K>, V> Fetch(K from, K to, DateTime fromTime, DateTime toTime)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<Windowed<K>, V> FetchAll(DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
