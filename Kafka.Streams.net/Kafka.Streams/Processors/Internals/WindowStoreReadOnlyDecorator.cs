using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class WindowStoreReadOnlyDecorator<K, V>
            : StateStoreReadOnlyDecorator<IWindowStore<K, V>, K, V>
            , IWindowStore<K, V>
    {
        public WindowStoreReadOnlyDecorator(
            KafkaStreamsContext context,
            IWindowStore<K, V> inner)
            : base(context, inner)
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
            return Wrapped.Fetch(key, time);
        }

        [Obsolete]
        public IWindowStoreIterator<V> Fetch(
            K key,
            long timeFrom,
            long timeTo)
        {
            return Wrapped.Fetch(key, timeFrom, timeTo);
        }

        public IKeyValueIterator<Windowed<K>, V> All()
        {
            return Wrapped.All();
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
