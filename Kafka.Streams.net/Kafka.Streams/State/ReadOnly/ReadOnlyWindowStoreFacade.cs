using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Window;
using System;

namespace Kafka.Streams.State.ReadOnly
{
    public class ReadOnlyWindowStoreFacade<K, V> : IReadOnlyWindowStore<K, V>
    {
        protected ITimestampedWindowStore<K, V> inner { get; }

        public ReadOnlyWindowStoreFacade(ITimestampedWindowStore<K, V> store)
        {
            inner = store;
        }

        public V Fetch(K key, long time)
        {
            return ValueAndTimestamp.GetValueOrNull(inner.Fetch(key, time));
        }

        public IWindowStoreIterator<V> Fetch(K key, long timeFrom, long timeTo)
        {
            return new WindowStoreIteratorFacade<V>(inner.Fetch(key, timeFrom, timeTo));
        }

        public IWindowStoreIterator<V> Fetch(K key, DateTime from, DateTime to)
        {
            return new WindowStoreIteratorFacade<V>(inner.Fetch(key, from, to));
        }

        public IKeyValueIterator<Windowed<K>, V> Fetch(K from, K to, long timeFrom, long timeTo)
        {
            return new KeyValueIteratorFacade<Windowed<K>, V>((IKeyValueIterator<Windowed<K>, V>)inner.Fetch(from, to, timeFrom, timeTo));
        }

        public IKeyValueIterator<Windowed<K>, V> Fetch(
            K from,
            K to,
            DateTime fromTime,
            DateTime toTime)
        {
            return new KeyValueIteratorFacade<Windowed<K>, V>((IKeyValueIterator<Windowed<K>, V>)inner.Fetch(from, to, fromTime, toTime));
        }

        public IKeyValueIterator<Windowed<K>, V> FetchAll(long timeFrom,
                                                         long timeTo)
        {
            return new KeyValueIteratorFacade<Windowed<K>, V>((IKeyValueIterator<Windowed<K>, V>)inner.FetchAll(timeFrom, timeTo));
        }

        public IKeyValueIterator<Windowed<K>, V> FetchAll(DateTime from, DateTime to)
        {
            IKeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.FetchAll(from, to);

            return (IKeyValueIterator<Windowed<K>, V>)new KeyValueIteratorFacade<Windowed<K>, ValueAndTimestamp<V>>(innerIterator);
        }

        public IKeyValueIterator<Windowed<K>, V> All()
        {
            IKeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.All();

            return (IKeyValueIterator<Windowed<K>, V>)new KeyValueIteratorFacade<Windowed<K>, ValueAndTimestamp<V>>(innerIterator);
        }
    }
}