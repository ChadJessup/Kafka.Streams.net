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

        public V fetch(K key, long time)
        {
            return ValueAndTimestamp.GetValueOrNull(inner.fetch(key, time));
        }

        public IWindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo)
        {
            return new WindowStoreIteratorFacade<V>(inner.fetch(key, timeFrom, timeTo));
        }

        public IWindowStoreIterator<V> fetch(K key, DateTime from, DateTime to)
        {
            return new WindowStoreIteratorFacade<V>(inner.fetch(key, from, to));
        }

        public IKeyValueIterator<Windowed<K>, V> fetch(K from, K to, long timeFrom, long timeTo)
        {
            return new KeyValueIteratorFacade<Windowed<K>, V>((IKeyValueIterator<Windowed<K>, V>)inner.fetch(from, to, timeFrom, timeTo));
        }

        public IKeyValueIterator<Windowed<K>, V> fetch(
            K from,
            K to,
            DateTime fromTime,
            DateTime toTime)
        {
            return new KeyValueIteratorFacade<Windowed<K>, V>((IKeyValueIterator<Windowed<K>, V>)inner.fetch(from, to, fromTime, toTime));
        }

        public IKeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom,
                                                         long timeTo)
        {
            return new KeyValueIteratorFacade<Windowed<K>, V>((IKeyValueIterator<Windowed<K>, V>)inner.fetchAll(timeFrom, timeTo));
        }

        public IKeyValueIterator<Windowed<K>, V> fetchAll(DateTime from, DateTime to)
        {
            IKeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.fetchAll(from, to);

            return (IKeyValueIterator<Windowed<K>, V>)new KeyValueIteratorFacade<Windowed<K>, ValueAndTimestamp<V>>(innerIterator);
        }

        public IKeyValueIterator<Windowed<K>, V> all()
        {
            IKeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator = inner.all();

            return (IKeyValueIterator<Windowed<K>, V>)new KeyValueIteratorFacade<Windowed<K>, ValueAndTimestamp<V>>(innerIterator);
        }
    }
}