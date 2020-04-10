using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;
using System;

namespace Kafka.Streams.State.ReadOnly
{
    public class ReadOnlyWindowStoreFacade<K, V> : IReadOnlyWindowStore<K, V>
    {
        protected ITimestampedWindowStore<K, V> inner { get; }

        public ReadOnlyWindowStoreFacade(ITimestampedWindowStore<K, V> store)
        {
            this.inner = store;
        }

        public V Fetch(K key, long time)
        {
            return ValueAndTimestamp.GetValueOrNull(this.inner.Fetch(key, time));
        }

        public IWindowStoreIterator<V> Fetch(K key, long timeFrom, long timeTo)
        {
            return new WindowStoreIteratorFacade<V>(this.inner.Fetch(key, timeFrom, timeTo));
        }

        public IWindowStoreIterator<V> Fetch(K key, DateTime from, DateTime to)
        {
            return new WindowStoreIteratorFacade<V>(this.inner.Fetch(key, from, to));
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(K from, K to, long timeFrom, long timeTo)
        {
            return new KeyValueIteratorFacade<IWindowed<K>, V>((IKeyValueIterator<IWindowed<K>, V>)this.inner.Fetch(from, to, timeFrom, timeTo));
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(
            K from,
            K to,
            DateTime fromTime,
            DateTime toTime)
        {
            return new KeyValueIteratorFacade<IWindowed<K>, V>((IKeyValueIterator<IWindowed<K>, V>)this.inner.Fetch(from, to, fromTime, toTime));
        }

        public IKeyValueIterator<IWindowed<K>, V> FetchAll(long timeFrom,
                                                         long timeTo)
        {
            return new KeyValueIteratorFacade<IWindowed<K>, V>((IKeyValueIterator<IWindowed<K>, V>)this.inner.FetchAll(timeFrom, timeTo));
        }

        public IKeyValueIterator<IWindowed<K>, V> FetchAll(DateTime from, DateTime to)
        {
            IKeyValueIterator<IWindowed<K>, ValueAndTimestamp<V>> innerIterator = this.inner.FetchAll(from, to);

            return (IKeyValueIterator<IWindowed<K>, V>)new KeyValueIteratorFacade<IWindowed<K>, ValueAndTimestamp<V>>(innerIterator);
        }

        public IKeyValueIterator<IWindowed<K>, V> All()
        {
            IKeyValueIterator<IWindowed<K>, ValueAndTimestamp<V>> innerIterator = this.inner.All();

            return (IKeyValueIterator<IWindowed<K>, V>)new KeyValueIteratorFacade<IWindowed<K>, ValueAndTimestamp<V>>(innerIterator);
        }
    }
}