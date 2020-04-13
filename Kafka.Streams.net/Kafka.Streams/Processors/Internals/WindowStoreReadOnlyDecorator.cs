using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class WindowStoreReadOnlyDecorator<K, V>
        : StateStoreReadOnlyDecorator<IWindowStore<K, V>, K, V>,
        IWindowStore<K, V>
    {
        public WindowStoreReadOnlyDecorator(
            KafkaStreamsContext context,
            IWindowStore<K, V> inner)
            : base(context, inner)
        {
        }

        public void Put(K key, V value)
        {
        }

        public void Put(K key, V value, DateTime windowStartTimestamp)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public V Fetch(K key, DateTime time)
        {
            return this.Wrapped.Fetch(key, time);
        }

        public IKeyValueIterator<IWindowed<K>, V> All()
        {
            return this.Wrapped.All();
        }

        public IWindowStoreIterator<V> Fetch(K key, DateTime timeFrom, DateTime timeTo)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<IWindowed<K>, V> Fetch(K from, K to, DateTime timeFrom, DateTime timeTo)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<IWindowed<K>, V> FetchAll(DateTime timeFrom, DateTime timeTo)
        {
            throw new NotImplementedException();
        }

        public void Add(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
