using Kafka.Streams.KStream;
using Kafka.Streams.State.Interfaces;
using System;

namespace Kafka.Streams.Processor.Internals
{
    public class WindowStoreReadWriteDecorator<K, V>
        : StateStoreReadWriteDecorator<IWindowStore<K, V>, K, V>
        , IWindowStore<K, V>
    {
        public WindowStoreReadWriteDecorator(IWindowStore<K, V> inner)
            : base(inner)
        {
        }


        public void put(K key,
                        V value)
        {
            wrapped.Add(key, value);
        }


        public void put(K key,
                        V value,
                        long windowStartTimestamp)
        {
            wrapped.Add(key, value, windowStartTimestamp);
        }


        public V fetch(K key,
                       long time)
        {
            return wrapped.fetch(key, time);
        }



        public IWindowStoreIterator<V> fetch(K key,
                                            long timeFrom,
                                            long timeTo)
        {
            return wrapped.fetch(key, timeFrom, timeTo);
        }



        public IKeyValueIterator<Windowed<K>, V> fetch(K from,
                                                      K to,
                                                      long timeFrom,
                                                      long timeTo)
        {
            return wrapped.fetch(from, to, timeFrom, timeTo);
        }



        public IKeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom,
                                                         long timeTo)
        {
            return wrapped.fetchAll(timeFrom, timeTo);
        }


        public IKeyValueIterator<Windowed<K>, V> all()
        {
            return wrapped.all();
        }
    }
}
