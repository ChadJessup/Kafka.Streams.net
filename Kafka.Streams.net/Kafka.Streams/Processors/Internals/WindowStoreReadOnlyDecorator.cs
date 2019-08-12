//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Interfaces;
//using System;

//namespace Kafka.Streams.Processor.Internals
//{
//    public class WindowStoreReadOnlyDecorator<K, V>
//            : StateStoreReadOnlyDecorator<IWindowStore<K, V>, K, V>
//            , IWindowStore<K, V>
//    {

//        public WindowStoreReadOnlyDecorator(IWindowStore<K, V> inner)
//            : base(inner)
//        {
//        }


//        public void put(K key,
//                        V value)
//        {
//            throw new InvalidOperationException(ERROR_MESSAGE);
//        }


//        public void put(K key,
//                        V value,
//                        long windowStartTimestamp)
//        {
//            throw new InvalidOperationException(ERROR_MESSAGE);
//        }


//        public V fetch(K key,
//                       long time)
//        {
//            return wrapped.fetch(key, time);
//        }


//        [System.Obsolete]
//        public IWindowStoreIterator<V> fetch(K key,
//                                            long timeFrom,
//                                            long timeTo)
//        {
//            return wrapped.fetch(key, timeFrom, timeTo);
//        }


//        public IKeyValueIterator<Windowed<K>, V> all()
//        {
//            return wrapped.all();
//        }
//    }
//}

