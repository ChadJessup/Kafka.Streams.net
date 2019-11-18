
//using Kafka.Common.Utils;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemoryTimestampedWindowStoreMarker
//    : IWindowStore<Bytes, byte[]>, ITimestampedBytesStore
//    {

//        private IWindowStore<Bytes, byte[]> wrapped;

//        public InMemoryTimestampedWindowStoreMarker(IWindowStore<Bytes, byte[]> wrapped)
//        {
//            if (wrapped.persistent())
//            {
//                throw new System.ArgumentException("Provided store must not be a persistent store, but it is.");
//            }
//            this.wrapped = wrapped;
//        }


//        public void init(IProcessorContext<K, V> context,
//                         IStateStore root)
//        {
//            wrapped.init(context, root);
//        }


//        public void put(Bytes key,
//                        byte[] value)
//        {
//            wrapped.Add(key, value);
//        }


//        public void put(Bytes key,
//                        byte[] value,
//                        long windowStartTimestamp)
//        {
//            wrapped.Add(key, value, windowStartTimestamp);
//        }


//        public byte[] fetch(Bytes key,
//                            long time)
//        {
//            return wrapped.fetch(key, time);
//        }



//        public WindowStoreIterator<byte[]> fetch(Bytes key,
//                                                 long timeFrom,
//                                                 long timeTo)
//        {
//            return wrapped.fetch(key, timeFrom, timeTo);
//        }



//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
//                                                               Bytes to,
//                                                               long timeFrom,
//                                                               long timeTo)
//        {
//            return wrapped.fetch(from, to, timeFrom, timeTo);
//        }



//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
//                                                                  long timeTo)
//        {
//            return wrapped.fetchAll(timeFrom, timeTo);
//        }


//        public IKeyValueIterator<Windowed<Bytes>, byte[]> all()
//        {
//            return wrapped.all();
//        }


//        public void flush()
//        {
//            wrapped.flush();
//        }


//        public void close()
//        {
//            wrapped.close();
//        }

//        public bool isOpen()
//        {
//            return wrapped.isOpen();
//        }


//        public string name => wrapped.name;

//        public bool persistent()
//        {
//            return false;
//        }
//    }
//}
