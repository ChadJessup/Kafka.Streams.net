
//using Kafka.Common.Utils;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemoryTimestampedKeyValueStoreMarker : IKeyValueStore<Bytes, byte[]>, ITimestampedBytesStore
//    {
//        IKeyValueStore<Bytes, byte[]> wrapped;

//        public InMemoryTimestampedKeyValueStoreMarker(IKeyValueStore<Bytes, byte[]> wrapped)
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
//            wrapped.Init(context, root);
//        }


//        public void put(Bytes key,
//                        byte[] value)
//        {
//            wrapped.Add(key, value);
//        }


//        public byte[] putIfAbsent(Bytes key,
//                                  byte[] value)
//        {
//            return wrapped.putIfAbsent(key, value);
//        }


//        public void putAll(List<KeyValuePair<Bytes, byte[]>> entries)
//        {
//            wrapped.putAll(entries);
//        }


//        public byte[] delete(Bytes key)
//        {
//            return wrapped.delete(key);
//        }


//        public byte[] get(Bytes key)
//        {
//            return wrapped[key];
//        }


//        public IKeyValueIterator<Bytes, byte[]> range(Bytes from,
//                                                     Bytes to)
//        {
//            return wrapped.Range(from, to);
//        }


//        public IKeyValueIterator<Bytes, byte[]> all()
//        {
//            return wrapped.all();
//        }


//        public long approximateNumEntries
//        {
//            return wrapped.approximateNumEntries;
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
