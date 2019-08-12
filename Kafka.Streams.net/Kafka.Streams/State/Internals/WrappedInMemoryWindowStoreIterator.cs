//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedInMemoryWindowStoreIterator : InMemoryWindowStoreIteratorWrapper, IWindowStoreIterator<byte[]>
//    {

//        WrappedInMemoryWindowStoreIterator(Bytes keyFrom,
//                                           Bytes keyTo,
//                                           IEnumerator<KeyValuePair<long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
//                                           ClosingCallback callback,
//                                           bool retainDuplicates)
//            : base(keyFrom, keyTo, segmentIterator, callback, retainDuplicates)
//        {
//        }


//        public long peekNextKey()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return base.currentTime;
//        }


//        public KeyValue<long, byte[]> next()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }

//            KeyValue<long, byte[]> result = new KeyValue<>(base.currentTime, base.next.value);
//            base.next = null;
//            return result;
//        }

//        public static WrappedInMemoryWindowStoreIterator emptyIterator()
//        {
//            return new WrappedInMemoryWindowStoreIterator(null, null, null, it => { }, false);
//        }
//    }
//}