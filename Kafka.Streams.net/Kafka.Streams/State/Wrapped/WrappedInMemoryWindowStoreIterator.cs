﻿//using Kafka.Common.Utils;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedInMemoryWindowStoreIterator : InMemoryWindowStoreIteratorWrapper, IWindowStoreIterator<byte[]>
//    {

//        WrappedInMemoryWindowStoreIterator(Bytes keyFrom,
//                                           Bytes keyTo,
//                                           IEnumerator<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> segmentIterator,
//                                           ClosingCallback callback,
//                                           bool retainDuplicates)
//            : base(keyFrom, keyTo, segmentIterator, callback, retainDuplicates)
//        {
//        }


//        public long PeekNextKey()
//        {
//            if (!HasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return base.currentTime;
//        }


//        public KeyValuePair<long, byte[]> next()
//        {
//            if (!HasNext())
//            {
//                throw new NoSuchElementException();
//            }

//            KeyValuePair<long, byte[]> result = KeyValuePair.Create(base.currentTime, base.next.value);
//            base.next = null;
//            return result;
//        }

//        public static WrappedInMemoryWindowStoreIterator emptyIterator()
//        {
//            return new WrappedInMemoryWindowStoreIterator(null, null, null, it => { }, false);
//        }
//    }
//}