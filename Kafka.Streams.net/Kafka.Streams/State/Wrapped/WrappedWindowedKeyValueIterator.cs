﻿//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Interfaces;
//using Microsoft.Extensions.Logging;

//namespace Kafka.Streams.State.Internals
//{
//    public class WrappedWindowedKeyValueIterator : InMemoryWindowStoreIteratorWrapper, IKeyValueIterator<IWindowed<Bytes>, byte[]>
//    {

//        private long windowSize;

//        WrappedWindowedKeyValueIterator(Bytes keyFrom,
//                                        Bytes keyTo,
//                                        IEnumerator<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> segmentIterator,
//                                        ClosingCallback callback,
//                                        bool retainDuplicates,
//                                        long windowSize)
//        {
//            base(keyFrom, keyTo, segmentIterator, callback, retainDuplicates);
//            this.windowSize = windowSize;
//        }

//        public IWindowed<Bytes> PeekNextKey()
//        {
//            if (!HasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return getWindowedKey();
//        }

//        public KeyValuePair<IWindowed<Bytes>, byte[]> next()
//        {
//            if (!HasNext())
//            {
//                throw new NoSuchElementException();
//            }

//            KeyValuePair<IWindowed<Bytes>, byte[]> result = KeyValuePair.Create(getWindowedKey(), base.next.value);
//            base.next = null;
//            return result;
//        }

//        private IWindowed<Bytes> getWindowedKey()
//        {
//            Bytes key = base.retainDuplicates ? getKey(base.next.key) : base.next.key;
//            long endTime = base.currentTime + windowSize;

//            if (endTime < 0)
//            {
//                LOG.LogWarning("Warning: window end time was truncated to long.MAX");
//                endTime = long.MaxValue;
//            }

//            TimeWindow timeWindow = new TimeWindow(base.currentTime, endTime);
//            return new IWindowed<>(key, timeWindow);
//        }
//    }
//}