//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;

//namespace Kafka.Streams.State.Internals
//{
//    public class CacheIteratorWrapper : IPeekingKeyValueIterator<Bytes, LRUCacheEntry>
//    {
//        private long segmentInterval;

//        private Bytes keyFrom;
//        private Bytes keyTo;
//        private long latestSessionStartTime;
//        private long lastSegmentId;

//        private long currentSegmentId;
//        private Bytes cacheKeyFrom;
//        private Bytes cacheKeyTo;

//        private MemoryLRUCacheBytesIterator current;

//        public CacheIteratorWrapper(
//            Bytes key,
//            long earliestSessionEndTime,
//            long latestSessionStartTime)
//            : this(key, key, earliestSessionEndTime, latestSessionStartTime)
//        {
//        }

//        public CacheIteratorWrapper(Bytes keyFrom,
//                                     Bytes keyTo,
//                                     long earliestSessionEndTime,
//                                     long latestSessionStartTime)
//        {
//            this.keyFrom = keyFrom;
//            this.keyTo = keyTo;
//            this.latestSessionStartTime = latestSessionStartTime;
//            this.lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);
//            this.segmentInterval = cacheFunction.getSegmentInterval();

//            this.currentSegmentId = cacheFunction.segmentId(earliestSessionEndTime);

//            setCacheKeyRange(earliestSessionEndTime, currentSegmentLastTime());

//            this.current = cache.Range(cacheName, cacheKeyFrom, cacheKeyTo);
//        }


//        public bool hasNext()
//        {
//            if (current == null)
//            {
//                return false;
//            }

//            if (current.hasNext())
//            {
//                return true;
//            }

//            while (!current.hasNext())
//            {
//                getNextSegmentIterator();
//                if (current == null)
//                {
//                    return false;
//                }
//            }
//            return true;
//        }


//        public Bytes peekNextKey()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return current.peekNextKey();
//        }


//        public KeyValuePair<Bytes, LRUCacheEntry> peekNext()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return current.peekNext();
//        }


//        public KeyValuePair<Bytes, LRUCacheEntry> next()
//        {
//            if (!hasNext())
//            {
//                throw new NoSuchElementException();
//            }
//            return current.MoveNext();
//        }


//        public void close()
//        {
//            current.close();
//        }

//        private long currentSegmentBeginTime()
//        {
//            return currentSegmentId * segmentInterval;
//        }

//        private long currentSegmentLastTime()
//        {
//            return currentSegmentBeginTime() + segmentInterval - 1;
//        }

//        private void getNextSegmentIterator()
//        {
//            ++currentSegmentId;
//            lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);

//            if (currentSegmentId > lastSegmentId)
//            {
//                current = null;
//                return;
//            }

//            setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

//            current.close();
//            current = cache.Range(cacheName, cacheKeyFrom, cacheKeyTo);
//        }

//        private void setCacheKeyRange(long lowerRangeEndTime, long upperRangeEndTime)
//        {
//            if (cacheFunction.segmentId(lowerRangeEndTime) != cacheFunction.segmentId(upperRangeEndTime))
//            {
//                throw new InvalidOperationException("Error iterating over segments: segment interval has changed");
//            }

//            if (keyFrom == keyTo)
//            {
//                cacheKeyFrom = cacheFunction.cacheKey(segmentLowerRangeFixedSize(keyFrom, lowerRangeEndTime));
//                cacheKeyTo = cacheFunction.cacheKey(segmentUpperRangeFixedSize(keyTo, upperRangeEndTime));
//            }
//            else
//            {
//                cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
//                cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime), currentSegmentId);
//            }
//        }

//        private Bytes segmentLowerRangeFixedSize(Bytes key, long segmentBeginTime)
//        {
//            Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(0, Math.Max(0, segmentBeginTime)));
//            return SessionKeySchema.toBinary(sessionKey);
//        }

//        private Bytes segmentUpperRangeFixedSize(Bytes key, long segmentEndTime)
//        {
//            Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(Math.Min(latestSessionStartTime, segmentEndTime), segmentEndTime));
//            return SessionKeySchema.toBinary(sessionKey);
//        }
//    }
//}
