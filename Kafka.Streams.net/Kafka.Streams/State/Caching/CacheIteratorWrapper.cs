using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Common.Utils;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.State.Internals
{
    public class CacheIteratorWrapper : IPeekingKeyValueIterator<Bytes, LRUCacheEntry>
    {
        private long segmentInterval;

        private Bytes keyFrom;
        private Bytes keyTo;
        private long latestSessionStartTime;
        private long lastSegmentId;

        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private MemoryLRUCacheBytesIterator current;

        public KeyValuePair<Bytes, LRUCacheEntry> Current { get; }
        object IEnumerator.Current { get; }

        public CacheIteratorWrapper(
            Bytes key,
            long earliestSessionEndTime,
            long latestSessionStartTime)
            : this(key, key, earliestSessionEndTime, latestSessionStartTime)
        {
        }

        public CacheIteratorWrapper(
            Bytes keyFrom,
            Bytes keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;
            // this.lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);
            // this.segmentInterval = cacheFunction.getSegmentInterval();

            // this.currentSegmentId = cacheFunction.segmentId(earliestSessionEndTime);

            SetCacheKeyRange(earliestSessionEndTime, CurrentSegmentLastTime());

            // this.current = cache.Range(cacheName, cacheKeyFrom, cacheKeyTo);
        }

        public bool HasNext()
        {
            if (current == null)
            {
                return false;
            }

            if (current.HasNext())
            {
                return true;
            }

            while (!current.HasNext())
            {
                GetNextSegmentIterator();
                if (current == null)
                {
                    return false;
                }
            }
            return true;
        }

        public Bytes PeekNextKey()
        {
            if (!HasNext())
            {
                throw new KeyNotFoundException();
            }

            return current.PeekNextKey();
        }


        public KeyValuePair<Bytes, LRUCacheEntry> PeekNext()
        {
            if (!HasNext())
            {
                throw new KeyNotFoundException();
            }

            return current.Current;
        }


        public KeyValuePair<Bytes, LRUCacheEntry> Next()
        {
            if (!HasNext())
            {
                throw new KeyNotFoundException();
            }

            return current.Current;
        }


        public void Close()
        {
            current.Close();
        }

        private long CurrentSegmentBeginTime()
        {
            return currentSegmentId * segmentInterval;
        }

        private long CurrentSegmentLastTime()
        {
            return CurrentSegmentBeginTime() + segmentInterval - 1;
        }

        private void GetNextSegmentIterator()
        {
            ++currentSegmentId;
            //lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);

            if (currentSegmentId > lastSegmentId)
            {
                current = null;
                return;
            }

            SetCacheKeyRange(CurrentSegmentBeginTime(), CurrentSegmentLastTime());

            current.Close();
            // current = cache.Range(cacheName, cacheKeyFrom, cacheKeyTo);
        }

        private void SetCacheKeyRange(long lowerRangeEndTime, long upperRangeEndTime)
        {
            // if (cacheFunction.segmentId(lowerRangeEndTime) != cacheFunction.segmentId(upperRangeEndTime))
            // {
            //     throw new InvalidOperationException("Error iterating over segments: segment interval has changed");
            // }

            if (keyFrom == keyTo)
            {
                // cacheKeyFrom = cacheFunction.cacheKey(SegmentLowerRangeFixedSize(keyFrom, lowerRangeEndTime));
                // cacheKeyTo = cacheFunction.cacheKey(SegmentUpperRangeFixedSize(keyTo, upperRangeEndTime));
            }
            else
            {
                // cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                // cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime), currentSegmentId);
            }
        }

        private Bytes SegmentLowerRangeFixedSize(Bytes key, long segmentBeginTime)
        {
            Windowed<Bytes> sessionKey = new Windowed<Bytes>(
                key,
                new SessionWindow(0, Math.Max(0, segmentBeginTime)));

            return SessionKeySchema.ToBinary(sessionKey);
        }

        private Bytes SegmentUpperRangeFixedSize(Bytes key, long segmentEndTime)
        {
            Windowed<Bytes> sessionKey = new Windowed<Bytes>(key, new SessionWindow(Math.Min(latestSessionStartTime, segmentEndTime), segmentEndTime));
            return SessionKeySchema.ToBinary(sessionKey);
        }

        KeyValuePair<Bytes, LRUCacheEntry>? IPeekingKeyValueIterator<Bytes, LRUCacheEntry>.PeekNext()
        {
            throw new NotImplementedException();
        }

        public bool MoveNext()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
