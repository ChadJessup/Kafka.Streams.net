using System;
using System.Collections;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Common.Extensions;
using Kafka.Common.Utils;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.State.Internals
{
    public class CacheIteratorWrapper : IPeekingKeyValueIterator<Bytes, LRUCacheEntry>
    {
        private Bytes keyFrom;
        private Bytes keyTo;

        private TimeSpan segmentInterval;
        private DateTime latestSessionStartTime;

        private DateTime lastSegmentId;
        private DateTime currentSegmentId;
        
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private MemoryLRUCacheBytesIterator current;

        public KeyValuePair<Bytes, LRUCacheEntry> Current { get; }
        object IEnumerator.Current { get; }

        public CacheIteratorWrapper(
            Bytes key,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
            : this(key, key, earliestSessionEndTime, latestSessionStartTime)
        {
        }

        public CacheIteratorWrapper(
            Bytes keyFrom,
            Bytes keyTo,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;
            // this.lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);
            // this.segmentInterval = cacheFunction.getSegmentInterval();

            // this.currentSegmentId = cacheFunction.segmentId(earliestSessionEndTime);

            this.SetCacheKeyRange(earliestSessionEndTime, this.CurrentSegmentLastTime());

            // this.current = cache.Range(cacheName, cacheKeyFrom, cacheKeyTo);
        }

        public bool HasNext()
        {
            if (this.current == null)
            {
                return false;
            }

            if (this.current.MoveNext())
            {
                return true;
            }

            while (!this.current.MoveNext())
            {
                this.GetNextSegmentIterator();
                if (this.current == null)
                {
                    return false;
                }
            }
            return true;
        }

        public Bytes PeekNextKey()
        {
            if (!this.MoveNext())
            {
                throw new KeyNotFoundException();
            }

            return this.current.PeekNextKey();
        }


        public KeyValuePair<Bytes, LRUCacheEntry> PeekNext()
        {
            if (!this.MoveNext())
            {
                throw new KeyNotFoundException();
            }

            return this.current.Current;
        }

        public KeyValuePair<Bytes, LRUCacheEntry> Next()
        {
            if (!this.MoveNext())
            {
                throw new KeyNotFoundException();
            }

            return this.current.Current;
        }

        public void Close()
        {
            this.current.Close();
        }

        private DateTime CurrentSegmentBeginTime()
        {
            return Timestamp.UnixTimestampMsToDateTime(this.currentSegmentId.ToEpochMilliseconds() * (long)this.segmentInterval.TotalMilliseconds);
        }

        private DateTime CurrentSegmentLastTime()
        {
            return this.CurrentSegmentBeginTime() + this.segmentInterval;// - 1;
        }

        private void GetNextSegmentIterator()
        {
            //++this.currentSegmentId;
            //lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);

            if (this.currentSegmentId > this.lastSegmentId)
            {
                this.current = null;
                return;
            }

            this.SetCacheKeyRange(this.CurrentSegmentBeginTime(), this.CurrentSegmentLastTime());

            this.current.Close();
            // current = cache.Range(cacheName, cacheKeyFrom, cacheKeyTo);
        }

        private void SetCacheKeyRange(DateTime lowerRangeEndTime, DateTime upperRangeEndTime)
        {
            // if (cacheFunction.segmentId(lowerRangeEndTime) != cacheFunction.segmentId(upperRangeEndTime))
            // {
            //     throw new InvalidOperationException("Error iterating over segments: segment interval has changed");
            // }

            if (this.keyFrom == this.keyTo)
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
            IWindowed<Bytes> sessionKey = new Windowed<Bytes>(
                key,
                new SessionWindow(0, Math.Max(0, segmentBeginTime)));

            return SessionKeySchema.ToBinary(sessionKey);
        }

        private Bytes SegmentUpperRangeFixedSize(Bytes key, DateTime segmentEndTime)
        {
            IWindowed<Bytes> sessionKey = new Windowed<Bytes>(
                key, 
                new SessionWindow(this.latestSessionStartTime.GetOldest(segmentEndTime), segmentEndTime));

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
