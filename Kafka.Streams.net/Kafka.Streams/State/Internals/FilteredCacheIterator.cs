using Kafka.Common.Utils;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class FilteredCacheIterator : IPeekingKeyValueIterator<Bytes, LRUCacheEntry>
    {
        private IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
        private bool hasNextCondition;
        private IPeekingKeyValueIterator<Bytes, LRUCacheEntry> wrappedIterator;

        public KeyValuePair<Bytes, LRUCacheEntry> Current { get; }
        object IEnumerator.Current { get; }

        public FilteredCacheIterator(
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
            bool hasNextCondition,
            ICacheFunction cacheFunction)
        {
            this.cacheIterator = cacheIterator;
            this.hasNextCondition = hasNextCondition;
            this.wrappedIterator = null; // new IPeekingKeyValueIterator<Bytes, LRUCacheEntry>()
                                         //    {


            //    public KeyValuePair<Bytes, LRUCacheEntry> PeekNext()
            //    {
            //        return cachedPair(cacheIterator.PeekNext());
            //    }


            //    public void close()
            //    {
            //        cacheIterator.close();
            //    }


            //    public Bytes PeekNextKey()
            //    {
            //        return cacheFunction.key(cacheIterator.PeekNextKey());
            //    }


            //    public bool HasNext()
            //    {
            //        return cacheIterator.HasNext();
            //    }


            //    public KeyValuePair<Bytes, LRUCacheEntry> next()
            //    {
            //        return cachedPair(cacheIterator.MoveNext());
            //    }

            //    private KeyValuePair<Bytes, LRUCacheEntry> cachedPair(KeyValuePair<Bytes, LRUCacheEntry> next)
            //    {
            //        return KeyValuePair.pair(cacheFunction.key(next.key), next.value);
            //    }


            //    public void Remove()
            //    {
            //        cacheIterator.Remove();
            //    }
            //};
        }

        public void Close()
        {
            // no-op
        }

        public Bytes PeekNextKey()
        {
            if (!HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return cacheIterator.PeekNextKey();
        }

        public bool HasNext()
        {
            return hasNextCondition; //.HasNext(wrappedIterator);
        }

        public KeyValuePair<Bytes, LRUCacheEntry>? MoveNext()
        {
            if (!HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return cacheIterator.PeekNext();
        }

        public void Remove()
        {
            throw new InvalidOperationException();
        }

        public KeyValuePair<Bytes, LRUCacheEntry>? PeekNext()
        {
            if (!HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return cacheIterator.PeekNext();
        }

        bool IEnumerator.MoveNext()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
        }

        public void Dispose()
        {
        }
    }
}
