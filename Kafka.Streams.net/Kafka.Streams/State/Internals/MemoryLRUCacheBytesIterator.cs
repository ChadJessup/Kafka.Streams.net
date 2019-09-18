using Kafka.Common.Utils;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class MemoryLRUCacheBytesIterator : IPeekingKeyValueIterator<Bytes, LRUCacheEntry>
    {
        private readonly IEnumerator<KeyValuePair<Bytes, LRUNode>> underlying;
        private KeyValue<Bytes, LRUCacheEntry> nextEntry;

        public KeyValue<Bytes, LRUCacheEntry> Current { get; }
        object IEnumerator.Current { get; }

        public MemoryLRUCacheBytesIterator()
        { }

        public MemoryLRUCacheBytesIterator(IEnumerator<KeyValuePair<Bytes, LRUNode>> underlying)
            : this()
        {
            this.underlying = underlying;
        }

        public Bytes peekNextKey()
        {
            if (!hasNext())
            {
                throw new IndexOutOfRangeException();
            }
            return nextEntry.key;
        }


        public KeyValue<Bytes, LRUCacheEntry> peekNext()
        {
            if (!hasNext())
            {
                throw new IndexOutOfRangeException();
            }
            return nextEntry;
        }


        public bool hasNext()
        {
            if (nextEntry != null)
            {
                return true;
            }

            while (underlying.MoveNext() && nextEntry == null)
            {
                internalNext();
            }

            return nextEntry != null;
        }


        public KeyValue<Bytes, LRUCacheEntry> next()
        {
            if (!hasNext())
            {
                throw new IndexOutOfRangeException();
            }
            KeyValue<Bytes, LRUCacheEntry> result = nextEntry;
            nextEntry = null;
            return result;
        }

        private void internalNext()
        {
            KeyValuePair<Bytes, LRUNode> mapEntry = underlying.Current;
            Bytes cacheKey = mapEntry.Key;
            LRUCacheEntry entry = mapEntry.Value.entry;

            if (entry == null)
            {
                return;
            }

            nextEntry = new KeyValue<Bytes, LRUCacheEntry>(cacheKey, entry);
        }


        public void Remove()
        {
            throw new InvalidOperationException("Remove not supported by MemoryLRUCacheBytesIterator");
        }

        public void close()
        {
            // do nothing
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
