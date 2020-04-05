using System;
using System.Collections;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class MemoryLRUCacheBytesIterator : IPeekingKeyValueIterator<Bytes, LRUCacheEntry>
    {
        private readonly IEnumerator<KeyValuePair<Bytes, LRUNode>> underlying;
        private KeyValuePair<Bytes, LRUCacheEntry>? nextEntry;

        public KeyValuePair<Bytes, LRUCacheEntry> Current { get; }
        object IEnumerator.Current { get; }

        public MemoryLRUCacheBytesIterator()
        { }

        public MemoryLRUCacheBytesIterator(IEnumerator<KeyValuePair<Bytes, LRUNode>> underlying)
            : this()
        {
            this.underlying = underlying;
        }

        public Bytes? PeekNextKey()
        {
            if (!HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return nextEntry?.Key;
        }

        public KeyValuePair<Bytes, LRUCacheEntry>? PeekNext()
        {
            if (!HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return nextEntry;
        }


        public bool HasNext()
        {
            if (nextEntry != null)
            {
                return true;
            }

            while (underlying.MoveNext() && nextEntry == null)
            {
                InternalNext();
            }

            return nextEntry != null;
        }

        public KeyValuePair<Bytes, LRUCacheEntry>? Next()
        {
            if (!HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            var result = nextEntry;
            nextEntry = null;

            return result;
        }

        private void InternalNext()
        {
            KeyValuePair<Bytes, LRUNode> mapEntry = underlying.Current;
            Bytes cacheKey = mapEntry.Key;
            LRUCacheEntry entry = mapEntry.Value.entry;

            if (entry == null)
            {
                return;
            }

            nextEntry = new KeyValuePair<Bytes, LRUCacheEntry>(cacheKey, entry);
        }

        public void Remove()
        {
            throw new InvalidOperationException("Remove not supported by MemoryLRUCacheBytesIterator");
        }

        public void Close()
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
