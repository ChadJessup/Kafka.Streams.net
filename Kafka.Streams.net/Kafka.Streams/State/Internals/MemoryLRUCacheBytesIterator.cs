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
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return this.nextEntry?.Key;
        }

        public KeyValuePair<Bytes, LRUCacheEntry>? PeekNext()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return this.nextEntry;
        }


        public bool HasNext()
        {
            if (this.nextEntry != null)
            {
                return true;
            }

            while (this.underlying.MoveNext() && this.nextEntry == null)
            {
                this.InternalNext();
            }

            return this.nextEntry != null;
        }

        public KeyValuePair<Bytes, LRUCacheEntry>? Next()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            var result = this.nextEntry;
            this.nextEntry = null;

            return result;
        }

        private void InternalNext()
        {
            KeyValuePair<Bytes, LRUNode> mapEntry = this.underlying.Current;
            Bytes cacheKey = mapEntry.Key;
            LRUCacheEntry entry = mapEntry.Value.entry;

            if (entry == null)
            {
                return;
            }

            this.nextEntry = new KeyValuePair<Bytes, LRUCacheEntry>(cacheKey, entry);
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
