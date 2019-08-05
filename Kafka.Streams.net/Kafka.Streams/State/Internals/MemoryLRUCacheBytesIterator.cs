using Kafka.Common.Utils;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class MemoryLRUCacheBytesIterator : PeekingKeyValueIterator<Bytes, LRUCacheEntry>
    {
        private IEnumerator<KeyValuePair<Bytes, LRUNode>> underlying;
        private KeyValue<Bytes, LRUCacheEntry> nextEntry;

        MemoryLRUCacheBytesIterator(IEnumerator<KeyValuePair<Bytes, LRUNode>> underlying)
        {
            this.underlying = underlying;
        }

        public Bytes peekNextKey()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return nextEntry.key;
        }


        public KeyValue<Bytes, LRUCacheEntry> peekNext()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return nextEntry;
        }


        public bool hasNext()
        {
            if (nextEntry != null)
            {
                return true;
            }

            while (underlying.hasNext() && nextEntry == null)
            {
                internalNext();
            }

            return nextEntry != null;
        }


        public KeyValue<Bytes, LRUCacheEntry> next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            KeyValue<Bytes, LRUCacheEntry> result = nextEntry;
            nextEntry = null;
            return result;
        }

        private void internalNext()
        {
            KeyValuePair<Bytes, LRUNode> mapEntry = underlying.next();
            Bytes cacheKey = mapEntry.Key;
            LRUCacheEntry entry = mapEntry.Value.entry();
            if (entry == null)
            {
                return;
            }

            nextEntry = new KeyValue<>(cacheKey, entry);
        }


        public void Remove()
        {
            throw new InvalidOperationException("Remove not supported by MemoryLRUCacheBytesIterator");
        }

        public void close()
        {
            // do nothing
        }
    }
}
