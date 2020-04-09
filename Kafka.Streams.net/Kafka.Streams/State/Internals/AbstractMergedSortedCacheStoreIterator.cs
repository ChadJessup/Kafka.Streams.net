using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Internals
{
    public abstract class AbstractMergedSortedCacheStoreIterator<K, KS, V, VS> : IKeyValueIterator<K, V>
    {
        private IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
        private IKeyValueIterator<KS, VS> storeIterator;
        private readonly KafkaStreamsContext context;

        public KeyValuePair<K, V> Current { get; }
        object IEnumerator.Current { get; }

        public AbstractMergedSortedCacheStoreIterator(
            KafkaStreamsContext context,
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
            IKeyValueIterator<KS, VS> storeIterator)
        {
            this.context = context;
            this.cacheIterator = cacheIterator;
            this.storeIterator = storeIterator;
        }

        public abstract int Compare(Bytes cacheKey, KS storeKey);
        public abstract K DeserializeStoreKey(KS key);
        public abstract KeyValuePair<K, V> DeserializeStorePair(KeyValuePair<KS, VS> pair);
        public abstract K DeserializeCacheKey(Bytes cacheKey);
        public abstract V DeserializeCacheValue(LRUCacheEntry cacheEntry);

        private bool IsDeletedCacheEntry(KeyValuePair<Bytes, LRUCacheEntry>? nextFromCache)
        {
            return nextFromCache?.Value.Value() == null;
        }

        public bool HasNext()
        {
            // skip over items deleted from cache, and corresponding store items if they have the same key
            while (cacheIterator.MoveNext() && IsDeletedCacheEntry(cacheIterator.PeekNext()))
            {
                if (storeIterator.Current.Key != null)
                {
                    KS nextStoreKey = storeIterator.PeekNextKey();
                    // advance the store iterator if the key is the same as the deleted cache key
                    if (Compare(cacheIterator.PeekNextKey(), nextStoreKey) == 0)
                    {
                        storeIterator.MoveNext();
                    }
                }
                cacheIterator.MoveNext();
            }

            return cacheIterator.MoveNext() || storeIterator.MoveNext();
        }

        public KeyValuePair<K, V> Next()
        {
            if (!HasNext())
            {
                throw new KeyNotFoundException();
            }

            Bytes nextCacheKey = cacheIterator.MoveNext()
                ? cacheIterator.PeekNextKey()
                : null;

            KS nextStoreKey = storeIterator.MoveNext()
                ? storeIterator.PeekNextKey()
                : default;

            if (nextCacheKey == null)
            {
                return NextStoreValue(nextStoreKey);
            }

            if (nextStoreKey == null)
            {
                return nextCacheValue(nextCacheKey);
            }

            int comparison = Compare(nextCacheKey, nextStoreKey);
            if (comparison > 0)
            {
                return NextStoreValue(nextStoreKey);
            }
            else if (comparison < 0)
            {
                return nextCacheValue(nextCacheKey);
            }
            else
            {
                // skip the same keyed element
                storeIterator.MoveNext();

                return nextCacheValue(nextCacheKey);
            }
        }

        private KeyValuePair<K, V> NextStoreValue(KS nextStoreKey)
        {
            KeyValuePair<KS, VS> next = storeIterator.Current;

            if (!next.Key.Equals(nextStoreKey))
            {
                throw new InvalidOperationException("Next record key is not the peeked key value; this should not happen");
            }

            return DeserializeStorePair(next);
        }

        private KeyValuePair<K, V> nextCacheValue(Bytes nextCacheKey)
        {
            KeyValuePair<Bytes, LRUCacheEntry> next = cacheIterator.Current;

            if (!next.Key.Equals(nextCacheKey))
            {
                throw new InvalidOperationException("Next record key is not the peeked key value; this should not happen");
            }

            return KeyValuePair.Create(DeserializeCacheKey(next.Key), DeserializeCacheValue(next.Value));
        }

        public K PeekNextKey()
        {
            if (!HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            Bytes nextCacheKey = cacheIterator.MoveNext()
                ? cacheIterator.PeekNextKey()
                : null;

            KS nextStoreKey = storeIterator.MoveNext()
                ? storeIterator.PeekNextKey()
                : default;

            if (nextCacheKey == null)
            {
                return DeserializeStoreKey(nextStoreKey);
            }

            if (nextStoreKey == null)
            {
                return DeserializeCacheKey(nextCacheKey);
            }

            int comparison = Compare(nextCacheKey, nextStoreKey);
            if (comparison > 0)
            {
                return DeserializeStoreKey(nextStoreKey);
            }
            else if (comparison < 0)
            {
                return DeserializeCacheKey(nextCacheKey);
            }
            else
            {
                // skip the same keyed element
                storeIterator.MoveNext();

                return DeserializeCacheKey(nextCacheKey);
            }
        }

        public void Remove()
        {
            throw new InvalidOperationException("Remove() is not supported in " + GetType().FullName);
        }

        public void Close()
        {
            cacheIterator.Close();
            storeIterator.Close();
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
