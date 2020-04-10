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
            while (this.cacheIterator.MoveNext() && this.IsDeletedCacheEntry(this.cacheIterator.PeekNext()))
            {
                if (this.storeIterator.Current.Key != null)
                {
                    KS nextStoreKey = this.storeIterator.PeekNextKey();
                    // advance the store iterator if the key is the same as the deleted cache key
                    if (this.Compare(this.cacheIterator.PeekNextKey(), nextStoreKey) == 0)
                    {
                        this.storeIterator.MoveNext();
                    }
                }
                this.cacheIterator.MoveNext();
            }

            return this.cacheIterator.MoveNext() || this.storeIterator.MoveNext();
        }

        public KeyValuePair<K, V> Next()
        {
            if (!this.HasNext())
            {
                throw new KeyNotFoundException();
            }

            Bytes nextCacheKey = this.cacheIterator.MoveNext()
                ? this.cacheIterator.PeekNextKey()
                : null;

            KS nextStoreKey = this.storeIterator.MoveNext()
                ? this.storeIterator.PeekNextKey()
                : default;

            if (nextCacheKey == null)
            {
                return this.NextStoreValue(nextStoreKey);
            }

            if (nextStoreKey == null)
            {
                return this.nextCacheValue(nextCacheKey);
            }

            int comparison = this.Compare(nextCacheKey, nextStoreKey);
            if (comparison > 0)
            {
                return this.NextStoreValue(nextStoreKey);
            }
            else if (comparison < 0)
            {
                return this.nextCacheValue(nextCacheKey);
            }
            else
            {
                // skip the same keyed element
                this.storeIterator.MoveNext();

                return this.nextCacheValue(nextCacheKey);
            }
        }

        private KeyValuePair<K, V> NextStoreValue(KS nextStoreKey)
        {
            KeyValuePair<KS, VS> next = this.storeIterator.Current;

            if (!next.Key.Equals(nextStoreKey))
            {
                throw new InvalidOperationException("Next record key is not the peeked key value; this should not happen");
            }

            return this.DeserializeStorePair(next);
        }

        private KeyValuePair<K, V> nextCacheValue(Bytes nextCacheKey)
        {
            KeyValuePair<Bytes, LRUCacheEntry> next = this.cacheIterator.Current;

            if (!next.Key.Equals(nextCacheKey))
            {
                throw new InvalidOperationException("Next record key is not the peeked key value; this should not happen");
            }

            return KeyValuePair.Create(this.DeserializeCacheKey(next.Key), this.DeserializeCacheValue(next.Value));
        }

        public K PeekNextKey()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            Bytes nextCacheKey = this.cacheIterator.MoveNext()
                ? this.cacheIterator.PeekNextKey()
                : null;

            KS nextStoreKey = this.storeIterator.MoveNext()
                ? this.storeIterator.PeekNextKey()
                : default;

            if (nextCacheKey == null)
            {
                return this.DeserializeStoreKey(nextStoreKey);
            }

            if (nextStoreKey == null)
            {
                return this.DeserializeCacheKey(nextCacheKey);
            }

            int comparison = this.Compare(nextCacheKey, nextStoreKey);
            if (comparison > 0)
            {
                return this.DeserializeStoreKey(nextStoreKey);
            }
            else if (comparison < 0)
            {
                return this.DeserializeCacheKey(nextCacheKey);
            }
            else
            {
                // skip the same keyed element
                this.storeIterator.MoveNext();

                return this.DeserializeCacheKey(nextCacheKey);
            }
        }

        public void Remove()
        {
            throw new InvalidOperationException("Remove() is not supported in " + this.GetType().FullName);
        }

        public void Close()
        {
            this.cacheIterator.Close();
            this.storeIterator.Close();
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
