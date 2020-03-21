
//public abstract class AbstractMergedSortedCacheStoreIterator<K, KS, V, VS> : KeyValueIterator<K, V>
//{
//    private PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
//    private KeyValueIterator<KS, VS> storeIterator;

//    AbstractMergedSortedCacheStoreIterator(
//        PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
//        KeyValueIterator<KS, VS> storeIterator)
//    {
//        this.cacheIterator = cacheIterator;
//        this.storeIterator = storeIterator;
//    }

//    abstract int compare(Bytes cacheKey, KS storeKey);

//    abstract K deserializeStoreKey(KS key);

//    abstract KeyValuePair<K, V> deserializeStorePair(KeyValuePair<KS, VS> pair);

//    abstract K deserializeCacheKey(Bytes cacheKey);

//    abstract V deserializeCacheValue(LRUCacheEntry cacheEntry);

//    private bool isDeletedCacheEntry(KeyValuePair<Bytes, LRUCacheEntry> nextFromCache)
//{
//        return nextFromCache.value.value() == null;
//    }

//    public override bool hasNext()
//{
//        // skip over items deleted from cache, and corresponding store items if they have the same key
//        while (cacheIterator.hasNext() && isDeletedCacheEntry(cacheIterator.peekNext()))
//{
//            if (storeIterator.hasNext())
//{
//                KS nextStoreKey = storeIterator.peekNextKey();
//                // advance the store iterator if the key is the same as the deleted cache key
//                if (compare(cacheIterator.peekNextKey(), nextStoreKey) == 0)
//{
//                    storeIterator.next();
//                }
//            }
//            cacheIterator.next();
//        }

//        return cacheIterator.hasNext() || storeIterator.hasNext();
//    }

//    public override KeyValuePair<K, V> next()
//{
//        if (!hasNext())
//{
//            throw new NoSuchElementException();
//        }

//        Bytes nextCacheKey = cacheIterator.hasNext() ? cacheIterator.peekNextKey() : null;
//        KS nextStoreKey = storeIterator.hasNext() ? storeIterator.peekNextKey() : null;

//        if (nextCacheKey == null)
//{
//            return nextStoreValue(nextStoreKey);
//        }

//        if (nextStoreKey == null)
//{
//            return nextCacheValue(nextCacheKey);
//        }

//        int comparison = compare(nextCacheKey, nextStoreKey);
//        if (comparison > 0)
//{
//            return nextStoreValue(nextStoreKey);
//        } else if (comparison < 0)
//{
//            return nextCacheValue(nextCacheKey);
//        } else
//{
//            // skip the same keyed element
//            storeIterator.next();
//            return nextCacheValue(nextCacheKey);
//        }
//    }

//    private KeyValuePair<K, V> nextStoreValue(KS nextStoreKey)
//{
//        KeyValuePair<KS, VS> next = storeIterator.next();

//        if (!next.key.Equals(nextStoreKey))
//{
//            throw new InvalidOperationException("Next record key is not the peeked key value; this should not happen");
//        }

//        return deserializeStorePair(next);
//    }

//    private KeyValuePair<K, V> nextCacheValue(Bytes nextCacheKey)
//{
//        KeyValuePair<Bytes, LRUCacheEntry> next = cacheIterator.next();

//        if (!next.key.Equals(nextCacheKey))
//{
//            throw new InvalidOperationException("Next record key is not the peeked key value; this should not happen");
//        }

//        return KeyValuePair.pair(deserializeCacheKey(next.key), deserializeCacheValue(next.value));
//    }

//    public override K peekNextKey()
//{
//        if (!hasNext())
//{
//            throw new NoSuchElementException();
//        }

//        Bytes nextCacheKey = cacheIterator.hasNext() ? cacheIterator.peekNextKey() : null;
//        KS nextStoreKey = storeIterator.hasNext() ? storeIterator.peekNextKey() : null;

//        if (nextCacheKey == null)
//{
//            return deserializeStoreKey(nextStoreKey);
//        }

//        if (nextStoreKey == null)
//{
//            return deserializeCacheKey(nextCacheKey);
//        }

//        int comparison = compare(nextCacheKey, nextStoreKey);
//        if (comparison > 0)
//{
//            return deserializeStoreKey(nextStoreKey);
//        } else if (comparison < 0)
//{
//            return deserializeCacheKey(nextCacheKey);
//        } else
//{
//            // skip the same keyed element
//            storeIterator.next();
//            return deserializeCacheKey(nextCacheKey);
//        }
//    }

//    public override void Remove()
//{
//        throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
//    }

//    public override void close()
//{
//        cacheIterator.close();
//        storeIterator.close();
//    }
//}

