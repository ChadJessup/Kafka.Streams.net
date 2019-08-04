/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractMergedSortedCacheStoreIterator<K, KS, V, VS> : KeyValueIterator<K, V>
{
    private PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
    private KeyValueIterator<KS, VS> storeIterator;

    AbstractMergedSortedCacheStoreIterator(
        PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
        KeyValueIterator<KS, VS> storeIterator)
    {
        this.cacheIterator = cacheIterator;
        this.storeIterator = storeIterator;
    }

    abstract int compare(Bytes cacheKey, KS storeKey);

    abstract K deserializeStoreKey(KS key);

    abstract KeyValue<K, V> deserializeStorePair(KeyValue<KS, VS> pair);

    abstract K deserializeCacheKey(Bytes cacheKey);

    abstract V deserializeCacheValue(LRUCacheEntry cacheEntry);

    private bool isDeletedCacheEntry(KeyValue<Bytes, LRUCacheEntry> nextFromCache)
{
        return nextFromCache.value.value() == null;
    }

    public override bool hasNext()
{
        // skip over items deleted from cache, and corresponding store items if they have the same key
        while (cacheIterator.hasNext() && isDeletedCacheEntry(cacheIterator.peekNext()))
{
            if (storeIterator.hasNext())
{
                KS nextStoreKey = storeIterator.peekNextKey();
                // advance the store iterator if the key is the same as the deleted cache key
                if (compare(cacheIterator.peekNextKey(), nextStoreKey) == 0)
{
                    storeIterator.next();
                }
            }
            cacheIterator.next();
        }

        return cacheIterator.hasNext() || storeIterator.hasNext();
    }

    public override KeyValue<K, V> next()
{
        if (!hasNext())
{
            throw new NoSuchElementException();
        }

        Bytes nextCacheKey = cacheIterator.hasNext() ? cacheIterator.peekNextKey() : null;
        KS nextStoreKey = storeIterator.hasNext() ? storeIterator.peekNextKey() : null;

        if (nextCacheKey == null)
{
            return nextStoreValue(nextStoreKey);
        }

        if (nextStoreKey == null)
{
            return nextCacheValue(nextCacheKey);
        }

        int comparison = compare(nextCacheKey, nextStoreKey);
        if (comparison > 0)
{
            return nextStoreValue(nextStoreKey);
        } else if (comparison < 0)
{
            return nextCacheValue(nextCacheKey);
        } else
{
            // skip the same keyed element
            storeIterator.next();
            return nextCacheValue(nextCacheKey);
        }
    }

    private KeyValue<K, V> nextStoreValue(KS nextStoreKey)
{
        KeyValue<KS, VS> next = storeIterator.next();

        if (!next.key.Equals(nextStoreKey))
{
            throw new InvalidOperationException("Next record key is not the peeked key value; this should not happen");
        }

        return deserializeStorePair(next);
    }

    private KeyValue<K, V> nextCacheValue(Bytes nextCacheKey)
{
        KeyValue<Bytes, LRUCacheEntry> next = cacheIterator.next();

        if (!next.key.Equals(nextCacheKey))
{
            throw new InvalidOperationException("Next record key is not the peeked key value; this should not happen");
        }

        return KeyValue.pair(deserializeCacheKey(next.key), deserializeCacheValue(next.value));
    }

    public override K peekNextKey()
{
        if (!hasNext())
{
            throw new NoSuchElementException();
        }

        Bytes nextCacheKey = cacheIterator.hasNext() ? cacheIterator.peekNextKey() : null;
        KS nextStoreKey = storeIterator.hasNext() ? storeIterator.peekNextKey() : null;

        if (nextCacheKey == null)
{
            return deserializeStoreKey(nextStoreKey);
        }

        if (nextStoreKey == null)
{
            return deserializeCacheKey(nextCacheKey);
        }

        int comparison = compare(nextCacheKey, nextStoreKey);
        if (comparison > 0)
{
            return deserializeStoreKey(nextStoreKey);
        } else if (comparison < 0)
{
            return deserializeCacheKey(nextCacheKey);
        } else
{
            // skip the same keyed element
            storeIterator.next();
            return deserializeCacheKey(nextCacheKey);
        }
    }

    public override void Remove()
{
        throw new InvalidOperationException("Remove() is not supported in " + GetType().getName());
    }

    public override void close()
{
        cacheIterator.close();
        storeIterator.close();
    }
}

