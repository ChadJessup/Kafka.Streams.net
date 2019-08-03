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
package org.apache.kafka.streams.state.internals;

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;

import java.util.NoSuchElementException;

class FilteredCacheIterator : PeekingKeyValueIterator<Bytes, LRUCacheEntry>
{
    private PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator;
    private HasNextCondition hasNextCondition;
    private PeekingKeyValueIterator<Bytes, LRUCacheEntry> wrappedIterator;

    FilteredCacheIterator(PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                          HasNextCondition hasNextCondition,
                          CacheFunction cacheFunction)
{
        this.cacheIterator = cacheIterator;
        this.hasNextCondition = hasNextCondition;
        this.wrappedIterator = new PeekingKeyValueIterator<Bytes, LRUCacheEntry>()
{
            @Override
            public KeyValue<Bytes, LRUCacheEntry> peekNext()
{
                return cachedPair(cacheIterator.peekNext());
            }

            @Override
            public void close()
{
                cacheIterator.close();
            }

            @Override
            public Bytes peekNextKey()
{
                return cacheFunction.key(cacheIterator.peekNextKey());
            }

            @Override
            public bool hasNext()
{
                return cacheIterator.hasNext();
            }

            @Override
            public KeyValue<Bytes, LRUCacheEntry> next()
{
                return cachedPair(cacheIterator.next());
            }

            private KeyValue<Bytes, LRUCacheEntry> cachedPair(KeyValue<Bytes, LRUCacheEntry> next)
{
                return KeyValue.pair(cacheFunction.key(next.key), next.value);
            }

            @Override
            public void remove()
{
                cacheIterator.remove();
            }
        };
    }

    public override void close()
{
        // no-op
    }

    public override Bytes peekNextKey()
{
        if (!hasNext())
{
            throw new NoSuchElementException();
        }
        return cacheIterator.peekNextKey();
    }

    public override bool hasNext()
{
        return hasNextCondition.hasNext(wrappedIterator);
    }

    public override KeyValue<Bytes, LRUCacheEntry> next()
{
        if (!hasNext())
{
            throw new NoSuchElementException();
        }
        return cacheIterator.next();

    }

    public override void remove()
{
        throw new UnsupportedOperationException();
    }

    public override KeyValue<Bytes, LRUCacheEntry> peekNext()
{
        if (!hasNext())
{
            throw new NoSuchElementException();
        }
        return cacheIterator.peekNext();
    }
}
