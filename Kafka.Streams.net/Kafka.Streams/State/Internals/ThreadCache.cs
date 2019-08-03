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
using Kafka.Common.Utils.LogContext;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Processor.internals.metrics.StreamsMetricsImpl;
using Kafka.Streams.State.internals.NamedCache.LRUNode;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * An in-memory LRU cache store similar to {@link MemoryLRUCache} but byte-based, not
 * record based
 */
public class ThreadCache
{
    private Logger log;
    private long maxCacheSizeBytes;
    private StreamsMetricsImpl metrics;
    private Dictionary<string, NamedCache> caches = new HashMap<>();

    // internal stats
    private long numPuts = 0;
    private long numGets = 0;
    private long numEvicts = 0;
    private long numFlushes = 0;

    public interface DirtyEntryFlushListener
{
        void apply(List<DirtyEntry> dirty);
    }

    public ThreadCache(LogContext logContext, long maxCacheSizeBytes, StreamsMetricsImpl metrics)
{
        this.maxCacheSizeBytes = maxCacheSizeBytes;
        this.metrics = metrics;
        this.log = logContext.logger(GetType());
    }

    public long puts()
{
        return numPuts;
    }

    public long gets()
{
        return numGets;
    }

    public long evicts()
{
        return numEvicts;
    }

    public long flushes()
{
        return numFlushes;
    }

    /**
     * The thread cache maintains a set of {@link NamedCache}s whose names are a concatenation of the task ID and the
     * underlying store name. This method creates those names.
     * @param taskIDString Task ID
     * @param underlyingStoreName Underlying store name
     * @return
     */
    public static string nameSpaceFromTaskIdAndStore(string taskIDString, string underlyingStoreName)
{
        return taskIDString + "-" + underlyingStoreName;
    }

    /**
     * Given a cache name of the form taskid-storename, return the task ID.
     * @param cacheName
     * @return
     */
    public static string taskIDfromCacheName(string cacheName)
{
        string[] tokens = cacheName.split("-", 2);
        return tokens[0];
    }

    /**
     * Given a cache name of the form taskid-storename, return the store name.
     * @param cacheName
     * @return
     */
    public static string underlyingStoreNamefromCacheName(string cacheName)
{
        string[] tokens = cacheName.split("-", 2);
        return tokens[1];
    }


    /**
     * Add a listener that is called each time an entry is evicted from the cache or an explicit flush is called
     *
     * @param namespace
     * @param listener
     */
    public void addDirtyEntryFlushListener(string namespace, DirtyEntryFlushListener listener)
{
        NamedCache cache = getOrCreateCache(namespace);
        cache.setListener(listener);
    }

    public void flush(string namespace)
{
        numFlushes++;

        NamedCache cache = getCache(namespace);
        if (cache == null)
{
            return;
        }
        cache.flush();

        if (log.isTraceEnabled())
{
            log.trace("Cache stats on flush: #puts={}, #gets={}, #evicts={}, #flushes={}", puts(), gets(), evicts(), flushes());
        }
    }

    public LRUCacheEntry get(string namespace, Bytes key)
{
        numGets++;

        if (key == null)
{
            return null;
        }

        NamedCache cache = getCache(namespace);
        if (cache == null)
{
            return null;
        }
        return cache.get(key);
    }

    public void put(string namespace, Bytes key, LRUCacheEntry value)
{
        numPuts++;

        NamedCache cache = getOrCreateCache(namespace);
        cache.put(key, value);
        maybeEvict(namespace);
    }

    public LRUCacheEntry putIfAbsent(string namespace, Bytes key, LRUCacheEntry value)
{
        NamedCache cache = getOrCreateCache(namespace);

        LRUCacheEntry result = cache.putIfAbsent(key, value);
        maybeEvict(namespace);

        if (result == null)
{
            numPuts++;
        }
        return result;
    }

    public void putAll(string namespace, List<KeyValue<Bytes, LRUCacheEntry>> entries)
{
        for (KeyValue<Bytes, LRUCacheEntry> entry : entries)
{
            put(namespace, entry.key, entry.value);
        }
    }

    public LRUCacheEntry delete(string namespace, Bytes key)
{
        NamedCache cache = getCache(namespace);
        if (cache == null)
{
            return null;
        }

        return cache.delete(key);
    }

    public MemoryLRUCacheBytesIterator range(string namespace, Bytes from, Bytes to)
{
        NamedCache cache = getCache(namespace);
        if (cache == null)
{
            return new MemoryLRUCacheBytesIterator(Collections.emptyIterator());
        }
        return new MemoryLRUCacheBytesIterator(cache.subMapIterator(from, to));
    }

    public MemoryLRUCacheBytesIterator all(string namespace)
{
        NamedCache cache = getCache(namespace);
        if (cache == null)
{
            return new MemoryLRUCacheBytesIterator(Collections.emptyIterator());
        }
        return new MemoryLRUCacheBytesIterator(cache.allIterator());
    }
    
    public long size()
{
        long size = 0;
        for (NamedCache cache : caches.values())
{
            size += cache.size();
            if (isOverflowing(size))
{
                return Long.MAX_VALUE;
            }
        }
        return size;
    }

    private bool isOverflowing(long size)
{
        return size < 0;
    }

    long sizeBytes()
{
        long sizeInBytes = 0;
        for (NamedCache namedCache : caches.values())
{
            sizeInBytes += namedCache.sizeInBytes();
            if (isOverflowing(sizeInBytes))
{
                return Long.MAX_VALUE;
            }
        }
        return sizeInBytes;
    }

    synchronized void close(string namespace)
{
        NamedCache removed = caches.remove(namespace);
        if (removed != null)
{
            removed.close();
        }
    }

    private void maybeEvict(string namespace)
{
        int numEvicted = 0;
        while (sizeBytes() > maxCacheSizeBytes)
{
            NamedCache cache = getOrCreateCache(namespace);
            // we abort here as the put on this cache may have triggered
            // a put on another cache. So even though the sizeInBytes() is
            // still > maxCacheSizeBytes there is nothing to evict from this
            // namespaced cache.
            if (cache.size() == 0)
{
                return;
            }
            cache.evict();
            numEvicts++;
            numEvicted++;
        }
        if (log.isTraceEnabled())
{
            log.trace("Evicted {} entries from cache {}", numEvicted, namespace);
        }
    }

    private synchronized NamedCache getCache(string namespace)
{
        return caches.get(namespace);
    }

    private synchronized NamedCache getOrCreateCache(string name)
{
        NamedCache cache = caches.get(name);
        if (cache == null)
{
            cache = new NamedCache(name, this.metrics);
            caches.put(name, cache);
        }
        return cache;
    }

    static class MemoryLRUCacheBytesIterator : PeekingKeyValueIterator<Bytes, LRUCacheEntry>
{
        private Iterator<Map.Entry<Bytes, LRUNode>> underlying;
        private KeyValue<Bytes, LRUCacheEntry> nextEntry;

        MemoryLRUCacheBytesIterator(Iterator<Map.Entry<Bytes, LRUNode>> underlying)
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

        @Override
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

        @Override
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
            Map.Entry<Bytes, LRUNode> mapEntry = underlying.next();
            Bytes cacheKey = mapEntry.getKey();
            LRUCacheEntry entry = mapEntry.getValue().entry();
            if (entry == null)
{
                return;
            }

            nextEntry = new KeyValue<>(cacheKey, entry);
        }

        @Override
        public void remove()
{
            throw new UnsupportedOperationException("remove not supported by MemoryLRUCacheBytesIterator");
        }

        @Override
        public void close()
{
            // do nothing
        }
    }

    static class DirtyEntry
{
        private Bytes key;
        private byte[] newValue;
        private LRUCacheEntry recordContext;

        DirtyEntry(Bytes key, byte[] newValue, LRUCacheEntry recordContext)
{
            this.key = key;
            this.newValue = newValue;
            this.recordContext = recordContext;
        }

        public Bytes key()
{
            return key;
        }

        public byte[] newValue()
{
            return newValue;
        }

        public LRUCacheEntry entry()
{
            return recordContext;
        }
    }
}
