using Kafka.Common.Utils;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.Processor.Internals.Metrics;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{
    /**
     * An in-memory LRU cache store similar to {@link MemoryLRUCache} but byte-based, not
     * record based
     */
    public class ThreadCache
    {
        private ILogger log;
        private long maxCacheSizeBytes;
        private StreamsMetricsImpl metrics;
        private Dictionary<string, NamedCache> caches = new Dictionary<string, NamedCache>();

        // internal stats
        private long numPuts = 0;
        private long numGets = 0;
        private long numEvicts = 0;
        private long numFlushes = 0;

        public ThreadCache(
            LogContext logContext,
            long maxCacheSizeBytes,
            StreamsMetricsImpl metrics)
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
            string[] tokens = cacheName.Split(new[] { '-' }, 2);
            return tokens[0];
        }

        /**
         * Given a cache name of the form taskid-storename, return the store name.
         * @param cacheName
         * @return
         */
        public static string underlyingStoreNamefromCacheName(string cacheName)
        {
            string[] tokens = cacheName.Split(new[] { '-' }, 2);
            return tokens[1];
        }


        /**
         * Add a listener that is called each time an entry is evicted from the cache or an explicit flush is called
         *
         * @param @namespace
         * @param listener
         */
        public void addDirtyEntryFlushListener(string @namespace, IDirtyEntryFlushListener listener)
        {
            NamedCache cache = getOrCreateCache(@namespace);
            cache.setListener(listener);
        }

        public void flush(string @namespace)
        {
            numFlushes++;

            NamedCache cache = getCache(@namespace);
            if (cache == null)
            {
                return;
            }
            cache.flush();

            log.LogTrace("Cache stats on flush: #puts={}, #gets={}, #evicts={}, #flushes={}", puts(), gets(), evicts(), flushes());
        }

        public LRUCacheEntry get(string @namespace, Bytes key)
        {
            numGets++;

            if (key == null)
            {
                return null;
            }

            NamedCache cache = getCache(@namespace);
            if (cache == null)
            {
                return null;
            }
            return cache[key];
        }

        public void put(string @namespace, Bytes key, LRUCacheEntry value)
        {
            numPuts++;

            NamedCache cache = getOrCreateCache(@namespace);
            cache.Add(key, value);
            maybeEvict(@namespace);
        }

        public LRUCacheEntry putIfAbsent(string @namespace, Bytes key, LRUCacheEntry value)
        {
            NamedCache cache = getOrCreateCache(@namespace);

            LRUCacheEntry result = cache.putIfAbsent(key, value);
            maybeEvict(@namespace);

            if (result == null)
            {
                numPuts++;
            }
            return result;
        }

        public void putAll(string @namespace, List<KeyValue<Bytes, LRUCacheEntry>> entries)
        {
            foreach (KeyValue<Bytes, LRUCacheEntry> entry in entries)
            {
                put(@namespace, entry.key, entry.value);
            }
        }

        public LRUCacheEntry delete(string @namespace, Bytes key)
        {
            NamedCache cache = getCache(@namespace);
            if (cache == null)
            {
                return null;
            }

            return cache.delete(key);
        }

        public MemoryLRUCacheBytesIterator range(string @namespace, Bytes from, Bytes to)
        {
            NamedCache cache = getCache(@namespace);
            if (cache == null)
            {
                return new MemoryLRUCacheBytesIterator(Collections.emptyIterator());
            }
            return new MemoryLRUCacheBytesIterator(cache.subMapIterator(from, to));
        }

        public MemoryLRUCacheBytesIterator all(string @namespace)
        {
            NamedCache cache = getCache(@namespace);
            if (cache == null)
            {
                return new MemoryLRUCacheBytesIterator();
            }
            return new MemoryLRUCacheBytesIterator(cache.allIterator());
        }

        public long size()
        {
            long size = 0;
            foreach (NamedCache cache in caches.Values)
            {
                size += cache.size();
                if (isOverflowing(size))
                {
                    return long.MaxValue;
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
            foreach (NamedCache namedCache in caches.Values)
            {
                sizeInBytes += namedCache.sizeInBytes();
                if (isOverflowing(sizeInBytes))
                {
                    return long.MaxValue;
                }
            }
            return sizeInBytes;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        void close(string @namespace)
        {
            NamedCache removed = caches.Remove(@namespace);
            if (removed != null)
            {
                removed.close();
            }
        }

        private void maybeEvict(string @namespace)
        {
            int numEvicted = 0;
            while (sizeBytes() > maxCacheSizeBytes)
            {
                NamedCache cache = getOrCreateCache(@namespace);
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

            log.LogTrace("Evicted {} entries from cache {}", numEvicted, @namespace);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private NamedCache getCache(string @namespace)
        {
            return caches[@namespace];
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private NamedCache getOrCreateCache(string name)
        {
            NamedCache cache = caches[name];
            if (cache == null)
            {
                cache = new NamedCache(name, this.metrics);
                caches.Add(name, cache);
            }
            return cache;
        }
    }
}
