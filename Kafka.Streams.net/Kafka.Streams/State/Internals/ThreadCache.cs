using Kafka.Streams.Configs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{
    /**
     * An in-memory LRU cache store similar to {@link MemoryLRUCache} but byte-based, not
     * record based
     */
    public class ThreadCache
    {
        private readonly StreamsConfig? config;
        private readonly ILogger<ThreadCache> logger;
        private readonly long maxCacheSizeBytes;
        //private readonly StreamsMetricsImpl metrics;
        private readonly Dictionary<string, NamedCache> caches = new Dictionary<string, NamedCache>();

        // internal stats
        private long numPuts = 0;
        private long numGets = 0;
        private long numEvicts = 0;
        private long numFlushes = 0;

        public ThreadCache(ILogger<ThreadCache> logger, long maxCacheSizeBytes)
        {
            this.maxCacheSizeBytes = maxCacheSizeBytes;
            this.logger = logger;
        }

        public ThreadCache(
            ILogger<ThreadCache> logger,
            StreamsConfig config)
        {
            this.config = config;
            this.logger = logger;
        }

        public long Puts()
        {
            return numPuts;
        }

        public long Gets()
        {
            return numGets;
        }

        public long Evicts()
        {
            return numEvicts;
        }

        public long Flushes()
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
        public static string NameSpaceFromTaskIdAndStore(string taskIDString, string underlyingStoreName)
        {
            return taskIDString + "-" + underlyingStoreName;
        }

        /**
         * Given a cache name of the form taskid-storename, return the task ID.
         * @param cacheName
         * @return
         */
        public static string TaskIDfromCacheName(string cacheName)
        {
            if (cacheName is null)
            {
                throw new ArgumentNullException(nameof(cacheName));
            }

            var tokens = cacheName.Split(new[] { '-' }, 2);
            return tokens[0];
        }

        /**
         * Given a cache name of the form taskid-storename, return the store name.
         * @param cacheName
         * @return
         */
        public static string UnderlyingStoreNamefromCacheName(string cacheName)
        {
            if (cacheName is null)
            {
                throw new ArgumentNullException(nameof(cacheName));
            }


            var tokens = cacheName.Split(new[] { '-' }, 2);
            return tokens[1];
        }


        /**
         * Add a listener that is called each time an entry is evicted from the cache or an explicit flush is called
         *
         * @param @namespace
         * @param listener
         */
        public void AddDirtyEntryFlushListener(string @namespace, IDirtyEntryFlushListener listener)
        {
            NamedCache cache = GetOrCreateCache(@namespace);
            cache.SetListener(listener);
        }

        public void Flush(string @namespace)
        {
            numFlushes++;

            NamedCache cache = GetCache(@namespace);
            if (cache == null)
            {
                return;
            }

            cache.Flush();

            logger.LogTrace("Cache stats on flush: #puts={}, #gets={}, #evicts={}, #flushes={}", Puts(), Gets(), Evicts(), Flushes());
        }

        public LRUCacheEntry? Get(string @namespace, Bytes key)
        {
            numGets++;

            if (key == null)
            {
                return null;
            }

            NamedCache cache = GetCache(@namespace);
            if (cache == null)
            {
                return null;
            }

            return cache.Get(key);
        }

        public void Put(string @namespace, Bytes key, LRUCacheEntry value)
        {
            numPuts++;

            NamedCache cache = GetOrCreateCache(@namespace);
            cache.Put(key, value);
            MaybeEvict(@namespace);
        }

        public LRUCacheEntry? PutIfAbsent(string @namespace, Bytes key, LRUCacheEntry value)
        {
            NamedCache cache = GetOrCreateCache(@namespace);

            LRUCacheEntry result = cache.PutIfAbsent(key, value);
            MaybeEvict(@namespace);

            if (result == null)
            {
                numPuts++;
            }

            return result;
        }

        public void PutAll(string @namespace, List<KeyValuePair<Bytes, LRUCacheEntry>> entries)
        {
            foreach (KeyValuePair<Bytes, LRUCacheEntry> entry in entries ?? Enumerable.Empty<KeyValuePair<Bytes, LRUCacheEntry>>())
            {
                Put(@namespace, entry.Key, entry.Value);
            }
        }

        public LRUCacheEntry? Delete(string @namespace, Bytes key)
        {
            NamedCache cache = GetCache(@namespace);
            if (cache == null)
            {
                return null;
            }

            return cache.Delete(key);
        }

        public MemoryLRUCacheBytesIterator Range(string @namespace, Bytes from, Bytes to)
        {
            NamedCache cache = GetCache(@namespace);
            if (cache == null)
            {
                return new MemoryLRUCacheBytesIterator(Enumerable.Empty<KeyValuePair<Bytes, LRUNode>>().GetEnumerator());
            }

            return new MemoryLRUCacheBytesIterator(cache.SubMapIterator(from, to));
        }

        public MemoryLRUCacheBytesIterator All(string @namespace)
        {
            NamedCache cache = GetCache(@namespace);
            if (cache == null)
            {
                return new MemoryLRUCacheBytesIterator();
            }

            return new MemoryLRUCacheBytesIterator(cache.AllIterator());
        }

        public long Size()
        {
            long size = 0;
            foreach (NamedCache cache in caches.Values)
            {
                size += cache.Size();
                if (IsOverflowing(size))
                {
                    return long.MaxValue;
                }
            }
            return size;
        }

        private bool IsOverflowing(long size)
        {
            return size < 0;
        }

        long SizeBytes()
        {
            long sizeInBytes = 0;
            foreach (NamedCache namedCache in caches.Values)
            {
                sizeInBytes += namedCache.SizeInBytes();
                if (IsOverflowing(sizeInBytes))
                {
                    return long.MaxValue;
                }
            }
            return sizeInBytes;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Close(string @namespace)
        {
            NamedCache? removed = null;
            if (caches.ContainsKey(@namespace))
            {
                removed = caches[@namespace];
                caches.Remove(@namespace);
            }

            removed?.Close();
        }

        private void MaybeEvict(string @namespace)
        {
            var numEvicted = 0;
            while (SizeBytes() > maxCacheSizeBytes)
            {
                NamedCache cache = GetOrCreateCache(@namespace);
                // we abort here as the put on this cache may have triggered
                // a put on another cache. So even though the sizeInBytes() is
                // still > maxCacheSizeBytes there is nothing to evict from this
                // namespaced cache.
                if (cache.Size() == 0)
                {
                    return;
                }

                cache.Evict();
                numEvicts++;
                numEvicted++;
            }

            logger.LogTrace("Evicted {} entries from cache {}", numEvicted, @namespace);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private NamedCache GetCache(string @namespace)
        {
            return caches[@namespace];
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private NamedCache GetOrCreateCache(string name)
        {
            NamedCache cache = caches[name];
            if (cache == null)
            {
                cache = new NamedCache(name);//, this.metrics);
                caches.Add(name, cache);
            }

            return cache;
        }
    }
}
