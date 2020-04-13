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
            return this.numPuts;
        }

        public long Gets()
        {
            return this.numGets;
        }

        public long Evicts()
        {
            return this.numEvicts;
        }

        public long Flushes()
        {
            return this.numFlushes;
        }

        /**
         * The thread cache maintains a set of {@link NamedCache}s whose names are a concatenation of the task ID and the
         * underlying store Name. This method creates those names.
         * @param taskIDString Task ID
         * @param underlyingStoreName Underlying store Name
         * @return
         */
        public static string NameSpaceFromTaskIdAndStore(string taskIDString, string underlyingStoreName)
        {
            return taskIDString + "-" + underlyingStoreName;
        }

        /**
         * Given a cache Name of the form taskid-storename, return the task ID.
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
         * Given a cache Name of the form taskid-storename, return the store Name.
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
         * Add a listener that is called each time an entry is evicted from the cache or an explicit Flush is called
         *
         * @param @namespace
         * @param listener
         */
        public void AddDirtyEntryFlushListener(string @namespace, Action<IEnumerable<DirtyEntry>> listener)
        {
            NamedCache cache = this.GetOrCreateCache(@namespace);
            cache.SetListener(listener);
        }

        public void Flush(string @namespace)
        {
            this.numFlushes++;

            NamedCache cache = this.GetCache(@namespace);
            if (cache == null)
            {
                return;
            }

            cache.Flush();

            this.logger.LogTrace("Cache stats on Flush: #puts={}, #gets={}, #evicts={}, #flushes={}", this.Puts(), this.Gets(), this.Evicts(), this.Flushes());
        }

        public LRUCacheEntry? Get(string @namespace, Bytes key)
        {
            this.numGets++;

            if (key == null)
            {
                return null;
            }

            NamedCache cache = this.GetCache(@namespace);
            if (cache == null)
            {
                return null;
            }

            return cache.Get(key);
        }

        public void Put(string @namespace, Bytes key, LRUCacheEntry value)
        {
            this.numPuts++;

            NamedCache cache = this.GetOrCreateCache(@namespace);
            cache.Put(key, value);
            this.MaybeEvict(@namespace);
        }

        public LRUCacheEntry? PutIfAbsent(string @namespace, Bytes key, LRUCacheEntry value)
        {
            NamedCache cache = this.GetOrCreateCache(@namespace);

            LRUCacheEntry result = cache.PutIfAbsent(key, value);
            this.MaybeEvict(@namespace);

            if (result == null)
            {
                this.numPuts++;
            }

            return result;
        }

        public void PutAll(string @namespace, List<KeyValuePair<Bytes, LRUCacheEntry>> entries)
        {
            foreach (KeyValuePair<Bytes, LRUCacheEntry> entry in entries ?? Enumerable.Empty<KeyValuePair<Bytes, LRUCacheEntry>>())
            {
                this.Put(@namespace, entry.Key, entry.Value);
            }
        }

        public LRUCacheEntry? Delete(string @namespace, Bytes key)
        {
            NamedCache cache = this.GetCache(@namespace);
            if (cache == null)
            {
                return null;
            }

            return cache.Delete(key);
        }

        public MemoryLRUCacheBytesIterator Range(string @namespace, Bytes from, Bytes to)
        {
            NamedCache cache = this.GetCache(@namespace);
            if (cache == null)
            {
                return new MemoryLRUCacheBytesIterator(Enumerable.Empty<KeyValuePair<Bytes, LRUNode>>().GetEnumerator());
            }

            return new MemoryLRUCacheBytesIterator(cache.SubMapIterator(from, to));
        }

        public MemoryLRUCacheBytesIterator All(string @namespace)
        {
            NamedCache cache = this.GetCache(@namespace);
            if (cache == null)
            {
                return new MemoryLRUCacheBytesIterator();
            }

            return new MemoryLRUCacheBytesIterator(cache.AllIterator());
        }

        public long Size()
        {
            long size = 0;
            foreach (NamedCache cache in this.caches.Values)
            {
                size += cache.Size();
                if (this.IsOverflowing(size))
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

        private long SizeBytes()
        {
            long sizeInBytes = 0;
            foreach (NamedCache namedCache in this.caches.Values)
            {
                sizeInBytes += namedCache.SizeInBytes();
                if (this.IsOverflowing(sizeInBytes))
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
            if (this.caches.ContainsKey(@namespace))
            {
                removed = this.caches[@namespace];
                this.caches.Remove(@namespace);
            }

            removed?.Close();
        }

        private void MaybeEvict(string @namespace)
        {
            var numEvicted = 0;
            while (this.SizeBytes() > this.maxCacheSizeBytes)
            {
                NamedCache cache = this.GetOrCreateCache(@namespace);
                // we abort here as the Put on this cache may have triggered
                // a Put on another cache. So even though the sizeInBytes() is
                // still > maxCacheSizeBytes there is nothing to evict from this
                // namespaced cache.
                if (cache.Size() == 0)
                {
                    return;
                }

                cache.Evict();
                this.numEvicts++;
                numEvicted++;
            }

            this.logger.LogTrace("Evicted {} entries from cache {}", numEvicted, @namespace);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private NamedCache GetCache(string @namespace)
        {
            return this.caches[@namespace];
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private NamedCache GetOrCreateCache(string Name)
        {
            NamedCache cache = this.caches[Name];
            if (cache == null)
            {
                cache = new NamedCache(Name);//, this.metrics);
                this.caches.Add(Name, cache);
            }

            return cache;
        }
    }
}
