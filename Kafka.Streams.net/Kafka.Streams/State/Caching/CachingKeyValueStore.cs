
//using Microsoft.Extensions.Logging;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Common.Utils;
//using Kafka.Streams.Processors.Internals;
//using System.Collections.Generic;
//using System.Threading;

//namespace Kafka.Streams.State.Internals
//{
//    public class CachingKeyValueStore
//        : WrappedStateStore<IKeyValueStore<Bytes, byte[]>, byte[], byte[]>
//        , IKeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<CachingKeyValueStore>();

//        private ICacheFlushListener<byte[], byte[]> flushListener;
//        private bool sendOldValues;
//        private string cacheName;
//        private ThreadCache cache;
//        private IInternalProcessorContext<Bytes, byte[]>  context;
//        private Thread streamThread;
//        //private ReadWriteLock @lock = new ReentrantReadWriteLock();

//        public CachingKeyValueStore(IKeyValueStore<Bytes, byte[]> underlying)
//            : base(underlying)
//        {
//        }

//        public override void Init(IProcessorContext<Bytes, byte[]> context,
//                         IStateStore root)
//        {
//            initInternal(context);
//            base.Init(context, root);
//            // save the stream thread as we only ever want to trigger a Flush
//            // when the stream thread is the current thread.
//            streamThread = Thread.CurrentThread;
//        }


//        private void initInternal(IProcessorContext<Bytes, byte[]> context)
//        {
//            this.context = (IInternalProcessorContext<Bytes, byte[]>)context;

//            this.cache = this.context.getCache();
//            this.cacheName = ThreadCache.nameSpaceFromTaskIdAndStore(context.taskId.ToString(), Name);
//            //        cache.AddDirtyEntryFlushListener(cacheName, entries =>
//            //{
//            //    foreach (DirtyEntry entry in entries)
//            //    {
//            //        putAndMaybeForward(entry, (IInternalProcessorContext)context);
//            //    }
//            //});
//        }

//        private void putAndMaybeForward(
//            DirtyEntry entry,
//            IInternalProcessorContext<Bytes, byte[]> context)
//        {
//            if (flushListener != null)
//            {
//                byte[] rawNewValue = entry.newValue();
//                byte[] rawOldValue = rawNewValue == null || sendOldValues ? wrapped[entry.key()] : null;

//                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
//                // we can skip flushing to downstream as well as writing to underlying store
//                if (rawNewValue != null || rawOldValue != null)
//                {
//                    // we need to get the old values if needed, and then Put to store, and then Flush
//                    wrapped.Add(entry.key(), entry.newValue());

//                    ProcessorRecordContext current = context.recordContext();
//                    context.setRecordContext(entry.entry().context);
//                    try
//                    {
//                        flushListener.apply(
//                            entry.key(),
//                            rawNewValue,
//                            sendOldValues ? rawOldValue : null,
//                            entry.entry().context.timestamp());
//                    }
//                    finally
//                    {
//                        context.setRecordContext(current);
//                    }
//                }
//            }
//            else
//            {
//                wrapped.Add(entry.key(), entry.newValue());
//            }
//        }

//        public override bool setFlushListener(ICacheFlushListener<byte[], byte[]> flushListener,
//                                        bool sendOldValues)
//        {
//            this.flushListener = flushListener;
//            this.sendOldValues = sendOldValues;

//            return true;
//        }

//        public override void Put(Bytes key,
//                        byte[] value)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            validateStoreOpen();
//            lock (writeLock().@lock)
//            {
//                try
//                {
//                    // for null bytes, we still Put it into cache indicating tombstones
//                    putInternal(key, value);
//                }
//                finally
//                {
//                    @lock.writeLock().unlock();
//                }
//            }
//        }

//        private void putInternal(Bytes key,
//                                 byte[] value)
//        {
//            cache.Add(
//                cacheName,
//                key,
//                new LRUCacheEntry(
//                    value,
//                    context.Headers,
//                    true,
//                    context.offset(),
//                    context.timestamp(),
//                    context.Partition,
//                    context.Topic));
//        }

//        public override byte[] putIfAbsent(Bytes key,
//                                  byte[] value)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            validateStoreOpen();
//            @lock.writeLock().@lock();
//            try
//            {
//                byte[] v = getInternal(key);
//                if (v == null)
//                {
//                    putInternal(key, value);
//                }
//                return v;
//            }
//            finally
//            {
//                @lock.writeLock().unlock();
//            }
//        }

//        public override void putAll(List<KeyValuePair<Bytes, byte[]>> entries)
//        {
//            validateStoreOpen();
//            @lock.writeLock().@lock();
//            try
//            {
//                foreach (KeyValuePair<Bytes, byte[]> entry in entries)
//                {
//                    entry.key = entry.key ?? throw new ArgumentNullException(nameof(entry.key));
//                    Put(entry.key, entry.value);
//                }
//            }
//            finally
//            {
//                @lock.writeLock().unlock();
//            }
//        }

//        public override byte[] delete(Bytes key)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            validateStoreOpen();
//            @lock.writeLock().@lock();
//            try
//            {
//                return deleteInternal(key);
//            }
//            finally
//            {
//                @lock.writeLock().unlock();
//            }
//        }

//        private byte[] deleteInternal(Bytes key)
//        {
//            byte[] v = getInternal(key);
//            putInternal(key, null);
//            return v;
//        }

//        public override byte[] get(Bytes key)
//        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            validateStoreOpen();
//            Lock theLock;
//            if (Thread.CurrentThread.Equals(streamThread))
//            {
//                theLock = @lock.writeLock();
//            }
//            else
//            {
//                theLock = @lock.readLock();
//            }
//            lock (theLock.@lock)
//            {
//                try
//                {
//                    return getInternal(key);
//                }
//                finally
//                {
//                    theLock.unlock();
//                }
//            }
//        }

//        private byte[] getInternal(Bytes key)
//        {
//            LRUCacheEntry entry = null;
//            if (cache != null)
//            {
//                entry = cache[cacheName, key];
//            }
//            if (entry == null)
//            {
//                byte[] RawValue = wrapped[key];
//                if (RawValue == null)
//                {
//                    return null;
//                }
//                // only update the cache if this call is on the streamThread
//                // as we don't want other threads to trigger an eviction/Flush
//                if (Thread.CurrentThread.Equals(streamThread))
//                {
//                    cache.Add(cacheName, key, new LRUCacheEntry(RawValue));
//                }
//                return RawValue;
//            }
//            else
//            {
//                return entry.value();
//            }
//        }

//        public override IKeyValueIterator<Bytes, byte[]> range(Bytes from,
//                                                     Bytes to)
//        {
//            if (from.CompareTo(to) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//                return KeyValueIterators.emptyIterator();
//            }

//            validateStoreOpen();
//            IKeyValueIterator<Bytes, byte[]> storeIterator = wrapped.Range(from, to);
//            MemoryLRUCacheBytesIterator cacheIterator = cache.Range(cacheName, from, to);
//            return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
//        }

//        public override IKeyValueIterator<Bytes, byte[]> All()
//        {
//            validateStoreOpen();
//            IKeyValueIterator<Bytes, byte[]> storeIterator =
//                new DelegatingPeekingKeyValueIterator<>(this.Name, wrapped.All());
//            MemoryLRUCacheBytesIterator cacheIterator = cache.All(cacheName);
//            return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
//        }

//        public override long approximateNumEntries
//        {
//            validateStoreOpen();
//            lock (readLock().@lock)
//            {
//                try
//                {
//                    return wrapped.approximateNumEntries;
//                }
//                finally
//                {
//                    @lock.readLock().unlock();
//                }
//            }
//        }

//        public override void Flush()
//        {
//            @lock.writeLock().@lock();
//            try
//            {
//                cache.Flush(cacheName);
//                base.Flush();
//            }
//            finally
//            {
//                @lock.writeLock().unlock();
//            }
//        }

//        public override void Close()
//        {
//            try
//            {
//                Flush();
//            }
//            finally
//            {
//                try
//                {
//                    base.Close();
//                }
//                finally
//                {
//                    cache.Close(cacheName);
//                }
//            }
//        }
//    }
//}