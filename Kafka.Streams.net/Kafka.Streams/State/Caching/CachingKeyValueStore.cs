using Microsoft.Extensions.Logging;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Common.Utils;
using Kafka.Streams.Processors.Internals;
using System.Collections.Generic;
using System.Threading;
using Kafka.Streams.State.KeyValues;
using System;

namespace Kafka.Streams.State.Internals
{
    public class CachingKeyValueStore
        : WrappedStateStore<IKeyValueStore<Bytes, byte[]>, byte[], byte[]>,
        IKeyValueStore<Bytes, byte[]>,
        ICachedStateStore<byte[], byte[]>
    {
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<CachingKeyValueStore>();

        private FlushListener<byte[], byte[]> flushListener;
        private bool sendOldValues;
        private string cacheName;
        private ThreadCache cache;
        private IInternalProcessorContext context;
        private Thread streamThread;
        //private ReadWriteLock @lock = new ReentrantReadWriteLock();

        public CachingKeyValueStore(
            KafkaStreamsContext context,
            IKeyValueStore<Bytes, byte[]> underlying)
            : base(context, underlying)
        {
        }

        public override void Init(
            IProcessorContext context,
            IStateStore root)
        {
            this.initInternal(context);
            base.Init(context, root);
            // save the stream thread as we only ever want to trigger a Flush
            // when the stream thread is the current thread.
            this.streamThread = Thread.CurrentThread;
        }

        private void initInternal(IProcessorContext context)
        {
            this.context = (IInternalProcessorContext)context;

            this.cache = this.context.GetCache();
            this.cacheName = ThreadCache.NameSpaceFromTaskIdAndStore(context.TaskId.ToString(), this.Name);
            //        cache.AddDirtyEntryFlushListener(cacheName, entries =>
            //{
            //    foreach (DirtyEntry entry in entries)
            //    {
            //        putAndMaybeForward(entry, (IInternalProcessorContext)context);
            //    }
            //});
        }

        private void putAndMaybeForward(
            DirtyEntry entry,
            IInternalProcessorContext context)
        {
            if (this.flushListener != null)
            {
                byte[] rawNewValue = entry.NewValue;
                byte[] rawOldValue = rawNewValue == null || this.sendOldValues ? this.Wrapped.Get(entry.Key) : null;

                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream as well as writing to underlying store
                if (rawNewValue != null || rawOldValue != null)
                {
                    // we need to get the old values if needed, and then Put to store, and then Flush
                    this.Wrapped.Add(entry.Key, entry.NewValue);

                    ProcessorRecordContext current = context.RecordContext;
                    context.SetRecordContext(entry.Entry().context);
                    try
                    {
                        this.flushListener?.Invoke(
                            entry.Key,
                            rawNewValue,
                            this.sendOldValues ? rawOldValue : null,
                            entry.Entry().context.Timestamp);
                    }
                    finally
                    {
                        context.SetRecordContext(current);
                    }
                }
            }
            else
            {
                this.Wrapped.Add(entry.Key, entry.NewValue);
            }
        }

        public override bool SetFlushListener(FlushListener<byte[], byte[]> flushListener, bool sendOldValues)
        {
            this.flushListener = flushListener;
            this.sendOldValues = sendOldValues;

            return true;
        }

        public void Add(Bytes key, byte[] value)
        {
//            key = key ?? throw new ArgumentNullException(nameof(key));
//            this.ValidateStoreOpen();
//            lock (writeLock().@lock)
//            {
//                try
//                {
//                    // for null bytes, we still Put it into cache indicating tombstones
//                    this.putInternal(key, value);
//                }
//                finally
//                {
//                    @lock.writeLock().unlock();
//                }
//            }
        }

        private void putInternal(Bytes key, byte[] value)
        {
            this.cache.Put(
                this.cacheName,
                key,
                new LRUCacheEntry(
                    value,
                    this.context.Headers,
                    true,
                    this.context.Offset,
                    this.context.Timestamp,
                    this.context.Partition,
                    this.context.Topic));
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            //            key = key ?? throw new ArgumentNullException(nameof(key));
            //            this.ValidateStoreOpen();
            //            @lock.writeLock().@lock();
            //            try
            //            {
            //                byte[] v = this.getInternal(key);
            //                if (v == null)
            //                {
            //                    this.putInternal(key, value);
            //                }
            //
            //                return v;
            //            }
            //            finally
            //            {
            //                @lock.writeLock().unlock();
            //            }

            return Array.Empty<byte>();
        }

        public void PutAll(List<KeyValuePair<Bytes, byte[]>> entries)
        {
            this.ValidateStoreOpen();
            //@lock.writeLock().@lock();
            try
            {
                foreach (KeyValuePair<Bytes, byte[]> entry in entries)
                {
                    var key = entry.Key ?? throw new ArgumentNullException(nameof(entry.Key));
                    this.Add(key, entry.Value);
                }
            }
            finally
            {
              //  @lock.writeLock().unlock();
            }
        }

        public byte[] Delete(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            this.ValidateStoreOpen();
            // @lock.writeLock().@lock();
            try
            {
                return this.deleteInternal(key);
            }
            finally
            {
                // @lock.writeLock().unlock();
            }
        }

        private byte[] deleteInternal(Bytes key)
        {
            byte[] v = this.getInternal(key);
            this.putInternal(key, null);
            return v;
        }

        public byte[] Get(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            this.ValidateStoreOpen();
            object theLock = new object();

            if (Thread.CurrentThread.Equals(this.streamThread))
            {
                //theLock = @lock.writeLock();
            }
            else
            {
                //theLock = @lock.readLock();
            }
            //lock (theLock.@lock)
            {
                try
                {
                    return this.getInternal(key);
                }
                finally
                {
              //      theLock.unlock();
                }
            }
        }

        private byte[] getInternal(Bytes key)
        {
            LRUCacheEntry entry = null;
            if (this.cache != null)
            {
                entry = this.cache.Get(this.cacheName, key);
            }
            if (entry == null)
            {
                byte[] RawValue = this.Wrapped.Get(key);
                if (RawValue == null)
                {
                    return null;
                }
                // only update the cache if this call is on the streamThread
                // as we don't want other threads to trigger an eviction/Flush
                if (Thread.CurrentThread.Equals(this.streamThread))
                {
                    this.cache.Put(this.cacheName, key, new LRUCacheEntry(RawValue));
                }

                return RawValue;
            }
            else
            {
                return entry.Value();
            }
        }

        public IKeyValueIterator<Bytes, byte[]> Range(Bytes from, Bytes to)
        {
            if (from.CompareTo(to) > 0)
            {
                LOG.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");

                return null;// KeyValueIterators<Bytes, byte[]>.EMPTY_ITERATOR;
            }

            this.ValidateStoreOpen();
            IKeyValueIterator<Bytes, byte[]> storeIterator = this.Wrapped.Range(from, to);
            MemoryLRUCacheBytesIterator cacheIterator = this.cache.Range(this.cacheName, from, to);
            return null; //new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
        }

        public IKeyValueIterator<Bytes, byte[]> All()
        {
            this.ValidateStoreOpen();
            IKeyValueIterator<Bytes, byte[]> storeIterator =
                new DelegatingPeekingKeyValueIterator<Bytes, byte[]>(this.Name, this.Wrapped.All());
            MemoryLRUCacheBytesIterator cacheIterator = this.cache.All(this.cacheName);
            return null; //new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
        }

        public long approximateNumEntries
        {
            get
            {
                this.ValidateStoreOpen();
                //lock (readLock().@lock)
                {
                    try
                    {
                        return this.Wrapped.approximateNumEntries;
                    }
                    finally
                    {
                        // @lock.readLock().unlock();
                    }
                }
            }
        }

        public override void Flush()
        {
            //@lock.writeLock().@lock();
            try
            {
                this.cache.Flush(this.cacheName);
                base.Flush();
            }
            finally
            {
              //  @lock.writeLock().unlock();
            }
        }

        public override void Close()
        {
            try
            {
                this.Flush();
            }
            finally
            {
                try
                {
                    base.Close();
                }
                finally
                {
                    this.cache.Close(this.cacheName);
                }
            }
        }
    }
}
