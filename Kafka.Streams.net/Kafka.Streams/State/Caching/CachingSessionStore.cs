using System;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.MergeSorted;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.State.Internals
{
    public class CachingSessionStore
        : WrappedStateStore<ISessionStore<Bytes, byte[]>, byte[], byte[]>
        , ISessionStore<Bytes, byte[]>, ICachedStateStore<byte[], byte[]>
    {
        private SessionKeySchema keySchema;
        private SegmentedCacheFunction cacheFunction;
        private string cacheName;
        private ThreadCache cache;
        private IInternalProcessorContext context;
        private FlushListener<byte[], byte[]> flushListener;
        private bool sendOldValues;

        private DateTime maxObservedTimestamp; // Refers to the window end time (determines segmentId)

        public CachingSessionStore(
            KafkaStreamsContext context,
            ISessionStore<Bytes, byte[]> bytesStore,
            TimeSpan segmentInterval)
            : base(context, bytesStore)
        {
            this.keySchema = new SessionKeySchema();
            this.cacheFunction = new SegmentedCacheFunction(this.keySchema, segmentInterval);
            this.maxObservedTimestamp = RecordQueue.UNKNOWN;
        }

        public override void Init(IProcessorContext context, IStateStore root)
        {
            this.InitInternal((IInternalProcessorContext)context);
            base.Init(context, root);
        }

        private void InitInternal(IInternalProcessorContext context)
        {
            this.context = context;

            this.cacheName = context.TaskId + "-" + this.Name;
            this.cache = context.GetCache();

            //cache.AddDirtyEntryFlushListener(cacheName, Entries);
            //=>
            //{
            //    foreach (DirtyEntry entry in entries)
            //    {
            //        putAndMaybeForward(entry, context);
            //    }
            //});
        }

        private void PutAndMaybeForward(DirtyEntry entry, IInternalProcessorContext context)
        {
            Bytes binaryKey = this.cacheFunction.Key(entry.Key);
            IWindowed<Bytes> bytesKey = SessionKeySchema.From(binaryKey);
            if (this.flushListener != null)
            {
                byte[] newValueBytes = entry.NewValue;
                byte[] oldValueBytes = newValueBytes == null || this.sendOldValues ?
                    this.Wrapped.FetchSession(bytesKey.Key, bytesKey.Window.StartTime, bytesKey.Window.EndTime)
                    : null;

                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream as well as writing to underlying store
                if (newValueBytes != null || oldValueBytes != null)
                {
                    // we need to get the old values if needed, and then Put to store, and then Flush
                    this.Wrapped.Put(bytesKey, entry.NewValue);

                    ProcessorRecordContext current = context.RecordContext;
                    context.SetRecordContext(entry.Entry().context);
                    try
                    {
                        this.flushListener?.Invoke(
                            binaryKey.Get(),
                            newValueBytes,
                            this.sendOldValues ? oldValueBytes : null,
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
                this.Wrapped.Put(bytesKey, entry.NewValue);
            }
        }

        public override bool SetFlushListener(FlushListener<byte[], byte[]> listener, bool sendOldValues)
        {
            this.flushListener = listener;
            this.sendOldValues = sendOldValues;

            return true;
        }

        public void Put(IWindowed<Bytes> key, byte[] value)
        {
            this.ValidateStoreOpen();
            Bytes binaryKey = SessionKeySchema.ToBinary(key);
            LRUCacheEntry entry =
                new LRUCacheEntry(
                    value,
                    this.context.Headers,
                    true,
                    this.context.Offset,
                    this.context.Timestamp,
                    this.context.Partition,
                    this.context.Topic);

            this.cache.Put(this.cacheName, this.cacheFunction.CacheKey(binaryKey), entry);

            this.maxObservedTimestamp = this.keySchema.SegmentTimestamp(binaryKey).GetNewest(this.maxObservedTimestamp);
        }

        public void Remove(IWindowed<Bytes> sessionKey)
        {
            this.ValidateStoreOpen();
            this.Put(sessionKey, null);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FindSessions(
            Bytes key,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            this.ValidateStoreOpen();

            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = this.Wrapped.Persistent()
                ? (IPeekingKeyValueIterator<Bytes, LRUCacheEntry>)new CacheIteratorWrapper(key, earliestSessionEndTime, latestSessionStartTime)
                : this.cache.Range(
                    this.cacheName,
                    this.cacheFunction.CacheKey(this.keySchema.LowerRangeFixedSize(key, earliestSessionEndTime)),
                    this.cacheFunction.CacheKey(this.keySchema.UpperRangeFixedSize(key, latestSessionStartTime)));

            IKeyValueIterator<IWindowed<Bytes>, byte[]> storeIterator = this.Wrapped.FindSessions(
                key,
                earliestSessionEndTime,
                latestSessionStartTime);

            var hasNextCondition = this.keySchema.HasNextCondition(
                key,
                key,
                earliestSessionEndTime,
                latestSessionStartTime);

            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
                new FilteredCacheIterator(cacheIterator, hasNextCondition, this.cacheFunction);

            return new MergedSortedCacheSessionStoreIterator(
                this.Context,
                filteredCacheIterator,
                storeIterator,
                this.cacheFunction);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FindSessions(
            Bytes keyFrom,
            Bytes keyTo,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            if (keyFrom.CompareTo(keyTo) > 0)
            {
                //LOG.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
                //    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                //    "Note that the built-in numerical serdes do not follow this for negative numbers");

                return null;// KeyValueIterators.EMPTY_ITERATOR;
            }

            this.ValidateStoreOpen();

            Bytes cacheKeyFrom = this.cacheFunction.CacheKey(this.keySchema.LowerRange(keyFrom, earliestSessionEndTime));
            Bytes cacheKeyTo = this.cacheFunction.CacheKey(this.keySchema.UpperRange(keyTo, latestSessionStartTime));
            MemoryLRUCacheBytesIterator cacheIterator = this.cache.Range(this.cacheName, cacheKeyFrom, cacheKeyTo);

            IKeyValueIterator<IWindowed<Bytes>, byte[]> storeIterator = this.Wrapped.FindSessions(
                keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);

            var hasNextCondition = this.keySchema.HasNextCondition(
                keyFrom,
                keyTo,
                earliestSessionEndTime,
                latestSessionStartTime);

            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
                new FilteredCacheIterator(cacheIterator, hasNextCondition, this.cacheFunction);
            return new MergedSortedCacheSessionStoreIterator(
                this.Context,
                filteredCacheIterator,
                storeIterator,
                this.cacheFunction);
        }

        public byte[] FetchSession(Bytes key, DateTime startTime, DateTime endTime)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            this.ValidateStoreOpen();
            if (this.cache == null)
            {
                return this.Wrapped.FetchSession(key, startTime, endTime);
            }
            else
            {
                Bytes bytesKey = SessionKeySchema.ToBinary(key, startTime, endTime);
                Bytes cacheKey = this.cacheFunction.CacheKey(bytesKey);
                LRUCacheEntry entry = this.cache.Get(this.cacheName, cacheKey);

                if (entry == null)
                {
                    return this.Wrapped.FetchSession(key, startTime, endTime);
                }
                else
                {
                    return entry.Value();
                }
            }
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            return this.FindSessions(key, DateTime.MinValue, DateTime.MaxValue);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to)
        {
            from = from ?? throw new ArgumentNullException(nameof(from));
            to = to ?? throw new ArgumentNullException(nameof(to));

            return this.FindSessions(from, to, DateTime.MinValue, DateTime.MaxValue);
        }

        public override void Flush()
        {
            this.cache.Flush(this.cacheName);
            base.Flush();
        }

        public override void Close()
        {
            this.Flush();
            this.cache.Close(this.cacheName);
            base.Close();
        }

        public bool SetFlushListener(Action<byte[], byte[], byte[], long> listener, bool sendOldValues)
        {
            throw new NotImplementedException();
        }
    }
}
