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

        private long maxObservedTimestamp; // Refers to the window end time (determines segmentId)

        public CachingSessionStore(KafkaStreamsContext context, ISessionStore<Bytes, byte[]> bytesStore, long segmentInterval)
            : base(context, bytesStore)
        {
            this.keySchema = new SessionKeySchema();
            this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
            this.maxObservedTimestamp = RecordQueue.UNKNOWN;
        }

        public override void Init(IProcessorContext context, IStateStore root)
        {
            InitInternal((IInternalProcessorContext)context);
            base.Init(context, root);
        }


        private void InitInternal(IInternalProcessorContext context)
        {
            this.context = context;

            cacheName = context.TaskId + "-" + Name;
            cache = context.GetCache();

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
            Bytes binaryKey = cacheFunction.Key(entry.Key);
            Windowed<Bytes> bytesKey = SessionKeySchema.From(binaryKey);
            if (flushListener != null)
            {
                byte[] newValueBytes = entry.NewValue;
                byte[] oldValueBytes = newValueBytes == null || sendOldValues ?
                    Wrapped.FetchSession(bytesKey.Key, bytesKey.window.Start(), bytesKey.window.End()) : null;

                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream as well as writing to underlying store
                if (newValueBytes != null || oldValueBytes != null)
                {
                    // we need to get the old values if needed, and then put to store, and then flush
                    Wrapped.Put(bytesKey, entry.NewValue);

                    ProcessorRecordContext current = context.RecordContext;
                    context.SetRecordContext(entry.Entry().context);
                    try
                    {
                        flushListener?.Invoke(
                            binaryKey.Get(),
                            newValueBytes,
                            sendOldValues ? oldValueBytes : null,
                            entry.Entry().context.timestamp);
                    }
                    finally
                    {
                        context.SetRecordContext(current);
                    }
                }
            }
            else
            {
                Wrapped.Put(bytesKey, entry.NewValue);
            }
        }

        public override bool SetFlushListener(FlushListener<byte[], byte[]> listener, bool sendOldValues)
        {
            this.flushListener = listener;
            this.sendOldValues = sendOldValues;

            return true;
        }

        public void Put(Windowed<Bytes> key, byte[] value)
        {
            ValidateStoreOpen();
            Bytes binaryKey = SessionKeySchema.ToBinary(key);
            LRUCacheEntry entry =
                new LRUCacheEntry(
                    value,
                    context.Headers,
                    true,
                    context.Offset,
                    context.Timestamp,
                    context.Partition,
                    context.Topic);

            cache.Put(cacheName, cacheFunction.CacheKey(binaryKey), entry);

            maxObservedTimestamp = Math.Max(keySchema.SegmentTimestamp(binaryKey), maxObservedTimestamp);
        }

        public void Remove(Windowed<Bytes> sessionKey)
        {
            ValidateStoreOpen();
            Put(sessionKey, null);
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> FindSessions(
            Bytes key,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            ValidateStoreOpen();

            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = Wrapped.Persistent()
                ? (IPeekingKeyValueIterator<Bytes, LRUCacheEntry>)new CacheIteratorWrapper(key, earliestSessionEndTime, latestSessionStartTime)
                : cache.Range(
                    cacheName,
                    cacheFunction.CacheKey(keySchema.LowerRangeFixedSize(key, earliestSessionEndTime)),
                    cacheFunction.CacheKey(keySchema.UpperRangeFixedSize(key, latestSessionStartTime)));

            IKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = Wrapped.FindSessions(
                key,
                earliestSessionEndTime,
                latestSessionStartTime);

            var hasNextCondition = keySchema.HasNextCondition(
                key,
                key,
                earliestSessionEndTime,
                latestSessionStartTime);

            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
                new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

            return new MergedSortedCacheSessionStoreIterator(
                this.Context,
                filteredCacheIterator,
                storeIterator,
                cacheFunction);
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> FindSessions(
            Bytes keyFrom,
            Bytes keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            if (keyFrom.CompareTo(keyTo) > 0)
            {
                //LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                //    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                //    "Note that the built-in numerical serdes do not follow this for negative numbers");

                return null;// KeyValueIterators.EMPTY_ITERATOR;
            }

            ValidateStoreOpen();

            Bytes cacheKeyFrom = cacheFunction.CacheKey(keySchema.LowerRange(keyFrom, earliestSessionEndTime));
            Bytes cacheKeyTo = cacheFunction.CacheKey(keySchema.UpperRange(keyTo, latestSessionStartTime));
            MemoryLRUCacheBytesIterator cacheIterator = cache.Range(cacheName, cacheKeyFrom, cacheKeyTo);

            IKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = Wrapped.FindSessions(
                keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);

            var hasNextCondition = keySchema.HasNextCondition(
                keyFrom,
                keyTo,
                earliestSessionEndTime,
                latestSessionStartTime);

            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
                new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
            return new MergedSortedCacheSessionStoreIterator(
                this.Context,
                filteredCacheIterator,
                storeIterator,
                cacheFunction);
        }

        public byte[] FetchSession(Bytes key, long startTime, long endTime)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            ValidateStoreOpen();
            if (cache == null)
            {
                return Wrapped.FetchSession(key, startTime, endTime);
            }
            else
            {
                Bytes bytesKey = SessionKeySchema.ToBinary(key, startTime, endTime);
                Bytes cacheKey = cacheFunction.CacheKey(bytesKey);
                LRUCacheEntry entry = cache.Get(cacheName, cacheKey);

                if (entry == null)
                {
                    return Wrapped.FetchSession(key, startTime, endTime);
                }
                else
                {
                    return entry.Value();
                }
            }
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));
            return FindSessions(key, 0, long.MaxValue);
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to)
        {
            from = from ?? throw new ArgumentNullException(nameof(from));
            to = to ?? throw new ArgumentNullException(nameof(to));

            return FindSessions(from, to, 0, long.MaxValue);
        }

        public override void Flush()
        {
            cache.Flush(cacheName);
            base.Flush();
        }

        public override void Close()
        {
            Flush();
            cache.Close(cacheName);
            base.Close();
        }

        public new bool SetFlushListener(Action<byte[], byte[], byte[], long> listener, bool sendOldValues)
        {
            throw new NotImplementedException();
        }
    }
}
