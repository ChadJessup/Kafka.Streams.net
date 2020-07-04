using Kafka.Common.Extensions;
using Kafka.Common.Utils;
using Kafka.Streams.Internals;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Serialization;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.MergeSorted;
using Kafka.Streams.State.ReadOnly;
using Kafka.Streams.State.Windowed;
using Microsoft.Extensions.Logging;
using System;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{
    public class CachingWindowStore
        : WrappedStateStore<IWindowStore<Bytes, byte[]>, byte[], byte[]>,
        IWindowStore<Bytes, byte[]>, ICachedStateStore<byte[], byte[]>
    {
        private readonly ILogger<CachingWindowStore> logger;

        private readonly TimeSpan windowSize;
        private readonly IKeySchema keySchema;

        private ThreadCache cache;
        private bool sendOldValues;
        private IInternalProcessorContext context;
        private IStateSerdes<Bytes, byte[]> bytesSerdes;
        private FlushListener<byte[], byte[]> flushListener;

        private DateTime maxObservedTimestamp;

        private readonly SegmentedCacheFunction cacheFunction;

        public CachingWindowStore(
            KafkaStreamsContext context,
            IWindowStore<Bytes, byte[]> underlying,
            TimeSpan windowSize,
            TimeSpan segmentInterval)
            : base(context, underlying)
        {
            this.windowSize = windowSize;
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
            string topic = ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, this.Name);

            this.bytesSerdes = new StateSerdes<Bytes, byte[]>(
                topic,
                new BytesSerdes(),
                Serdes.ByteArray());

            this.Name = context.TaskId + "-" + this.Name;
            this.cache = this.context.GetCache();

            this.cache.AddDirtyEntryFlushListener(this.Name, entries =>
            {
                foreach (var entry in entries)
                {
                    this.PutAndMaybeForward(entry, context);
                }
            });
        }

        private void PutAndMaybeForward(DirtyEntry entry, IInternalProcessorContext context)
        {
            byte[] binaryWindowKey = this.cacheFunction.Key(entry.Key).Get();
            IWindowed<Bytes> windowedKeyBytes = WindowKeySchema.FromStoreBytesKey(binaryWindowKey, this.windowSize);
            var windowStartTimestamp = windowedKeyBytes.Window.StartTime;
            Bytes binaryKey = windowedKeyBytes.Key;

            if (this.flushListener != null)
            {
                byte[] rawNewValue = entry.NewValue;
                byte[]? rawOldValue = rawNewValue == null || this.sendOldValues
                    ? this.Wrapped.Fetch(binaryKey, windowStartTimestamp)
                    : null;

                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream as well as writing to underlying store
                if (rawNewValue != null || rawOldValue != null)
                {
                    // we need to get the old values if needed, and then Put to store, and then Flush
                    this.Wrapped.Put(binaryKey, entry.NewValue, windowStartTimestamp);

                    ProcessorRecordContext current = context.RecordContext;
                    context.SetRecordContext(entry.Entry().context);
                    try
                    {
                        this.flushListener?.Invoke(
                            binaryWindowKey,
                            rawNewValue,
                            this.sendOldValues
                                ? rawOldValue
                                : null,
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
                this.Wrapped.Put(binaryKey, entry.NewValue, windowStartTimestamp);
            }
        }

        public override bool SetFlushListener(
            FlushListener<byte[], byte[]> flushListener,
            bool sendOldValues)
        {
            this.flushListener = flushListener;
            this.sendOldValues = sendOldValues;

            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Put(Bytes key, byte[] value)
        {
            this.Put(key, value, this.context.Timestamp);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Put(Bytes key, byte[] value, DateTime windowStartTimestamp)
        {
            // since this function may not access the underlying inner store, we need to validate
            // if store is open outside as well.
            this.ValidateStoreOpen();

            Bytes keyBytes = WindowKeySchema.ToStoreKeyBinary(key, windowStartTimestamp, 0);
            LRUCacheEntry entry =
                new LRUCacheEntry(
                    value,
                    this.context.Headers,
                    true,
                    this.context.Offset,
                    this.context.Timestamp,
                    this.context.Partition,
                    this.context.Topic);

            this.cache.Put(this.Name, this.cacheFunction.CacheKey(keyBytes), entry);

            this.maxObservedTimestamp = this.keySchema.SegmentTimestamp(keyBytes).GetNewest(this.maxObservedTimestamp);
        }

        public byte[] Fetch(Bytes key, DateTime timestamp)
        {
            this.ValidateStoreOpen();
            Bytes bytesKey = WindowKeySchema.ToStoreKeyBinary(key, timestamp, 0);
            Bytes cacheKey = this.cacheFunction.CacheKey(bytesKey);
            if (this.cache == null)
            {
                return this.Wrapped.Fetch(key, timestamp);
            }

            LRUCacheEntry entry = this.cache.Get(this.Name, cacheKey);

            if (entry == null)
            {
                return this.Wrapped.Fetch(key, timestamp);
            }
            else
            {
                return entry.Value();
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IWindowStoreIterator<byte[]> Fetch(Bytes key, DateTime timeFrom, DateTime timeTo)
        {
            // since this function may not access the underlying inner store, we need to validate
            // if store is open outside as well.
            this.ValidateStoreOpen();

            var underlyingIterator = this.Wrapped.Fetch(key, timeFrom, timeTo);
            if (this.cache == null)
            {
                return underlyingIterator;
            }

            var cacheIterator = this.Wrapped.Persistent()
                ? (IPeekingKeyValueIterator<Bytes, LRUCacheEntry>)new CacheIteratorWrapper(key, timeFrom, timeTo)
                : this.cache.Range(
                    this.Name,
                    this.cacheFunction.CacheKey(this.keySchema.LowerRangeFixedSize(key, timeFrom)),
                    this.cacheFunction.CacheKey(this.keySchema.UpperRangeFixedSize(key, timeTo)));

            var HasNextCondition = this.keySchema.HasNextCondition(key, key, timeFrom, timeTo);

            var filteredCacheIterator = new FilteredCacheIterator(
                cacheIterator, HasNextCondition, this.cacheFunction);

            return new MergedSortedCacheWindowStoreIterator(
                this.Context,
                filteredCacheIterator,
                underlyingIterator);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(
            Bytes from,
            Bytes to,
            DateTime timeFrom,
            DateTime timeTo)
        {
            if (from.CompareTo(to) > 0)
            {
                this.logger.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");

                return null; // KeyValueIterators<IWindowed<Bytes>, byte[]>.EMPTY_ITERATOR;
            }

            // since this function may not access the underlying inner store, we need to validate
            // if store is open outside as well.
            this.ValidateStoreOpen();

            IKeyValueIterator<IWindowed<Bytes>, byte[]> underlyingIterator =
                this.Wrapped.Fetch(from, to, timeFrom, timeTo);

            if (this.cache == null)
            {
                return underlyingIterator;
            }

            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = this.Wrapped.Persistent()
                ? new CacheIteratorWrapper(from, to, timeFrom, timeTo)
                : (IPeekingKeyValueIterator<Bytes, LRUCacheEntry>)this.cache.Range(
                    this.Name,
                    this.cacheFunction.CacheKey(this.keySchema.LowerRange(from, timeFrom)),
                    this.cacheFunction.CacheKey(this.keySchema.UpperRange(to, timeTo)));

            var HasNextCondition = this.keySchema.HasNextCondition(from, to, timeFrom, timeTo);
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, HasNextCondition, this.cacheFunction);

            return new MergedSortedCacheWindowStoreKeyValueIterator(
                this.Context,
                filteredCacheIterator,
                underlyingIterator,
                this.bytesSerdes,
                this.windowSize,
                this.cacheFunction
            );
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(DateTime timeFrom, DateTime timeTo)
        {
            this.ValidateStoreOpen();

            IKeyValueIterator<IWindowed<Bytes>, byte[]> underlyingIterator = this.Wrapped.FetchAll(timeFrom, timeTo);
            MemoryLRUCacheBytesIterator cacheIterator = this.cache.All(this.Name);

            var hasNextCondition = this.keySchema.HasNextCondition(null, null, timeFrom, timeTo);
            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
                new FilteredCacheIterator(cacheIterator, hasNextCondition, this.cacheFunction);

            return new MergedSortedCacheWindowStoreKeyValueIterator(
                this.Context,
                filteredCacheIterator,
                underlyingIterator,
                this.bytesSerdes,
                this.windowSize,
                this.cacheFunction);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
        {
            this.ValidateStoreOpen();

            IKeyValueIterator<IWindowed<Bytes>, byte[]> underlyingIterator = this.Wrapped.All();
            MemoryLRUCacheBytesIterator cacheIterator = this.cache.All(this.Name);

            return new MergedSortedCacheWindowStoreKeyValueIterator(
                this.Context,
                cacheIterator,
                underlyingIterator,
                this.bytesSerdes,
                this.windowSize,
                this.cacheFunction);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public override void Flush()
        {
            this.cache.Flush(this.Name);
            this.Wrapped.Flush();
        }

        public override void Close()
        {
            this.Flush();
            this.cache.Close(this.Name);
            this.Wrapped.Close();
        }

        public void Add(Bytes key, byte[] value)
        {
        }

        public byte[] Fetch(Bytes key, long time)
        {
            throw new NotImplementedException();
        }

        public IWindowStoreIterator<byte[]> Fetch(Bytes key, long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to, long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }
    }
}
