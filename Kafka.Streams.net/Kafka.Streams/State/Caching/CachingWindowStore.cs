
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.Interfaces;
//using Microsoft.Extensions.Logging;
//using System;
//using System.Runtime.CompilerServices;

//namespace Kafka.Streams.State.Internals
//{
//    public class CachingWindowStore
//        : WrappedStateStore<IWindowStore<Bytes, byte[]>, byte[], byte[]>
//    , IWindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<CachingWindowStore>();

//        private long windowSize;
//        private ISegmentedBytesStore.KeySchema keySchema = new WindowKeySchema();

//        private string Name;
//        private ThreadCache cache;
//        private bool sendOldValues;
//        private IInternalProcessorContext<K, V> context;
//        private StateSerdes<Bytes, byte[]> bytesSerdes;
//        private ICacheFlushListener<byte[], byte[]> flushListener;

//        private long maxObservedTimestamp;

//        private SegmentedCacheFunction cacheFunction;

//        public CachingWindowStore(
//            IWindowStore<Bytes, byte[]> underlying,
//            long windowSize,
//            long segmentInterval)
//        {
//            base(underlying);
//            this.windowSize = windowSize;
//            this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
//            this.maxObservedTimestamp = RecordQueue.UNKNOWN;
//        }

//        public override void Init(IProcessorContext context, IStateStore root)
//        {
//            initInternal((IInternalProcessorContext)context);
//            base.Init(context, root);
//        }


//        private void initInternal(IInternalProcessorContext<K, V> context)
//        {
//            this.context = context;
//            string topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), Name);

//            bytesSerdes = new StateSerdes<>(
//                topic,
//                Serdes.Bytes(),
//                Serdes.ByteArray());
//            Name = context.taskId + "-" + Name;
//            cache = this.context.getCache();

//            cache.AddDirtyEntryFlushListener(Name, entries =>
//    {
//        foreach (DirtyEntry entry in entries)
//        {
//            putAndMaybeForward(entry, context);
//        }
//    });
//        }

//        private void putAndMaybeForward(DirtyEntry entry,
//                                        IInternalProcessorContext<K, V> context)
//        {
//            byte[] binaryWindowKey = cacheFunction.key(entry.key()).Get();
//            IWindowed<Bytes> windowedKeyBytes = WindowKeySchema.fromStoreBytesKey(binaryWindowKey, windowSize);
//            long windowStartTimestamp = windowedKeyBytes.window.start();
//            Bytes binaryKey = windowedKeyBytes.key;
//            if (flushListener != null)
//            {
//                byte[] rawNewValue = entry.newValue();
//                byte[] rawOldValue = rawNewValue == null || sendOldValues ?
//                    wrapped.Fetch(binaryKey, windowStartTimestamp) : null;

//                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
//                // we can skip flushing to downstream as well as writing to underlying store
//                if (rawNewValue != null || rawOldValue != null)
//                {
//                    // we need to get the old values if needed, and then Put to store, and then Flush
//                    wrapped.Add(binaryKey, entry.newValue(), windowStartTimestamp);

//                    ProcessorRecordContext current = context.recordContext();
//                    context.setRecordContext(entry.entry().context);
//                    try
//                    {
//                        flushListener.apply(
//                            binaryWindowKey,
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
//                wrapped.Add(binaryKey, entry.newValue(), windowStartTimestamp);
//            }
//        }

//        public override bool setFlushListener(ICacheFlushListener<byte[], byte[]> flushListener,
//                                        bool sendOldValues)
//        {
//            this.flushListener = flushListener;
//            this.sendOldValues = sendOldValues;

//            return true;
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override void Put(Bytes key,
//                                     byte[] value)
//        {
//            Put(key, value, context.timestamp());
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override void Put(Bytes key,
//                                     byte[] value,
//                                     long windowStartTimestamp)
//        {
//            // since this function may not access the underlying inner store, we need to validate
//            // if store is open outside as well.
//            validateStoreOpen();

//            Bytes keyBytes = WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, 0);
//            LRUCacheEntry entry =
//                new LRUCacheEntry(
//                    value,
//                    context.Headers,
//                    true,
//                    context.offset(),
//                    context.timestamp(),
//                    context.Partition,
//                    context.Topic);
//            cache.Add(Name, cacheFunction.cacheKey(keyBytes), entry);

//            maxObservedTimestamp = Math.Max(keySchema.segmentTimestamp(keyBytes), maxObservedTimestamp);
//        }

//        public override byte[] Fetch(Bytes key,
//                            long timestamp)
//        {
//            validateStoreOpen();
//            Bytes bytesKey = WindowKeySchema.toStoreKeyBinary(key, timestamp, 0);
//            Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
//            if (cache == null)
//            {
//                return wrapped.Fetch(key, timestamp);
//            }
//            LRUCacheEntry entry = cache[Name, cacheKey];
//            if (entry == null)
//            {
//                return wrapped.Fetch(key, timestamp);
//            }
//            else
//            {
//                return entry.value();
//            }
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override IWindowStoreIterator<byte[]> Fetch(Bytes key,
//                                                              long timeFrom,
//                                                              long timeTo)
//        {
//            // since this function may not access the underlying inner store, we need to validate
//            // if store is open outside as well.
//            validateStoreOpen();

//            IWindowStoreIterator<byte[]> underlyingIterator = wrapped.Fetch(key, timeFrom, timeTo);
//            if (cache == null)
//            {
//                return underlyingIterator;
//            }

//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped.Persistent() ?
//                new CacheIteratorWrapper(key, timeFrom, timeTo) :
//                cache.Range(Name,
//                            cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, timeFrom)),
//                            cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, timeTo))
//                );

//            HasNextCondition hasNextCondition = keySchema.hasNextCondition(key, key, timeFrom, timeTo);
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(
//                cacheIterator, hasNextCondition, cacheFunction
//            );

//            return new MergedSortedCacheWindowStoreIterator(filteredCacheIterator, underlyingIterator);
//        }


//        public override IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from,
//                                                               Bytes to,
//                                                               long timeFrom,
//                                                               long timeTo)
//        {
//            if (from.CompareTo(to) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//                return KeyValueIterators.emptyIterator();
//            }

//            // since this function may not access the underlying inner store, we need to validate
//            // if store is open outside as well.
//            validateStoreOpen();

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> underlyingIterator =
//                wrapped.Fetch(from, to, timeFrom, timeTo);
//            if (cache == null)
//            {
//                return underlyingIterator;
//            }

//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped.Persistent() ?
//                new CacheIteratorWrapper(from, to, timeFrom, timeTo) :
//                cache.Range(Name,
//                            cacheFunction.cacheKey(keySchema.lowerRange(from, timeFrom)),
//                            cacheFunction.cacheKey(keySchema.upperRange(to, timeTo))
//                );

//            HasNextCondition hasNextCondition = keySchema.hasNextCondition(from, to, timeFrom, timeTo);
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

//            return new MergedSortedCacheWindowStoreKeyValueIterator(
//                filteredCacheIterator,
//                underlyingIterator,
//                bytesSerdes,
//                windowSize,
//                cacheFunction
//            );
//        }


//        public override IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(long timeFrom,
//                                                                  long timeTo)
//        {
//            validateStoreOpen();

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> underlyingIterator = wrapped.FetchAll(timeFrom, timeTo);
//            MemoryLRUCacheBytesIterator cacheIterator = cache.All(Name);

//            HasNextCondition hasNextCondition = keySchema.hasNextCondition(null, null, timeFrom, timeTo);
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
//                new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
//            return new MergedSortedCacheWindowStoreKeyValueIterator(
//                    filteredCacheIterator,
//                    underlyingIterator,
//                    bytesSerdes,
//                    windowSize,
//                    cacheFunction
//            );
//        }

//        public override IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
//        {
//            validateStoreOpen();

//            IKeyValueIterator<IWindowed<Bytes>, byte[]> underlyingIterator = wrapped.All();
//            MemoryLRUCacheBytesIterator cacheIterator = cache.All(Name);

//            return new MergedSortedCacheWindowStoreKeyValueIterator(
//                cacheIterator,
//                underlyingIterator,
//                bytesSerdes,
//                windowSize,
//                cacheFunction);
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override void Flush()
//        {
//            cache.Flush(Name);
//            wrapped.Flush();
//        }

//        public override void Close()
//        {
//            Flush();
//            cache.Close(Name);
//            wrapped.Close();
//        }
//    }
//}