
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

//        private string name;
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

//        public override void init(IProcessorContext<K, V> context, IStateStore root)
//        {
//            initInternal((IInternalProcessorContext)context);
//            base.init(context, root);
//        }


//        private void initInternal(IInternalProcessorContext<K, V> context)
//        {
//            this.context = context;
//            string topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name);

//            bytesSerdes = new StateSerdes<>(
//                topic,
//                Serdes.Bytes(),
//                Serdes.ByteArray());
//            name = context.taskId() + "-" + name;
//            cache = this.context.getCache();

//            cache.AddDirtyEntryFlushListener(name, entries =>
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
//            byte[] binaryWindowKey = cacheFunction.key(entry.key()).get();
//            Windowed<Bytes> windowedKeyBytes = WindowKeySchema.fromStoreBytesKey(binaryWindowKey, windowSize);
//            long windowStartTimestamp = windowedKeyBytes.window.start();
//            Bytes binaryKey = windowedKeyBytes.key;
//            if (flushListener != null)
//            {
//                byte[] rawNewValue = entry.newValue();
//                byte[] rawOldValue = rawNewValue == null || sendOldValues ?
//                    wrapped.fetch(binaryKey, windowStartTimestamp) : null;

//                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
//                // we can skip flushing to downstream as well as writing to underlying store
//                if (rawNewValue != null || rawOldValue != null)
//                {
//                    // we need to get the old values if needed, and then put to store, and then flush
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
//        public override void put(Bytes key,
//                                     byte[] value)
//        {
//            put(key, value, context.timestamp());
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override void put(Bytes key,
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
//                    context.headers(),
//                    true,
//                    context.offset(),
//                    context.timestamp(),
//                    context.partition(),
//                    context.Topic);
//            cache.Add(name, cacheFunction.cacheKey(keyBytes), entry);

//            maxObservedTimestamp = Math.Max(keySchema.segmentTimestamp(keyBytes), maxObservedTimestamp);
//        }

//        public override byte[] fetch(Bytes key,
//                            long timestamp)
//        {
//            validateStoreOpen();
//            Bytes bytesKey = WindowKeySchema.toStoreKeyBinary(key, timestamp, 0);
//            Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
//            if (cache == null)
//            {
//                return wrapped.fetch(key, timestamp);
//            }
//            LRUCacheEntry entry = cache[name, cacheKey];
//            if (entry == null)
//            {
//                return wrapped.fetch(key, timestamp);
//            }
//            else
//            {
//                return entry.value();
//            }
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override WindowStoreIterator<byte[]> fetch(Bytes key,
//                                                              long timeFrom,
//                                                              long timeTo)
//        {
//            // since this function may not access the underlying inner store, we need to validate
//            // if store is open outside as well.
//            validateStoreOpen();

//            IWindowStoreIterator<byte[]> underlyingIterator = wrapped.fetch(key, timeFrom, timeTo);
//            if (cache == null)
//            {
//                return underlyingIterator;
//            }

//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped.persistent() ?
//                new CacheIteratorWrapper(key, timeFrom, timeTo) :
//                cache.range(name,
//                            cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, timeFrom)),
//                            cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, timeTo))
//                );

//            HasNextCondition hasNextCondition = keySchema.hasNextCondition(key, key, timeFrom, timeTo);
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(
//                cacheIterator, hasNextCondition, cacheFunction
//            );

//            return new MergedSortedCacheWindowStoreIterator(filteredCacheIterator, underlyingIterator);
//        }


//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
//                                                               Bytes to,
//                                                               long timeFrom,
//                                                               long timeTo)
//        {
//            if (from.CompareTo(to) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//                return KeyValueIterators.emptyIterator();
//            }

//            // since this function may not access the underlying inner store, we need to validate
//            // if store is open outside as well.
//            validateStoreOpen();

//            IKeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator =
//                wrapped.fetch(from, to, timeFrom, timeTo);
//            if (cache == null)
//            {
//                return underlyingIterator;
//            }

//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped.persistent() ?
//                new CacheIteratorWrapper(from, to, timeFrom, timeTo) :
//                cache.range(name,
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


//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
//                                                                  long timeTo)
//        {
//            validateStoreOpen();

//            IKeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped.fetchAll(timeFrom, timeTo);
//            MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);

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

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> all()
//        {
//            validateStoreOpen();

//            IKeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped.all();
//            MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);

//            return new MergedSortedCacheWindowStoreKeyValueIterator(
//                cacheIterator,
//                underlyingIterator,
//                bytesSerdes,
//                windowSize,
//                cacheFunction);
//        }

//        [MethodImpl(MethodImplOptions.Synchronized)]
//        public override void flush()
//        {
//            cache.flush(name);
//            wrapped.flush();
//        }

//        public override void close()
//        {
//            flush();
//            cache.close(name);
//            wrapped.close();
//        }
//    }
//}