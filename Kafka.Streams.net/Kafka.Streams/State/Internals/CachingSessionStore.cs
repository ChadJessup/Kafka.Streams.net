//using Kafka.Streams.State;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.Interfaces;
//using Microsoft.Extensions.Logging;
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;

//namespace Kafka.Streams.State.Internals
//{
//    public class CachingSessionStore
//        : WrappedStateStore<ISessionStore<Bytes, byte[]>, byte[], byte[]>
//        , ISessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]>
//    {

//        private static ILogger LOG = new LoggerFactory().CreateLogger<CachingSessionStore>();

//        private SessionKeySchema keySchema;
//        private SegmentedCacheFunction cacheFunction;
//        private string cacheName;
//        private ThreadCache cache;
//        private IInternalProcessorContext<Bytes, byte[]> context;
//        private ICacheFlushListener<byte[], byte[]> flushListener;
//        private bool sendOldValues;

//        private long maxObservedTimestamp; // Refers to the window end time (determines segmentId)

//        public CachingSessionStore(ISessionStore<Bytes, byte[]> bytesStore, long segmentInterval)
//            : base(bytesStore)
//        {
//            this.keySchema = new SessionKeySchema();
//            this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
//            this.maxObservedTimestamp = RecordQueue.UNKNOWN;
//        }

//        public override void init(IProcessorContext<Bytes, byte[]> context, IStateStore root)
//        {
//            initInternal((IInternalProcessorContext<Bytes, byte[]>)context);
//            base.init(context, root);
//        }


//        private void initInternal(IInternalProcessorContext<Bytes, byte[]> context)
//        {
//            this.context = context;

//            cacheName = context.taskId() + "-" + name;
//            cache = context.getCache();
//            cache.addDirtyEntryFlushListener(cacheName, entries);
//            //=>
//            //{
//            //    foreach (DirtyEntry entry in entries)
//            //    {
//            //        putAndMaybeForward(entry, context);
//            //    }
//            //});
//        }

//        private void putAndMaybeForward(DirtyEntry entry, IInternalProcessorContext<K, V> context)
//        {
//            Bytes binaryKey = cacheFunction.key(entry.key());
//            Windowed<Bytes> bytesKey = SessionKeySchema.from(binaryKey);
//            if (flushListener != null)
//            {
//                byte[] newValueBytes = entry.newValue();
//                byte[] oldValueBytes = newValueBytes == null || sendOldValues ?
//                    wrapped.fetchSession(bytesKey.key, bytesKey.window.start(), bytesKey.window.end()) : null;

//                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
//                // we can skip flushing to downstream as well as writing to underlying store
//                if (newValueBytes != null || oldValueBytes != null)
//                {
//                    // we need to get the old values if needed, and then put to store, and then flush
//                    wrapped.Add(bytesKey, entry.newValue());

//                    ProcessorRecordContext current = context.recordContext();
//                    context.setRecordContext(entry.entry().context);
//                    try
//                    {
//                        flushListener.apply(
//                            binaryKey.get(),
//                            newValueBytes,
//                            sendOldValues ? oldValueBytes : null,
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
//                wrapped.Add(bytesKey, entry.newValue());
//            }
//        }

//        public override bool setFlushListener(ICacheFlushListener<byte[], byte[]> flushListener,
//                                        bool sendOldValues)
//        {
//            this.flushListener = flushListener;
//            this.sendOldValues = sendOldValues;

//            return true;
//        }

//        public override void put(Windowed<Bytes> key, byte[] value)
//        {
//            validateStoreOpen();
//            Bytes binaryKey = SessionKeySchema.toBinary(key);
//            LRUCacheEntry entry =
//                new LRUCacheEntry(
//                    value,
//                    context.headers(),
//                    true,
//                    context.offset(),
//                    context.timestamp(),
//                    context.partition(),
//                    context.Topic);
//            cache.Add(cacheName, cacheFunction.cacheKey(binaryKey), entry);

//            maxObservedTimestamp = Math.Max(keySchema.segmentTimestamp(binaryKey), maxObservedTimestamp);
//        }

//        public override void Remove(Windowed<Bytes> sessionKey)
//        {
//            validateStoreOpen();
//            put(sessionKey, null);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key,
//                                                                      long earliestSessionEndTime,
//                                                                      long latestSessionStartTime)
//        {
//            validateStoreOpen();

//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped.persistent() ?
//                new CacheIteratorWrapper(key, earliestSessionEndTime, latestSessionStartTime) :
//                cache.range(cacheName,
//                            cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, earliestSessionEndTime)),
//                            cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, latestSessionStartTime))
//                );

//            IKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped.findSessions(key,
//                                                                                                   earliestSessionEndTime,
//                                                                                                   latestSessionStartTime);
//            HasNextCondition hasNextCondition = keySchema.hasNextCondition(key,
//                                                                                 key,
//                                                                                 earliestSessionEndTime,
//                                                                                 latestSessionStartTime);
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
//                new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
//            return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom,
//                                                                      Bytes keyTo,
//                                                                      long earliestSessionEndTime,
//                                                                      long latestSessionStartTime)
//        {
//            if (keyFrom.CompareTo(keyTo) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//                return KeyValueIterators.emptyIterator();
//            }

//            validateStoreOpen();

//            Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, earliestSessionEndTime));
//            Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime));
//            MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);

//            IKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped.findSessions(
//                keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
//            );
//            HasNextCondition hasNextCondition = keySchema.hasNextCondition(keyFrom,
//                                                                                 keyTo,
//                                                                                 earliestSessionEndTime,
//                                                                                 latestSessionStartTime);
//            IPeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
//                new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
//            return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction);
//        }

//        public override byte[] fetchSession(Bytes key, long startTime, long endTime)
//        {
//            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
//            validateStoreOpen();
//            if (cache == null)
//            {
//                return wrapped.fetchSession(key, startTime, endTime);
//            }
//            else
//            {
//                Bytes bytesKey = SessionKeySchema.toBinary(key, startTime, endTime);
//                Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
//                LRUCacheEntry entry = cache[cacheName, cacheKey];
//                if (entry == null)
//                {
//                    return wrapped.fetchSession(key, startTime, endTime);
//                }
//                else
//                {
//                    return entry.value();
//                }
//            }
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
//        {
//            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
//            return findSessions(key, 0, long.MaxValue);
//        }

//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
//                                                               Bytes to)
//        {
//            from = from ?? throw new System.ArgumentNullException("from cannot be null", nameof(from));
//            to = to ?? throw new System.ArgumentNullException("to cannot be null", nameof(to));
//            return findSessions(from, to, 0, long.MaxValue);
//        }

//        public void flush()
//        {
//            cache.flush(cacheName);
//            base.flush();
//        }

//        public void close()
//        {
//            flush();
//            cache.close(cacheName);
//            base.close();
//        }
//    }
//}