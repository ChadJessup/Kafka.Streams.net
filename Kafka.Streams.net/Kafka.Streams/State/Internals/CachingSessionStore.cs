using Kafka.Streams.State;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State.Interfaces;
using Microsoft.Extensions.Logging;
using Kafka.Common.Utils;

namespace Kafka.Streams.State.Internals
{
    public class CachingSessionStore
        : WrappedStateStore<ISessionStore<Bytes, byte[]>, byte[], byte[]>
        , ISessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]>
    {

        private static ILogger LOG = new LoggerFactory().CreateLogger<CachingSessionStore>();

        private SessionKeySchema keySchema;
        private SegmentedCacheFunction cacheFunction;
        private string cacheName;
        private ThreadCache cache;
        private IInternalProcessorContext<K, V>  context;
        private CacheFlushListener<byte[], byte[]> flushListener;
        private bool sendOldValues;

        private long maxObservedTimestamp; // Refers to the window end time (determines segmentId)

        CachingSessionStore(ISessionStore<Bytes, byte[]> bytesStore, long segmentInterval)
            : base(bytesStore)
        {
            this.keySchema = new SessionKeySchema();
            this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
            this.maxObservedTimestamp = RecordQueue.UNKNOWN;
        }

        public override void init(IProcessorContext<K, V> context, IStateStore root)
        {
            initInternal((IInternalProcessorContext)context);
            base.init(context, root);
        }


        private void initInternal(IInternalProcessorContext<K, V>  context)
        {
            this.context = context;

            cacheName = context.taskId() + "-" + name();
            cache = context.getCache();
            cache.AddDirtyEntryFlushListener(cacheName, entries=>
    {
                foreach (DirtyEntry entry in entries)
                {
                    putAndMaybeForward(entry, context);
                }
            });
        }

        private void putAndMaybeForward(DirtyEntry entry, IInternalProcessorContext<K, V>  context)
        {
            Bytes binaryKey = cacheFunction.key(entry.key());
            Windowed<Bytes> bytesKey = SessionKeySchema.from(binaryKey);
            if (flushListener != null)
            {
                byte[] newValueBytes = entry.newValue();
                byte[] oldValueBytes = newValueBytes == null || sendOldValues ?
                    wrapped.fetchSession(bytesKey.key(), bytesKey.window().start(), bytesKey.window().end()) : null;

                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream as well as writing to underlying store
                if (newValueBytes != null || oldValueBytes != null)
                {
                    // we need to get the old values if needed, and then put to store, and then flush
                    wrapped.Add(bytesKey, entry.newValue());

                    ProcessorRecordContext current = context.recordContext();
                    context.setRecordContext(entry.entry().context);
                    try
                    {
                        flushListener.apply(
                            binaryKey[],
                            newValueBytes,
                            sendOldValues ? oldValueBytes : null,
                            entry.entry().context.timestamp());
                    }
                    finally
                    {
                        context.setRecordContext(current);
                    }
                }
            }
            else
            {
                wrapped.Add(bytesKey, entry.newValue());
            }
        }

        public override bool setFlushListener(CacheFlushListener<byte[], byte[]> flushListener,
                                        bool sendOldValues)
        {
            this.flushListener = flushListener;
            this.sendOldValues = sendOldValues;

            return true;
        }

        public override void put(Windowed<Bytes> key, byte[] value)
        {
            validateStoreOpen();
            Bytes binaryKey = SessionKeySchema.toBinary(key);
            LRUCacheEntry entry =
                new LRUCacheEntry(
                    value,
                    context.headers(),
                    true,
                    context.offset(),
                    context.timestamp(),
                    context.partition(),
                    context.Topic);
            cache.Add(cacheName, cacheFunction.cacheKey(binaryKey), entry);

            maxObservedTimestamp = Math.Max(keySchema.segmentTimestamp(binaryKey), maxObservedTimestamp);
        }

        public override void Remove(Windowed<Bytes> sessionKey)
        {
            validateStoreOpen();
            put(sessionKey, null);
        }

        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key,
                                                                      long earliestSessionEndTime,
                                                                      long latestSessionStartTime)
        {
            validateStoreOpen();

            PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped.persistent() ?
                new CacheIteratorWrapper(key, earliestSessionEndTime, latestSessionStartTime) :
                cache.range(cacheName,
                            cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, earliestSessionEndTime)),
                            cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, latestSessionStartTime))
                );

            IKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped.findSessions(key,
                                                                                                   earliestSessionEndTime,
                                                                                                   latestSessionStartTime);
            HasNextCondition hasNextCondition = keySchema.hasNextCondition(key,
                                                                                 key,
                                                                                 earliestSessionEndTime,
                                                                                 latestSessionStartTime);
            PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
                new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
            return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction);
        }

        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom,
                                                                      Bytes keyTo,
                                                                      long earliestSessionEndTime,
                                                                      long latestSessionStartTime)
        {
            if (keyFrom.CompareTo(keyTo) > 0)
            {
                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
                return KeyValueIterators.emptyIterator();
            }

            validateStoreOpen();

            Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, earliestSessionEndTime));
            Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime));
            MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);

            IKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped.findSessions(
                keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime
            );
            HasNextCondition hasNextCondition = keySchema.hasNextCondition(keyFrom,
                                                                                 keyTo,
                                                                                 earliestSessionEndTime,
                                                                                 latestSessionStartTime);
            PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
                new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
            return new MergedSortedCacheSessionStoreIterator(filteredCacheIterator, storeIterator, cacheFunction);
        }

        public override byte[] fetchSession(Bytes key, long startTime, long endTime)
        {
            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
            validateStoreOpen();
            if (cache == null)
            {
                return wrapped.fetchSession(key, startTime, endTime);
            }
            else
            {
                Bytes bytesKey = SessionKeySchema.toBinary(key, startTime, endTime);
                Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
                LRUCacheEntry entry = cache[cacheName, cacheKey];
                if (entry == null)
                {
                    return wrapped.fetchSession(key, startTime, endTime);
                }
                else
                {
                    return entry.value();
                }
            }
        }

        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
        {
            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));
            return findSessions(key, 0, long.MaxValue);
        }

        public override IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
                                                               Bytes to)
        {
            from = from ?? throw new System.ArgumentNullException("from cannot be null", nameof(from));
            to = to ?? throw new System.ArgumentNullException("to cannot be null", nameof(to));
            return findSessions(from, to, 0, long.MaxValue);
        }

        public void flush()
        {
            cache.flush(cacheName);
            base.flush();
        }

        public void close()
        {
            flush();
            cache.close(cacheName);
            base.close();
        }

        private CacheIteratorWrapper : PeekingKeyValueIterator<Bytes, LRUCacheEntry>
{

        private long segmentInterval;

        private Bytes keyFrom;
        private Bytes keyTo;
        private long latestSessionStartTime;
        private long lastSegmentId;

        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private MemoryLRUCacheBytesIterator current;

        private CacheIteratorWrapper(Bytes key,
                                     long earliestSessionEndTime,
                                     long latestSessionStartTime)
        {
            this(key, key, earliestSessionEndTime, latestSessionStartTime);
        }

        private CacheIteratorWrapper(Bytes keyFrom,
                                     Bytes keyTo,
                                     long earliestSessionEndTime,
                                     long latestSessionStartTime)
        {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;
            this.lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);
            this.segmentInterval = cacheFunction.getSegmentInterval();

            this.currentSegmentId = cacheFunction.segmentId(earliestSessionEndTime);

            setCacheKeyRange(earliestSessionEndTime, currentSegmentLastTime());

            this.current = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);
        }


        public bool hasNext()
        {
            if (current == null)
            {
                return false;
            }

            if (current.hasNext())
            {
                return true;
            }

            while (!current.hasNext())
            {
                getNextSegmentIterator();
                if (current == null)
                {
                    return false;
                }
            }
            return true;
        }


        public Bytes peekNextKey()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return current.peekNextKey();
        }


        public KeyValue<Bytes, LRUCacheEntry> peekNext()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return current.peekNext();
        }


        public KeyValue<Bytes, LRUCacheEntry> next()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            return current.next();
        }


        public void close()
        {
            current.close();
        }

        private long currentSegmentBeginTime()
        {
            return currentSegmentId * segmentInterval;
        }

        private long currentSegmentLastTime()
        {
            return currentSegmentBeginTime() + segmentInterval - 1;
        }

        private void getNextSegmentIterator()
        {
            ++currentSegmentId;
            lastSegmentId = cacheFunction.segmentId(maxObservedTimestamp);

            if (currentSegmentId > lastSegmentId)
            {
                current = null;
                return;
            }

            setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

            current.close();
            current = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);
        }

        private void setCacheKeyRange(long lowerRangeEndTime, long upperRangeEndTime)
        {
            if (cacheFunction.segmentId(lowerRangeEndTime) != cacheFunction.segmentId(upperRangeEndTime))
            {
                throw new InvalidOperationException("Error iterating over segments: segment interval has changed");
            }

            if (keyFrom == keyTo)
            {
                cacheKeyFrom = cacheFunction.cacheKey(segmentLowerRangeFixedSize(keyFrom, lowerRangeEndTime));
                cacheKeyTo = cacheFunction.cacheKey(segmentUpperRangeFixedSize(keyTo, upperRangeEndTime));
            }
            else
            {
                cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime), currentSegmentId);
            }
        }

        private Bytes segmentLowerRangeFixedSize(Bytes key, long segmentBeginTime)
        {
            Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(0, Math.Max(0, segmentBeginTime)));
            return SessionKeySchema.toBinary(sessionKey);
        }

        private Bytes segmentUpperRangeFixedSize(Bytes key, long segmentEndTime)
        {
            Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(Math.Min(latestSessionStartTime, segmentEndTime), segmentEndTime));
            return SessionKeySchema.toBinary(sessionKey);
        }
    }
}
