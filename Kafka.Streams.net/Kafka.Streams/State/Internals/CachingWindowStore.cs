/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.Streams.State.Internals;


using Kafka.Common.serialization.Serdes;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.KStream.Windowed;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.Internals.InternalProcessorContext;
using Kafka.Streams.Processor.Internals.ProcessorRecordContext;
using Kafka.Streams.Processor.Internals.ProcessorStateManager;
using Kafka.Streams.Processor.Internals.RecordQueue;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.StateSerdes;
using Kafka.Streams.State.WindowStore;
using Kafka.Streams.State.WindowStoreIterator;



class CachingWindowStore
    : WrappedStateStore<WindowStore<Bytes, byte[]>, byte[], byte[]>
    : WindowStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]>
{

    private static ILogger LOG= new LoggerFactory().CreateLogger<CachingWindowStore);

    private long windowSize;
    private SegmentedBytesStore.KeySchema keySchema = new WindowKeySchema();

    private string name;
    private ThreadCache cache;
    private bool sendOldValues;
    private InternalProcessorContext context;
    private StateSerdes<Bytes, byte[]> bytesSerdes;
    private CacheFlushListener<byte[], byte[]> flushListener;

    private long maxObservedTimestamp;

    private SegmentedCacheFunction cacheFunction;

    CachingWindowStore(WindowStore<Bytes, byte[]> underlying,
                       long windowSize,
                       long segmentInterval)
{
        base(underlying);
        this.windowSize = windowSize;
        this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
        this.maxObservedTimestamp = RecordQueue.UNKNOWN;
    }

    public override void init(IProcessorContext context, IStateStore root)
{
        initInternal((InternalProcessorContext) context);
        base.init(context, root);
    }

    
    private void initInternal(InternalProcessorContext context)
{
        this.context = context;
        string topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), name());

        bytesSerdes = new StateSerdes<>(
            topic,
            Serdes.Bytes(),
            Serdes.ByteArray());
        name = context.taskId() + "-" + name();
        cache = this.context.getCache();

        cache.AddDirtyEntryFlushListener(name, entries ->
{
            foreach (ThreadCache.DirtyEntry entry in entries)
{
                putAndMaybeForward(entry, context);
            }
        });
    }

    private void putAndMaybeForward(ThreadCache.DirtyEntry entry,
                                    InternalProcessorContext context)
{
        byte[] binaryWindowKey = cacheFunction.key(entry.key())[];
        Windowed<Bytes> windowedKeyBytes = WindowKeySchema.fromStoreBytesKey(binaryWindowKey, windowSize);
        long windowStartTimestamp = windowedKeyBytes.window().start();
        Bytes binaryKey = windowedKeyBytes.key();
        if (flushListener != null)
{
            byte[] rawNewValue = entry.newValue();
            byte[] rawOldValue = rawNewValue == null || sendOldValues ?
                wrapped().fetch(binaryKey, windowStartTimestamp) : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (rawNewValue != null || rawOldValue != null)
{
                // we need to get the old values if needed, and then put to store, and then flush
                wrapped().Add(binaryKey, entry.newValue(), windowStartTimestamp);

                ProcessorRecordContext current = context.recordContext();
                context.setRecordContext(entry.entry().context());
                try
{
                    flushListener.apply(
                        binaryWindowKey,
                        rawNewValue,
                        sendOldValues ? rawOldValue : null,
                        entry.entry().context().timestamp());
                } finally
{
                    context.setRecordContext(current);
                }
            }
        } else
{
            wrapped().Add(binaryKey, entry.newValue(), windowStartTimestamp);
        }
    }

    public override bool setFlushListener(CacheFlushListener<byte[], byte[]> flushListener,
                                    bool sendOldValues)
{
        this.flushListener = flushListener;
        this.sendOldValues = sendOldValues;

        return true;
    }

    public override synchronized void put(Bytes key,
                                 byte[] value)
{
        put(key, value, context.timestamp());
    }

    public override synchronized void put(Bytes key,
                                 byte[] value,
                                 long windowStartTimestamp)
{
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();
        
        Bytes keyBytes = WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, 0);
        LRUCacheEntry entry =
            new LRUCacheEntry(
                value,
                context.headers(),
                true,
                context.offset(),
                context.timestamp(),
                context.partition(),
                context.topic());
        cache.Add(name, cacheFunction.cacheKey(keyBytes), entry);

        maxObservedTimestamp = Math.Max(keySchema.segmentTimestamp(keyBytes), maxObservedTimestamp);
    }

    public override byte[] fetch(Bytes key,
                        long timestamp)
{
        validateStoreOpen();
        Bytes bytesKey = WindowKeySchema.toStoreKeyBinary(key, timestamp, 0);
        Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
        if (cache == null)
{
            return wrapped().fetch(key, timestamp);
        }
        LRUCacheEntry entry = cache[name, cacheKey];
        if (entry == null)
{
            return wrapped().fetch(key, timestamp);
        } else
{
            return entry.value();
        }
    }

    
    public override synchronized WindowStoreIterator<byte[]> fetch(Bytes key,
                                                          long timeFrom,
                                                          long timeTo)
{
        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        WindowStoreIterator<byte[]> underlyingIterator = wrapped().fetch(key, timeFrom, timeTo);
        if (cache == null)
{
            return underlyingIterator;
        }

        PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(key, timeFrom, timeTo) :
            cache.range(name,
                        cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, timeFrom)),
                        cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, timeTo))
            );

        HasNextCondition hasNextCondition = keySchema.hasNextCondition(key, key, timeFrom, timeTo);
        PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(
            cacheIterator, hasNextCondition, cacheFunction
        );

        return new MergedSortedCacheWindowStoreIterator(filteredCacheIterator, underlyingIterator);
    }

    
    public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
                                                           Bytes to,
                                                           long timeFrom,
                                                           long timeTo)
{
        if (from.compareTo(to) > 0)
{
            LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        // since this function may not access the underlying inner store, we need to validate
        // if store is open outside as well.
        validateStoreOpen();

        KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator =
            wrapped().fetch(from, to, timeFrom, timeTo);
        if (cache == null)
{
            return underlyingIterator;
        }

        PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(from, to, timeFrom, timeTo) :
            cache.range(name,
                        cacheFunction.cacheKey(keySchema.lowerRange(from, timeFrom)),
                        cacheFunction.cacheKey(keySchema.upperRange(to, timeTo))
            );

        HasNextCondition hasNextCondition = keySchema.hasNextCondition(from, to, timeFrom, timeTo);
        PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator = new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);

        return new MergedSortedCacheWindowStoreKeyValueIterator(
            filteredCacheIterator,
            underlyingIterator,
            bytesSerdes,
            windowSize,
            cacheFunction
        );
    }

    
    public override KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom,
                                                              long timeTo)
{
        validateStoreOpen();

        KeyValueIterator<Windowed<Bytes>, byte[]> underlyingIterator = wrapped().fetchAll(timeFrom, timeTo);
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);

        HasNextCondition hasNextCondition = keySchema.hasNextCondition(null, null, timeFrom, timeTo);
        PeekingKeyValueIterator<Bytes, LRUCacheEntry> filteredCacheIterator =
            new FilteredCacheIterator(cacheIterator, hasNextCondition, cacheFunction);
        return new MergedSortedCacheWindowStoreKeyValueIterator(
                filteredCacheIterator,
                underlyingIterator,
                bytesSerdes,
                windowSize,
                cacheFunction
        );
    }

    public override KeyValueIterator<Windowed<Bytes>, byte[]> all()
{
        validateStoreOpen();

        KeyValueIterator<Windowed<Bytes>, byte[]>  underlyingIterator = wrapped().all();
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(name);

        return new MergedSortedCacheWindowStoreKeyValueIterator(
            cacheIterator,
            underlyingIterator,
            bytesSerdes,
            windowSize,
            cacheFunction
        );
    }

    public override synchronized void flush()
{
        cache.flush(name);
        wrapped().flush();
    }

    public override void close()
{
        flush();
        cache.close(name);
        wrapped().close();
    }

    private CacheIteratorWrapper : PeekingKeyValueIterator<Bytes, LRUCacheEntry>
{

        private long segmentInterval;

        private Bytes keyFrom;
        private Bytes keyTo;
        private long timeTo;
        private long lastSegmentId;

        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private ThreadCache.MemoryLRUCacheBytesIterator current;

        private CacheIteratorWrapper(Bytes key,
                                     long timeFrom,
                                     long timeTo)
{
            this(key, key, timeFrom, timeTo);
        }

        private CacheIteratorWrapper(Bytes keyFrom,
                                     Bytes keyTo,
                                     long timeFrom,
                                     long timeTo)
{
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.timeTo = timeTo;
            this.lastSegmentId = cacheFunction.segmentId(Math.Min(timeTo, maxObservedTimestamp));

            this.segmentInterval = cacheFunction.getSegmentInterval();
            this.currentSegmentId = cacheFunction.segmentId(timeFrom);

            setCacheKeyRange(timeFrom, currentSegmentLastTime());

            this.current = cache.range(name, cacheKeyFrom, cacheKeyTo);
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
            return Math.Min(timeTo, currentSegmentBeginTime() + segmentInterval - 1);
        }

        private void getNextSegmentIterator()
{
            ++currentSegmentId;
            lastSegmentId = cacheFunction.segmentId(Math.Min(timeTo, maxObservedTimestamp));

            if (currentSegmentId > lastSegmentId)
{
                current = null;
                return;
            }

            setCacheKeyRange(currentSegmentBeginTime(), currentSegmentLastTime());

            current.close();
            current = cache.range(name, cacheKeyFrom, cacheKeyTo);
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
            } else
{
                cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, timeTo), currentSegmentId);
            }
        }

        private Bytes segmentLowerRangeFixedSize(Bytes key, long segmentBeginTime)
{
            return WindowKeySchema.toStoreKeyBinary(key, Math.Max(0, segmentBeginTime), 0);
        }

        private Bytes segmentUpperRangeFixedSize(Bytes key, long segmentEndTime)
{
            return WindowKeySchema.toStoreKeyBinary(key, segmentEndTime, int.MaxValue);
        }
    }
}
