/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package org.apache.kafka.streams.state.internals;

import java.util.NoSuchElementException;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.kstream.internals.SessionWindow;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.internals.InternalProcessorContext;
using Kafka.Streams.Processor.internals.ProcessorRecordContext;
using Kafka.Streams.Processor.internals.RecordQueue;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.SessionStore;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CachingSessionStore
    : WrappedStateStore<SessionStore<Bytes, byte[]>, byte[], byte[]>
    : SessionStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]>
{

    private static Logger LOG = LoggerFactory.getLogger(CachingSessionStore.class);

    private SessionKeySchema keySchema;
    private SegmentedCacheFunction cacheFunction;
    private string cacheName;
    private ThreadCache cache;
    private InternalProcessorContext context;
    private CacheFlushListener<byte[], byte[]> flushListener;
    private bool sendOldValues;

    private long maxObservedTimestamp; // Refers to the window end time (determines segmentId)

    CachingSessionStore(SessionStore<Bytes, byte[]> bytesStore,
                        long segmentInterval)
{
        super(bytesStore);
        this.keySchema = new SessionKeySchema();
        this.cacheFunction = new SegmentedCacheFunction(keySchema, segmentInterval);
        this.maxObservedTimestamp = RecordQueue.UNKNOWN;
    }

    public override void init(IProcessorContext context, IStateStore root)
{
        initInternal((InternalProcessorContext) context);
        super.init(context, root);
    }

    @SuppressWarnings("unchecked")
    private void initInternal(InternalProcessorContext context)
{
        this.context = context;

        cacheName = context.taskId() + "-" + name();
        cache = context.getCache();
        cache.addDirtyEntryFlushListener(cacheName, entries ->
{
            for (ThreadCache.DirtyEntry entry : entries)
{
                putAndMaybeForward(entry, context);
            }
        });
    }

    private void putAndMaybeForward(ThreadCache.DirtyEntry entry, InternalProcessorContext context)
{
        Bytes binaryKey = cacheFunction.key(entry.key());
        Windowed<Bytes> bytesKey = SessionKeySchema.from(binaryKey);
        if (flushListener != null)
{
            byte[] newValueBytes = entry.newValue();
            byte[] oldValueBytes = newValueBytes == null || sendOldValues ?
                wrapped().fetchSession(bytesKey.key(), bytesKey.window().start(), bytesKey.window().end()) : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (newValueBytes != null || oldValueBytes != null)
{
                // we need to get the old values if needed, and then put to store, and then flush
                wrapped().put(bytesKey, entry.newValue());

                ProcessorRecordContext current = context.recordContext();
                context.setRecordContext(entry.entry().context());
                try
{
                    flushListener.apply(
                        binaryKey.get(),
                        newValueBytes,
                        sendOldValues ? oldValueBytes : null,
                        entry.entry().context().timestamp());
                } finally
{
                    context.setRecordContext(current);
                }
            }
        } else
{
            wrapped().put(bytesKey, entry.newValue());
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
                context.topic());
        cache.put(cacheName, cacheFunction.cacheKey(binaryKey), entry);

        maxObservedTimestamp = Math.max(keySchema.segmentTimestamp(binaryKey), maxObservedTimestamp);
    }

    public override void remove(Windowed<Bytes> sessionKey)
{
        validateStoreOpen();
        put(sessionKey, null);
    }

    public override KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key,
                                                                  long earliestSessionEndTime,
                                                                  long latestSessionStartTime)
{
        validateStoreOpen();

        PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator = wrapped().persistent() ?
            new CacheIteratorWrapper(key, earliestSessionEndTime, latestSessionStartTime) :
            cache.range(cacheName,
                        cacheFunction.cacheKey(keySchema.lowerRangeFixedSize(key, earliestSessionEndTime)),
                        cacheFunction.cacheKey(keySchema.upperRangeFixedSize(key, latestSessionStartTime))
            );

        KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped().findSessions(key,
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

    public override KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom,
                                                                  Bytes keyTo,
                                                                  long earliestSessionEndTime,
                                                                  long latestSessionStartTime)
{
        if (keyFrom.compareTo(keyTo) > 0)
{
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();

        Bytes cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, earliestSessionEndTime));
        Bytes cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime));
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, cacheKeyFrom, cacheKeyTo);

        KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator = wrapped().findSessions(
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
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        if (cache == null)
{
            return wrapped().fetchSession(key, startTime, endTime);
        } else
{
            Bytes bytesKey = SessionKeySchema.toBinary(key, startTime, endTime);
            Bytes cacheKey = cacheFunction.cacheKey(bytesKey);
            LRUCacheEntry entry = cache.get(cacheName, cacheKey);
            if (entry == null)
{
                return wrapped().fetchSession(key, startTime, endTime);
            } else
{
                return entry.value();
            }
        }
    }

    public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
{
        Objects.requireNonNull(key, "key cannot be null");
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
                                                           Bytes to)
{
        Objects.requireNonNull(from, "from cannot be null");
        Objects.requireNonNull(to, "to cannot be null");
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }

    public void flush()
{
        cache.flush(cacheName);
        super.flush();
    }

    public void close()
{
        flush();
        cache.close(cacheName);
        super.close();
    }

    private class CacheIteratorWrapper : PeekingKeyValueIterator<Bytes, LRUCacheEntry>
{

        private long segmentInterval;

        private Bytes keyFrom;
        private Bytes keyTo;
        private long latestSessionStartTime;
        private long lastSegmentId;

        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private ThreadCache.MemoryLRUCacheBytesIterator current;

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

        @Override
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

        @Override
        public Bytes peekNextKey()
{
            if (!hasNext())
{
                throw new NoSuchElementException();
            }
            return current.peekNextKey();
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> peekNext()
{
            if (!hasNext())
{
                throw new NoSuchElementException();
            }
            return current.peekNext();
        }

        @Override
        public KeyValue<Bytes, LRUCacheEntry> next()
{
            if (!hasNext())
{
                throw new NoSuchElementException();
            }
            return current.next();
        }

        @Override
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
            } else
{
                cacheKeyFrom = cacheFunction.cacheKey(keySchema.lowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = cacheFunction.cacheKey(keySchema.upperRange(keyTo, latestSessionStartTime), currentSegmentId);
            }
        }

        private Bytes segmentLowerRangeFixedSize(Bytes key, long segmentBeginTime)
{
            Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(0, Math.max(0, segmentBeginTime)));
            return SessionKeySchema.toBinary(sessionKey);
        }

        private Bytes segmentUpperRangeFixedSize(Bytes key, long segmentEndTime)
{
            Windowed<Bytes> sessionKey = new Windowed<>(key, new SessionWindow(Math.min(latestSessionStartTime, segmentEndTime), segmentEndTime));
            return SessionKeySchema.toBinary(sessionKey);
        }
    }
}
