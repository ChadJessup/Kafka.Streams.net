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

using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.internals.InternalProcessorContext;
using Kafka.Streams.Processor.internals.ProcessorRecordContext;
using Kafka.Streams.State.KeyValueIterator;
using Kafka.Streams.State.KeyValueStore;









public class CachingKeyValueStore
    : WrappedStateStore<IKeyValueStore<Bytes, byte[]>, byte[], byte[]>
    : IKeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], byte[]>
{

    private static Logger LOG = LoggerFactory.getLogger(CachingKeyValueStore.class);

    private CacheFlushListener<byte[], byte[]> flushListener;
    private bool sendOldValues;
    private string cacheName;
    private ThreadCache cache;
    private InternalProcessorContext context;
    private Thread streamThread;
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    CachingKeyValueStore(IKeyValueStore<Bytes, byte[]> underlying)
{
        super(underlying);
    }

    public override void init(IProcessorContext context,
                     IStateStore root)
{
        initInternal(context);
        super.init(context, root);
        // save the stream thread as we only ever want to trigger a flush
        // when the stream thread is the current thread.
        streamThread = Thread.currentThread();
    }

    
    private void initInternal(IProcessorContext context)
{
        this.context = (InternalProcessorContext) context;

        this.cache = this.context.getCache();
        this.cacheName = ThreadCache.nameSpaceFromTaskIdAndStore(context.taskId().ToString(), name());
        cache.AddDirtyEntryFlushListener(cacheName, entries ->
{
            foreach (ThreadCache.DirtyEntry entry in entries)
{
                putAndMaybeForward(entry, (InternalProcessorContext) context);
            }
        });
    }

    private void putAndMaybeForward(ThreadCache.DirtyEntry entry,
                                    InternalProcessorContext context)
{
        if (flushListener != null)
{
            byte[] rawNewValue = entry.newValue();
            byte[] rawOldValue = rawNewValue == null || sendOldValues ? wrapped()[entry.key()] : null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (rawNewValue != null || rawOldValue != null)
{
                // we need to get the old values if needed, and then put to store, and then flush
                wrapped().Add(entry.key(), entry.newValue());

                ProcessorRecordContext current = context.recordContext();
                context.setRecordContext(entry.entry().context());
                try
{
                    flushListener.apply(
                        entry.key()[],
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
            wrapped().Add(entry.key(), entry.newValue());
        }
    }

    public override bool setFlushListener(CacheFlushListener<byte[], byte[]> flushListener,
                                    bool sendOldValues)
{
        this.flushListener = flushListener;
        this.sendOldValues = sendOldValues;

        return true;
    }

    public override void put(Bytes key,
                    byte[] value)
{
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try
{
            // for null bytes, we still put it into cache indicating tombstones
            putInternal(key, value);
        } finally
{
            lock.writeLock().unlock();
        }
    }

    private void putInternal(Bytes key,
                             byte[] value)
{
        cache.Add(
            cacheName,
            key,
            new LRUCacheEntry(
                value,
                context.headers(),
                true,
                context.offset(),
                context.timestamp(),
                context.partition(),
                context.topic()));
    }

    public override byte[] putIfAbsent(Bytes key,
                              byte[] value)
{
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try
{
            byte[] v = getInternal(key);
            if (v == null)
{
                putInternal(key, value);
            }
            return v;
        } finally
{
            lock.writeLock().unlock();
        }
    }

    public override void putAll(List<KeyValue<Bytes, byte[]>> entries)
{
        validateStoreOpen();
        lock.writeLock().lock();
        try
{
            foreach (KeyValue<Bytes, byte[]> entry in entries)
{
                Objects.requireNonNull(entry.key, "key cannot be null");
                put(entry.key, entry.value);
            }
        } finally
{
            lock.writeLock().unlock();
        }
    }

    public override byte[] delete(Bytes key)
{
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try
{
            return deleteInternal(key);
        } finally
{
            lock.writeLock().unlock();
        }
    }

    private byte[] deleteInternal(Bytes key)
{
        byte[] v = getInternal(key);
        putInternal(key, null);
        return v;
    }

    public override byte[] get(Bytes key)
{
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        Lock theLock;
        if (Thread.currentThread().Equals(streamThread))
{
            theLock = lock.writeLock();
        } else
{
            theLock = lock.readLock();
        }
        theLock.lock();
        try
{
            return getInternal(key);
        } finally
{
            theLock.unlock();
        }
    }

    private byte[] getInternal(Bytes key)
{
        LRUCacheEntry entry = null;
        if (cache != null)
{
            entry = cache[cacheName, key];
        }
        if (entry == null)
{
            byte[] rawValue = wrapped()[key);
            if (rawValue == null)
{
                return null;
            }
            // only update the cache if this call is on the streamThread
            // as we don't want other threads to trigger an eviction/flush
            if (Thread.currentThread().Equals(streamThread))
{
                cache.Add(cacheName, key, new LRUCacheEntry(rawValue));
            }
            return rawValue;
        } else
{
            return entry.value();
        }
    }

    public override KeyValueIterator<Bytes, byte[]> range(Bytes from,
                                                 Bytes to)
{
        if (from.compareTo(to) > 0)
{
            LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();
        KeyValueIterator<Bytes, byte[]> storeIterator = wrapped().range(from, to);
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.range(cacheName, from, to);
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
    }

    public override KeyValueIterator<Bytes, byte[]> all()
{
        validateStoreOpen();
        KeyValueIterator<Bytes, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>(this.name(), wrapped().all());
        ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = cache.all(cacheName);
        return new MergedSortedCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator);
    }

    public override long approximateNumEntries()
{
        validateStoreOpen();
        lock.readLock().lock();
        try
{
            return wrapped().approximateNumEntries();
        } finally
{
            lock.readLock().unlock();
        }
    }

    public override void flush()
{
        lock.writeLock().lock();
        try
{
            cache.flush(cacheName);
            super.flush();
        } finally
{
            lock.writeLock().unlock();
        }
    }

    public override void close()
{
        try
{
            flush();
        } finally
{
            try
{
                super.close();
            } finally
{
                cache.close(cacheName);
            }
        }
    }
}
