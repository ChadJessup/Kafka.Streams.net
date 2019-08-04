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
namespace Kafka.streams.state.internals;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
using Kafka.Common.metrics.Sensor;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.kstream.Windowed;
using Kafka.Streams.kstream.internals.TimeWindow;
using Kafka.Streams.Processor.IProcessorContext;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.internals.InternalProcessorContext;
using Kafka.Streams.Processor.internals.metrics.StreamsMetricsImpl;
using Kafka.Streams.State.WindowStore;
using Kafka.Streams.State.WindowStoreIterator;
using Kafka.Streams.State.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.EXPIRED_WINDOW_RECORD_DROP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreKeyBytes;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreTimestamp;


public class InMemoryWindowStore : WindowStore<Bytes, byte[]>
{

    private static Logger LOG = LoggerFactory.getLogger(InMemoryWindowStore.class);
    private static int SEQNUM_SIZE = 4;

    private string name;
    private string metricScope;
    private InternalProcessorContext context;
    private Sensor expiredRecordSensor;
    private int seqnum = 0;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private long retentionPeriod;
    private long windowSize;
    private bool retainDuplicates;

    private ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> segmentMap = new ConcurrentSkipListMap<>();
    private Set<InMemoryWindowStoreIteratorWrapper> openIterators = ConcurrentHashMap.newKeySet();

    private volatile bool open = false;

    InMemoryWindowStore(string name,
                        long retentionPeriod,
                        long windowSize,
                        bool retainDuplicates,
                        string metricScope)
{
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
        this.metricScope = metricScope;
    }

    public override string name()
{
        return name;
    }

    public override void init(IProcessorContext context, IStateStore root)
{
        this.context = (InternalProcessorContext) context;

        StreamsMetricsImpl metrics = this.context.metrics();
        string taskName = context.taskId().ToString();
        expiredRecordSensor = metrics.storeLevelSensor(
            taskName,
            name(),
            EXPIRED_WINDOW_RECORD_DROP,
            RecordingLevel.INFO
        );
        addInvocationRateAndCount(
            expiredRecordSensor,
            "stream-" + metricScope + "-metrics",
            metrics.tagMap("task-id", taskName, metricScope + "-id", name()),
            EXPIRED_WINDOW_RECORD_DROP
        );

        if (root != null)
{
            context.register(root, (key, value) ->
{
                put(Bytes.wrap(extractStoreKeyBytes(key)), value, extractStoreTimestamp(key));
            });
        }
        open = true;
    }

    public override void put(Bytes key, byte[] value)
{
        put(key, value, context.timestamp());
    }

    public override void put(Bytes key, byte[] value, long windowStartTimestamp)
{
        removeExpiredSegments();
        maybeUpdateSeqnumForDups();
        observedStreamTime = Math.Max(observedStreamTime, windowStartTimestamp);

        Bytes keyBytes = retainDuplicates ? wrapForDups(key, seqnum) : key;

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod)
{
            expiredRecordSensor.record();
            LOG.warn("Skipping record for expired segment.");
        } else
{
            if (value != null)
{
                segmentMap.computeIfAbsent(windowStartTimestamp, t -> new ConcurrentSkipListMap<>());
                segmentMap[windowStartTimestamp).Add(keyBytes, value];
            } else
{
                segmentMap.computeIfPresent(windowStartTimestamp, (t, kvMap) ->
{
                    kvMap.Remove(keyBytes);
                    if (kvMap.isEmpty())
{
                        segmentMap.Remove(windowStartTimestamp);
                    }
                    return kvMap;
                });
            }
        }
    }

    public override byte[] fetch(Bytes key, long windowStartTimestamp)
{

        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod)
{
            return null;
        }

        ConcurrentNavigableMap<Bytes, byte[]> kvMap = segmentMap[windowStartTimestamp];
        if (kvMap == null)
{
            return null;
        } else
{
            return kvMap[key];
        }
    }

    @Deprecated
    public override WindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo)
{

        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        // add one b/c records expire exactly retentionPeriod ms after created
        long minTime = Math.Max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime)
{
            return WrappedInMemoryWindowStoreIterator.emptyIterator();
        }

        return registerNewWindowStoreIterator(
            key, segmentMap.subMap(minTime, true, timeTo, true).entrySet().iterator());
    }

    @Deprecated
    public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
                                                           Bytes to,
                                                           long timeFrom,
                                                           long timeTo)
{
        Objects.requireNonNull(from, "from key cannot be null");
        Objects.requireNonNull(to, "to key cannot be null");

        removeExpiredSegments();

        if (from.compareTo(to) > 0)
{
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        // add one b/c records expire exactly retentionPeriod ms after created
        long minTime = Math.Max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime)
{
            return KeyValueIterators.emptyIterator();
        }

        return registerNewWindowedKeyValueIterator(
            from, to, segmentMap.subMap(minTime, true, timeTo, true).entrySet().iterator());
    }

    @Deprecated
    public override KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo)
{
        removeExpiredSegments();

        // add one b/c records expire exactly retentionPeriod ms after created
        long minTime = Math.Max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime)
{
            return KeyValueIterators.emptyIterator();
        }

        return registerNewWindowedKeyValueIterator(
            null, null, segmentMap.subMap(minTime, true, timeTo, true).entrySet().iterator());
    }

    public override KeyValueIterator<Windowed<Bytes>, byte[]> all()
{
        removeExpiredSegments();

        long minTime = observedStreamTime - retentionPeriod;

        return registerNewWindowedKeyValueIterator(
            null, null, segmentMap.tailMap(minTime, false).entrySet().iterator());
    }

    public override bool persistent()
{
        return false;
    }

    public override bool isOpen()
{
        return open;
    }

    public override void flush()
{
        // do-nothing since it is in-memory
    }

    public override void close()
{
        if (openIterators.size() != 0)
{
            LOG.warn("Closing {} open iterators for store {}", openIterators.size(), name);
            foreach (InMemoryWindowStoreIteratorWrapper it in openIterators)
{
                it.close();
            }
        }
        
        segmentMap.clear();
        open = false;
    }

    private void removeExpiredSegments()
{
        long minLiveTime = Math.Max(0L, observedStreamTime - retentionPeriod + 1);
        foreach (InMemoryWindowStoreIteratorWrapper it in openIterators)
{
            minLiveTime = Math.Min(minLiveTime, it.minTime());
        }
        segmentMap.headMap(minLiveTime, false).clear();
    }

    private void maybeUpdateSeqnumForDups()
{
        if (retainDuplicates)
{
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }

    private static Bytes wrapForDups(Bytes key, int seqnum)
{
        ByteBuffer buf = ByteBuffer.allocate(key().Length + SEQNUM_SIZE];
        buf.Add(key()];
        buf.putInt(seqnum);

        return Bytes.wrap(buf.array());
    }

    private static Bytes getKey(Bytes keyBytes)
{
        byte[] bytes = new byte[keyBytes[].Length  - SEQNUM_SIZE];
        System.arraycopy(keyBytes(), 0, bytes, 0, bytes.Length];
        return Bytes.wrap(bytes);

    }

    private WrappedInMemoryWindowStoreIterator registerNewWindowStoreIterator(Bytes key,
                                                                              Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator)
{
        Bytes keyFrom = retainDuplicates ? wrapForDups(key, 0) : key;
        Bytes keyTo = retainDuplicates ? wrapForDups(key, Integer.MAX_VALUE) : key;

        WrappedInMemoryWindowStoreIterator iterator =
            new WrappedInMemoryWindowStoreIterator(keyFrom, keyTo, segmentIterator, openIterators::Remove, retainDuplicates);

        openIterators.add(iterator);
        return iterator;
    }

    private WrappedWindowedKeyValueIterator registerNewWindowedKeyValueIterator(Bytes keyFrom,
                                                                                Bytes keyTo,
                                                                                Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator)
{
        Bytes from = (retainDuplicates && keyFrom != null) ? wrapForDups(keyFrom, 0) : keyFrom;
        Bytes to = (retainDuplicates && keyTo != null) ? wrapForDups(keyTo, Integer.MAX_VALUE) : keyTo;

        WrappedWindowedKeyValueIterator iterator =
            new WrappedWindowedKeyValueIterator(from,
                                                to,
                                                segmentIterator,
                                                openIterators::Remove,
                                                retainDuplicates,
                                                windowSize);
        openIterators.add(iterator);
        return iterator;
    }


    interface ClosingCallback
{
        void deregisterIterator(InMemoryWindowStoreIteratorWrapper iterator);
    }

    private static abstract class InMemoryWindowStoreIteratorWrapper
{

        private Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator;
        private Iterator<Map.Entry<Bytes, byte[]>> recordIterator;
        private KeyValue<Bytes, byte[]> next;
        private long currentTime;

        private bool allKeys;
        private Bytes keyFrom;
        private Bytes keyTo;
        private bool retainDuplicates;
        private ClosingCallback callback;

        InMemoryWindowStoreIteratorWrapper(Bytes keyFrom,
                                           Bytes keyTo,
                                           Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                                           ClosingCallback callback,
                                           bool retainDuplicates)
{
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            allKeys = (keyFrom == null) && (keyTo == null);
            this.retainDuplicates = retainDuplicates;

            this.segmentIterator = segmentIterator;
            this.callback = callback;
            recordIterator = segmentIterator == null ? null : setRecordIterator();
        }

        public bool hasNext()
{
            if (next != null)
{
                return true;
            }
            if (recordIterator == null || (!recordIterator.hasNext() && !segmentIterator.hasNext()))
{
                return false;
            }

            next = getNext();
            if (next == null)
{
                return false;
            }

            if (allKeys || !retainDuplicates)
{
                return true;
            }

            Bytes key = getKey(next.key);
            if (key.compareTo(getKey(keyFrom)) >= 0 && key.compareTo(getKey(keyTo)) <= 0)
{
                return true;
            } else
{
                next = null;
                return hasNext();
            }
        }

        public void close()
{
            next = null;
            recordIterator = null;
            callback.deregisterIterator(this);
        }

        // getNext is only called when either recordIterator or segmentIterator has a next
        // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
        protected KeyValue<Bytes, byte[]> getNext()
{
            while (!recordIterator.hasNext())
{
                recordIterator = setRecordIterator();
                if (recordIterator == null)
{
                    return null;
                }
            }
            Map.Entry<Bytes, byte[]> nextRecord = recordIterator.next();
            return new KeyValue<>(nextRecord.getKey(), nextRecord.getValue());
        }

        // Resets recordIterator to point to the next segment and returns null if there are no more segments
        // Note it may not actually point to anything if no keys in range exist in the next segment
        Iterator<Map.Entry<Bytes, byte[]>> setRecordIterator()
{
            if (!segmentIterator.hasNext())
{
                return null;
            }

            Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>> currentSegment = segmentIterator.next();
            currentTime = currentSegment.getKey();

            if (allKeys)
{
                return currentSegment.getValue().entrySet().iterator();
            } else
{
                return currentSegment.getValue().subMap(keyFrom, true, keyTo, true).entrySet().iterator();
            }
        }

        Long minTime()
{
            return currentTime;
        }
    }

    private static class WrappedInMemoryWindowStoreIterator : InMemoryWindowStoreIteratorWrapper : WindowStoreIterator<byte[]> 
{

        WrappedInMemoryWindowStoreIterator(Bytes keyFrom,
                                           Bytes keyTo,
                                           Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                                           ClosingCallback callback,
                                           bool retainDuplicates) 
{
            super(keyFrom, keyTo, segmentIterator, callback, retainDuplicates);
        }

        
        public Long peekNextKey()
{
            if (!hasNext())
{
                throw new NoSuchElementException();
            }
            return super.currentTime;
        }

        
        public KeyValue<Long, byte[]> next()
{
            if (!hasNext())
{
                throw new NoSuchElementException();
            }

            KeyValue<Long, byte[]> result = new KeyValue<>(super.currentTime, super.next.value];
            super.next = null;
            return result;
        }

        public static WrappedInMemoryWindowStoreIterator emptyIterator()
{
            return new WrappedInMemoryWindowStoreIterator(null, null, null, it -> { }, false);
        }
    }

    private static class WrappedWindowedKeyValueIterator : InMemoryWindowStoreIteratorWrapper : KeyValueIterator<Windowed<Bytes>, byte[]>
{

        private long windowSize;

        WrappedWindowedKeyValueIterator(Bytes keyFrom,
                                        Bytes keyTo,
                                        Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                                        ClosingCallback callback,
                                        bool retainDuplicates,
                                        long windowSize)
{
            super(keyFrom, keyTo, segmentIterator, callback, retainDuplicates);
            this.windowSize = windowSize;
        }

        public Windowed<Bytes> peekNextKey()
{
            if (!hasNext())
{
                throw new NoSuchElementException();
            }
            return getWindowedKey();
        }

        public KeyValue<Windowed<Bytes>, byte[]> next()
{
            if (!hasNext())
{
                throw new NoSuchElementException();
            }

            KeyValue<Windowed<Bytes>, byte[]> result = new KeyValue<>(getWindowedKey(), super.next.value];
            super.next = null;
            return result;
        }

        private Windowed<Bytes> getWindowedKey()
{
            Bytes key = super.retainDuplicates ? getKey(super.next.key) : super.next.key;
            long endTime = super.currentTime + windowSize;

            if (endTime < 0)
{
                LOG.warn("Warning: window end time was truncated to Long.MAX");
                endTime = Long.MAX_VALUE;
            }

            TimeWindow timeWindow = new TimeWindow(super.currentTime, endTime);
            return new Windowed<>(key, timeWindow);
        }
    }
}