//using Kafka.Common.Metrics;
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.Processor.Internals;
//using Kafka.Streams.Processor.Internals.Metrics;
//using Kafka.Streams.State.Interfaces;
//using Microsoft.Extensions.Logging;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemoryWindowStore : IWindowStore<Bytes, byte[]>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<InMemoryWindowStore>();
//        private static int SEQNUM_SIZE = 4;

//        private string name;
//        private string metricScope;
//        private IInternalProcessorContext<K, V> context;
//        private Sensor expiredRecordSensor;
//        private int seqnum = 0;
//        private long observedStreamTime = ConsumeResult.NO_TIMESTAMP;

//        private long retentionPeriod;
//        private long windowSize;
//        private bool retainDuplicates;

//        private ConcurrentNavigableMap<long, ConcurrentNavigableMap<Bytes, byte[]>> segmentMap = new ConcurrentSkipListMap<>();
//        private HashSet<InMemoryWindowStoreIteratorWrapper> openIterators = ConcurrentHashMap.newKeySet();

//        private volatile bool open = false;

//        public InMemoryWindowStore(
//            string name,
//            long retentionPeriod,
//            long windowSize,
//            bool retainDuplicates,
//            string metricScope)
//        {
//            this.name = name;
//            this.retentionPeriod = retentionPeriod;
//            this.windowSize = windowSize;
//            this.retainDuplicates = retainDuplicates;
//            this.metricScope = metricScope;
//        }

//        public override void init(IProcessorContext<K, V> context, IStateStore root)
//        {
//            this.context = (IInternalProcessorContext)context;

//            StreamsMetricsImpl metrics = this.context.metrics;
//            string taskName = context.taskId().ToString();
//            expiredRecordSensor = metrics.storeLevelSensor(
//                taskName,
//                name,
//                EXPIRED_WINDOW_RECORD_DROP,
//                RecordingLevel.INFO
//            );
//            addInvocationRateAndCount(
//                 expiredRecordSensor,
//                 "stream-" + metricScope + "-metrics",
//                 metrics.tagMap("task-id", taskName, metricScope + "-id", name),
//                 EXPIRED_WINDOW_RECORD_DROP
//             );

//            if (root != null)
//            {
//    //            context.register(root, (key, value) =>
//    //{
//    //    put(Bytes.wrap(extractStoreKeyBytes(key)), value, extractStoreTimestamp(key));
//    //});
//            }
//            open = true;
//        }

//        public void put(Bytes key, byte[] value)
//        {
//            put(key, value, context.timestamp());
//        }

//        public void put(Bytes key, byte[] value, long windowStartTimestamp)
//        {
//            removeExpiredSegments();
//            maybeUpdateSeqnumForDups();
//            observedStreamTime = Math.Max(observedStreamTime, windowStartTimestamp);

//            Bytes keyBytes = retainDuplicates ? wrapForDups(key, seqnum) : key;

//            if (windowStartTimestamp <= observedStreamTime - retentionPeriod)
//            {
//                expiredRecordSensor.record();
//                LOG.LogWarning("Skipping record for expired segment.");
//            }
//            else
//            {
//                if (value != null)
//                {
//                    segmentMap.computeIfAbsent(windowStartTimestamp, t => new ConcurrentSkipListMap<>());
//                    segmentMap[windowStartTimestamp].Add(keyBytes, value);
//                }
//                else
//                {
//                    //                  segmentMap.computeIfPresent(windowStartTimestamp, (t, kvMap) =>
//                    //{
//                    //    kvMap.Remove(keyBytes);
//                    //    if (kvMap.isEmpty())
//                    //    {
//                    //        segmentMap.Remove(windowStartTimestamp);
//                    //    }
//                    //    return kvMap;
//                    //});
//                }
//            }
//        }

//        public byte[] fetch(Bytes key, long windowStartTimestamp)
//        {

//            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));

//            removeExpiredSegments();

//            if (windowStartTimestamp <= observedStreamTime - retentionPeriod)
//            {
//                return null;
//            }

//            ConcurrentNavigableMap<Bytes, byte[]> kvMap = segmentMap[windowStartTimestamp];
//            if (kvMap == null)
//            {
//                return null;
//            }
//            else
//            {
//                return kvMap[key];
//            }
//        }

//        [System.Obsolete]
//        public IWindowStoreIterator<byte[]> fetch(Bytes key, long timeFrom, long timeTo)
//        {

//            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));

//            removeExpiredSegments();

//            //.Add one b/c records expire exactly retentionPeriod ms after created
//            long minTime = Math.Max(timeFrom, observedStreamTime - retentionPeriod + 1);

//            if (timeTo < minTime)
//            {
//                return WrappedInMemoryWindowStoreIterator.emptyIterator();
//            }

//            return registerNewWindowStoreIterator(
//                key, segmentMap.subMap(minTime, true, timeTo, true).iterator());
//        }

//        [System.Obsolete]
//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from,
//                                                               Bytes to,
//                                                               long timeFrom,
//                                                               long timeTo)
//        {
//            from = from ?? throw new System.ArgumentNullException("from key cannot be null", nameof(from));
//            to = to ?? throw new System.ArgumentNullException("to key cannot be null", nameof(to));

//            removeExpiredSegments();

//            if (from.CompareTo(to) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//                return KeyValueIterators.emptyIterator();
//            }

//            //.Add one b/c records expire exactly retentionPeriod ms after created
//            long minTime = Math.Max(timeFrom, observedStreamTime - retentionPeriod + 1);

//            if (timeTo < minTime)
//            {
//                return KeyValueIterators.emptyIterator();
//            }

//            return registerNewWindowedKeyValueIterator(
//                from, to, segmentMap.subMap(minTime, true, timeTo, true).iterator());
//        }

//        [System.Obsolete]
//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(long timeFrom, long timeTo)
//        {
//            removeExpiredSegments();

//            //.Add one b/c records expire exactly retentionPeriod ms after created
//            long minTime = Math.Max(timeFrom, observedStreamTime - retentionPeriod + 1);

//            if (timeTo < minTime)
//            {
//                return KeyValueIterators.emptyIterator();
//            }

//            return registerNewWindowedKeyValueIterator(
//                null, null, segmentMap.subMap(minTime, true, timeTo, true).iterator());
//        }

//        public IKeyValueIterator<Windowed<Bytes>, byte[]> all()
//        {
//            removeExpiredSegments();

//            long minTime = observedStreamTime - retentionPeriod;

//            return registerNewWindowedKeyValueIterator(
//                null, null, segmentMap.tailMap(minTime, false).iterator());
//        }

//        public bool persistent()
//        {
//            return false;
//        }

//        public bool isOpen()
//        {
//            return open;
//        }

//        public void flush()
//        {
//            // do-nothing since it is in-memory
//        }

//        public void close()
//        {
//            if (openIterators.Count != 0)
//            {
//                LOG.LogWarning("Closing {} open iterators for store {}", openIterators.size(), name);
//                foreach (InMemoryWindowStoreIteratorWrapper it in openIterators)
//                {
//                    it.close();
//                }
//            }

//            segmentMap.clear();
//            open = false;
//        }

//        private void removeExpiredSegments()
//        {
//            long minLiveTime = Math.Max(0L, observedStreamTime - retentionPeriod + 1);
//            foreach (InMemoryWindowStoreIteratorWrapper it in openIterators)
//            {
//                minLiveTime = Math.Min(minLiveTime, it.minTime());
//            }
//            segmentMap.headMap(minLiveTime, false).clear();
//        }

//        private void maybeUpdateSeqnumForDups()
//        {
//            if (retainDuplicates)
//            {
//                seqnum = (seqnum + 1) & 0x7FFFFFFF;
//            }
//        }

//        private static Bytes wrapForDups(Bytes key, int seqnum)
//        {
//            ByteBuffer buf = ByteBuffer.allocate(key.get().Length + SEQNUM_SIZE);
//            buf.Add(key.get());
//            buf.putInt(seqnum);

//            return Bytes.wrap(buf.array());
//        }

//        private static Bytes getKey(Bytes keyBytes)
//        {
//            byte[] bytes = new byte[keyBytes.get().Length - SEQNUM_SIZE];
//            System.arraycopy(keyBytes.get(), 0, bytes, 0, bytes.Length);
//            return Bytes.wrap(bytes);

//        }

//        private WrappedInMemoryWindowStoreIterator registerNewWindowStoreIterator(Bytes key,
//                                                                                  IEnumerator<KeyValuePair<long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator)
//        {
//            Bytes keyFrom = retainDuplicates ? wrapForDups(key, 0) : key;
//            Bytes keyTo = retainDuplicates ? wrapForDups(key, int.MaxValue) : key;

//            WrappedInMemoryWindowStoreIterator iterator =
//                new WrappedInMemoryWindowStoreIterator(keyFrom, keyTo, segmentIterator, openIterators::Remove, retainDuplicates);

//            openIterators.Add(iterator);
//            return iterator;
//        }

//        private WrappedWindowedKeyValueIterator registerNewWindowedKeyValueIterator(Bytes keyFrom,
//                                                                                    Bytes keyTo,
//                                                                                    IEnumerator<KeyValuePair<long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator)
//        {
//            Bytes from = (retainDuplicates && keyFrom != null) ? wrapForDups(keyFrom, 0) : keyFrom;
//            Bytes to = (retainDuplicates && keyTo != null) ? wrapForDups(keyTo, int.MaxValue) : keyTo;

//            WrappedWindowedKeyValueIterator iterator =
//                new WrappedWindowedKeyValueIterator(from,
//                                                    to,
//                                                    segmentIterator,
//                                                    openIterators::Remove,
//                                                    retainDuplicates,
//                                                    windowSize);
//            openIterators.Add(iterator);
//            return iterator;
//        }


//        public interface ClosingCallback
//        {
//            void deregisterIterator(InMemoryWindowStoreIteratorWrapper iterator);
//        }

//        private static abstract class InMemoryWindowStoreIteratorWrapper
//        {

//            private IEnumerator<KeyValuePair<long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator;
//            private IEnumerator<KeyValuePair<Bytes, byte[]>> recordIterator;
//            private KeyValue<Bytes, byte[]> next;
//            private long currentTime;

//            private bool allKeys;
//            private Bytes keyFrom;
//            private Bytes keyTo;
//            private bool retainDuplicates;
//            private ClosingCallback callback;

//            InMemoryWindowStoreIteratorWrapper(Bytes keyFrom,
//                                               Bytes keyTo,
//                                               IEnumerator<KeyValuePair<long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
//                                               ClosingCallback callback,
//                                               bool retainDuplicates)
//            {
//                this.keyFrom = keyFrom;
//                this.keyTo = keyTo;
//                allKeys = (keyFrom == null) && (keyTo == null);
//                this.retainDuplicates = retainDuplicates;

//                this.segmentIterator = segmentIterator;
//                this.callback = callback;
//                recordIterator = segmentIterator == null ? null : setRecordIterator();
//            }

//            public bool hasNext()
//            {
//                if (next != null)
//                {
//                    return true;
//                }
//                if (recordIterator == null || (!recordIterator.hasNext() && !segmentIterator.hasNext()))
//                {
//                    return false;
//                }

//                next = getNext();
//                if (next == null)
//                {
//                    return false;
//                }

//                if (allKeys || !retainDuplicates)
//                {
//                    return true;
//                }

//                Bytes key = getKey(next.key);
//                if (key.CompareTo(getKey(keyFrom)) >= 0 && key.CompareTo(getKey(keyTo)) <= 0)
//                {
//                    return true;
//                }
//                else
//                {
//                    next = null;
//                    return hasNext();
//                }
//            }

//            public void close()
//            {
//                next = null;
//                recordIterator = null;
//                callback.deregisterIterator(this);
//            }

//            // getNext is only called when either recordIterator or segmentIterator has a next
//            // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
//            protected KeyValue<Bytes, byte[]> getNext()
//            {
//                while (!recordIterator.hasNext())
//                {
//                    recordIterator = setRecordIterator();
//                    if (recordIterator == null)
//                    {
//                        return null;
//                    }
//                }
//                KeyValuePair<Bytes, byte[]> nextRecord = recordIterator.next();
//                return new KeyValue<>(nextRecord.Key, nextRecord.Value);
//            }

//            // Resets recordIterator to point to the next segment and returns null if there are no more segments
//            // Note it may not actually point to anything if no keys in range exist in the next segment
//            IEnumerator<KeyValuePair<Bytes, byte[]>> setRecordIterator()
//            {
//                if (!segmentIterator.hasNext())
//                {
//                    return null;
//                }

//                KeyValuePair<long, ConcurrentNavigableMap<Bytes, byte[]>> currentSegment = segmentIterator.next();
//                currentTime = currentSegment.Key;

//                if (allKeys)
//                {
//                    return currentSegment.Value.iterator();
//                }
//                else
//                {
//                    return currentSegment.Value.subMap(keyFrom, true, keyTo, true).iterator();
//                }
//            }

//            public long minTime()
//            {
//                return currentTime;
//            }
//        }
//    }
//}