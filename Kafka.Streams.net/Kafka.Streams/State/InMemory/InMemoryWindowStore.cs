//using Confluent.Kafka;
//using Kafka.Common;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Windowed;
//using Microsoft.Extensions.Logging;
//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemoryWindowStore : IWindowStore<Bytes, byte[]>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<InMemoryWindowStore>();
//        private static int SEQNUM_SIZE = 4;

//        private string Name;
//        private string metricScope;
//        private IInternalProcessorContext context;
//        //private Sensor expiredRecordSensor;
//        private int seqnum = 0;
//        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

//        private long retentionPeriod;
//        private long windowSize;
//        private bool retainDuplicates;

//        private ConcurrentDictionary<long, ConcurrentDictionary<Bytes, byte[]>> segmentMap = new ConcurrentDictionary<long, ConcurrentDictionary<Bytes, byte[]>>();
//        private HashSet<InMemoryWindowStoreIteratorWrapper> openIterators = new HashSet<InMemoryWindowStoreIteratorWrapper>();

//        private volatile bool open = false;

//        string IStateStore.Name { get; }

//        public InMemoryWindowStore(
//            string Name,
//            long retentionPeriod,
//            long windowSize,
//            bool retainDuplicates,
//            string metricScope)
//        {
//            this.Name = Name;
//            this.retentionPeriod = retentionPeriod;
//            this.windowSize = windowSize;
//            this.retainDuplicates = retainDuplicates;
//            this.metricScope = metricScope;
//        }

//        public override void Init(IProcessorContext context, IStateStore root)
//        {
//            this.context = (IInternalProcessorContext)context;

//            //StreamsMetricsImpl metrics = this.context.metrics;
//            string taskName = context.taskId.ToString();
//            //expiredRecordSensor = metrics.storeLevelSensor(
//            //    taskName,
//            //    Name,
//            //    EXPIRED_WINDOW_RECORD_DROP,
//            //    RecordingLevel.INFO
//            //);
//            //addInvocationRateAndCount(
//            //     expiredRecordSensor,
//            //     "stream-" + metricScope + "-metrics",
//            //     metrics.tagMap("task-id", taskName, metricScope + "-id", Name),
//            //     EXPIRED_WINDOW_RECORD_DROP
//            // );

//            if (root != null)
//            {
//                //            context.register(root, (key, value) =>
//                //{
//                //    Put(Bytes.Wrap(extractStoreKeyBytes(key)), value, extractStoreTimestamp(key));
//                //});
//            }
//            open = true;
//        }

//        public void Put(Bytes key, byte[] value)
//        {
//            Put(key, value, context.Timestamp);
//        }

//        public void Put(Bytes key, byte[] value, long windowStartTimestamp)
//        {
//            removeExpiredSegments();
//            maybeUpdateSeqnumForDups();
//            observedStreamTime = Math.Max(observedStreamTime, windowStartTimestamp);

//            Bytes keyBytes = retainDuplicates ? wrapForDups(key, seqnum) : key;

//            if (windowStartTimestamp <= observedStreamTime - retentionPeriod)
//            {
//                //expiredRecordSensor.record();
//                //LOG.LogWarning("Skipping record for expired segment.");
//            }
//            else
//            {
//                if (value != null)
//                {
//                    //segmentMap.computeIfAbsent(windowStartTimestamp, t => new ConcurrentSkipListMap<>());
//                    //segmentMap[windowStartTimestamp].Add(keyBytes, value);
//                }
//                else
//                {
//                    //                  segmentMap.computeIfPresent(windowStartTimestamp, (t, kvMap) =>
//                    //{
//                    //    kvMap.Remove(keyBytes);
//                    //    if (kvMap.IsEmpty())
//                    //    {
//                    //        segmentMap.Remove(windowStartTimestamp);
//                    //    }
//                    //    return kvMap;
//                    //});
//                }
//            }
//        }

//        public byte[] Fetch(Bytes key, long windowStartTimestamp)
//        {

//            key = key ?? throw new ArgumentNullException(nameof(key));

//            removeExpiredSegments();

//            if (windowStartTimestamp <= observedStreamTime - retentionPeriod)
//            {
//                return null;
//            }

//            ConcurrentDictionary<Bytes, byte[]> kvMap = segmentMap[windowStartTimestamp];
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
//        public IWindowStoreIterator<byte[]> Fetch(Bytes key, long timeFrom, long timeTo)
//        {

//            key = key ?? throw new ArgumentNullException(nameof(key));

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
//        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from,
//                                                               Bytes to,
//                                                               long timeFrom,
//                                                               long timeTo)
//        {
//            from = from ?? throw new ArgumentNullException(nameof(from));
//            to = to ?? throw new ArgumentNullException(nameof(to));

//            removeExpiredSegments();

//            if (from.CompareTo(to) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
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
//        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(long timeFrom, long timeTo)
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

//        public IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
//        {
//            removeExpiredSegments();

//            long minTime = observedStreamTime - retentionPeriod;

//            return registerNewWindowedKeyValueIterator(
//                null, null, segmentMap.tailMap(minTime, false).iterator());
//        }

//        public bool Persistent()
//        {
//            return false;
//        }

//        public bool IsOpen()
//        {
//            return open;
//        }

//        public void Flush()
//        {
//            // do-nothing since it is in-memory
//        }

//        public void Close()
//        {
//            if (openIterators.Count != 0)
//            {
//                LOG.LogWarning("Closing {} open iterators for store {}", openIterators.size(), Name);
//                foreach (InMemoryWindowStoreIteratorWrapper it in openIterators)
//                {
//                    it.Close();
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
//            ByteBuffer buf = new ByteBuffer().Allocate(key.Get().Length + SEQNUM_SIZE);
//            buf.Add(key.Get());
//            buf.putInt(seqnum);

//            return Bytes.Wrap(buf.array());
//        }

//        private static Bytes getKey(Bytes keyBytes)
//        {
//            byte[] bytes = new byte[keyBytes.Get().Length - SEQNUM_SIZE];
//            System.arraycopy(keyBytes.Get(), 0, bytes, 0, bytes.Length);
//            return Bytes.Wrap(bytes);

//        }

//        private WrappedInMemoryWindowStoreIterator registerNewWindowStoreIterator(Bytes key,
//                                                                                  IEnumerator<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> segmentIterator)
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
//                                                                                    IEnumerator<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> segmentIterator)
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

//        public void Put(Bytes key, byte[] value)
//        {
//            throw new NotImplementedException();
//        }

//        public void Put(Bytes key, byte[] value, long windowStartTimestamp)
//        {
//            throw new NotImplementedException();
//        }

//        public IWindowStoreIterator<byte[]> Fetch(Bytes key, long timeFrom, long timeTo)
//        {
//            throw new NotImplementedException();
//        }

//        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to, long timeFrom, long timeTo)
//        {
//            throw new NotImplementedException();
//        }

//        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(long timeFrom, long timeTo)
//        {
//            throw new NotImplementedException();
//        }

//        public void Add(Bytes key, byte[] value)
//        {
//            throw new NotImplementedException();
//        }

//        public void Init(IProcessorContext context, IStateStore root)
//        {
//            throw new NotImplementedException();
//        }

//        public void Flush()
//        {
//            throw new NotImplementedException();
//        }

//        public void Close()
//        {
//            throw new NotImplementedException();
//        }

//        public bool Persistent()
//        {
//            throw new NotImplementedException();
//        }

//        public bool IsOpen()
//        {
//            throw new NotImplementedException();
//        }

//        public bool IsPresent()
//        {
//            throw new NotImplementedException();
//        }

//        public byte[] Fetch(Bytes key, long time)
//        {
//            throw new NotImplementedException();
//        }

//        public IWindowStoreIterator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
//        {
//            throw new NotImplementedException();
//        }

//        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to, DateTime fromTime, DateTime toTime)
//        {
//            throw new NotImplementedException();
//        }

//        public IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
//        {
//            throw new NotImplementedException();
//        }

//        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(DateTime from, DateTime to)
//        {
//            throw new NotImplementedException();
//        }

//        public interface IClosingCallback
//        {
//            void DeregisterIterator(InMemoryWindowStoreIteratorWrapper iterator);
//        }

//        private abstract class InMemoryWindowStoreIteratorWrapper
//        {
//            private IEnumerator<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> segmentIterator;
//            private IEnumerator<KeyValuePair<Bytes, byte[]>>? recordIterator;
//            private KeyValuePair<Bytes, byte[]>? next;
//            private long currentTime;

//            private bool allKeys;
//            private Bytes keyFrom;
//            private Bytes keyTo;
//            private bool retainDuplicates;
//            private ClosingCallback callback;

//            InMemoryWindowStoreIteratorWrapper(Bytes keyFrom,
//                                               Bytes keyTo,
//                                               IEnumerator<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> segmentIterator,
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

//            public bool HasNext()
//            {
//                if (next != null)
//                {
//                    return true;
//                }
//                if (recordIterator == null || (!recordIterator.HasNext() && !segmentIterator.HasNext()))
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
//                    return HasNext();
//                }
//            }

//            public void Close()
//            {
//                next = null;
//                recordIterator = null;
//                callback.deregisterIterator(this);
//            }

//            // getNext is only called when either recordIterator or segmentIterator has a next
//            // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
//            protected KeyValuePair<Bytes, byte[]> getNext()
//            {
//                while (!recordIterator.HasNext())
//                {
//                    recordIterator = setRecordIterator();
//                    if (recordIterator == null)
//                    {
//                        return null;
//                    }
//                }
//                KeyValuePair<Bytes, byte[]> nextRecord = recordIterator.MoveNext();
//                return KeyValuePair.Create(nextRecord.Key, nextRecord.Value);
//            }

//            // Resets recordIterator to point to the next segment and returns null if there are no more segments
//            // Note it may not actually point to anything if no keys in range exist in the next segment
//            IEnumerator<KeyValuePair<Bytes, byte[]>> setRecordIterator()
//            {
//                if (!segmentIterator.HasNext())
//                {
//                    return null;
//                }

//                KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>> currentSegment = segmentIterator.MoveNext();
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