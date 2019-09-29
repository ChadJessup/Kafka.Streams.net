///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */

//using Kafka.Common.Metrics;
//using Kafka.Common.Utils;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals.Metrics;
//using Kafka.Streams.State.Interfaces;
//using Microsoft.Extensions.Logging;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public class InMemorySessionStore : ISessionStore<Bytes, byte[]>
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<InMemorySessionStore>();

//        private string metricScope;
//        private Sensor expiredRecordSensor;
//        private long observedStreamTime = ConsumeResult.NO_TIMESTAMP;

//        private long retentionPeriod;

//        private ConcurrentNavigableMap<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>> endTimeMap = new ConcurrentSkipListMap<>();
//        private HashSet<InMemorySessionStoreIterator> openIterators = new HashSet<InMemorySessionStoreIterator>();

//        private volatile bool open = false;

//        public InMemorySessionStore(
//            string name,
//            long retentionPeriod,
//            string metricScope)
//        {
//            this.name = name;
//            this.retentionPeriod = retentionPeriod;
//            this.metricScope = metricScope;
//        }

//        public string name { get; }

//        public override void init(IProcessorContext<K, V> context, IStateStore root)
//        {
//            StreamsMetricsImpl metrics = ((IInternalProcessorContext)context).metrics;
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
//                context.register(root, (key, value) => put(SessionKeySchema.from(Bytes.wrap(key)), value));
//            }
//            open = true;
//        }

//        public override void put(Windowed<Bytes> sessionKey, byte[] aggregate)
//        {
//            removeExpiredSegments();

//            long windowEndTimestamp = sessionKey.window.end();
//            observedStreamTime = Math.Max(observedStreamTime, windowEndTimestamp);

//            if (windowEndTimestamp <= observedStreamTime - retentionPeriod)
//            {
//                expiredRecordSensor.record();
//                LOG.LogDebug("Skipping record for expired segment.");
//            }
//            else
//            {
//                if (aggregate != null)
//                {
//                    endTimeMap.computeIfAbsent(windowEndTimestamp, t => new ConcurrentSkipListMap<>());
//                    ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>> keyMap = endTimeMap[windowEndTimestamp];
//                    keyMap.computeIfAbsent(sessionKey.key, t => new ConcurrentSkipListMap<>());
//                    keyMap[sessionKey.key].Add(sessionKey.window.start(), aggregate);
//                }
//                else
//                {
//                    Remove(sessionKey);
//                }
//            }
//        }

//        public void Remove(Windowed<Bytes> sessionKey)
//        {
//            ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>> keyMap = endTimeMap[sessionKey.window.end()];
//            if (keyMap == null)
//            {
//                return;
//            }

//            ConcurrentNavigableMap<long, byte[]> startTimeMap = keyMap[sessionKey.key];
//            if (startTimeMap == null)
//            {
//                return;
//            }

//            startTimeMap.Remove(sessionKey.window.start());

//            if (startTimeMap.isEmpty())
//            {
//                keyMap.Remove(sessionKey.key);
//                if (keyMap.isEmpty())
//                {
//                    endTimeMap.Remove(sessionKey.window.end());
//                }
//            }
//        }

//        public override byte[] fetchSession(Bytes key, long startTime, long endTime)
//        {
//            removeExpiredSegments();

//            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));

//            // Only need to search if the record hasn't expired yet
//            if (endTime > observedStreamTime - retentionPeriod)
//            {
//                ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>> keyMap = endTimeMap[endTime];
//                if (keyMap != null)
//                {
//                    ConcurrentNavigableMap<long, byte[]> startTimeMap = keyMap[key];
//                    if (startTimeMap != null)
//                    {
//                        return startTimeMap[startTime];
//                    }
//                }
//            }
//            return null;
//        }

//        [System.Obsolete]
//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key,
//                                                                      long earliestSessionEndTime,
//                                                                      long latestSessionStartTime)
//        {
//            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));

//            removeExpiredSegments();

//            return registerNewIterator(key,
//                                       key,
//                                       latestSessionStartTime,
//                                       endTimeMap.tailMap(earliestSessionEndTime, true).iterator());
//        }

//        [System.Obsolete]
//        public override IKeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom,
//                                                                      Bytes keyTo,
//                                                                      long earliestSessionEndTime,
//                                                                      long latestSessionStartTime)
//        {
//            keyFrom = keyFrom ?? throw new System.ArgumentNullException("from key cannot be null", nameof(keyFrom));
//            keyTo = keyTo ?? throw new System.ArgumentNullException("to key cannot be null", nameof(keyTo));

//            removeExpiredSegments();

//            if (keyFrom.CompareTo(keyTo) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");
//                return KeyValueIterators.emptyIterator();
//            }

//            return registerNewIterator(keyFrom,
//                                       keyTo,
//                                       latestSessionStartTime,
//                                       endTimeMap.tailMap(earliestSessionEndTime, true).iterator());
//        }

//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
//        {

//            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));

//            removeExpiredSegments();

//            return registerNewIterator(key, key, long.MaxValue, endTimeMap.iterator());
//        }

//        public IKeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to)
//        {

//            from = from ?? throw new System.ArgumentNullException("from key cannot be null", nameof(from));
//            to = to ?? throw new System.ArgumentNullException("to key cannot be null", nameof(to));

//            removeExpiredSegments();


//            return registerNewIterator(from, to, long.MaxValue, endTimeMap.iterator());
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
//                foreach (InMemorySessionStoreIterator it in openIterators)
//                {
//                    it.close();
//                }
//            }

//            endTimeMap.clear();
//            openIterators.clear();
//            open = false;
//        }

//        private void removeExpiredSegments()
//        {
//            long minLiveTime = Math.Max(0L, observedStreamTime - retentionPeriod + 1);

//            foreach (InMemorySessionStoreIterator it in openIterators)
//            {
//                minLiveTime = Math.Min(minLiveTime, it.minTime());
//            }

//            endTimeMap.headMap(minLiveTime, false).clear();
//        }

//        private InMemorySessionStoreIterator registerNewIterator(Bytes keyFrom,
//                                                                 Bytes keyTo,
//                                                                 long latestSessionStartTime,
//                                                                 IEnumerator<Entry<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>>> endTimeIterator)
//        {
//            InMemorySessionStoreIterator iterator = new InMemorySessionStoreIterator(keyFrom, keyTo, latestSessionStartTime, endTimeIterator, it => openIterators.Remove(it));
//            openIterators.Add(iterator);
//            return iterator;
//        }

//        public interface ClosingCallback
//        {
//            void deregisterIterator(InMemorySessionStoreIterator iterator);
//        }
//    }
//}