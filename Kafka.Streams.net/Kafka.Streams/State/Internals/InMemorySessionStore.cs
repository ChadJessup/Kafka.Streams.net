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

using Kafka.Common.Metrics;
using Kafka.Common.Utils;
using Kafka.Streams.State.Interfaces;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.State.Internals
{
    public class InMemorySessionStore : ISessionStore<Bytes, byte[]>
    {

        private static ILogger LOG = new LoggerFactory().CreateLogger<InMemorySessionStore>();

        private string name;
        private string metricScope;
        private Sensor expiredRecordSensor;
        private long observedStreamTime = ConsumeResult.NO_TIMESTAMP;

        private long retentionPeriod;

        private ConcurrentNavigableMap<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>> endTimeMap = new ConcurrentSkipListMap<>();
        private HashSet<InMemorySessionStoreIterator> openIterators = ConcurrentHashMap.newKeySet();

        private volatile bool open = false;

        InMemorySessionStore(string name,
                             long retentionPeriod,
                             string metricScope)
        {
            this.name = name;
            this.retentionPeriod = retentionPeriod;
            this.metricScope = metricScope;
        }

        public override string name()
        {
            return name;
        }

        public override void init(IProcessorContext context, IStateStore root)
        {
            StreamsMetricsImpl metrics = ((IInternalProcessorContext)context).metrics();
            string taskName = context.taskId().ToString();
            expiredRecordSensor = metrics.storeLevelSensor(
                taskName,
                name(),
                EXPIRED_WINDOW_RECORD_DROP,
                RecordingLevel.INFO
            );
            AddInvocationRateAndCount(
                 expiredRecordSensor,
                 "stream-" + metricScope + "-metrics",
                 metrics.tagMap("task-id", taskName, metricScope + "-id", name()),
                 EXPIRED_WINDOW_RECORD_DROP
             );

            if (root != null)
            {
                context.register(root, (key, value)->put(SessionKeySchema.from(Bytes.wrap(key)), value));
            }
            open = true;
        }

        public override void put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
            removeExpiredSegments();

            long windowEndTimestamp = sessionKey.window().end();
            observedStreamTime = Math.Max(observedStreamTime, windowEndTimestamp);

            if (windowEndTimestamp <= observedStreamTime - retentionPeriod)
            {
                expiredRecordSensor.record();
                LOG.LogDebug("Skipping record for expired segment.");
            }
            else
            {
                if (aggregate != null)
                {
                    endTimeMap.computeIfAbsent(windowEndTimestamp, t-> new ConcurrentSkipListMap<>());
                    ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>> keyMap = endTimeMap[windowEndTimestamp];
                    keyMap.computeIfAbsent(sessionKey.key(), t-> new ConcurrentSkipListMap<>());
                    keyMap[sessionKey.key()].Add(sessionKey.window().start(), aggregate);
                }
                else
                {
                    Remove(sessionKey);
                }
            }
        }

        public override void Remove(Windowed<Bytes> sessionKey)
        {
            ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>> keyMap = endTimeMap[sessionKey.window().end()];
            if (keyMap == null)
            {
                return;
            }

            ConcurrentNavigableMap<long, byte[]> startTimeMap = keyMap[sessionKey.key()];
            if (startTimeMap == null)
            {
                return;
            }

            startTimeMap.Remove(sessionKey.window().start());

            if (startTimeMap.isEmpty())
            {
                keyMap.Remove(sessionKey.key());
                if (keyMap.isEmpty())
                {
                    endTimeMap.Remove(sessionKey.window().end());
                }
            }
        }

        public override byte[] fetchSession(Bytes key, long startTime, long endTime)
        {
            removeExpiredSegments();

            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));

            // Only need to search if the record hasn't expired yet
            if (endTime > observedStreamTime - retentionPeriod)
            {
                ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>> keyMap = endTimeMap[endTime];
                if (keyMap != null)
                {
                    ConcurrentNavigableMap<long, byte[]> startTimeMap = keyMap[key];
                    if (startTimeMap != null)
                    {
                        return startTimeMap[startTime];
                    }
                }
            }
            return null;
        }

        [System.Obsolete]
        public override KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes key,
                                                                      long earliestSessionEndTime,
                                                                      long latestSessionStartTime)
        {
            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));

            removeExpiredSegments();

            return registerNewIterator(key,
                                       key,
                                       latestSessionStartTime,
                                       endTimeMap.tailMap(earliestSessionEndTime, true).entrySet().iterator());
        }

        [System.Obsolete]
        public override KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(Bytes keyFrom,
                                                                      Bytes keyTo,
                                                                      long earliestSessionEndTime,
                                                                      long latestSessionStartTime)
        {
            keyFrom = keyFrom ?? throw new System.ArgumentNullException("from key cannot be null", nameof(keyFrom));
            keyTo = keyTo ?? throw new System.ArgumentNullException("to key cannot be null", nameof(keyTo));

            removeExpiredSegments();

            if (keyFrom.CompareTo(keyTo) > 0)
            {
                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
                return KeyValueIterators.emptyIterator();
            }

            return registerNewIterator(keyFrom,
                                       keyTo,
                                       latestSessionStartTime,
                                       endTimeMap.tailMap(earliestSessionEndTime, true).entrySet().iterator());
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes key)
        {

            key = key ?? throw new System.ArgumentNullException("key cannot be null", nameof(key));

            removeExpiredSegments();

            return registerNewIterator(key, key, long.MaxValue, endTimeMap.entrySet().iterator());
        }

        public override KeyValueIterator<Windowed<Bytes>, byte[]> fetch(Bytes from, Bytes to)
        {

            from = from ?? throw new System.ArgumentNullException("from key cannot be null", nameof(from));
            to = to ?? throw new System.ArgumentNullException("to key cannot be null", nameof(to));

            removeExpiredSegments();


            return registerNewIterator(from, to, long.MaxValue, endTimeMap.entrySet().iterator());
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
                LOG.LogWarning("Closing {} open iterators for store {}", openIterators.size(), name);
                foreach (InMemorySessionStoreIterator it in openIterators)
                {
                    it.close();
                }
            }

            endTimeMap.clear();
            openIterators.clear();
            open = false;
        }

        private void removeExpiredSegments()
        {
            long minLiveTime = Math.Max(0L, observedStreamTime - retentionPeriod + 1);

            foreach (InMemorySessionStoreIterator it in openIterators)
            {
                minLiveTime = Math.Min(minLiveTime, it.minTime());
            }

            endTimeMap.headMap(minLiveTime, false).clear();
        }

        private InMemorySessionStoreIterator registerNewIterator(Bytes keyFrom,
                                                                 Bytes keyTo,
                                                                 long latestSessionStartTime,
                                                                 IEnumerator<Entry<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>>> endTimeIterator)
        {
            InMemorySessionStoreIterator iterator = new InMemorySessionStoreIterator(keyFrom, keyTo, latestSessionStartTime, endTimeIterator, it->openIterators.Remove(it));
            openIterators.Add(iterator);
            return iterator;
        }

        public interface ClosingCallback
        {
            void deregisterIterator(InMemorySessionStoreIterator iterator);
        }

        private static class InMemorySessionStoreIterator : KeyValueIterator<Windowed<Bytes>, byte[]>
        {

            private IEnumerator<Entry<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>>> endTimeIterator;
            private IEnumerator<Entry<Bytes, ConcurrentNavigableMap<long, byte[]>>> keyIterator;
            private IEnumerator<Entry<long, byte[]>> recordIterator;

            private KeyValue<Windowed<Bytes>, byte[]> next;
            private Bytes currentKey;
            private long currentEndTime;

            private Bytes keyFrom;
            private Bytes keyTo;
            private long latestSessionStartTime;

            private ClosingCallback callback;

            InMemorySessionStoreIterator(Bytes keyFrom,
                                         Bytes keyTo,
                                         long latestSessionStartTime,
                                         IEnumerator<Entry<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>>> endTimeIterator,
                                         ClosingCallback callback)
            {
                this.keyFrom = keyFrom;
                this.keyTo = keyTo;
                this.latestSessionStartTime = latestSessionStartTime;

                this.endTimeIterator = endTimeIterator;
                this.callback = callback;
                setAllIterators();
            }


            public bool hasNext()
            {
                if (next != null)
                {
                    return true;
                }
                else if (recordIterator == null)
                {
                    return false;
                }
                else
                {
                    next = getNext();
                    return next != null;
                }
            }


            public Windowed<Bytes> peekNextKey()
            {
                if (!hasNext())
                {
                    throw new NoSuchElementException();
                }
                return next.key;
            }


            public KeyValue<Windowed<Bytes>, byte[]> next()
            {
                if (!hasNext())
                {
                    throw new NoSuchElementException();
                }

                KeyValue<Windowed<Bytes>, byte[]> ret = next;
                next = null;
                return ret;
            }


            public void close()
            {
                next = null;
                recordIterator = null;
                callback.deregisterIterator(this);
            }

            long minTime()
            {
                return currentEndTime;
            }

            // getNext is only called when either recordIterator or segmentIterator has a next
            // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
            private KeyValue<Windowed<Bytes>, byte[]> getNext()
            {
                if (!recordIterator.hasNext())
                {
                    getNextIterators();
                }

                if (recordIterator == null)
                {
                    return null;
                }

                KeyValuePair<long, byte[]> nextRecord = recordIterator.next();
                SessionWindow sessionWindow = new SessionWindow(nextRecord.Key, currentEndTime);
                Windowed<Bytes> windowedKey = new Windowed<>(currentKey, sessionWindow);

                return new KeyValue<>(windowedKey, nextRecord.Value);
            }

            // Called when the inner two (key and starttime) iterators are empty to roll to the next endTimestamp
            // Rolls all three iterators forward until recordIterator has a next entry
            // Sets recordIterator to null if there are no records to return
            private void setAllIterators()
            {
                while (endTimeIterator.hasNext())
                {
                    Entry<long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<long, byte[]>>> nextEndTimeEntry = endTimeIterator.next();
                    currentEndTime = nextEndTimeEntry.Key;
                    keyIterator = nextEndTimeEntry.Value.subMap(keyFrom, true, keyTo, true).entrySet().iterator();

                    if (setInnerIterators())
                    {
                        return;
                    }
                }
                recordIterator = null;
            }

            // Rolls the inner two iterators (key and record) forward until recordIterators has a next entry
            // Returns false if no more records are found (for the current end time)
            private bool setInnerIterators()
            {
                while (keyIterator.hasNext())
                {
                    Entry<Bytes, ConcurrentNavigableMap<long, byte[]>> nextKeyEntry = keyIterator.next();
                    currentKey = nextKeyEntry.Key;

                    if (latestSessionStartTime == long.MaxValue)
                    {
                        recordIterator = nextKeyEntry.Value.entrySet().iterator();
                    }
                    else
                    {
                        recordIterator = nextKeyEntry.Value.headMap(latestSessionStartTime, true).entrySet().iterator();
                    }

                    if (recordIterator.hasNext())
                    {
                        return true;
                    }
                }
                return false;
            }

            // Called when the current recordIterator has no entries left to roll it to the next valid entry
            // When there are no more records to return, recordIterator will be set to null
            private void getNextIterators()
            {
                if (setInnerIterators())
                {
                    return;
                }

                setAllIterators();
            }
        }
    }
}