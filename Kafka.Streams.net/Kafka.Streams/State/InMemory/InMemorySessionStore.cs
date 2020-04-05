using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.State.Internals
{
    public class InMemorySessionStore : ISessionStore<Bytes, byte[]>
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<InMemorySessionStore>();

        private string metricScope;
        //private Sensor expiredRecordSensor;
        private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

        private TimeSpan retentionPeriod;

        private ConcurrentDictionary<long, ConcurrentDictionary<Bytes, ConcurrentDictionary<long, byte[]>>> endTimeMap = new ConcurrentDictionary<long, ConcurrentDictionary<Bytes, ConcurrentDictionary<long, byte[]>>>();
        private HashSet<InMemorySessionStoreIterator> openIterators = new HashSet<InMemorySessionStoreIterator>();

        private volatile bool open = false;

        public InMemorySessionStore(
            string name,
            TimeSpan retentionPeriod)
        {
            this.Name = name;
            this.retentionPeriod = retentionPeriod;
            //this.metricScope = metricScope;
        }

        public string Name { get; }

        public void Init(IProcessorContext context, IStateStore root)
        {
            //StreamsMetricsImpl metrics = ((IInternalProcessorContext)context).metrics;
            string taskName = context.TaskId.ToString();
            // expiredRecordSensor = metrics.storeLevelSensor(
            //     taskName,
            //     name,
            //     EXPIRED_WINDOW_RECORD_DROP,
            //     RecordingLevel.INFO
            // );
            //addInvocationRateAndCount(
            //     expiredRecordSensor,
            //     "stream-" + metricScope + "-metrics",
            //     metrics.tagMap("task-id", taskName, metricScope + "-id", name),
            //     EXPIRED_WINDOW_RECORD_DROP
            // );

            if (root != null)
            {
                // context.Register(root, (key, value) => put(SessionKeySchema.from(Bytes.Wrap(key)), value));
            }

            open = true;
        }

        public void Put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
            RemoveExpiredSegments();

            long windowEndTimestamp = sessionKey.window.End();
            observedStreamTime = Math.Max(observedStreamTime, windowEndTimestamp);

            if (windowEndTimestamp <= observedStreamTime - retentionPeriod.TotalMilliseconds)
            {
                //expiredRecordSensor.record();
                //LOG.LogDebug("Skipping record for expired segment.");
            }
            else
            {
                if (aggregate != null)
                {
                    // endTimeMap.computeIfAbsent(windowEndTimestamp, t => new ConcurrentSkipListMap<>());
                    // ConcurrentDictionary<Bytes, ConcurrentDictionary<long, byte[]>> keyMap = endTimeMap[windowEndTimestamp];
                    // keyMap.ComputeIfAbsent(sessionKey.Key, t => new ConcurrentSkipListMap<>());
                    // keyMap[sessionKey.Key].Add(sessionKey.window.Start(), aggregate);
                }
                else
                {
                    Remove(sessionKey);
                }
            }
        }

        public void Remove(Windowed<Bytes> sessionKey)
        {
            ConcurrentDictionary<Bytes, ConcurrentDictionary<long, byte[]>> keyMap = endTimeMap[sessionKey.window.End()];
            if (keyMap == null)
            {
                return;
            }

            ConcurrentDictionary<long, byte[]> startTimeMap = keyMap[sessionKey.Key];
            if (startTimeMap == null)
            {
                return;
            }

            startTimeMap.Remove(sessionKey.window.Start(), out var _);

            if (!startTimeMap.Any())
            {
                keyMap.Remove(sessionKey.Key, out var _);
                if (!keyMap.Any())
                {
                    endTimeMap.Remove(sessionKey.window.End(), out var _);
                }
            }
        }

        public byte[]? FetchSession(Bytes key, long startTime, long endTime)
        {
            RemoveExpiredSegments();

            key = key ?? throw new ArgumentNullException(nameof(key));

            // Only need to search if the record hasn't expired yet
            if (endTime > observedStreamTime - retentionPeriod.TotalMilliseconds)
            {
                ConcurrentDictionary<Bytes, ConcurrentDictionary<long, byte[]>> keyMap = endTimeMap[endTime];
                if (keyMap != null)
                {
                    ConcurrentDictionary<long, byte[]> startTimeMap = keyMap[key];
                    if (startTimeMap != null)
                    {
                        return startTimeMap[startTime];
                    }
                }
            }

            return null;
        }

        [Obsolete]
        public IKeyValueIterator<Windowed<Bytes>, byte[]> FindSessions(
            Bytes key,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            RemoveExpiredSegments();

            return RegisterNewIterator(
                key,
                key,
                latestSessionStartTime,
                endTimeMap.GetEnumerator());//.tailMap(earliestSessionEndTime, true)
        }

        [Obsolete]
        public IKeyValueIterator<Windowed<Bytes>, byte[]> FindSessions(
            Bytes keyFrom,
            Bytes keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            keyFrom = keyFrom ?? throw new ArgumentNullException(nameof(keyFrom));
            keyTo = keyTo ?? throw new ArgumentNullException(nameof(keyTo));

            RemoveExpiredSegments();

            if (keyFrom.CompareTo(keyTo) > 0)
            {
                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
                return null;// KeyValueIterators.EMPTY_ITERATOR;
            }

            return RegisterNewIterator(
                keyFrom,
                keyTo,
                latestSessionStartTime,
                endTimeMap.GetEnumerator()); //.tailMap(earliestSessionEndTime, true)
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            RemoveExpiredSegments();

            return RegisterNewIterator(
                key,
                key,
                long.MaxValue,
                endTimeMap.GetEnumerator());
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to)
        {
            from = from ?? throw new ArgumentNullException(nameof(from));
            to = to ?? throw new ArgumentNullException(nameof(to));

            RemoveExpiredSegments();

            return RegisterNewIterator(
                from,
                to,
                long.MaxValue,
                endTimeMap.GetEnumerator());
        }

        public bool Persistent()
        {
            return false;
        }

        public bool IsOpen()
        {
            return open;
        }

        public void Flush()
        {
            // do-nothing since it is in-memory
        }

        public void Close()
        {
            if (openIterators.Count != 0)
            {
                LOG.LogWarning($"Closing {openIterators.Count} open iterators for store {Name}");
                foreach (var it in openIterators)
                {
                    it.Close();
                }
            }

            endTimeMap.Clear();
            openIterators.Clear();
            open = false;
        }

        private void RemoveExpiredSegments()
        {
            long minLiveTime = Math.Max(0L, observedStreamTime - (long)retentionPeriod.TotalMilliseconds + 1);

            foreach (var it in openIterators)
            {
                minLiveTime = Math.Min(minLiveTime, it.MinTime());
            }

            //endTimeMap.headMap(minLiveTime, false).clear();
        }

        private InMemorySessionStoreIterator RegisterNewIterator(
            Bytes keyFrom,
            Bytes keyTo,
            long latestSessionStartTime,
            IEnumerator<KeyValuePair<long, ConcurrentDictionary<Bytes, ConcurrentDictionary<long, byte[]>>>> endTimeIterator)
        {
            InMemorySessionStoreIterator iterator = null;// new InMemorySessionStoreIterator(keyFrom, keyTo, latestSessionStartTime, endTimeIterator, it => openIterators.Remove(it));
            openIterators.Add(iterator);
            return iterator;
        }

        public bool IsPresent()
        {
            throw new NotImplementedException();
        }
    }
}
