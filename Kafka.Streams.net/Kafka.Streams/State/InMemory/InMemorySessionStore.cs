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
        private static readonly ILogger LOG = new LoggerFactory().CreateLogger<InMemorySessionStore>();

        private readonly string metricScope;
        //private Sensor expiredRecordSensor;
        private DateTime observedStreamTime = DateTime.MinValue; // ConsumerRecord.NO_TIMESTAMP;

        private readonly TimeSpan retentionPeriod;

        private readonly ConcurrentDictionary<DateTime, ConcurrentDictionary<Bytes, ConcurrentDictionary<DateTime, byte[]>>> endTimeMap = new ConcurrentDictionary<DateTime, ConcurrentDictionary<Bytes, ConcurrentDictionary<DateTime, byte[]>>>();
        private readonly HashSet<InMemorySessionStoreIterator> openIterators = new HashSet<InMemorySessionStoreIterator>();

        private volatile bool open = false;

        public InMemorySessionStore(
            string Name,
            TimeSpan retentionPeriod)
        {
            this.Name = Name;
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
            //     Name,
            //     EXPIRED_WINDOW_RECORD_DROP,
            //     RecordingLevel.INFO
            // );
            //addInvocationRateAndCount(
            //     expiredRecordSensor,
            //     "stream-" + metricScope + "-metrics",
            //     metrics.tagMap("task-id", taskName, metricScope + "-id", Name),
            //     EXPIRED_WINDOW_RECORD_DROP
            // );

            if (root != null)
            {
                // context.Register(root, (key, value) => Put(SessionKeySchema.from(Bytes.Wrap(key)), value));
            }

            this.open = true;
        }

        public void Put(IWindowed<Bytes> sessionKey, byte[] aggregate)
        {
            this.RemoveExpiredSegments();

            var windowEndTimestamp = sessionKey.Window.EndTime;
            this.observedStreamTime = this.observedStreamTime.GetNewest(windowEndTimestamp);

            if (windowEndTimestamp <= this.observedStreamTime - this.retentionPeriod)
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
                    this.Remove(sessionKey);
                }
            }
        }

        public void Remove(IWindowed<Bytes> sessionKey)
        {
            ConcurrentDictionary<Bytes, ConcurrentDictionary<DateTime, byte[]>> keyMap = this.endTimeMap[sessionKey.Window.EndTime];
            if (keyMap == null)
            {
                return;
            }

            ConcurrentDictionary<DateTime, byte[]> startTimeMap = keyMap[sessionKey.Key];
            if (startTimeMap == null)
            {
                return;
            }

            startTimeMap.Remove(sessionKey.Window.StartTime, out var _);

            if (!startTimeMap.Any())
            {
                keyMap.Remove(sessionKey.Key, out var _);
                if (!keyMap.Any())
                {
                    this.endTimeMap.Remove(sessionKey.Window.EndTime, out var _);
                }
            }
        }

        public byte[]? FetchSession(Bytes key, DateTime startTime, DateTime endTime)
        {
            this.RemoveExpiredSegments();

            key = key ?? throw new ArgumentNullException(nameof(key));

            // Only need to search if the record hasn't expired yet
            if (endTime > this.observedStreamTime - this.retentionPeriod)
            {
                ConcurrentDictionary<Bytes, ConcurrentDictionary<DateTime, byte[]>> keyMap = this.endTimeMap[endTime];
                if (keyMap != null)
                {
                    ConcurrentDictionary<DateTime, byte[]> startTimeMap = keyMap[key];
                    if (startTimeMap != null)
                    {
                        return startTimeMap[startTime];
                    }
                }
            }

            return null;
        }

        [Obsolete]
        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FindSessions(
            Bytes key,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            this.RemoveExpiredSegments();

            return this.RegisterNewIterator(
                key,
                key,
                latestSessionStartTime,
                this.endTimeMap.GetEnumerator());//.tailMap(earliestSessionEndTime, true)
        }

        [Obsolete]
        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FindSessions(
            Bytes keyFrom,
            Bytes keyTo,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            keyFrom = keyFrom ?? throw new ArgumentNullException(nameof(keyFrom));
            keyTo = keyTo ?? throw new ArgumentNullException(nameof(keyTo));

            this.RemoveExpiredSegments();

            if (keyFrom.CompareTo(keyTo) > 0)
            {
                LOG.LogWarning("Returning empty iterator for Fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
                return null;// KeyValueIterators.EMPTY_ITERATOR;
            }

            return this.RegisterNewIterator(
                keyFrom,
                keyTo,
                latestSessionStartTime,
                this.endTimeMap.GetEnumerator()); //.tailMap(earliestSessionEndTime, true)
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes key)
        {
            key = key ?? throw new ArgumentNullException(nameof(key));

            this.RemoveExpiredSegments();

            return this.RegisterNewIterator(
                key,
                key,
                DateTime.MaxValue,
                this.endTimeMap.GetEnumerator());
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to)
        {
            from = from ?? throw new ArgumentNullException(nameof(from));
            to = to ?? throw new ArgumentNullException(nameof(to));

            this.RemoveExpiredSegments();

            return this.RegisterNewIterator(
                from,
                to,
                DateTime.MaxValue,
                this.endTimeMap.GetEnumerator());
        }

        public bool Persistent()
        {
            return false;
        }

        public bool IsOpen()
        {
            return this.open;
        }

        public void Flush()
        {
            // do-nothing since it is in-memory
        }

        public void Close()
        {
            if (this.openIterators.Count != 0)
            {
                LOG.LogWarning($"Closing {this.openIterators.Count} open iterators for store {this.Name}");
                foreach (var it in this.openIterators)
                {
                    it.Close();
                }
            }

            this.endTimeMap.Clear();
            this.openIterators.Clear();
            this.open = false;
        }

        private void RemoveExpiredSegments()
        {
            var minLiveTime = this.observedStreamTime - (this.retentionPeriod + TimeSpan.FromMilliseconds(1));

            foreach (var it in this.openIterators)
            {
                minLiveTime = minLiveTime.GetOldest(it.MinTime());
            }

            //endTimeMap.headMap(minLiveTime, false).clear();
        }

        private InMemorySessionStoreIterator RegisterNewIterator(
            Bytes keyFrom,
            Bytes keyTo,
            DateTime latestSessionStartTime,
            IEnumerator<KeyValuePair<DateTime, ConcurrentDictionary<Bytes, ConcurrentDictionary<DateTime, byte[]>>>> endTimeIterator)
        {
            InMemorySessionStoreIterator iterator = null;// new InMemorySessionStoreIterator(keyFrom, keyTo, latestSessionStartTime, endTimeIterator, it => openIterators.Remove(it));
            this.openIterators.Add(iterator);
            return iterator;
        }

        public bool IsPresent()
        {
            throw new NotImplementedException();
        }
    }
}
