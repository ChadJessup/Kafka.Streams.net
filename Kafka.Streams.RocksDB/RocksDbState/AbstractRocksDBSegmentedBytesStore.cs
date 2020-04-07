//using Confluent.Kafka;
//using Kafka.Common.Utils;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.State.Internals;
//using Kafka.Streams.State.KeyValues;
//using Microsoft.Extensions.Logging;
//using RocksDbSharp;
//using System;
//using System.Collections.Generic;
//using System.Collections.ObjectModel;

//namespace Kafka.Streams.RocksDbState
//{
//    public class AbstractRocksDbSegmentedBytesStore<S> : ISegmentedBytesStore
//        where S : ISegment
//    {
//        private static ILogger LOG = new LoggerFactory().CreateLogger<AbstractRocksDbSegmentedBytesStore<S>>();
//        public string name { get; }
//        private AbstractSegments<S> segments;
//        private string metricScope;
//        private IKeySchema keySchema;
//        private IInternalProcessorContext context;
//        private volatile bool open;
//        private HashSet<S> bulkLoadSegments;
//        //private Sensor expiredRecordSensor;
//        private long observedStreamTime = (long)TimestampType.NotAvailable;

//        public AbstractRocksDbSegmentedBytesStore(
//            string name,
//            string metricScope,
//            IKeySchema keySchema,
//            AbstractSegments<S> segments)
//        {
//            this.name = name;
//            this.metricScope = metricScope;
//            this.keySchema = keySchema;
//            this.segments = segments;
//        }

//        public IKeyValueIterator<Bytes, byte[]> Fetch(
//            Bytes key,
//            long from,
//            long to)
//        {
//            List<S> searchSpace = keySchema.SegmentsToSearch(segments, from, to);

//            Bytes binaryFrom = keySchema.LowerRangeFixedSize(key, from);
//            Bytes binaryTo = keySchema.UpperRangeFixedSize(key, to);

//            return new SegmentIterator<>(
//                searchSpace.GetEnumerator(),
//                keySchema.HasNextCondition(key, key, from, to),
//                binaryFrom,
//                binaryTo);
//        }

//        public IKeyValueIterator<Bytes, byte[]> Fetch(
//            Bytes keyFrom,
//            Bytes keyTo,
//            long from,
//            long to)
//        {
//            if (keyFrom.CompareTo(keyTo) > 0)
//            {
//                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
//                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
//                    "Note that the built-in numerical serdes do not follow this for negative numbers");

//                return KeyValueIterators.EMPTY_ITERATOR;
//            }

//            List<S> searchSpace = keySchema.SegmentsToSearch(segments, from, to);

//            Bytes binaryFrom = keySchema.LowerRange(keyFrom, from);
//            Bytes binaryTo = keySchema.UpperRange(keyTo, to);

//            return new SegmentIterator<>(
//                searchSpace.GetEnumerator(),
//                keySchema.HasNextCondition(keyFrom, keyTo, from, to),
//                binaryFrom,
//                binaryTo);
//        }

//        public IKeyValueIterator<Bytes, byte[]> All()
//        {
//            List<S> searchSpace = segments.AllSegments();

//            return new SegmentIterator<>(
//                searchSpace.GetEnumerator(),
//                keySchema.HasNextCondition(null, null, 0, long.MaxValue),
//                null,
//                null);
//        }

//        public IKeyValueIterator<Bytes, byte[]> FetchAll(long timeFrom, long timeTo)
//        {
//            List<S> searchSpace = this.segments.GetSegments(timeFrom, timeTo);

//            return new SegmentIterator<>(
//                searchSpace.GetEnumerator(),
//                keySchema.HasNextCondition(null, null, timeFrom, timeTo),
//                null,
//                null);
//        }

//        public void Remove(Bytes key)
//        {
//            long timestamp = keySchema.SegmentTimestamp(key);
//            observedStreamTime = Math.Max(observedStreamTime, timestamp);
//            S segment = segments.GetSegmentForTimestamp(timestamp);
//            if (segment == null)
//            {
//                return;
//            }

//            segment.Delete(key);
//        }

//        public void Put(Bytes key, byte[] value)
//        {
//            long timestamp = keySchema.SegmentTimestamp(key);
//            observedStreamTime = Math.Max(observedStreamTime, timestamp);
//            long segmentId = segments.SegmentId(timestamp);
//            S segment = segments.GetOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
//            if (segment == null)
//            {
//                //expiredRecordSensor.record();
//                //LOG.LogDebug("Skipping record for expired segment.");
//            }
//            else
//            {
//                segment.Add(key, value);
//            }
//        }

//        public byte[] Get(Bytes key)
//        {
//            S segment = segments.GetSegmentForTimestamp(keySchema.SegmentTimestamp(key));
//            if (segment == null)
//            {
//                return null;
//            }

//            return segment.Get(key);
//        }

//        public string Name()
//        {
//            return name;
//        }

//        public void Init(IProcessorContext context, IStateStore root)
//        {
//            this.context = (IInternalProcessorContext)context;

//            //StreamsMetricsImpl metrics = this.context.metrics;
//            string taskName = context.taskId.ToString();

//            //expiredRecordSensor = metrics.storeLevelSensor(
//            //    taskName,
//            //    name,
//            //    EXPIRED_WINDOW_RECORD_DROP,
//            //    RecordingLevel.INFO
//            //);

//            //addInvocationRateAndCount(
//            //     expiredRecordSensor,
//            //     "stream-" + metricScope + "-metrics",
//            //     metrics.tagMap("task-id", taskName, metricScope + "-id", name),
//            //     EXPIRED_WINDOW_RECORD_DROP
//            // );

//            segments.OpenExisting(this.context, observedStreamTime);

//            bulkLoadSegments = new HashSet<S>(segments.AllSegments());

//            // register and possibly restore the state from the logs
//            context.Register(root, new RocksDbSegmentsBatchingRestoreCallback());

//            open = true;
//        }

//        public void Flush()
//        {
//            segments.Flush();
//        }

//        public void Close()
//        {
//            open = false;
//            segments.Close();
//        }

//        public bool Persistent()
//        {
//            return true;
//        }

//        public bool IsOpen()
//        {
//            return open;
//        }

//        // Visible for testing
//        List<S> GetSegments()
//        {
//            return segments.AllSegments();
//        }

//        // Visible for testing
//        void RestoreAllInternal(List<KeyValuePair<byte[], byte[]>> records)
//        {
//            try
//            {
//                Dictionary<S, WriteBatch> writeBatchMap = GetWriteBatches(records);
//                foreach (var entry in writeBatchMap)
//                {
//                    S segment = entry.Key;
//                    WriteBatch batch = entry.Value;
//                    segment.Write(batch);
//                    batch.Close();
//                }
//            }
//            catch (RocksDbException e)
//            {
//                throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
//            }
//        }

//        // Visible for testing
//        Dictionary<S, WriteBatch> GetWriteBatches(List<KeyValuePair<byte[], byte[]>> records)
//        {
//            // advance stream time to the max timestamp in the batch
//            foreach (KeyValuePair<byte[], byte[]> record in records)
//            {
//                long timestamp = keySchema.SegmentTimestamp(Bytes.Wrap(record.Key));
//                observedStreamTime = Math.Max(observedStreamTime, timestamp);
//            }

//            Dictionary<S, WriteBatch> writeBatchMap = new Dictionary<S, WriteBatch>();
//            foreach (KeyValuePair<byte[], byte[]> record in records)
//            {
//                long timestamp = keySchema.SegmentTimestamp(Bytes.Wrap(record.Key));
//                long segmentId = segments.SegmentId(timestamp);
//                S segment = segments.GetOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
//                if (segment != null)
//                {
//                    // This handles the case that state store is moved to a new client and does not
//                    // have the local RocksDb instance for the segment. In this case, toggleDBForBulkLoading
//                    // will only close the database and open it again with bulk loading enabled.
//                    if (!bulkLoadSegments.Contains(segment))
//                    {
//                        segment.ToggleDbForBulkLoading(true);
//                        // If the store does not exist yet, the getOrCreateSegmentIfLive will call openDB that
//                        // makes the open flag for the newly created store.
//                        // if the store does exist already, then toggleDbForBulkLoading will make sure that
//                        // the store is already open here.
//                        bulkLoadSegments = new HashSet<S>(segments.AllSegments());
//                    }
//                    try
//                    {
//                        WriteBatch batch = writeBatchMap.ComputeIfAbsent(segment, s => new WriteBatch());
//                        segment.AddToBatch(record, batch);
//                    }
//                    catch (RocksDbException e)
//                    {
//                        throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
//                    }
//                }
//            }
//            return writeBatchMap;
//        }

//        private void ToggleForBulkLoading(bool prepareForBulkload)
//        {
//            foreach (var segment in segments.AllSegments())
//            {
//                segment.ToggleDbForBulkLoading(prepareForBulkload);
//            }
//        }

//        public bool IsPresent()
//        {
//            throw new NotImplementedException();
//        }
//    }
//}
