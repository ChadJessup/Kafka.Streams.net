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
using Confluent.Kafka;
using Kafka.Common.Utils;
using Kafka.Streams.State.Interfaces;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Kafka.Streams.State.Internals
{
    public class AbstractRocksDbSegmentedBytesStore<S> : SegmentedBytesStore
        where S : ISegment
    {
        private static ILogger LOG = new LoggerFactory().CreateLogger<AbstractRocksDbSegmentedBytesStore<S>>();
        private string name;
        private AbstractSegments<S> segments;
        private string metricScope;
        private KeySchema keySchema;
        private IInternalProcessorContext<K, V>  context;
        private volatile bool open;
        private HashSet<S> bulkLoadSegments;
        private Sensor expiredRecordSensor;
        private long observedStreamTime = (long)TimestampType.NotAvailable;

        AbstractRocksDbSegmentedBytesStore(string name,
                                           string metricScope,
                                           KeySchema keySchema,
                                           AbstractSegments<S> segments)
        {
            this.name = name;
            this.metricScope = metricScope;
            this.keySchema = keySchema;
            this.segments = segments;
        }

        public override IKeyValueIterator<Bytes, byte[]> fetch(Bytes key,
                                                     long from,
                                                     long to)
        {
            List<S> searchSpace = keySchema.segmentsToSearch(segments, from, to);

            Bytes binaryFrom = keySchema.lowerRangeFixedSize(key, from);
            Bytes binaryTo = keySchema.upperRangeFixedSize(key, to);

            return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(key, key, from, to),
                binaryFrom,
                binaryTo);
        }

        public override IKeyValueIterator<Bytes, byte[]> fetch(Bytes keyFrom,
                                                     Bytes keyTo,
                                                     long from,
                                                     long to)
        {
            if (keyFrom.CompareTo(keyTo) > 0)
            {
                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
                return KeyValueIterators.emptyIterator();
            }

            List<S> searchSpace = keySchema.segmentsToSearch(segments, from, to);

            Bytes binaryFrom = keySchema.lowerRange(keyFrom, from);
            Bytes binaryTo = keySchema.upperRange(keyTo, to);

            return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(keyFrom, keyTo, from, to),
                binaryFrom,
                binaryTo);
        }

        public override IKeyValueIterator<Bytes, byte[]> all()
        {
            List<S> searchSpace = segments.allSegments();

            return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, 0, long.MaxValue),
                null,
                null);
        }

        public override IKeyValueIterator<Bytes, byte[]> fetchAll(long timeFrom,
                                                        long timeTo)
        {
            List<S> searchSpace = segments.segments(timeFrom, timeTo);

            return new SegmentIterator<>(
                searchSpace.iterator(),
                keySchema.hasNextCondition(null, null, timeFrom, timeTo),
                null,
                null);
        }

        public override void Remove(Bytes key)
        {
            long timestamp = keySchema.segmentTimestamp(key);
            observedStreamTime = Math.Max(observedStreamTime, timestamp);
            S segment = segments.getSegmentForTimestamp(timestamp);
            if (segment == null)
            {
                return;
            }

            segment.delete(key);
        }

        public override void put(Bytes key, byte[] value)
        {
            long timestamp = keySchema.segmentTimestamp(key);
            observedStreamTime = Math.Max(observedStreamTime, timestamp);
            long segmentId = segments.segmentId(timestamp);
            S segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
            if (segment == null)
            {
                expiredRecordSensor.record();
                LOG.LogDebug("Skipping record for expired segment.");
            }
            else
            {
                segment.Add(key, value);
            }
        }

        public override byte[] get(Bytes key)
        {
            S segment = segments.getSegmentForTimestamp(keySchema.segmentTimestamp(key));
            if (segment == null)
            {
                return null;
            }
            return segment[key];
        }

        public override string Name()
        {
            return name;
        }

        public override void init(IProcessorContext<K, V> context,
                         IStateStore root)
        {
            this.context = (IInternalProcessorContext)context;

            StreamsMetricsImpl metrics = this.context.metrics();
            string taskName = context.taskId().ToString();

            expiredRecordSensor = metrics.storeLevelSensor(
                taskName,
                name(),
                EXPIRED_WINDOW_RECORD_DROP,
                RecordingLevel.INFO
            );
           .AddInvocationRateAndCount(
                expiredRecordSensor,
                "stream-" + metricScope + "-metrics",
                metrics.tagMap("task-id", taskName, metricScope + "-id", name()),
                EXPIRED_WINDOW_RECORD_DROP
            );

            segments.openExisting(this.context, observedStreamTime);

            bulkLoadSegments = new HashSet<>(segments.allSegments());

            // register and possibly restore the state from the logs
            context.register(root, new RocksDbSegmentsBatchingRestoreCallback());

            open = true;
        }

        public override void flush()
        {
            segments.flush();
        }

        public override void close()
        {
            open = false;
            segments.close();
        }

        public override bool persistent()
        {
            return true;
        }

        public override bool isOpen()
        {
            return open;
        }

        // Visible for testing
        List<S> getSegments()
        {
            return segments.allSegments();
        }

        // Visible for testing
        void restoreAllInternal(List<KeyValue<byte[], byte[]>> records)
        {
            try
            {
                Dictionary<S, WriteBatch> writeBatchMap = getWriteBatches(records);
                foreach (var entry in writeBatchMap)
                {
                    S segment = entry.Key;
                    WriteBatch batch = entry.Value;
                    segment.write(batch);
                    batch.close();
                }
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
            }
        }

        // Visible for testing
        Dictionary<S, WriteBatch> getWriteBatches(List<KeyValue<byte[], byte[]>> records)
        {
            // advance stream time to the max timestamp in the batch
            foreach (KeyValue<byte[], byte[]> record in records)
            {
                long timestamp = keySchema.segmentTimestamp(Bytes.wrap(record.key));
                observedStreamTime = Math.Max(observedStreamTime, timestamp);
            }

            Dictionary<S, WriteBatch> writeBatchMap = new Dictionary<S, WriteBatch>();
            foreach (KeyValue<byte[], byte[]> record in records)
            {
                long timestamp = keySchema.segmentTimestamp(Bytes.wrap(record.key));
                long segmentId = segments.segmentId(timestamp);
                S segment = segments.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
                if (segment != null)
                {
                    // This handles the case that state store is moved to a new client and does not
                    // have the local RocksDb instance for the segment. In this case, toggleDBForBulkLoading
                    // will only close the database and open it again with bulk loading enabled.
                    if (!bulkLoadSegments.Contains(segment))
                    {
                        segment.toggleDbForBulkLoading(true);
                        // If the store does not exist yet, the getOrCreateSegmentIfLive will call openDB that
                        // makes the open flag for the newly created store.
                        // if the store does exist already, then toggleDbForBulkLoading will make sure that
                        // the store is already open here.
                        bulkLoadSegments = new HashSet<S>(segments.allSegments());
                    }
                    try
                    {
                        WriteBatch batch = writeBatchMap.computeIfAbsent(segment, s=> new WriteBatch());
                        segment.AddToBatch(record, batch);
                    }
                    catch (RocksDbException e)
                    {
                        throw new ProcessorStateException("Error restoring batch to store " + this.name, e);
                    }
                }
            }
            return writeBatchMap;
        }

        private void toggleForBulkLoading(bool prepareForBulkload)
        {
            foreach (var segment in segments.allSegments())
            {
                segment.toggleDbForBulkLoading(prepareForBulkload);
            }
        }

        private RocksDbSegmentsBatchingRestoreCallback : AbstractNotifyingBatchingRestoreCallback
        {
            public override void restoreAll(List<KeyValue<byte[], byte[]>> records)
        {
            restoreAllInternal(records);
        }

        public override void onRestoreStart(
            TopicPartition topicPartition,
            string storeName,
            long startingOffset,
            long endingOffset)
        {
            toggleForBulkLoading(true);
        }

        public override void onRestoreEnd(
            TopicPartition topicPartition,
            string storeName,
            long totalRestored)
        {
            toggleForBulkLoading(false);
        }
    }
}
