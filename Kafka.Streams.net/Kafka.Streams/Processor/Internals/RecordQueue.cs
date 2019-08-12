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
using Kafka.Common.Metrics;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumeResult + timestamp). It also keeps track of the
     * partition timestamp defined as the largest timestamp seen on the partition so far; this is passed to the
     * timestamp extractor.
     */

    public class RecordQueue
    {
        public static long UNKNOWN = ConsumeResult.NO_TIMESTAMP;

        internal void Clear()
        {
            throw new NotImplementedException();
        }

        internal Internals.ProcessorNode source()
        {
            throw new NotImplementedException();
        }

        internal int size()
        {
            throw new NotImplementedException();
        }
    }

    public static class ConsumeResult
    {
        public static long NO_TIMESTAMP { get; internal set; }
    }

    public class RecordQueue<K, V>
    {
        private ILogger log;
        private SourceNode<K, V> source;
        private TopicPartition partition;
        private IProcessorContext<K, V> processorContext;
        private ITimestampExtractor timestampExtractor;
        private RecordDeserializer<K, V> recordDeserializer;
        private Queue<ConsumeResult<byte[], byte[]>> fifoQueue;

        private StampedRecord headRecord = null;
        private long partitionTime = RecordQueue.UNKNOWN;

        private Sensor skipRecordsSensor;
        private SourceNode<object, object> source1;
        private ITimestampExtractor sourceTimestampExtractor;
        private IDeserializationExceptionHandler defaultDeserializationExceptionHandler;
        private object processorContext1;
        private object logContext;

        public RecordQueue(
            TopicPartition partition,
            SourceNode<K, V> source,
            ITimestampExtractor timestampExtractor,
            IDeserializationExceptionHandler deserializationExceptionHandler,
            IInternalProcessorContext<K, V> processorContext,
            LogContext logContext)
        {
            this.source = source;
            this.partition = partition;
            this.fifoQueue = new Queue<ConsumeResult<byte[], byte[]>>();
            this.timestampExtractor = timestampExtractor;
            this.processorContext = processorContext;
            skipRecordsSensor = ThreadMetrics.skipRecordSensor(processorContext.metrics);
            recordDeserializer = new RecordDeserializer<K, V>(
                source,
                deserializationExceptionHandler,
                logContext,
                skipRecordsSensor
            );
            this.log = logContext.logger<RecordQueue>();
        }

        public RecordQueue(TopicPartition partition, SourceNode<object, object> source1, ITimestampExtractor sourceTimestampExtractor, IDeserializationExceptionHandler defaultDeserializationExceptionHandler, object processorContext1, object logContext)
        {
            this.partition = partition;
            this.source1 = source1;
            this.sourceTimestampExtractor = sourceTimestampExtractor;
            this.defaultDeserializationExceptionHandler = defaultDeserializationExceptionHandler;
            this.processorContext1 = processorContext1;
            this.logContext = logContext;
        }

        /**
         * Returns the corresponding source node in the topology
         *
         * @return SourceNode
         */

        /**
         * Returns the partition with which this queue is associated
         *
         * @return TopicPartition
         */

        /**
         * Add a batch of {@link ConsumeResult} into the queue
         *
         * @param rawRecords the raw records
         * @return the size of this queue
         */
        int addRawRecords(IEnumerable<ConsumeResult<byte[], byte[]>> rawRecords)
        {
            foreach (ConsumeResult<byte[], byte[]> rawRecord in rawRecords)
            {
                fifoQueue.AddLast(rawRecord);
            }

            updateHead();

            return size();
        }

        /**
         * Get the next {@link StampedRecord} from the queue
         *
         * @return StampedRecord
         */
        public StampedRecord poll()
        {
            StampedRecord recordToReturn = headRecord;
            headRecord = null;

            updateHead();

            return recordToReturn;
        }

        /**
         * Returns the number of records in the queue
         *
         * @return the number of records
         */
        public int size()
        {
            // plus one deserialized head record for timestamp tracking
            return fifoQueue.size() + (headRecord == null ? 0 : 1);
        }

        /**
         * Tests if the queue is empty
         *
         * @return true if the queue is empty, otherwise false
         */
        public bool isEmpty()
        {
            return fifoQueue.isEmpty() && headRecord == null;
        }

        /**
         * Returns the head record's timestamp
         *
         * @return timestamp
         */
        public long headRecordTimestamp()
        {
            return headRecord == null ? UNKNOWN : headRecord.timestamp;
        }

        /**
         * Returns the tracked partition time
         *
         * @return partition time
         */

        /**
         * Clear the fifo queue of its elements, also clear the time tracker's kept stamped elements
         */
        public void clear()
        {
            fifoQueue.clear();
            headRecord = null;
            partitionTime = RecordQueue.UNKNOWN;
        }

        private void updateHead()
        {
            while (headRecord == null && !fifoQueue.isEmpty())
            {
                ConsumeResult<byte[], byte[]> raw = fifoQueue.pollFirst();
                ConsumeResult<object, object> deserialized = recordDeserializer.Deserialize(processorContext, raw);

                if (deserialized == null)
                {
                    // this only happens if the deserializer decides to skip. It has already logged the reason.
                    continue;
                }

                long timestamp;
                try
                {
                    timestamp = timestampExtractor.Extract(deserialized, partitionTime);
                }
                catch (StreamsException internalFatalExtractorException)
                {
                    throw internalFatalExtractorException;
                }
                catch (Exception fatalUserException)
                {
                    throw new StreamsException(
                            string.Format("Fatal user code error in ITimestampExtractor callback for record %s.", deserialized),
                            fatalUserException);
                }
                log.LogTrace("Source node {} extracted timestamp {} for record {}", source.name, timestamp, deserialized);

                // drop message if TS is invalid, i.e., negative
                if (timestamp < 0)
                {
                    log.LogWarning(
                            "Skipping record due to negative extracted timestamp. topic=[{}] partition=[{}] offset=[{}] extractedTimestamp=[{}] extractor=[{}]",
                            deserialized.Topic, deserialized.Partition, deserialized.Offset, timestamp, timestampExtractor.GetType().FullName);

                    skipRecordsSensor.record();
                    continue;
                }

                headRecord = new StampedRecord(deserialized, timestamp);

                partitionTime = Math.Max(partitionTime, timestamp);
            }
        }
    }
}