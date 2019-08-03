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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.ConsumerRecord;
using Kafka.Common.TopicPartition;
using Kafka.Common.metrics.Sensor;
using Kafka.Common.Utils.LogContext;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.slf4j.Logger;

import java.util.ArrayDeque;

/**
 * RecordQueue is a FIFO queue of {@link StampedRecord} (ConsumerRecord + timestamp). It also keeps track of the
 * partition timestamp defined as the largest timestamp seen on the partition so far; this is passed to the
 * timestamp extractor.
 */
public class RecordQueue {

    public static long UNKNOWN = ConsumerRecord.NO_TIMESTAMP;

    private Logger log;
    private SourceNode source;
    private TopicPartition partition;
    private IProcessorContext processorContext;
    private TimestampExtractor timestampExtractor;
    private RecordDeserializer recordDeserializer;
    private ArrayDeque<ConsumerRecord<byte[], byte[]>> fifoQueue;

    private StampedRecord headRecord = null;
    private long partitionTime = RecordQueue.UNKNOWN;

    private Sensor skipRecordsSensor;

    RecordQueue(TopicPartition partition,
                SourceNode source,
                TimestampExtractor timestampExtractor,
                DeserializationExceptionHandler deserializationExceptionHandler,
                InternalProcessorContext processorContext,
                LogContext logContext) {
        this.source = source;
        this.partition = partition;
        this.fifoQueue = new ArrayDeque<>();
        this.timestampExtractor = timestampExtractor;
        this.processorContext = processorContext;
        skipRecordsSensor = ThreadMetrics.skipRecordSensor(processorContext.metrics());
        recordDeserializer = new RecordDeserializer(
            source,
            deserializationExceptionHandler,
            logContext,
            skipRecordsSensor
        );
        this.log = logContext.logger(RecordQueue.class);
    }

    /**
     * Returns the corresponding source node in the topology
     *
     * @return SourceNode
     */
    public SourceNode source() {
        return source;
    }

    /**
     * Returns the partition with which this queue is associated
     *
     * @return TopicPartition
     */
    public TopicPartition partition() {
        return partition;
    }

    /**
     * Add a batch of {@link ConsumerRecord} into the queue
     *
     * @param rawRecords the raw records
     * @return the size of this queue
     */
    int addRawRecords(Iterable<ConsumerRecord<byte[], byte[]>> rawRecords) {
        for (ConsumerRecord<byte[], byte[]> rawRecord : rawRecords) {
            fifoQueue.addLast(rawRecord);
        }

        updateHead();

        return size();
    }

    /**
     * Get the next {@link StampedRecord} from the queue
     *
     * @return StampedRecord
     */
    public StampedRecord poll() {
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
    public int size() {
        // plus one deserialized head record for timestamp tracking
        return fifoQueue.size() + (headRecord == null ? 0 : 1);
    }

    /**
     * Tests if the queue is empty
     *
     * @return true if the queue is empty, otherwise false
     */
    public bool isEmpty() {
        return fifoQueue.isEmpty() && headRecord == null;
    }

    /**
     * Returns the head record's timestamp
     *
     * @return timestamp
     */
    public long headRecordTimestamp() {
        return headRecord == null ? UNKNOWN : headRecord.timestamp;
    }

    /**
     * Returns the tracked partition time
     *
     * @return partition time
     */
    long partitionTime() {
        return partitionTime;
    }

    /**
     * Clear the fifo queue of its elements, also clear the time tracker's kept stamped elements
     */
    public void clear() {
        fifoQueue.clear();
        headRecord = null;
        partitionTime = RecordQueue.UNKNOWN;
    }

    private void updateHead() {
        while (headRecord == null && !fifoQueue.isEmpty()) {
            ConsumerRecord<byte[], byte[]> raw = fifoQueue.pollFirst();
            ConsumerRecord<Object, object> deserialized = recordDeserializer.deserialize(processorContext, raw);

            if (deserialized == null) {
                // this only happens if the deserializer decides to skip. It has already logged the reason.
                continue;
            }

            long timestamp;
            try {
                timestamp = timestampExtractor.extract(deserialized, partitionTime);
            } catch (StreamsException internalFatalExtractorException) {
                throw internalFatalExtractorException;
            } catch (Exception fatalUserException) {
                throw new StreamsException(
                        string.format("Fatal user code error in TimestampExtractor callback for record %s.", deserialized),
                        fatalUserException);
            }
            log.trace("Source node {} extracted timestamp {} for record {}", source.name(), timestamp, deserialized);

            // drop message if TS is invalid, i.e., negative
            if (timestamp < 0) {
                log.warn(
                        "Skipping record due to negative extracted timestamp. topic=[{}] partition=[{}] offset=[{}] extractedTimestamp=[{}] extractor=[{}]",
                        deserialized.topic(), deserialized.partition(), deserialized.offset(), timestamp, timestampExtractor.GetType().getCanonicalName()
                );

                skipRecordsSensor.record();
                continue;
            }

            headRecord = new StampedRecord(deserialized, timestamp);

            partitionTime = Math.max(partitionTime, timestamp);
        }
    }
}
