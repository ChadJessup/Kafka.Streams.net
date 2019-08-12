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
using Kafka.Streams.Interfaces;

namespace Kafka.Streams.Processor
{
    /**
     * Retrieves embedded metadata timestamps from Kafka messages.
     * If a record has a negative (invalid) timestamp value, an error handler method is called.
     * <p>
     * Embedded metadata timestamp was introduced in "KIP-32: Add timestamps to Kafka message" for the new
     * 0.10+ Kafka message string.Format.
     * <p>
     * Here, "embedded metadata" refers to the fact that compatible Kafka producer clients automatically and
     * transparently embed such timestamps into message metadata they send to Kafka, which can then be retrieved
     * via this timestamp extractor.
     * <p>
     * If the embedded metadata timestamp represents <i>CreateTime</i> (cf. Kafka broker setting
     * {@code message.timestamp.type} and Kafka topic setting {@code log.message.timestamp.type}),
     * this extractor effectively provides <i>event-time</i> semantics.
     * If <i>LogAppendTime</i> is used as broker/topic setting to define the embedded metadata timestamps,
     * using this extractor effectively provides <i>ingestion-time</i> semantics.
     * <p>
     * If you need <i>processing-time</i> semantics, use {@link WallclockTimestampExtractor}.
     *
     * @see FailOnInvalidTimestamp
     * @see LogAndSkipOnInvalidTimestamp
     * @see UsePreviousTimeOnInvalidTimestamp
     * @see WallclockTimestampExtractor
     */
    public abstract class ExtractRecordMetadataTimestamp : ITimestampExtractor
    {
        /**
         * Extracts the embedded metadata timestamp from the given {@link ConsumeResult}.
         *
         * @param record a data record
         * @param partitionTime the highest extracted valid timestamp of the current record's partition˙ (could be -1 if unknown)
         * @return the embedded metadata timestamp of the given {@link ConsumeResult}
         */

        public long Extract(ConsumeResult<object, object> record, long partitionTime)
        {
            long timestamp = record.Timestamp.UnixTimestampMs;

            if (timestamp < 0)
            {
                return onInvalidTimestamp(record, timestamp, partitionTime);
            }

            return timestamp;
        }

        /**
         * Called if no valid timestamp is embedded in the record meta data.
         *
         * @param record a data record
         * @param recordTimestamp the timestamp extractor from the record
         * @param partitionTime the highest extracted valid timestamp of the current record's partition˙ (could be -1 if unknown)
         * @return a new timestamp for the record (if negative, record will not be processed but dropped silently)
         */
        public abstract long onInvalidTimestamp(ConsumeResult<object, object> record,
                                                long recordTimestamp,
                                                long partitionTime);
    }
}