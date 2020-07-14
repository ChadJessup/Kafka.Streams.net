using System;
using Confluent.Kafka;
using Kafka.Streams.Interfaces;

namespace Kafka.Streams.Processors
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
        public DateTime Extract<K, V>(ConsumeResult<K, V> record, DateTime partitionTime)
        {
            var timestamp = record.Message.Timestamp;

            if (timestamp.UnixTimestampMs < 0)
            {
                return this.OnInvalidTimestamp(record, timestamp, partitionTime);
            }

            return timestamp.UtcDateTime;
        }

        /**
         * Called if no valid timestamp is embedded in the record meta data.
         *
         * @param record a data record
         * @param recordTimestamp the timestamp extractor from the record
         * @param partitionTime the highest extracted valid timestamp of the current record's partition˙ (could be -1 if unknown)
         * @return a new timestamp for the record (if negative, record will not be processed but dropped silently)
         */
        public abstract DateTime OnInvalidTimestamp<K, V>(
            ConsumeResult<K, V> record,
            Timestamp recordTimestamp,
            DateTime partitionTime);
    }
}
