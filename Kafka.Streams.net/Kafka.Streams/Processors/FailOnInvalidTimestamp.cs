using System;
using Confluent.Kafka;
using Kafka.Streams.Errors;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processors
{
    /**
     * Retrieves embedded metadata timestamps from Kafka messages.
     * If a record has a negative (invalid) timestamp value, this extractor raises an exception.
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
     * @see LogAndSkipOnInvalidTimestamp
     * @see UsePreviousTimeOnInvalidTimestamp
     * @see WallclockTimestampExtractor
     */
    public class FailOnInvalidTimestamp : ExtractRecordMetadataTimestamp
    {
        private readonly ILogger<FailOnInvalidTimestamp>? logger;

        public FailOnInvalidTimestamp(ILogger<FailOnInvalidTimestamp>? logger = null)
            => this.logger = logger;

        /**
         * Raises an exception on every call.
         *
         * @param record a data record
         * @param recordTimestamp the timestamp extractor from the record
         * @param partitionTime the highest extracted valid timestamp of the current record's partition˙ (could be -1 if unknown)
         * @return nothing; always raises an exception
         * @throws StreamsException on every invocation
         */
        public override DateTime OnInvalidTimestamp<K, V>(
            ConsumeResult<K, V> record,
            Timestamp recordTimestamp,
            DateTime partitionTime)
        {

            var message = $"Input record {record} has invalid (negative) timestamp. " +
                "Possibly because a pre-0.10 producer client was used to write this record to Kafka without embedding " +
                "a timestamp, or because the input topic was created before upgrading the Kafka cluster to 0.10+. " +
                "Use a different ITimestampExtractor to process this data.";

            this.logger?.LogError(message);

            throw new StreamsException(message);
        }
    }
}
