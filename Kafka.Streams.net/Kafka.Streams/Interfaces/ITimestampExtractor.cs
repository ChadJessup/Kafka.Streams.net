using Confluent.Kafka;

namespace Kafka.Streams.Interfaces
{
    /**
     * An interface that allows the Kafka Streams framework to extract a timestamp from an instance of {@link ConsumerRecord}.
     * The extracted timestamp is defined as milliseconds.
     */
    public interface ITimestampExtractor
   
{
        /**
         * Extracts a timestamp from a record. The timestamp must be positive to be considered a valid timestamp.
         * Returning a negative timestamp will cause the record not to be processed but rather silently skipped.
         * In case the record contains a negative timestamp and this is considered a fatal error for the application,
         * throwing a {@link RuntimeException} instead of returning the timestamp is a valid option too.
         * For this case, Streams will stop processing and shut down to allow you investigate in the root cause of the
         * negative timestamp.
         * <p>
         * The timestamp extractor implementation must be stateless.
         * <p>
         * The extracted timestamp MUST represent the milliseconds since midnight, January 1, 1970 UTC.
         * <p>
         * It is important to note that this timestamp may become the message timestamp for any messages sent to changelogs
         * updated by {@link KTable}s and joins.
         * The message timestamp is used for log retention and log rolling, so using nonsensical values may result in
         * excessive log rolling and therefore broker performance degradation.
         *
         *
         * @param record a data record
         * @param partitionTime the highest extracted valid timestamp of the current record's partition˙ (could be -1 if unknown)
         * @return the timestamp of the record
         */

        long Extract(ConsumeResult<object, object> record, long partitionTime);
    }
}
