using Confluent.Kafka;

namespace Kafka.Streams.Processors
{
    /**
     * The context associated with the current record being processed by
     * an {@link IProcessor}
     */
    public interface IRecordContext
    {
        /**
         * @return  The offset of the original record received from Kafka;
         *          could be -1 if it is not available
         */
        long offset { get; }

        /**
         * @return  The timestamp extracted from the record received from Kafka;
         *          could be -1 if it is not available
         */
        long timestamp { get; }

        /**
         * @return  The topic the record was received on;
         *          could be null if it is not available
         */
        string Topic { get; }

        /**
         * @return  The partition the record was received on;
         *          could be -1 if it is not available
         */
        int partition { get; }

        /**
         * @return  The headers from the record received from Kafka;
         *          could be null if it is not available
         */
        Headers headers { get; }
    }
}