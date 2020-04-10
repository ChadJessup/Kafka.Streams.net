using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using System;

namespace Kafka.Streams.Errors.Interfaces
{
    /**
     * Interface that specifies how an exception from source node deserialization
     * (e.g., reading from Kafka) should be handled.
     */
    public interface IDeserializationExceptionHandler
    {
        /**
         * Inspect a record and the exception received.
         * @param context processor context
         * @param record record that failed deserialization
         * @param exception the actual exception
         */
        DeserializationHandlerResponse Handle(
            IProcessorContext context,
            ConsumeResult<byte[], byte[]> record,
            Exception exception);
    }

    public enum DeserializationHandlerResponse
    {
        /* continue with processing */
        CONTINUE,
        /* fail the processing and stop */
        FAIL,
    }
}
