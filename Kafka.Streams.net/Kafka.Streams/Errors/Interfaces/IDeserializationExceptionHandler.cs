using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using System;

namespace Kafka.Streams.Errors.Interfaces
{
    /**
     * Interface that specifies how an exception from source node deserialization
     * (e.g., reading from Kafka) should be handled.
     */
    public interface IDeserializationExceptionHandler //: Configurable
    {
        /**
         * Inspect a record and the exception received.
         * @param context processor context
         * @param record record that failed deserialization
         * @param exception the actual exception
         */
        DeserializationHandlerResponses Handle<K, V>(
            IProcessorContext context,
            ConsumeResult<byte[], byte[]> record,
            Exception exception);
    }

    /**
     * Enumeration that describes the response from the exception handler.
     */
    public class DeserializationHandlerResponse
    {
        public static DeserializationHandlerResponses Response { get; }
        public static DeserializationHandlerResponse FAIL { get; }
    }

    public enum DeserializationHandlerResponses
    {
        /* continue with processing */
        CONTINUE,
        /* fail the processing and stop */
        FAIL,
    }
}
