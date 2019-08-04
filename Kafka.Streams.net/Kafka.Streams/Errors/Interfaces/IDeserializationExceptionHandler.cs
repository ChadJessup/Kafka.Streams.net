using Confluent.Kafka;
using Kafka.Streams.Processor.Interfaces;
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
        DeserializationHandlerResponse handle(
            IProcessorContext context,
            ConsumeResult<byte[], byte[]> record,
            Exception exception);

        /**
         * Enumeration that describes the response from the exception handler.
         */
        public enum DeserializationHandlerResponse
        {
            /* continue with processing */
            CONTINUE, //(0, "CONTINUE"),
                      /* fail the processing and stop */
            FAIL, //(1, "FAIL");
        }

        /** an english description of the api--this is for debugging and can change */
        string name { get; }

        /** the permanent and immutable id of an API--this can't change ever */
        int id { get; }

        //DeserializationHandlerResponse( int id,  string name)
//{
        //    this.id = id;
        //    this.name = name;
        //}
    }
}
