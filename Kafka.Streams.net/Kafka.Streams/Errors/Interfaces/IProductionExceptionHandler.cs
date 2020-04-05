using Confluent.Kafka;
using System;

namespace Kafka.Streams.Errors.Interfaces
{
    /**
     * Interface that specifies how an exception when attempting to produce a result to
     * Kafka should be handled.
     */
    public interface IProductionExceptionHandler// : Configurable
    {
        /**
         * Inspect a record that we attempted to produce, and the exception that resulted
         * from attempting to produce it and determine whether or not to continue processing.
         *
         * @param record The record that failed to produce
         * @param exception The exception that occurred during production
         */
        ProductionExceptionHandlerResponse Handle(DeliveryResult<byte[], byte[]> record, Exception exception);

        /**
         * an english description of the api--this is for debugging and can change
         */
        string name { get; }

        /**
         * the permanent and immutable id of an API--this can't change ever
         */
        int id { get; }
    }

    public enum ProductionExceptionHandlerResponse
    {
        /* continue processing */
        CONTINUE,//        CONTINUE(0, "CONTINUE"),
                 /* fail processing */
        FAIL, //IProductionExceptionHandler(1, "FAIL");
    }
}