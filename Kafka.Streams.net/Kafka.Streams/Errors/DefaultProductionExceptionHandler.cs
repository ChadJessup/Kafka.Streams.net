using Confluent.Kafka;
using Kafka.Streams.Errors.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Errors
{
    /**
     * {@code ProductionExceptionHandler} that always instructs streams to fail when an exception
     * happens while attempting to produce result records.
     */
    public class DefaultProductionExceptionHandler : IProductionExceptionHandler
    {
        public string name { get; }
        public int id { get; }

        public ProductionExceptionHandlerResponse handle(
            DeliveryReport<byte[], byte[]> record,
            Exception exception)
        {
            return ProductionExceptionHandlerResponse.FAIL;
        }

        public void Configure(Dictionary<string, object> configs)
        {
            // ignore
        }
    }
}
