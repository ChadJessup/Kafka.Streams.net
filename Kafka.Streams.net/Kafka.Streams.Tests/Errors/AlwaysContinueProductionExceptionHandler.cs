using Confluent.Kafka;
using Kafka.Streams.Errors.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Errors
{
    /**
     * Production exception handler that always instructs streams to continue when an exception
     * happens while attempting to produce result records.
     */
    public class AlwaysContinueProductionExceptionHandler : IProductionExceptionHandler
    {
        public string Name { get; }
        public int Id { get; }

        public ProductionExceptionHandlerResponse Handle(
            DeliveryResult<byte[], byte[]> record,
            Exception exception)
        {
            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        public void Configure(Dictionary<string, string?> configs)
        {
            // ignore
        }
    }
}
