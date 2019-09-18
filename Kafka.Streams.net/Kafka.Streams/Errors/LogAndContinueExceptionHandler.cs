using Confluent.Kafka;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Errors
{
    /**
     * Deserialization handler that logs a deserialization exception and then
     * signals the processing pipeline to continue processing more records.
     */
    public class LogAndContinueExceptionHandler : IDeserializationExceptionHandler
    {
        private readonly ILogger<LogAndContinueExceptionHandler> logger;

        public LogAndContinueExceptionHandler(ILogger<LogAndContinueExceptionHandler> logger)
            => this.logger = logger;

        public DeserializationHandlerResponses handle<K, V>(
            IProcessorContext<K, V> context,
            ConsumeResult<byte[], byte[]> record,
            Exception exception)
        {
            logger.LogWarning(exception, 
                "Exception caught during Deserialization, " +
                     $"taskId: {context.taskId}, topic: {record.Topic}, partition: {record.Partition}, offset: {record.Offset}");

            return DeserializationHandlerResponses.CONTINUE;
        }

        public void configure(Dictionary<string, object> configs)
        {
            // ignore
        }
    }
}
