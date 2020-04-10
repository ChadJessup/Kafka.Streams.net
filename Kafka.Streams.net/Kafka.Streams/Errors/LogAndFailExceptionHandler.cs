using System;
using Confluent.Kafka;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Error
{
    public class LogAndFailExceptionHandler : IDeserializationExceptionHandler
    {
        private readonly ILogger<LogAndFailExceptionHandler> logger;

        public LogAndFailExceptionHandler(ILogger<LogAndFailExceptionHandler> logger)
        {
            this.logger = logger;
        }

        public DeserializationHandlerResponse Handle(
            IProcessorContext context,
            ConsumeResult<byte[], byte[]> record,
            Exception exception)
        {
            this.logger.LogError(exception,
                "Exception caught during Deserialization, " +
                      $"taskId: {context.TaskId}, topic: {record.Topic}, partition: {record.Partition}, offset: {record.Offset}");

            return DeserializationHandlerResponse.FAIL;
        }
    }
}
