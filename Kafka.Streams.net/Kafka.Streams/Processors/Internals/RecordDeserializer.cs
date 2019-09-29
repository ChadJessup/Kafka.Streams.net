using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class RecordDeserializer<K, V>
    {
        private readonly SourceNode<K, V> sourceNode;
        private readonly IDeserializationExceptionHandler deserializationExceptionHandler;
        private readonly ILogger log;
        private readonly Sensor skippedRecordSensor;

        public RecordDeserializer(
            SourceNode<K, V> sourceNode,
            IDeserializationExceptionHandler deserializationExceptionHandler,
            LogContext logContext,
            Sensor skippedRecordsSensor)
        {
            this.sourceNode = sourceNode;
            this.deserializationExceptionHandler = deserializationExceptionHandler;
            this.log = logContext.logger<RecordDeserializer<K, V>>();
            this.skippedRecordSensor = skippedRecordsSensor;
        }

        /**
         * @throws StreamsException if a deserialization error occurs and the deserialization callback returns
         *                          {@link IDeserializationExceptionHandler.DeserializationHandlerResponse#FAIL FAIL}
         *                          oritself
         */
        public ConsumeResult<K, V> deserialize(
            IProcessorContext processorContext,
            ConsumeResult<byte[], byte[]> rawRecord)
        {
            try
            {
                return new ConsumeResult<K, V>()
                {
                     Topic = rawRecord.Topic,
                     Partition = rawRecord.Partition,
                     Offset = rawRecord.Offset,
                     Timestamp = rawRecord.Timestamp,
                    //TimestampType.CREATE_TIME,
                    //rawRecord.checksum(),
                    //rawRecord.serializedKeySize(),
                    //rawRecord.serializedValueSize(),
                    //sourceNode.deserializeKey(rawRecord.Topic, rawRecord.Headers, rawRecord.Key),
                    //sourceNode.deserializeValue(rawRecord.Topic, rawRecord.headers(), rawRecord.value()), rawRecord.headers(),
                };
            }
            catch (Exception deserializationException)
            {
                DeserializationHandlerResponses response;
                try
                {
                    response = deserializationExceptionHandler.handle<K, V>(processorContext, rawRecord, deserializationException);
                }
                catch (Exception fatalUserException)
                {
                    log.LogError(
                        "Deserialization error callback failed after deserialization error for record {}",
                        rawRecord,
                        deserializationException);
                    throw new StreamsException("Fatal user code error in deserialization error callback", fatalUserException);
                }

                if (response == DeserializationHandlerResponses.FAIL)
                {
                    throw new StreamsException("Deserialization exception handler is set to fail upon" +
                        " a deserialization error. If you would rather have the streaming pipeline" +
                        " continue after a deserialization error, please set the " +
                        StreamsConfigPropertyNames.DefaultDeserializationExceptionHandlerClass + " appropriately.",
                        deserializationException);
                }
                else
                {

                    log.LogWarning(
                        "Skipping record due to deserialization error. topic=[{}] partition=[{}] offset=[{}]",
                        rawRecord.Topic,
                        rawRecord.Partition,
                        rawRecord.Offset,
                        deserializationException);

                    skippedRecordSensor.record();

                    return null;
                }
            }
        }
    }
}