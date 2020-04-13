using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class RecordDeserializer<K, V> : RecordDeserializer
    {
        private readonly ILogger<RecordDeserializer<K, V>> logger;
        private readonly ISourceNode sourceNode;
        private readonly IDeserializationExceptionHandler deserializationExceptionHandler;

        public RecordDeserializer(
            ILogger<RecordDeserializer<K, V>> logger,
            ISourceNode sourceNode,
            IDeserializationExceptionHandler deserializationExceptionHandler)
            : base(null, sourceNode, deserializationExceptionHandler)
        {
            this.sourceNode = sourceNode;
            this.deserializationExceptionHandler = deserializationExceptionHandler;
            this.logger = logger;
        }
    }

    public class RecordDeserializer
    {
        private readonly ILogger<RecordDeserializer> logger;

        public ISourceNode SourceNode { get; }
        private IDeserializationExceptionHandler deserializationExceptionHandler;

        public RecordDeserializer(
            ILogger<RecordDeserializer> logger,
            ISourceNode source,
            IDeserializationExceptionHandler deserializationExceptionHandler)
        {
            this.logger = logger;
            this.SourceNode = source;
            this.deserializationExceptionHandler = deserializationExceptionHandler;
        }

        /**
         * @throws StreamsException if a deserialization error occurs and the deserialization callback returns
         *                          {@link IDeserializationExceptionHandler.DeserializationHandlerResponse#FAIL FAIL}
         *                          oritself
         */
        public ConsumeResult<K, V> Deserialize<K, V>(
            IProcessorContext processorContext,
            ConsumeResult<byte[], byte[]> rawRecord)
        {
            try
            {
                return new ConsumeResult<K, V>()
                {
                    TopicPartitionOffset = new TopicPartitionOffset(rawRecord.TopicPartition, rawRecord.Offset),
                    Message = new Message<K, V>
                    {
                        Timestamp = rawRecord.Timestamp,
                        //TimestampType.CREATE_TIME,
                        //rawRecord.Checksum,
                        //rawRecord.serializedKeySize(),
                        //rawRecord.serializedValueSize(),
                        //sourceNode.deserializeKey(rawRecord.Topic, rawRecord.Headers, rawRecord.Key),
                        //sourceNode.deserializeValue(rawRecord.Topic, rawRecord.Headers, rawRecord.value()), rawRecord.Headers,
                    }
                };
            }
            catch (Exception deserializationException)
            {
                DeserializationHandlerResponse response;
                try
                {
                    response = this.deserializationExceptionHandler.Handle(processorContext, rawRecord, deserializationException);
                }
                catch (Exception fatalUserException)
                {
                    this.logger.LogError(
                        "Deserialization error callback failed after deserialization error for record {}",
                        rawRecord,
                        deserializationException);
                    throw new StreamsException("Fatal user code error in deserialization error callback", fatalUserException);
                }

                if (response == DeserializationHandlerResponse.FAIL)
                {
                    throw new StreamsException("Deserialization exception handler is set to fail upon" +
                        " a deserialization error. If you would rather have the streaming pipeline" +
                        " continue after a deserialization error, please set the " +
                        StreamsConfig.DefaultDeserializationExceptionHandlerClass + " appropriately.",
                        deserializationException);
                }
                else
                {

                    this.logger.LogWarning(
                        "Skipping record due to deserialization error. topic=[{}] partition=[{}] offset=[{}]",
                        rawRecord.Topic,
                        rawRecord.Partition,
                        rawRecord.Offset,
                        deserializationException);

                    return null;
                }
            }
        }
    }
}
