/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.Streams.Processor.Internals;


using Kafka.Common.metrics.Sensor;
using Kafka.Common.record.TimestampType;
using Kafka.Common.Utils.LogContext;







class RecordDeserializer
{

    private SourceNode sourceNode;
    private IDeserializationExceptionHandler deserializationExceptionHandler;
    private ILogger log;
    private Sensor skippedRecordSensor;

    RecordDeserializer(SourceNode sourceNode,
                       IDeserializationExceptionHandler deserializationExceptionHandler,
                       LogContext logContext,
                       Sensor skippedRecordsSensor)
{
        this.sourceNode = sourceNode;
        this.deserializationExceptionHandler = deserializationExceptionHandler;
        this.log = logContext.logger<RecordDeserializer>();
        this.skippedRecordSensor = skippedRecordsSensor;
    }

    /**
     * @throws StreamsException if a deserialization error occurs and the deserialization callback returns
     *                          {@link IDeserializationExceptionHandler.DeserializationHandlerResponse#FAIL FAIL}
     *                          oritself
     */
    
    ConsumeResult<object, object> deserialize(IProcessorContext<K, V> processorContext,
                                               ConsumeResult<byte[], byte[]> rawRecord)
{

        try
{

            return new ConsumeResult<>(
                rawRecord.Topic,
                rawRecord.partition(),
                rawRecord.offset(),
                rawRecord.timestamp(),
                TimestampType.CREATE_TIME,
                rawRecord.checksum(),
                rawRecord.serializedKeySize(),
                rawRecord.serializedValueSize(),
                sourceNode.deserializeKey(rawRecord.Topic, rawRecord.headers(), rawRecord.key()),
                sourceNode.deserializeValue(rawRecord.Topic, rawRecord.headers(), rawRecord.value()), rawRecord.headers());
        } catch (Exception deserializationException)
{
            IDeserializationExceptionHandler.DeserializationHandlerResponse response;
            try
{

                response = deserializationExceptionHandler.handle(processorContext, rawRecord, deserializationException);
            } catch (Exception fatalUserException)
{
                log.LogError(
                    "Deserialization error callback failed after deserialization error for record {}",
                    rawRecord,
                    deserializationException);
                throw new StreamsException("Fatal user code error in deserialization error callback", fatalUserException);
            }

            if (response == IDeserializationExceptionHandler.DeserializationHandlerResponse.FAIL)
{
                throw new StreamsException("Deserialization exception handler is set to fail upon" +
                    " a deserialization error. If you would rather have the streaming pipeline" +
                    " continue after a deserialization error, please set the " +
                    DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG + " appropriately.",
                    deserializationException);
            } else
{

                log.LogWarning(
                    "Skipping record due to deserialization error. topic=[{}] partition=[{}] offset=[{}]",
                    rawRecord.Topic,
                    rawRecord.partition(),
                    rawRecord.offset(),
                    deserializationException
                );
                skippedRecordSensor.record();
                return null;
            }
        }
    }

    SourceNode sourceNode()
{
        return sourceNode;
    }
}
