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
using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class RecordCollectorImpl : RecordCollector
    {
        private ILogger log;
        private string logPrefix;
        private Sensor skippedRecordsSensor;
        private IProducer<byte[], byte[]> producer;
        private Dictionary<TopicPartition, long> offsets;
        private IProductionExceptionHandler productionExceptionHandler;

        private static string LOG_MESSAGE = "Error sending record to topic {} due to {}; " +
            "No more records will be sent and no more offsets will be recorded for this task. " +
            "Enable TRACE logging to view failed record key and value.";
        private static string EXCEPTION_MESSAGE = "%sAbort sending since %s with a previous record (timestamp %d) to topic %s due to %s";
        private static string PARAMETER_HINT = "\nYou can increase the producer configs `delivery.timeout.ms` and/or " +
            "`retries` to avoid this error. Note that `retries` is set to infinite by default.";

        private volatile KafkaException sendException;

        public RecordCollectorImpl(string streamTaskId,
                                   LogContext logContext,
                                   IProductionExceptionHandler productionExceptionHandler,
                                   Sensor skippedRecordsSensor)
        {
            this.offsets = new HashMap<>();
            this.logPrefix = string.Format("task [%s] ", streamTaskId);
            this.log = logContext.logger(GetType());
            this.productionExceptionHandler = productionExceptionHandler;
            this.skippedRecordsSensor = skippedRecordsSensor;
        }


        public void init(IProducer<byte[], byte[]> producer)
        {
            this.producer = producer;
        }


        public void send(string topic,
                                K key,
                                V value,
                                Headers headers,
                                long timestamp,
                                ISerializer<K> keySerializer,
                                ISerializer<V> valueSerializer,
                                StreamPartitioner<K, V> partitioner)
        {
            int partition = null;

            if (partitioner != null)
            {
                List<PartitionInfo> partitions = producer.partitionsFor(topic);
                if (partitions.size() > 0)
                {
                    partition = partitioner.partition(topic, key, value, partitions.size());
                }
                else
                {

                    throw new StreamsException("Could not get partition information for topic '" + topic + "'." +
                        " This can happen if the topic does not exist.");
                }
            }

            send(topic, key, value, headers, partition, timestamp, keySerializer, valueSerializer);
        }

        private bool productionExceptionIsFatal(Exception exception)
        {
            bool securityException = exception is AuthenticationException ||
                exception is AuthorizationException ||
                exception is SecurityDisabledException;

            bool communicationException = exception is InvalidTopicException ||
                exception is UnknownServerException ||
                exception is SerializationException ||
                exception is OffsetMetadataTooLarge ||
                exception is InvalidOperationException;

            return securityException || communicationException;
        }

        private void recordSendError(
            K key,
            V value,
            long timestamp,
            string topic,
            Exception exception
        )
        {
            string errorLogMessage = LOG_MESSAGE;
            string errorMessage = EXCEPTION_MESSAGE;
            // There is no documented API for detecting retriable errors, so we rely on `RetriableException`
            // even though it's an implementation detail (i.e. we do the best we can given what's available)
            if (exception is RetriableException)
            {
                errorLogMessage += PARAMETER_HINT;
                errorMessage += PARAMETER_HINT;
            }
            log.LogError(errorLogMessage, topic, exception.getMessage(), exception);

            // KAFKA-7510 put message key and value in TRACE level log so we don't leak data by default
            log.LogTrace("Failed message: key {} value {} timestamp {}", key, value, timestamp);

            sendException = new StreamsException(
                string.Format(
                    errorMessage,
                    logPrefix,
                    "an error caught",
                    timestamp,
                    topic,
                    exception.ToString()
                ),
                exception);
        }


        public void send(string topic,
                                K key,
                                V value,
                                Headers headers,
                                int partition,
                                long timestamp,
                                ISerializer<K> keySerializer,
                                ISerializer<V> valueSerializer)
        {
            checkForException();
            byte[] keyBytes = keySerializer.Serialize(topic, headers, key);
            byte[] valBytes = valueSerializer.Serialize(topic, headers, value);

            ProducerRecord<byte[], byte[]> serializedRecord = new ProducerRecord<>(topic, partition, timestamp, keyBytes, valBytes, headers);

            try
            {

                //    producer.send(serializedRecord, new Callback()
                //    {

                //    public void onCompletion(RecordMetadata metadata,
                //                             Exception exception)
                //    {
                //        if (exception == null)
                //        {
                //            if (sendException != null)
                //            {
                //                return;
                //            }
                //            TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                //            offsets.Add(tp, metadata.offset());
                //        }
                //        else
                //        {

                //            if (sendException == null)
                //            {
                //                if (exception is ProducerFencedException)
                //                {
                //                    log.LogWarning(LOG_MESSAGE, topic, exception.getMessage(), exception);

                //                    // KAFKA-7510 put message key and value in TRACE level log so we don't leak data by default
                //                    log.LogTrace("Failed message: (key {} value {} timestamp {}) topic=[{}] partition=[{}]", key, value, timestamp, topic, partition);

                //                    sendException = new ProducerFencedException(
                //                        string.Format(
                //                            EXCEPTION_MESSAGE,
                //                            logPrefix,
                //                            "producer got fenced",
                //                            timestamp,
                //                            topic,
                //                            exception.ToString()
                //                        )
                //                    );
                //                }
                //                else
                //                {

                //                    if (productionExceptionIsFatal(exception))
                //                    {
                //                        recordSendError(key, value, timestamp, topic, exception);
                //                    }
                //                    else if (productionExceptionHandler.handle(serializedRecord, exception) == ProductionExceptionHandlerResponse.FAIL)
                //                    {
                //                        recordSendError(key, value, timestamp, topic, exception);
                //                    }
                //                    else
                //                    {

                //                        log.LogWarning(
                //                            "Error sending records topic=[{}] and partition=[{}]; " +
                //                                "The exception handler chose to CONTINUE processing in spite of this error. " +
                //                                "Enable TRACE logging to view failed messages key and value.",
                //                            topic, partition, exception
                //                        );

                //                        // KAFKA-7510 put message key and value in TRACE level log so we don't leak data by default
                //                        log.LogTrace("Failed message: (key {} value {} timestamp {}) topic=[{}] partition=[{}]", key, value, timestamp, topic, partition);

                //                        skippedRecordsSensor.record();
                //                    }
                //                }
                //            }
                //        }
                //    }
                //});
            }
            catch (TimeoutException e)
            {
                log.LogError(
                        "Timeout exception caught when sending record to topic {}. " +
                        "This might happen if the producer cannot send data to the Kafka cluster and thus, " +
                        "its internal buffer fills up. " +
                        "This can also happen if the broker is slow to respond, if the network connection to " +
                        "the broker was interrupted, or if similar circumstances arise. " +
                        "You can increase producer parameter `max.block.ms` to increase this timeout.",
                    topic,
                    e
                );

                throw new StreamsException(
                    string.Format("%sFailed to send record to topic %s due to timeout.", logPrefix, topic),
                    e
                );
            }
            catch (Exception uncaughtException)
            {
                if (uncaughtException is KafkaException &&
                    uncaughtException.getCause() is ProducerFencedException)
                {
                    KafkaException kafkaException = (KafkaException)uncaughtException;
                    // producer.send() call may throw a KafkaException which wraps a FencedException,
                    // in this case we should throw its wrapped inner cause so that it can be captured and re-wrapped as TaskMigrationException
                    throw (ProducerFencedException)kafkaException.getCause();
                }
                else
                {

                    throw new StreamsException(
                        string.Format(
                            EXCEPTION_MESSAGE,
                            logPrefix,
                            "an error caught",
                            timestamp,
                            topic,
                            uncaughtException.ToString()
                        ),
                        uncaughtException);
                }
            }
        }

        private void checkForException()
        {
            if (sendException != null)
            {
                throw sendException;
            }
        }


        public void flush()
        {
            log.LogDebug("Flushing producer");
            producer.flush();
            checkForException();
        }


        public void close()
        {
            log.LogDebug("Closing producer");
            if (producer != null)
            {
                producer.close();
                producer = null;
            }
            checkForException();
        }
    }
}