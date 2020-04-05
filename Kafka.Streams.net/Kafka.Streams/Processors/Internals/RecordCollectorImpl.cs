using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class RecordCollectorImpl : IRecordCollector
    {
        private readonly ILogger log;
        private readonly string logPrefix;
        private IProducer<byte[], byte[]> producer;
        public Dictionary<TopicPartition, long> offsets { get; }
        public ISupplier Supplier { get; }

        private readonly IProductionExceptionHandler productionExceptionHandler;

        private const string LOG_MESSAGE = "Error sending record to topic {} due to {}; " +
            "No more records will be sent and no more offsets will be recorded for this task. " +
            "Enable TRACE logging to view failed record key and value.";
        private const string EXCEPTION_MESSAGE = "%sAbort sending since %s with a previous record (timestamp %d) to topic %s due to %s";
        private const string PARAMETER_HINT = "\nYou can increase the producer configs `delivery.timeout.ms` and/or " +
            "`retries` to avoid this error. Note that `retries` is set to infinite by default.";

        private volatile KafkaException sendException;

        public RecordCollectorImpl(
            string streamTaskId,
            IProductionExceptionHandler productionExceptionHandler)
        {
            this.offsets = new Dictionary<TopicPartition, long>();

            this.logPrefix = string.Format("task [%s] ", streamTaskId);
            this.productionExceptionHandler = productionExceptionHandler;
        }

        public void Init(IProducer<byte[], byte[]> producer)
        {
            this.producer = producer;
        }

        public void Send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IStreamPartitioner<K, V> partitioner)
        {
            int? partition = null;

            if (partitioner != null)
            {
                var partitions = new List<Partition>();// producer.partitionsFor(topic);

                if (partitions.Count > 0)
                {
                    partition = partitioner.Partition(
                        topic,
                        key,
                        value,
                        partitions.Count);
                }
                else
                {
                    throw new StreamsException("Could not get partition information for topic '" + topic + "'." +
                        " This can happen if the topic does not exist.");
                }
            }

            Send(
                topic,
                key,
                value,
                headers,
                partition,
                timestamp,
                keySerializer,
                valueSerializer);
        }

        public void Send<K, V>(
            string topic,
            K key,
            V value,
            Headers headers,
            int? partition,
            long timestamp,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer)
        {
            CheckForException();
            var keyBytes = keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic));
            var valBytes = valueSerializer.Serialize(value, new SerializationContext(MessageComponentType.Value, topic));

            var serializedRecord = new DeliveryReport<byte[], byte[]>
            {
                Topic = topic,
                Partition = partition.Value,
                Timestamp = new Timestamp(timestamp, TimestampType.CreateTime),
                Key = keyBytes,
                Value = valBytes,
                Headers = headers,
            };

            try
            {
                //producer.pr(serializedRecord, new Callback());
                //{

                //    public void onCompletion(RecordMetadata metadata,
                //                             Exception exception)
                //{
                //    if (exception == null)
                //    {
                //        if (sendException != null)
                //        {
                //            return;
                //        }
                //        TopicPartition tp = new TopicPartition(metadata.Topic, metadata.Partition);
                //        offsets.Add(tp, metadata.offset());
                //    }
                //    else
                //    {

                //        if (sendException == null)
                //        {
                //            if (exception is ProducerFencedException)
                //            {
                //                log.LogWarning(LOG_MESSAGE, topic, exception.getMessage(), exception);

                //                // KAFKA-7510 put message key and value in TRACE level log so we don't leak data by default
                //                log.LogTrace("Failed message: (key {} value {} timestamp {}) topic=[{}] partition=[{}]", key, value, timestamp, topic, partition);

                //                sendException = new ProducerFencedException(
                //                    string.Format(
                //                        EXCEPTION_MESSAGE,
                //                        logPrefix,
                //                        "producer got fenced",
                //                        timestamp,
                //                        topic,
                //                        exception.ToString()));
                //            }
                //            else
                //            {
                //                if (productionExceptionIsFatal(exception))
                //                {
                //                    recordSendError(key, value, timestamp, topic, exception);
                //                }
                //                else if (productionExceptionHandler.handle(serializedRecord, exception)
                //                    == ProductionExceptionHandlerResponse.FAIL)
                //                {
                //                    recordSendError(key, value, timestamp, topic, exception);
                //                }
                //                else
                //                {
                //                    log.LogWarning(
                //                        "Error sending records topic=[{}] and partition=[{}]; " +
                //                            "The exception handler chose to CONTINUE processing in spite of this error. " +
                //                            "Enable TRACE logging to view failed messages key and value.",
                //                        topic, partition, exception);

                //                    // KAFKA-7510 put message key and value in TRACE level log so we don't leak data by default
                //                    log.LogTrace("Failed message: (key {} value {} timestamp {}) topic=[{}] partition=[{}]", key, value, timestamp, topic, partition);

                //                    skippedRecordsSensor.record();
                //                }
                //            }
                //        }
                //    }
                //}
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
                if (uncaughtException is KafkaException
                    && uncaughtException.Message is ProducerFencedException)
                {
                    var kafkaException = (KafkaException)uncaughtException;
                    // producer.send() call may throw a KafkaException which wraps a FencedException,
                    // in this case we should throw its wrapped inner cause so that it can be captured and re-wrapped as TaskMigrationException
                    throw (ProducerFencedException)kafkaException.InnerException;
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

        private void CheckForException()
        {
            if (sendException != null)
            {
                throw sendException;
            }
        }

        public void Flush()
        {
            log.LogDebug("Flushing producer");
            producer.Flush();

            CheckForException();
        }


        public void Close()
        {
            log.LogDebug("Closing producer");
            if (producer != null)
            {
                producer.Dispose();
                producer = null;
            }

            CheckForException();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~RecordCollectorImpl()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}