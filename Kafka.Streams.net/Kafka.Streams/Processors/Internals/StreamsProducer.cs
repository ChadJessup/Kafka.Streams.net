using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.Stream;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processors.Internals
{
    /**
    * {@code StreamsProducer} manages the producers within a Kafka Streams application.
    * <p>
    * If EOS is enabled, it is responsible to init and begin transactions if necessary.
    * It also tracks the transaction status, ie, if a transaction is in-fight.
    * <p>
    * For non-EOS, the user should not call transaction related methods.
*/
    public class StreamsProducer
    {
        private readonly ILogger<StreamsProducer> log;

        private readonly ProducerConfig? eosBetaProducerConfigs;
        private readonly IKafkaClientSupplier clientSupplier;
        private readonly ProcessingMode processingMode;

        private IProducer<byte[], byte[]> producer;
        private bool transactionInFlight = false;
        private bool transactionInitialized = false;

        public StreamsProducer(
            StreamsConfig config,
            string threadId,
            IKafkaClientSupplier clientSupplier,
            TaskId? taskId,
            Guid? processId)
        {
            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            if (string.IsNullOrEmpty(threadId))
            {
                throw new ArgumentException("message", nameof(threadId));
            }

            if (clientSupplier is null)
            {
                throw new ArgumentNullException(nameof(clientSupplier));
            }

            if (taskId is null)
            {
                throw new ArgumentNullException(nameof(taskId));
            }

            this.clientSupplier = clientSupplier;

            // processingMode = StreamThread.processingMode(config);

            ProducerConfig producerConfigs;
            switch (this.processingMode)
            {
                case ProcessingMode.AT_LEAST_ONCE:
                    {
                        producerConfigs = config.GetProducerConfigs(StreamsBuilder.GetThreadProducerClientId(threadId));
                        this.eosBetaProducerConfigs = null;

                        break;
                    }
                case ProcessingMode.EXACTLY_ONCE_ALPHA:
                    {
                        producerConfigs = config.GetProducerConfigs(
                            StreamsBuilder.GetTaskProducerClientId(threadId, taskId));

                        String applicationId = config.ApplicationId;
                        producerConfigs.Set(StreamsConfig.TRANSACTIONAL_ID_CONFIGConfig, applicationId + "-" + taskId);

                        this.eosBetaProducerConfigs = null;

                        break;
                    }
                case ProcessingMode.EXACTLY_ONCE_BETA:
                    {
                        producerConfigs = config.GetProducerConfigs(StreamsBuilder.GetThreadProducerClientId(threadId));

                        String applicationId = config.ApplicationId;
                        producerConfigs.Set(
                            StreamsConfig.TRANSACTIONAL_ID_CONFIGConfig,
                            applicationId + "-" +
                                processId +
                                "-" + threadId.Split("-StreamThread-")[1]);

                        this.eosBetaProducerConfigs = producerConfigs;

                        break;
                    }
                default:
                    throw new ArgumentException("Unknown processing mode: " + this.processingMode);
            }

            this.producer = clientSupplier.GetProducer(new ProducerConfig(producerConfigs));
        }

        private string FormatException(string message)
        {
            return message; // + " [" + logPrefix + "]";
        }

        private bool EosEnabled()
        {
            return this.processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA ||
                this.processingMode == ProcessingMode.EXACTLY_ONCE_BETA;
        }

        /**
         * @throws InvalidOperationException if EOS is disabled
         */
        public void InitTransaction()
        {
            if (!this.EosEnabled())
            {
                throw new InvalidOperationException(this.FormatException("Exactly-once is not enabled"));
            }
            if (!this.transactionInitialized)
            {
                // initialize transactions if eos is turned on, which will block if the previous transaction has not
                // completed yet; do not start the first transaction until the topology has been initialized later
                try
                {
                    //this.producer.InitTransactions();
                    this.transactionInitialized = true;
                }
                catch (TimeoutException exception)
                {
                    this.log.LogWarning(
                        "Timeout exception caught trying to initialize transactions. " +
                            "The broker is either slow or in bad state (like not having enough replicas) in " +
                            "responding to the request, or the connection to broker was interrupted sending " +
                            "the request or receiving the response. " +
                            "Will retry initializing the task in the next loop. " +
                            "Consider overwriting {} to a larger value to avoid timeout errors",
                        StreamsConfig.MAX_BLOCK_MS_CONFIG);

                    throw;
                }
                catch (KafkaException exception)
                {
                    throw new StreamsException(
                        this.FormatException("Error encountered trying to initialize transactions"),
                        exception
                    );
                }
            }
        }

        public void ResetProducer()
        {
            if (this.processingMode != ProcessingMode.EXACTLY_ONCE_BETA)
            {
                throw new InvalidOperationException(this.FormatException("Exactly-once beta is not enabled"));
            }

            this.producer.Dispose();

            this.producer = this.clientSupplier.GetProducer(new ProducerConfig(this.eosBetaProducerConfigs));
            this.transactionInitialized = false;
        }

        private void MaybeBeginTransaction()
        {
            if (this.EosEnabled() && !this.transactionInFlight)
            {
                try
                {
                    //producer.BeginTransaction();
                    this.transactionInFlight = true;
                }
                catch (ProducerFencedException error)
                {
                    throw new TaskMigratedException(
                        this.FormatException("Producer got fenced trying to begin a new transaction"),
                        error);
                }

                catch (KafkaException error)
                {
                    throw new StreamsException(
                        this.FormatException("Error encountered trying to begin a new transaction"),
                        error
                    );
                }
            }
        }

        public Task<MessageMetadata> Send(DeliveryResult<byte[], byte[]> record, Action callback)
        {
            this.MaybeBeginTransaction();
            try
            {
                return null; //producer.Produce(record, callback);
            }
            catch (KafkaException uncaughtException)
            {
                if (IsRecoverable(uncaughtException))
                {
                    // producer.send() call may throw a KafkaException which wraps a FencedException,
                    // in this case we should throw its wrapped inner cause so that it can be
                    // captured and re-wrapped as TaskMigrationException
                    throw new TaskMigratedException(
                        this.FormatException("Producer got fenced trying to send a record"),
                        uncaughtException);
                }
                else
                {
                    throw new StreamsException(
                        this.FormatException(String.Format("Error encountered trying to send record to topic %s", record.Topic)),
                        uncaughtException
                    );
                }
            }
        }

        private static bool IsRecoverable(KafkaException uncaughtException)
        {
            return uncaughtException is ProducerFencedException;
        }

        /**
         * @throws InvalidOperationException if EOS is disabled
         * @throws TaskMigratedException
         */
        public void CommitTransaction(Dictionary<TopicPartition, OffsetAndMetadata> offsets,
               ConsumerGroupMetadata consumerGroupMetadata)
        {
            if (!this.EosEnabled())
            {
                throw new InvalidOperationException(this.FormatException("Exactly-once is not enabled"));
            }

            this.MaybeBeginTransaction();
            try
            {
                // producer.SendOffsetsToTransaction(offsets, consumerGroupMetadata);
                // producer.CommitTransaction();
                this.transactionInFlight = false;
            }
            catch (Exception error) when (error is ProducerFencedException || error is CommitFailedException)
            {
                throw new TaskMigratedException(
                    this.FormatException("Producer got fenced trying to commit a transaction"),
                    error);
            }
            catch (TimeoutException error)
            {
                // TODO KIP-447: we can consider treating it as non-fatal and retry on the thread level
                throw new StreamsException(this.FormatException("Timed out trying to commit a transaction"), error);
            }
            catch (KafkaException error)
            {
                throw new StreamsException(
                    this.FormatException("Error encountered trying to commit a transaction"),
                    error
                );
            }
        }

        /**
         * @throws InvalidOperationException if EOS is disabled
         */
        public void AbortTransaction()
        {
            if (!this.EosEnabled())
            {
                throw new InvalidOperationException(this.FormatException("Exactly-once is not enabled"));
            }

            if (this.transactionInFlight)
            {
                try
                {
                    this.producer.AbortTransaction(TimeSpan.FromMilliseconds(100.0));
                }
                catch (ProducerFencedException error)
                {
                    // The producer is aborting the txn when there's still an ongoing one,
                    // which means that we did not commit the task while closing it, which
                    // means that it is a dirty close. Therefore it is possible that the dirty
                    // close is due to an fenced exception already thrown previously, and hence
                    // when calling abortTxn here the same exception would be thrown again.
                    // Even if the dirty close was not due to an observed fencing exception but
                    // something else (e.g. task corrupted) we can still ignore the exception here
                    // since transaction already got aborted by brokers/transactional-coordinator if this happens
                    this.log.LogDebug("Encountered {} while aborting the transaction; this is expected and hence swallowed", error.Message);
                }
                catch (KafkaException error)
                {
                    throw new StreamsException(
                        this.FormatException("Error encounter trying to abort a transaction"),
                        error
                    );
                }
                this.transactionInFlight = false;
            }
        }

        // private List<PartitionInfo> PartitionsFor(String topic)
        // {
        //     return producer.PartitionsFor(topic);
        // }

        private void Flush()
        {
            this.producer.Flush();
        }

        public void Close()
        {
            this.producer.Dispose();
        }

        // for testing only
        private IProducer<byte[], byte[]> KafkaProducer()
        {
            return this.producer;
        }
    }
}
