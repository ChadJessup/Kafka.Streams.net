using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Kafka.Streams.Processor.Internals
{
    public class StreamThread : IThread<StreamThreadStates>
    {
        private readonly object stateLock = new object();

        public StreamThread(
            StreamThreadState state,
            StreamStateListener stateListener)
        {
            this.State = state;
            this.StateListener = stateListener;

            this.State.setTransitions(new List<StateTransition<StreamThreadStates>>
            {
                new StateTransition<StreamThreadStates>(StreamThreadStates.CREATED, 1, 5),
                new StateTransition<StreamThreadStates>(StreamThreadStates.STARTING, 2, 5),
                new StateTransition<StreamThreadStates>(StreamThreadStates.PARTITIONS_REVOKED, 3, 5),
                new StateTransition<StreamThreadStates>(StreamThreadStates.PARTITIONS_ASSIGNED, 2, 4, 5),
                new StateTransition<StreamThreadStates>(StreamThreadStates.RUNNING, 2, 5),
                new StateTransition<StreamThreadStates>(StreamThreadStates.PENDING_SHUTDOWN, 6),
                new StateTransition<StreamThreadStates>(StreamThreadStates.DEAD),
            });
        }

        public IStateListener StateListener { get; private set; }
        public IStateMachine<StreamThreadStates> State { get; }
        public Thread Thread { get; }
        public string ThreadClientId { get; }

        internal static StreamThread create(
            InternalTopologyBuilder builder,
            StreamsConfig config,
            IKafkaClientSupplier clientSupplier,
            IAdminClient adminClient,
            Guid processId,
            string clientId,
            MetricsRegistry metrics,
            ITime time,
            StreamsMetadataState streamsMetadataState,
            long cacheSizeBytes,
            StateDirectory stateDirectory, 
            IStateRestoreListener userStateRestoreListener,
            int threadId)
        {
            string threadClientId = $"{clientId}-StreamThread-{threadId}";

            string logPrefix = string.Format("stream-thread [%s] ", threadClientId);
            LogContext logContext = new LogContext(logPrefix);
            ILogger log = logContext.logger(typeof(StreamThread));

            log.LogInformation("Creating restore consumer client");
            Dictionary<string, object> restoreConsumerConfigs = config.GetRestoreConsumerConfigs(getRestoreConsumerClientId(threadClientId));

            IConsumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(restoreConsumerConfigs);
            TimeSpan pollTime = TimeSpan.FromMilliseconds(config.getLong(StreamsConfigPropertyNames.POLL_MS_CONFIG) ?? 100L);
            StoreChangelogReader changelogReader = new StoreChangelogReader(restoreConsumer, pollTime, userStateRestoreListener, logContext);

            IProducer<byte[], byte[]> threadProducer = null;
            bool eosEnabled = StreamsConfigPropertyNames.ExactlyOnce.Equals(config.getString(StreamsConfigPropertyNames.PROCESSING_GUARANTEE_CONFIG));
            if (!eosEnabled)
            {
                Dictionary<string, object> producerConfigs = config.getProducerConfigs(getThreadProducerClientId(threadClientId));
                log.LogInformation("Creating shared producer client");

                threadProducer = clientSupplier.getProducer(producerConfigs);
            }

            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, threadClientId);

            ThreadCache cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);

            AbstractTaskCreator<StreamTask> activeTaskCreator = new TaskCreator(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                changelogReader,
                cache,
                time,
                clientSupplier,
                threadProducer,
                threadClientId,
                log);

            AbstractTaskCreator<StandbyTask> standbyTaskCreator = new StandbyTaskCreator(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                changelogReader,
                time,
                log);

            TaskManager taskManager = new TaskManager(
                changelogReader,
                processId,
                logPrefix,
                restoreConsumer,
                streamsMetadataState,
                activeTaskCreator,
                standbyTaskCreator,
                adminClient,
                    new AssignedStreamsTasks(logContext),
                    new AssignedStandbyTasks(logContext));

            log.LogInformation("Creating consumer client");
            string applicationId = config.getString(StreamsConfigPropertyNames.ApplicationId);
            Dictionary<string, object> consumerConfigs = config.GetMainConsumerConfigs(applicationId, getConsumerClientId(threadClientId), threadId);
            consumerConfigs.Add(InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
            int assignmentErrorCode = 0;

            consumerConfigs.Add(InternalConfig.ASSIGNMENT_ERROR_CODE, assignmentErrorCode);
            string originalReset = null;

            if (!builder.latestResetTopicsPattern().IsMatch("") || !builder.earliestResetTopicsPattern().IsMatch(""))
            {
                originalReset = (string)consumerConfigs["AutoOffsetReset"];
                consumerConfigs.Add("AutoOffsetReset", "none");
            }

            IConsumer<byte[], byte[]> consumer = clientSupplier.getConsumer(consumerConfigs);
            taskManager.setConsumer(consumer);

            return StreamThread.create(
                time,
                config,
                threadProducer,
                restoreConsumer,
                consumer,
                originalReset,
                taskManager,
                streamsMetrics,
                builder,
                threadClientId,
                logContext,
                assignmentErrorCode)
                .updateThreadMetadata(getSharedAdminClientId(clientId));
        }

        public static string getTaskProducerClientId(string threadClientId, TaskId taskId)
            => $"{threadClientId}-{taskId}-producer";

        private static string getThreadProducerClientId(string threadClientId)
            => $"{threadClientId}-producer";

        private static string getConsumerClientId(string threadClientId)
            => $"{threadClientId}-consumer";

        private static string getRestoreConsumerClientId(string threadClientId)
            => $"{threadClientId}-restore-consumer";

        // currently admin client is shared among all threads
        public static string getSharedAdminClientId(string clientId)
            => $"{clientId}-admin";

        /**
         * Set the {@link StreamThread.StateListener} to be notified when state changes. Note this API is internal to
         * Kafka Streams and is not intended to be used by an external application.
         */
        public void setStateListener(IStateListener listener)
            => this.StateListener = listener;

        public bool isRunningAndNotRebalancing()
        {
            // we do not need to grab stateLock since it is a single read
            return this.State.CurrentState == StreamThreadStates.RUNNING;
        }

        public bool isRunning()
        {
            lock (stateLock)
            {
                return this.stillRunning();
            }
        }

        public bool stillRunning()
        {
            throw new NotImplementedException();
        }
    }
}