using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Clients;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Processors.Internals.Metrics;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Kafka.Streams.Threads.KafkaStream
{
    public class KafkaStreamThread : IKafkaStreamThread
    {
        private readonly object stateLock = new object();
        private readonly IServiceProvider services;
        private readonly ITime time;
        private readonly string logPrefix;
        private readonly TimeSpan pollTime;
        private readonly long commitTimeMs;
        private readonly int maxPollTimeMs;
        private readonly AutoOffsetReset? originalReset;

        public int AssignmentErrorCode { get; set; }
        private readonly Sensor commitSensor;
        private readonly Sensor pollSensor;
        private readonly Sensor punctuateSensor;
        private readonly Sensor processSensor;

        private long now;
        private long lastPollMs;
        private long lastCommitMs;
        private int numIterations;
        private Exception rebalanceException = null;
        private bool processStandbyRecords = false;
        private volatile ThreadMetadata threadMetadata;
        private Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> standbyRecords;

        // package-private for testing
        readonly IConsumerRebalanceListener rebalanceListener;
        readonly IProducer<byte[], byte[]> producer;
        readonly IConsumer<byte[], byte[]> restoreConsumer;
        readonly IConsumer<byte[], byte[]> consumer;
        readonly InternalTopologyBuilder builder;

        public KafkaStreamThread(
            ITime time,
            IProducer<byte[], byte[]> producer,
            IConsumer<byte[], byte[]> restoreConsumer,
            IConsumer<byte[], byte[]> consumer,
            AutoOffsetReset? originalReset,
            string threadClientId,
            int assignmentErrorCode)
        {
            this.standbyRecords = new Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>>();

            this.time = time;
            this.rebalanceListener = new RebalanceListener(time, this.Context.TaskManager, this, this.Logger);
            this.producer = producer;
            this.restoreConsumer = restoreConsumer;
            this.consumer = consumer;
            this.originalReset = originalReset;
            this.AssignmentErrorCode = assignmentErrorCode;

            this.pollTime = TimeSpan.FromMilliseconds(this.Context.StreamsConfig.PollMs);
            int dummyThreadIdx = 1;

            // this.maxPollTimeMs = new InternalConsumerConfig(config.GetMainConsumerConfigs("dummyGroupId", "dummyClientId", dummyThreadIdx))
            // .getInt(StreamsConfigPropertyNames.MAX_POLL_INTERVAL_MS_CONFIG);

            this.commitTimeMs = this.Context.StreamsConfig.getLong(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG) ?? 30000L;

            this.numIterations = 1;
        }

        public KafkaStreamThread(ILogger<KafkaStreamThread> logger, StreamsConfig config)
        {
            this.Logger = logger;
            this.Config = config;
            this.Thread = new Thread(Run);
        }

        public IStateListener StateListener { get; private set; }
        
        public string ThreadClientId { get; }
        public ILogger<KafkaStreamThread> Logger { get; }
        public StreamsConfig Config { get; }
        public IThreadContext<IThread<KafkaStreamThreadStates>, IStateMachine<KafkaStreamThreadStates>, KafkaStreamThreadStates> Context { get; private set; }
        public Thread Thread { get; }
        public int ManagedThreadId => this.Thread.ManagedThreadId;

        public void Initialize(
            ILogger<KafkaStreamThread> logger,
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
            string threadClientId = $"{clientId}-KafkaStreamThread-{threadId}";

            string logPrefix = $"stream-thread [{threadClientId}] ";
            LogContext logContext = new LogContext(logPrefix);

            this.Logger.LogInformation("Creating restore consumer client");
            var restoreCustomerId = this.Context.GetRestoreConsumerClientId(threadClientId);
            var restoreConsumerConfigs = config.GetRestoreConsumerConfigs(restoreCustomerId);

            IConsumer<byte[], byte[]> restoreConsumer = clientSupplier.GetRestoreConsumer(restoreConsumerConfigs);
            TimeSpan pollTime = TimeSpan.FromMilliseconds(config.PollMs);
            StoreChangelogReader changelogReader = new StoreChangelogReader(restoreConsumer, pollTime, userStateRestoreListener, logContext);

            IProducer<byte[], byte[]> threadProducer = null;
            bool eosEnabled = StreamsConfigPropertyNames.ExactlyOnce.Equals(config.getString(StreamsConfigPropertyNames.PROCESSING_GUARANTEE_CONFIG));
            if (!eosEnabled)
            {
                var producerConfigs = config.getProducerConfigs(this.Context.GetThreadProducerClientId(threadClientId));
                this.Logger.LogInformation("Creating shared producer client");

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
                logger);

            AbstractTaskCreator<StandbyTask> standbyTaskCreator = new StandbyTaskCreator(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                changelogReader,
                time,
                logger);

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

            this.Logger.LogInformation("Creating consumer client");
            string applicationId = config.getString(StreamsConfigPropertyNames.ApplicationId) ?? throw new ArgumentNullException(StreamsConfigPropertyNames.ApplicationId);

            var consumerConfigs = config.GetMainConsumerConfigs(applicationId, this.Context.GetConsumerClientId(threadClientId), threadId);
            // consumerConfigs.Add(InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, this.Context.TaskManager);

            int assignmentErrorCode = 0;

            // consumerConfigs.Set(InternalConfig.ASSIGNMENT_ERROR_CODE, assignmentErrorCode.ToString());
            AutoOffsetReset? originalReset = null;

            if (!builder.latestResetTopicsPattern().IsMatch("") || !builder.earliestResetTopicsPattern().IsMatch(""))
            {
                originalReset = consumerConfigs.AutoOffsetReset;
                consumerConfigs.AutoOffsetReset = null;
            }

            IConsumer<byte[], byte[]> consumer = clientSupplier.getConsumer(consumerConfigs);
            taskManager.setConsumer(consumer);

            this.Thread = new Thread(Start);

            this.UpdateThreadMetadata(this.Context.GetSharedAdminClientId(clientId));
        }
        public void SetContext(IThreadContext<IThread<KafkaStreamThreadStates>, IStateMachine<KafkaStreamThreadStates>, KafkaStreamThreadStates> context)
            => this.Context = context ?? throw new ArgumentNullException(nameof(context));

        public void UpdateThreadMetadata(
            Dictionary<TaskId, StreamTask> activeTasks,
            Dictionary<TaskId, StandbyTask> standbyTasks)
        {
            var producerClientIds = new HashSet<string>();
            var activeTasksMetadata = new HashSet<TaskMetadata>();

            foreach (var task in activeTasks ?? Enumerable.Empty<KeyValuePair<TaskId, StreamTask>>())
            {
                activeTasksMetadata.Add(new TaskMetadata(task.Key.ToString(), task.Value.partitions));
                producerClientIds.Add(this.Context.GetTaskProducerClientId(this.Thread.Name, task.Key));
            }

            var standbyTasksMetadata = new HashSet<TaskMetadata>();

            foreach (var task in standbyTasks ?? Enumerable.Empty<KeyValuePair<TaskId, StandbyTask>>())
            {
                standbyTasksMetadata.Add(new TaskMetadata(task.Key.ToString(), task.Value.partitions));
            }

            string adminClientId = threadMetadata.AdminClientId;

            threadMetadata = new ThreadMetadata(
                this.Thread.Name,
                this.Context.State.CurrentState.ToString(),
                this.Context.GetConsumerClientId(this.Thread.Name),
                this.Context.GetRestoreConsumerClientId(this.Thread.Name),
                producer == null ? producerClientIds : new HashSet<string> { this.Context.GetThreadProducerClientId(this.Thread.Name) },
                adminClientId,
                activeTasksMetadata,
                standbyTasksMetadata);
        }

        // package-private for testing only
        KafkaStreamThread UpdateThreadMetadata(string adminClientId)
        {
            threadMetadata = new ThreadMetadata(
                this.Thread.Name,
                this.Context.State.ToString(),
                this.Context.GetConsumerClientId(this.Thread.Name),
                this.Context.GetRestoreConsumerClientId(this.Thread.Name),
                producer == null ? new HashSet<string>() : new HashSet<string> { this.Context.GetThreadProducerClientId(this.Thread.Name) },
                adminClientId,
                new HashSet<TaskMetadata>(),
                new HashSet<TaskMetadata>());

            return this;
        }

        public bool IsRunningAndNotRebalancing()
        {
            // we do not need to grab stateLock since it is a single read
            return this.Context.State.CurrentState == KafkaStreamThreadStates.RUNNING;
        }

        public bool isRunning()
        {
            lock (stateLock)
            {
                return this.Context.State.IsRunning();
            }
        }

        /**
         * Execute the stream processors
         *
         * @throws KafkaException   for any Kafka-related exceptions
         * @throws RuntimeException for any other non-Kafka exceptions
         */

        public void Run()
        {
            this.Logger.LogInformation("Starting");

            if (!this.Context.State.SetState(KafkaStreamThreadStates.STARTING))
            {
                this.Logger.LogInformation("KafkaStreamThread already shutdown. Not running");

                return;
            }

            bool cleanRun = false;
            try
            {
                RunLoop();
                cleanRun = true;
            }
            catch (KafkaException e)
            {
                this.Logger.LogError("Encountered the following unexpected Kafka exception during processing, " +
                    "this usually indicate Streams internal errors:", e);
                
                throw;
            }
            catch (Exception e)
            {
                // we have caught all Kafka related exceptions, and other runtime exceptions
                // should be due to user application errors
                this.Logger.LogError("Encountered the following LogError during processing:", e);

                throw;
            }
            finally
            {
                CompleteShutdown(cleanRun);
            }
        }

        public void SetRebalanceException(Exception rebalanceException)
        {
            this.rebalanceException = rebalanceException;
        }

        /**
         * Main event loop for polling, and processing records through topologies.
         *
         * @throws IllegalStateException If store gets registered after initialized is already finished
         * @throws StreamsException      if the store's change log does not contain the partition
         */
        private void RunLoop()
        {
            consumer.Subscribe(builder.sourceTopicPattern().ToString());//, rebalanceListener);

            while (isRunning())
            {
                try
                {
                    RunOnce();
                    if (AssignmentErrorCode == (int)StreamsPartitionAssignor.Error.VERSION_PROBING)
                    {
                        this.Logger.LogInformation("Version probing detected. Triggering new rebalance.");
                        EnforceRebalance();
                    }
                }
                catch (TaskMigratedException ignoreAndRejoinGroup)
                {
                    this.Logger.LogWarning($"Detected task {ignoreAndRejoinGroup.MigratedTask.id} that got migrated to another thread. " +
                            "This implies that this thread missed a rebalance and dropped out of the consumer group. " +
                            $"Will try to rejoin the consumer group. Below is the detailed description of the task:\n{ignoreAndRejoinGroup.MigratedTask.ToString(">")}");

                    EnforceRebalance();
                }
            }
        }

        private void EnforceRebalance()
        {
            consumer.Unsubscribe();
            consumer.Subscribe(builder.sourceTopicPattern().ToString());//, rebalanceListener);
        }

        /**
         * @throws IllegalStateException If store gets registered after initialized is already finished
         * @throws StreamsException      If the store's change log does not contain the partition
         * @throws TaskMigratedException If another thread wrote to the changelog topic that is currently restored
         *                               or if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        // Visible for testing
        void RunOnce()
        {
            ConsumerRecords<byte[], byte[]> records;
            now = time.milliseconds();

            if (this.Context.State.CurrentState == KafkaStreamThreadStates.PARTITIONS_ASSIGNED)
            {
                // try to fetch some records with zero poll millis
                // to unblock the restoration as soon as possible
                records = PollRequests(TimeSpan.Zero);
            }
            else if (this.Context.State.CurrentState == KafkaStreamThreadStates.PARTITIONS_REVOKED)
            {
                // try to fetch some records with normal poll time
                // in order to wait long enough to get the join response
                records = PollRequests(pollTime);
            }
            else if (this.Context.State.CurrentState == KafkaStreamThreadStates.RUNNING || this.Context.State.CurrentState == KafkaStreamThreadStates.STARTING)
            {
                // try to fetch some records with normal poll time
                // in order to get long polling
                records = PollRequests(pollTime);
            }
            else
            {
                // any other state should not happen
                this.Logger.LogError("Unexpected state {} during normal iteration", this.Context.State.CurrentState);
                throw new StreamsException(logPrefix + "Unexpected state " + this.Context.State.CurrentState + " during normal iteration");
            }

            // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
            // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
            // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
            // could affect the task manager state beyond this point within #runOnce().
            if (!isRunning())
            {
                this.Logger.LogDebug("State already transits to {}, skipping the run once call after poll request", this.Context.State.CurrentState);
                return;
            }

            long pollLatency = AdvanceNowAndComputeLatency();

            if (records != null && records.Any())
            {
                pollSensor.record(pollLatency, now);
                AddRecordsToTasks(records);
            }

            // only try to initialize the assigned tasks
            // if the state is still in PARTITION_ASSIGNED after the poll call
            if (this.Context.State.CurrentState == KafkaStreamThreadStates.PARTITIONS_ASSIGNED)
            {
                if (this.Context.TaskManager.updateNewAndRestoringTasks())
                {
                    this.Context.State.SetState(KafkaStreamThreadStates.RUNNING);
                }
            }

            AdvanceNowAndComputeLatency();

            // TODO: we will process some tasks even if the state is not RUNNING, i.e. some other
            // tasks are still being restored.
            if (this.Context.TaskManager.hasActiveRunningTasks())
            {
                /*
                 * Within an iteration, after N (N initialized as 1 upon start up) round of processing one-record-each on the applicable tasks, check the current time:
                 *  1. If it is time to commit, do it;
                 *  2. If it is time to punctuate, do it;
                 *  3. If elapsed time is close to consumer's max.poll.interval.ms, end the current iteration immediately.
                 *  4. If none of the the above happens, increment N.
                 *  5. If one of the above happens, half the value of N.
                 */
                int processed = 0;
                long timeSinceLastPoll = 0L;

                do
                {
                    for (int i = 0; i < numIterations; i++)
                    {
                        processed = this.Context.TaskManager.process(now);

                        if (processed > 0)
                        {
                            long processLatency = AdvanceNowAndComputeLatency();
                            processSensor.record(processLatency / (double)processed, now);

                            // commit any tasks that have requested a commit
                            int committed = this.Context.TaskManager.maybeCommitActiveTasksPerUserRequested();

                            if (committed > 0)
                            {
                                long commitLatency = AdvanceNowAndComputeLatency();
                                commitSensor.record(commitLatency / (double)committed, now);
                            }
                        }
                        else
                        {
                            // if there is no records to be processed, exit immediately
                            break;
                        }
                    }

                    timeSinceLastPoll = Math.Max(now - lastPollMs, 0);

                    if (MaybePunctuate() || MaybeCommit())
                    {
                        numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                    }
                    else if (timeSinceLastPoll > maxPollTimeMs / 2)
                    {
                        numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                        break;
                    }
                    else if (processed > 0)
                    {
                        numIterations++;
                    }
                } while (processed > 0);
            }

            // update standby tasks and maybe commit the standby tasks as well
            MaybeUpdateStandbyTasks();

            MaybeCommit();
        }

        /**
         * Get the next batch of records by polling.
         *
         * @param pollTime how long to block in Consumer#poll
         * @return Next batch of records or null if no records available.
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private ConsumerRecords<byte[], byte[]> PollRequests(TimeSpan pollTime)
        {
            ConsumerRecords<byte[], byte[]> records = null;

            lastPollMs = now;

            try
            {
                records = consumer.Poll(pollTime);
            }
            catch (InvalidOffsetException e)
            {
                ResetInvalidOffsets(e);
            }

            if (rebalanceException != null)
            {
                if (rebalanceException is TaskMigratedException)
                {
                    throw (TaskMigratedException)rebalanceException;
                }
                else
                {
                    throw new StreamsException(logPrefix + "Failed to rebalance.", rebalanceException);
                }
            }

            return records;
        }

        private void ResetInvalidOffsets(InvalidOffsetException e)
        {
            HashSet<TopicPartition> partitions = e.partitions();
            HashSet<string> loggedTopics = new HashSet<string>();
            HashSet<TopicPartition> seekToBeginning = new HashSet<TopicPartition>();
            HashSet<TopicPartition> seekToEnd = new HashSet<TopicPartition>();

            foreach (TopicPartition partition in partitions)
            {
                if (builder.earliestResetTopicsPattern().IsMatch(partition.Topic))
                {
                    AddToResetList(partition, seekToBeginning, "Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
                }
                else if (builder.latestResetTopicsPattern().IsMatch(partition.Topic))
                {
                    AddToResetList(partition, seekToEnd, "Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
                }
                else
                {
                    if (originalReset == null || (!originalReset.Equals("earliest") && !originalReset.Equals("latest")))
                    {
                        string errorMessage = "No valid committed offset found for input topic %s (partition %s) and no valid reset policy configured." +
                            " You need to set configuration parameter \"auto.offset.reset\" or specify a topic specific reset " +
                            "policy via StreamsBuilder#stream(..., Consumed.with(Topology.AutoOffsetReset)) or StreamsBuilder#table(..., Consumed.with(Topology.AutoOffsetReset))";
                        throw new StreamsException(string.Format(errorMessage, partition.Topic, partition.Partition), e);
                    }

                    if (originalReset.Equals("earliest"))
                    {
                        AddToResetList(partition, seekToBeginning, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                    }
                    else if (originalReset.Equals("latest"))
                    {
                        AddToResetList(partition, seekToEnd, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
                    }
                }
            }

            if (seekToBeginning.Any())
            {
                consumer.SeekToBeginning(seekToBeginning);
            }

            if (seekToEnd.Any())
            {
                consumer.SeekToEnd(seekToEnd);
            }
        }

        private void AddToResetList(TopicPartition partition, HashSet<TopicPartition> partitions, string logMessage, string resetPolicy, HashSet<string> loggedTopics)
        {
            string topic = partition.Topic;

            if (loggedTopics.Add(topic))
            {
                this.Logger.LogInformation(logMessage, topic, resetPolicy);
            }

            partitions.Add(partition);
        }

        /**
         * Take records and add them to each respective task
         *
         * @param records Records, can be null
         */
        private void AddRecordsToTasks(ConsumerRecords<byte[], byte[]> records)
        {
            foreach (TopicPartition partition in records.Partitions)
            {
                StreamTask task = this.Context.TaskManager.activeTask(partition);

                if (task == null)
                {
                    this.Logger.LogError(
                        "Unable to locate active task for received-record partition {}. Current tasks: {}",
                        partition,
                        this.Context.TaskManager.ToString(">")
                    );
                    throw new NullReferenceException("Task was unexpectedly missing for partition " + partition);
                }
                else if (task.isClosed())
                {
                    this.Logger.LogInformation("Stream task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                 "Notifying the thread to trigger a new rebalance immediately.", task.id);
                    throw new TaskMigratedException(task);
                }

                task.addRecords(partition, records.GetRecords(partition));
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private bool MaybePunctuate()
        {
            int punctuated = this.Context.TaskManager.punctuate();
            if (punctuated > 0)
            {
                long punctuateLatency = AdvanceNowAndComputeLatency();
                punctuateSensor.record(punctuateLatency / (double)punctuated, now);
            }

            return punctuated > 0;
        }

        /**
         * Try to commit all active tasks owned by this thread.
         *
         * Visible for testing.
         *
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        bool MaybeCommit()
        {
            int committed = 0;

            if (now - lastCommitMs > commitTimeMs)
            {
                if (this.Logger.IsEnabled(LogLevel.Trace))
                {
                    this.Logger.LogTrace("Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                        this.Context.TaskManager.activeTaskIds(), this.Context.TaskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);
                }

                committed += this.Context.TaskManager.commitAll();
                if (committed > 0)
                {
                    long intervalCommitLatency = AdvanceNowAndComputeLatency();
                    commitSensor.record(intervalCommitLatency / (double)committed, now);

                    // try to purge the committed records for repartition topics if possible
                    this.Context.TaskManager.maybePurgeCommitedRecords();

                    if (this.Logger.IsEnabled(LogLevel.Debug))
                    {
                        this.Logger.LogDebug("Committed all active tasks {} and standby tasks {} in {}ms",
                            this.Context.TaskManager.activeTaskIds(), this.Context.TaskManager.standbyTaskIds(), intervalCommitLatency);
                    }
                }

                lastCommitMs = now;
                processStandbyRecords = true;
            }
            else
            {
                int commitPerRequested = this.Context.TaskManager.maybeCommitActiveTasksPerUserRequested();
                if (commitPerRequested > 0)
                {
                    long requestCommitLatency = AdvanceNowAndComputeLatency();
                    commitSensor.record(requestCommitLatency / (double)committed, now);
                    committed += commitPerRequested;
                }
            }

            return committed > 0;
        }

        private void MaybeUpdateStandbyTasks()
        {
            if (this.Context.State.CurrentState == KafkaStreamThreadStates.RUNNING && this.Context.TaskManager.hasStandbyRunningTasks())
            {
                if (processStandbyRecords)
                {
                    if (standbyRecords.Any())
                    {
                        Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> remainingStandbyRecords = new Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>>();

                        foreach (var entry in standbyRecords)
                        {
                            TopicPartition partition = entry.Key;
                            List<ConsumeResult<byte[], byte[]>> remaining = entry.Value;
                            if (remaining != null)
                            {
                                StandbyTask task = this.Context.TaskManager.standbyTask(partition);

                                if (task.isClosed())
                                {
                                    this.Logger.LogInformation("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                        "Notifying the thread to trigger a new rebalance immediately.", task.id);
                                    throw new TaskMigratedException(task);
                                }

                                remaining = task.update(partition, remaining);
                                if (remaining.Any())
                                {
                                    remainingStandbyRecords.Add(partition, remaining);
                                }
                                else
                                {
                                    restoreConsumer.Resume(new[] { partition });
                                }
                            }
                        }

                        standbyRecords = remainingStandbyRecords;

                        if (this.Logger.IsEnabled(LogLevel.Debug))
                        {
                            this.Logger.LogDebug($"Updated standby tasks {this.Context.TaskManager.standbyTaskIds()} in {time.milliseconds() - now} ms");
                        }
                    }
                    processStandbyRecords = false;
                }

                try
                {
                    // poll(0): Since this is during the normal processing, not during restoration.
                    // We can afford to have slower restore (because we don't wait inside poll for results).
                    // Instead, we want to proceed to the next iteration to call the main consumer#poll()
                    // as soon as possible so as to not be kicked out of the group.
                    ConsumerRecords<byte[], byte[]> records = restoreConsumer.Poll(TimeSpan.Zero);

                    if (records.Any())
                    {
                        foreach (TopicPartition partition in records.Partitions)
                        {
                            StandbyTask task = this.Context.TaskManager.standbyTask(partition);

                            if (task == null)
                            {
                                throw new StreamsException(logPrefix + "Missing standby task for partition " + partition);
                            }

                            if (task.isClosed())
                            {
                                this.Logger.LogInformation("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                    "Notifying the thread to trigger a new rebalance immediately.", task.id);
                                throw new TaskMigratedException(task);
                            }

                            List<ConsumeResult<byte[], byte[]>> remaining = task.update(partition, records.GetRecords(partition));
                            if (remaining.Any())
                            {
                                restoreConsumer.Pause(new[] { partition });
                                standbyRecords.Add(partition, remaining);
                            }
                        }
                    }
                }
                catch (InvalidOffsetException recoverableException)
                {
                    this.Logger.LogWarning("Updating StandbyTasks failed. Deleting StandbyTasks stores to recreate from scratch.", recoverableException);
                    HashSet<TopicPartition> partitions = recoverableException.partitions();
                    foreach (TopicPartition partition in partitions)
                    {
                        StandbyTask task = this.Context.TaskManager.standbyTask(partition);

                        if (task.isClosed())
                        {
                            this.Logger.LogInformation("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                "Notifying the thread to trigger a new rebalance immediately.", task.id);
                            throw new TaskMigratedException(task);
                        }

                        this.Logger.LogInformation("Reinitializing StandbyTask {} from changelogs {}", task, recoverableException.partitions());
                        task.reinitializeStateStoresForPartitions(recoverableException.partitions().ToList());
                    }

                    restoreConsumer.SeekToBeginning(partitions);
                }

                // update now if the standby restoration indeed executed
                AdvanceNowAndComputeLatency();
            }
        }

        /**
         * Compute the latency based on the current marked timestamp, and update the marked timestamp
         * with the current system timestamp.
         *
         * @return latency
         */
        private long AdvanceNowAndComputeLatency()
        {
            long previous = now;
            now = time.milliseconds();

            return Math.Max(now - previous, 0);
        }

        /**
         * Shutdown this stream thread.
         * <p>
         * Note that there is nothing to prevent this function from being called multiple times
         * (e.g., in testing), hence the state is set only the first time
         */
        public void Shutdown()
        {
            this.Logger.LogInformation("Informed to shut down");
            var oldState = this.Context.State.CurrentState;
            this.Context.State.SetState(KafkaStreamThreadStates.PENDING_SHUTDOWN);

            if (oldState == KafkaStreamThreadStates.CREATED)
            {
                // The thread may not have been started. Take responsibility for shutting down
                CompleteShutdown(true);
            }
        }

        private void CompleteShutdown(bool cleanRun)
        {
            // set the state to pending shutdown first as it may be called due to LogError;
            // its state may already be PENDING_SHUTDOWN so it will return false but we
            // intentionally do not check the returned flag
            this.Context.State.SetState(KafkaStreamThreadStates.PENDING_SHUTDOWN);

            this.Logger.LogInformation("Shutting down");

            try
            {
                this.Context.TaskManager.shutdown(cleanRun);
            }
            catch (Exception e)
            {
                this.Logger.LogError("Failed to close task manager due to the following LogError:", e);
            }

            try
            {
                consumer.Close();
            }
            catch (Exception e)
            {
                this.Logger.LogError("Failed to close consumer due to the following LogError:", e);
            }

            try
            {
                restoreConsumer.Close();
            }
            catch (Exception e)
            {
                this.Logger.LogError("Failed to close restore consumer due to the following LogError:", e);
            }

            this.Context.State.SetState(KafkaStreamThreadStates.DEAD);
            this.Logger.LogInformation("Shutdown complete");
        }

        public void ClearStandbyRecords()
        {
            standbyRecords.Clear();
        }

        public Dictionary<TaskId, StreamTask> Tasks()
        {
            return this.Context.TaskManager.activeTasks();
        }

        /**
         * Produces a string representation containing useful information about a KafkaStreamThread.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the KafkaStreamThread instance.
         */

        public override string ToString()
        {
            return ToString("");
        }

        /**
         * Produces a string representation containing useful information about a KafkaStreamThread, starting with the given indent.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the KafkaStreamThread instance.
         */
        public string ToString(string indent)
        {
            return indent + "\tStreamsThread threadId: " + this.Thread.Name + "\n" + this.Context.TaskManager.ToString(indent);
        }

        public Dictionary<MetricName, IMetric> ProducerMetrics()
        {
            Dictionary<MetricName, IMetric> result = new Dictionary<MetricName, IMetric>();
            if (producer != null)
            {
                Dictionary<MetricName, IMetric> producerMetrics = new Dictionary<MetricName, IMetric>(); // producer.metrics();

                //if (producerMetrics != null)
                //{
                //    result.putAll(producerMetrics);
                //}
            }
            else
            {
                // When EOS is turned on, each task will have its own producer client
                // and the producer object passed in here will be null. We would then iterate through
                // all the active tasks and add their metrics to the output metrics map.
                foreach (StreamTask task in this.Context.TaskManager.activeTasks().Values)
                {
                    //Dictionary<MetricName, IMetric> taskProducerMetrics = task.getProducer().metrics;
                    //result.putAll(taskProducerMetrics);
                }
            }
            return result;
        }

        public Dictionary<MetricName, IMetric> ConsumerMetrics()
        {
            // Dictionary<MetricName, IMetric> consumerMetrics = consumer.metrics();
            // Dictionary<MetricName, IMetric> restoreConsumerMetrics = restoreConsumer.metrics();
            Dictionary<MetricName, IMetric> result = new Dictionary<MetricName, IMetric>();

            // result.putAll(consumerMetrics);
            // result.putAll(restoreConsumerMetrics);
            return result;
        }

        public Dictionary<MetricName, IMetric> AdminClientMetrics()
        {
            //            Dictionary<MetricName, IMetric> adminClientMetrics = this.Context.TaskManager.getAdminClient().metrics();
            Dictionary<MetricName, IMetric> result = new Dictionary<MetricName, IMetric>();
            //          result.putAll(adminClientMetrics);
            return result;
        }

        // the following are for testing only
        void SetNow(long now)
        {
            this.now = now;
        }

        int CurrentNumIterations()
        {
            return numIterations;
        }

        public void Start()
        {
            this.Thread.Start();
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
        // ~KafkaStreamThread()
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

        bool IThread.IsRunning()
        {
            throw new NotImplementedException();
        }

        void IDisposable.Dispose()
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}