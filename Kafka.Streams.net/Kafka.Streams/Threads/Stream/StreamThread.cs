using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Extensions;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Kafka.Streams.Threads.Stream
{
    public class StreamThread : IStreamThread
    {
        private readonly ILogger<StreamThread> logger;
        private readonly IDisposable logPrefix;

        private readonly StreamsConfig config;

        private static int threadId = 1;

        private readonly object stateLock = new object();
        private readonly IServiceProvider services;
        private readonly IClock clock;
        private readonly TimeSpan pollTime;
        private readonly long commitTimeMs;
        private readonly int maxPollTimeMs;
        private readonly AutoOffsetReset? originalReset;

        public int AssignmentErrorCode { get; set; }

        private long now;
        private long lastPollMs;
        private long lastCommitMs;
        private int numIterations;
        private Exception? rebalanceException = null;
        private bool processStandbyRecords = false;
        public ThreadMetadata ThreadMetadata { get; private set; }
        private Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> standbyRecords =
            new Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>>();

        public ITaskManager TaskManager { get; }
        public IConsumerRebalanceListener RebalanceListener { get; }
        public IConsumer<byte[], byte[]> Consumer { get; }
        readonly IProducer<byte[], byte[]>? producer;
        public RestoreConsumer RestoreConsumer { get; }
        readonly InternalTopologyBuilder builder;
        public IStateListener StateListener { get; private set; }

        public string ThreadClientId { get; }
        public Thread Thread { get; }
        public void Join() => this.Thread?.Join();
        public int ManagedThreadId => this.Thread.ManagedThreadId;
        public IStateMachine<StreamThreadStates> State { get; }

        public StreamThread(
            IServiceProvider services,
            IClock clock,
            ILogger<StreamThread> logger,
            ILoggerFactory loggerFactory,
            StreamsConfig config,
            IStateMachine<StreamThreadStates> states,
            IKafkaClientSupplier clientSupplier,
            IStateRestoreListener userStateRestoreListener,
            RestoreConsumer restoreConsumer,
            StateDirectory stateDirectory,
            StreamsMetadataState streamsMetadataState,
            Topology topology)
        {
            this.services = services ?? throw new ArgumentNullException(nameof(services));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.State = states ?? throw new ArgumentNullException(nameof(states));
            this.builder = topology?.internalTopologyBuilder ?? throw new ArgumentNullException(nameof(topology));

            this.clock = clock;

            var clientId = config.ClientId;
            var threadClientId = $"{clientId}-KafkaStreamThread-{threadId++}";

            this.Thread = new Thread(Run)
            {
                Name = threadClientId
            };

            this.pollTime = TimeSpan.FromMilliseconds(this.config.PollMs);
            this.commitTimeMs = this.config.CommitIntervalMs;
            this.numIterations = 1;

            this.logPrefix = this.logger.BeginScope($"stream-thread [{threadClientId}] ");

            //this.time = time;
            //this.consumer = consumer;
            //this.originalReset = originalReset;
            //this.AssignmentErrorCode = assignmentErrorCode;

            //int dummyThreadIdx = 1;

            //this.maxPollTimeMs = new InternalConsumerConfig(config.GetMainConsumerConfigs("dummyGroupId", "dummyClientId", dummyThreadIdx))
            // .GetInt(configPropertyNames.MAX_POLL_INTERVAL_MS_CONFIG);

            //this.RestoreConsumer = this.CreateRestoreConsumer(
            //    config,
            //    clientSupplier,
            //    userStateRestoreListener,
            //    threadClientId);

            var changelogReader = new StoreChangelogReader(
                loggerFactory.CreateLogger<StoreChangelogReader>(),
                config,
                RestoreConsumer,
                userStateRestoreListener);

            this.producer = this.CreateProducer(config, clientSupplier, threadClientId);

            ThreadCache? cache = null; // new ThreadCache(logger, cacheSizeBytes);

            //AbstractTaskCreator<StreamTask> activeTaskCreator = new TaskCreator(
            //    loggerFactory.CreateLogger<TaskCreator>(),
            //    builder,
            //    config,
            //    stateDirectory,
            //    changelogReader,
            //    cache,
            //    clock,
            //    clientSupplier,
            //    this.producer,
            //    threadClientId);

            //AbstractTaskCreator<StandbyTask> standbyTaskCreator = new StandbyTaskCreator(
            //    loggerFactory.CreateLogger<StandbyTaskCreator>(),
            //    loggerFactory,
            //    builder,
            //    config,
            //    stateDirectory,
            //    changelogReader,
            //    clock);

            this.TaskManager = ActivatorUtilities.GetServiceOrCreateInstance<ITaskManager>(this.services);
            this.TaskManager.SetThreadClientId(threadClientId);

            //this.TaskManager = new TaskManager(
            //    loggerFactory,
            //    loggerFactory.CreateLogger<TaskManager>(),
            //    changelogReader,
            //    //processId,
            //    RestoreConsumer,
            //    streamsMetadataState,
            //    activeTaskCreator,
            //    standbyTaskCreator,
            //    clientSupplier.GetAdminClient(config.GetAdminConfigs(clientId)),
            //    new AssignedStreamsTasks(loggerFactory.CreateLogger<AssignedStreamsTasks>()),
            //    new AssignedStandbyTasks(loggerFactory.CreateLogger<AssignedStandbyTasks>()));

            this.RebalanceListener = new StreamsRebalanceListener(clock, this.TaskManager, this, this.logger);
            this.Consumer = this.CreateConsumerClient(config, clientSupplier, threadClientId, TaskManager, this.RebalanceListener);

            (this.State as StreamThreadState)?.SetTaskManager(this.TaskManager);
            (this.State as StreamThreadState)?.SetThread(this);

            this.UpdateThreadMetadata(StreamsBuilder.GetSharedAdminClientId(clientId));
        }

        private IProducer<byte[], byte[]>? CreateProducer(StreamsConfig config, IKafkaClientSupplier clientSupplier, string threadClientId)
        {
            IProducer<byte[], byte[]>? threadProducer = null;

            var eosEnabled = StreamsConfigPropertyNames.ExactlyOnce.Equals(config.GetString(StreamsConfigPropertyNames.ProcessingGuarantee));
            if (!eosEnabled)
            {
                var producerConfigs = config.GetProducerConfigs(StreamsBuilder.GetThreadProducerClientId(threadClientId));
                this.logger.LogInformation("Creating shared producer client");

                threadProducer = clientSupplier.GetProducer(producerConfigs);
            }

            return threadProducer;
        }

        private IConsumer<byte[], byte[]> CreateConsumerClient(StreamsConfig config, IKafkaClientSupplier clientSupplier, string threadClientId, ITaskManager taskManager, IConsumerRebalanceListener rebalanceListener)
        {
            this.logger.LogInformation("Creating consumer client");

            var applicationId = config.ApplicationId ?? throw new ArgumentNullException(StreamsConfigPropertyNames.ApplicationId);

            var consumerConfigs = config.GetMainConsumerConfigs(applicationId, StreamsBuilder.GetConsumerClientId(threadClientId), threadId);
            // consumerConfigs.Add(InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, this.taskManager);

            var assignmentErrorCode = 0;

            // consumerConfigs.Set(InternalConfig.ASSIGNMENT_ERROR_CODE, assignmentErrorCode.ToString());
            AutoOffsetReset? originalReset = null;

            if (!builder.LatestResetTopicsPattern().IsMatch("") || !builder.EarliestResetTopicsPattern().IsMatch(""))
            {
                originalReset = consumerConfigs.AutoOffsetReset;
                consumerConfigs.AutoOffsetReset = null;
            }

            var consumer = clientSupplier.GetConsumer(consumerConfigs, rebalanceListener);

            taskManager.SetConsumer(consumer);

            return consumer;
        }

        public void UpdateThreadMetadata(
            Dictionary<TaskId, StreamTask> activeTasks,
            Dictionary<TaskId, StandbyTask> standbyTasks)
        {
            var producerClientIds = new HashSet<string>();
            var activeTasksMetadata = new HashSet<TaskMetadata>();

            foreach (var task in activeTasks ?? Enumerable.Empty<KeyValuePair<TaskId, StreamTask>>())
            {
                activeTasksMetadata.Add(new TaskMetadata(task.Key.ToString(), task.Value.partitions));
                producerClientIds.Add(StreamsBuilder.GetTaskProducerClientId(this.Thread.Name, task.Key));
            }

            var standbyTasksMetadata = new HashSet<TaskMetadata>();

            foreach (var task in standbyTasks ?? Enumerable.Empty<KeyValuePair<TaskId, StandbyTask>>())
            {
                standbyTasksMetadata.Add(new TaskMetadata(task.Key.ToString(), task.Value.partitions));
            }

            var adminClientId = this.ThreadMetadata.AdminClientId;

            this.ThreadMetadata = new ThreadMetadata(
                this.Thread.Name,
                this.State.CurrentState.ToString(),
                StreamsBuilder.GetConsumerClientId(this.Thread.Name),
                StreamsBuilder.GetRestoreConsumerClientId(this.Thread.Name),
                this.producer == null
                    ? producerClientIds
                    : new HashSet<string> { StreamsBuilder.GetThreadProducerClientId(this.Thread.Name) },
                adminClientId,
                activeTasksMetadata,
                standbyTasksMetadata);
        }

        public void SetStateListener(IStateListener stateListener)
        {
            this.StateListener = stateListener;
            this.State.SetStateListener(this.StateListener);
        }

        private StreamThread UpdateThreadMetadata(string adminClientId)
        {
            this.ThreadMetadata = new ThreadMetadata(
                this.Thread.Name,
                this.State.ToString(),
                StreamsBuilder.GetConsumerClientId(this.Thread.Name),
                StreamsBuilder.GetRestoreConsumerClientId(this.Thread.Name),
                this.producer == null
                    ? new HashSet<string>()
                    : new HashSet<string> { StreamsBuilder.GetThreadProducerClientId(this.Thread.Name) },
                adminClientId,
                new HashSet<TaskMetadata>(),
                new HashSet<TaskMetadata>());

            return this;
        }

        public bool IsRunningAndNotRebalancing()
        {
            // we do not need to grab stateLock since it is a single read
            return this.State.CurrentState == StreamThreadStates.RUNNING;
        }

        public bool IsRunning()
        {
            lock (stateLock)
            {
                var isRunning = this.State.IsRunning();
                return isRunning;
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
            this.logger.LogInformation("Starting");

            if (!this.State.SetState(StreamThreadStates.STARTING))
            {
                this.logger.LogInformation("KafkaStreamThread already shutdown. Not running");

                return;
            }

            var cleanRun = false;
            try
            {
                RunLoop();
                cleanRun = true;
            }
            catch (KafkaException e)
            {
                this.logger.LogError(e, "Encountered the following unexpected Kafka exception during processing, " +
                    "this usually indicate Streams internal errors");

                throw;
            }
            catch (Exception e)
            {
                // we have caught all Kafka related exceptions, and other runtime exceptions
                // should be due to user application errors
                this.logger.LogError(e, "Encountered the following LogError during processing");

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
            var sourceTopicPattern = builder.SourceTopicPattern().ToString();
            this.Consumer.Subscribe(sourceTopicPattern);//, rebalanceListener);

            SpinWait.SpinUntil(() => this.Consumer.Assignment.Any(), TimeSpan.FromSeconds(1.0));

            while (IsRunning())
            {
                try
                {
                    RunOnce();
                    if (AssignmentErrorCode == (int)StreamsPartitionAssignor.Error.VERSION_PROBING)
                    {
                        this.logger.LogInformation("Version probing detected. Triggering new rebalance.");
                        EnforceRebalance();
                    }
                }
                catch (TaskMigratedException ignoreAndRejoinGroup)
                {
                    this.logger.LogWarning($"Detected task {ignoreAndRejoinGroup.MigratedTask.id} that got migrated to another thread. " +
                            "This implies that this thread missed a rebalance and dropped out of the consumer group. " +
                            $"Will try to rejoin the consumer group. Below is the detailed description of the task:\n{ignoreAndRejoinGroup.MigratedTask.ToString(">")}");

                    EnforceRebalance();
                }
            }
        }

        private void EnforceRebalance()
        {
            Consumer.Unsubscribe();
            Consumer.Subscribe(builder.SourceTopicPattern().ToString());//, rebalanceListener);
        }

        // @throws IllegalStateException If store gets registered after initialized is already finished
        // @throws StreamsException      If the store's change log does not contain the partition
        // @throws TaskMigratedException If another thread wrote to the changelog topic that is currently restored
        //                               or if committing offsets failed (non-EOS)
        //                               or if the task producer got fenced (EOS)
        public void RunOnce()
        {
            ConsumerRecords<byte[], byte[]>? records;
            now = SystemClock.AsEpochMilliseconds;

            if (this.State.CurrentState == StreamThreadStates.PARTITIONS_ASSIGNED)
            {
                // try to fetch some records with zero poll millis
                // to unblock the restoration as soon as possible
                records = PollRequests(TimeSpan.Zero);
            }
            else if (this.State.CurrentState == StreamThreadStates.PARTITIONS_REVOKED)
            {
                // try to fetch some records with normal poll time
                // in order to wait long enough to get the join response
                records = PollRequests(pollTime);
            }
            else if (this.State.CurrentState == StreamThreadStates.RUNNING
                || this.State.CurrentState == StreamThreadStates.STARTING)
            {
                // try to fetch some records with normal poll time
                // in order to get long polling
                records = PollRequests(pollTime);

                if (records?.Any() == true)
                {

                }
            }
            else
            {
                // any other state should not happen
                this.logger.LogError($"Unexpected state {this.State.CurrentState} during normal iteration");

                throw new StreamsException(logPrefix + "Unexpected state " + this.State.CurrentState + " during normal iteration");
            }

            // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
            // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
            // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
            // could affect the task manager state beyond this point within #runOnce().
            if (!IsRunning())
            {
                this.logger.LogDebug($"State already transits to {this.State.CurrentState}, skipping the run once call after poll request");

                return;
            }

            var pollLatency = AdvanceNowAndComputeLatency();

            if (records != null && records.Any())
            {
                // pollSensor.record(pollLatency, now);
                AddRecordsToTasks(records);
            }

            // only try to initialize the assigned tasks
            // if the state is still in PARTITION_ASSIGNED after the poll call
            if (this.State.CurrentState == StreamThreadStates.PARTITIONS_ASSIGNED)
            {
                if (this.TaskManager.UpdateNewAndRestoringTasks())
                {
                    this.State.SetState(StreamThreadStates.RUNNING);
                }
            }

            AdvanceNowAndComputeLatency();

            // TODO: we will process some tasks even if the state is not RUNNING, i.e. some other
            // tasks are still being restored.
            if (this.TaskManager.HasActiveRunningTasks())
            {
                // Within an iteration, after N (N initialized as 1 upon start up) round of processing one-record-each on the applicable tasks, check the current time:
                // 1. If it is time to commit, do it;
                // 2. If it is time to punctuate, do it;
                // 3. If elapsed time is close to consumer's max.poll.interval.ms, end the current iteration immediately.
                // 4. If none of the the above happens, increment N.
                // 5. If one of the above happens, half the value of N.
                var processed = 0;
                var timeSinceLastPoll = 0L;

                do
                {
                    for (var i = 0; i < numIterations; i++)
                    {
                        processed = this.TaskManager.Process(now);

                        if (processed > 0)
                        {
                            var processLatency = AdvanceNowAndComputeLatency();
                            // processSensor.record(processLatency / (double)processed, now);

                            // commit any tasks that have requested a commit
                            var committed = this.TaskManager.MaybeCommitActiveTasksPerUserRequested();

                            if (committed > 0)
                            {
                                var commitLatency = AdvanceNowAndComputeLatency();
                                // commitSensor.record(commitLatency / (double)committed, now);
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
                        numIterations = numIterations > 1
                            ? numIterations / 2
                            : numIterations;
                    }
                    else if (timeSinceLastPoll > maxPollTimeMs / 2)
                    {
                        numIterations = numIterations > 1
                            ? numIterations / 2
                            : numIterations;

                        break;
                    }
                    else if (processed > 0)
                    {
                        numIterations++;
                    }
                }
                while (processed > 0);
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
        private ConsumerRecords<byte[], byte[]>? PollRequests(TimeSpan pollTime)
        {
            ConsumerRecords<byte[], byte[]>? records = null;

            lastPollMs = now;

            try
            {
                records = Consumer.Poll(pollTime);
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
            HashSet<TopicPartition> partitions = e.Partitions();
            var loggedTopics = new HashSet<string>();
            var seekToBeginning = new HashSet<TopicPartition>();
            var seekToEnd = new HashSet<TopicPartition>();

            foreach (TopicPartition partition in partitions)
            {
                if (builder.EarliestResetTopicsPattern().IsMatch(partition.Topic))
                {
                    AddToResetList(partition, seekToBeginning, "Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
                }
                else if (builder.LatestResetTopicsPattern().IsMatch(partition.Topic))
                {
                    AddToResetList(partition, seekToEnd, "Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
                }
                else
                {
                    if (originalReset == null || (!originalReset.Equals("earliest") && !originalReset.Equals("latest")))
                    {
                        var errorMessage = "No valid committed offset found for input topic %s (partition %s) and no valid reset policy configured." +
                            " You need to set configuration parameter \"auto.offset.reset\" or specify a topic specific reset " +
                            "policy via StreamsBuilder#stream(..., Consumed.With(Topology.AutoOffsetReset)) or StreamsBuilder#table(..., Consumed.With(Topology.AutoOffsetReset))";

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
                Consumer.SeekToBeginning(seekToBeginning);
            }

            if (seekToEnd.Any())
            {
                Consumer.SeekToEnd(seekToEnd);
            }
        }

        private void AddToResetList(
            TopicPartition partition,
            HashSet<TopicPartition> partitions,
            string logMessage,
            string resetPolicy,
            HashSet<string> loggedTopics)
        {
            var topic = partition.Topic;

            if (loggedTopics.Add(topic))
            {
                this.logger.LogInformation(logMessage, topic, resetPolicy);
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
                StreamTask task = this.TaskManager.ActiveTask(partition);

                if (task == null)
                {
                    this.logger.LogError(
                        $"Unable to locate active task for received-record partition {partition}. Current tasks: {this.TaskManager.ToString(">")}");
                    throw new NullReferenceException("Task was unexpectedly missing for partition " + partition);
                }
                else if (task.IsClosed())
                {
                    this.logger.LogInformation($"Stream task {task.id} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                 "Notifying the thread to trigger a new rebalance immediately.");

                    throw new TaskMigratedException(task);
                }

                task.AddRecords(partition, records.GetRecords(partition));
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private bool MaybePunctuate()
        {
            var punctuated = this.TaskManager.Punctuate();
            if (punctuated > 0)
            {
                var punctuateLatency = AdvanceNowAndComputeLatency();
                // punctuateSensor.record(punctuateLatency / (double)punctuated, now);
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
        public bool MaybeCommit()
        {
            var committed = 0;

            if (now - lastCommitMs > commitTimeMs)
            {
                if (this.logger.IsEnabled(LogLevel.Trace))
                {
                    this.logger.LogTrace($"Committing all active tasks {this.TaskManager.ActiveTaskIds().ToJoinedString()} and standby tasks {this.TaskManager.StandbyTaskIds().ToJoinedString()} since {now - lastCommitMs}ms has elapsed (commit interval is {commitTimeMs}ms)");
                }

                committed += this.TaskManager.CommitAll();
                if (committed > 0)
                {
                    var intervalCommitLatency = AdvanceNowAndComputeLatency();
                    // commitSensor.record(intervalCommitLatency / (double)committed, now);

                    // try to purge the committed records for repartition topics if possible
                    this.TaskManager.MaybePurgeCommitedRecords();

                    if (this.logger.IsEnabled(LogLevel.Debug))
                    {
                        this.logger.LogDebug($"Committed all active tasks {this.TaskManager.ActiveTaskIds()} and standby tasks {this.TaskManager.StandbyTaskIds()} in {intervalCommitLatency} ms");
                    }
                }

                lastCommitMs = now;
                processStandbyRecords = true;
            }
            else
            {
                var commitPerRequested = this.TaskManager.MaybeCommitActiveTasksPerUserRequested();
                if (commitPerRequested > 0)
                {
                    var requestCommitLatency = AdvanceNowAndComputeLatency();
                    // commitSensor.record(requestCommitLatency / (double)committed, now);
                    committed += commitPerRequested;
                }
            }

            return committed > 0;
        }

        private void MaybeUpdateStandbyTasks()
        {
            if (this.State.CurrentState == StreamThreadStates.RUNNING
                && this.TaskManager.HasStandbyRunningTasks())
            {
                if (processStandbyRecords)
                {
                    if (standbyRecords.Any())
                    {
                        var remainingStandbyRecords = new Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>>();

                        foreach (var entry in standbyRecords)
                        {
                            TopicPartition partition = entry.Key;
                            List<ConsumeResult<byte[], byte[]>> remaining = entry.Value;
                            if (remaining != null)
                            {
                                StandbyTask task = this.TaskManager.StandbyTask(partition);

                                if (task.IsClosed())
                                {
                                    this.logger.LogInformation($"Standby task {task.id} is already closed," +
                                        $"probably because it got unexpectedly migrated to another thread already. " +
                                        "Notifying the thread to trigger a new rebalance immediately.");

                                    throw new TaskMigratedException(task);
                                }

                                remaining = task.Update(partition, remaining);
                                if (remaining.Any())
                                {
                                    remainingStandbyRecords.Add(partition, remaining);
                                }
                                else
                                {
                                    RestoreConsumer.Resume(new[] { partition });
                                }
                            }
                        }

                        standbyRecords = remainingStandbyRecords;

                        if (this.logger.IsEnabled(LogLevel.Debug))
                        {
                            this.logger.LogDebug($"Updated standby tasks {this.TaskManager.StandbyTaskIds().ToJoinedString()} in {clock.NowAsEpochMilliseconds - now} ms");
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
                    ConsumerRecords<byte[], byte[]> records = RestoreConsumer.Poll(TimeSpan.Zero);

                    if (records.Any())
                    {
                        foreach (TopicPartition partition in records.Partitions)
                        {
                            StandbyTask task = this.TaskManager.StandbyTask(partition);

                            if (task == null)
                            {
                                throw new StreamsException(logPrefix + "Missing standby task for partition " + partition);
                            }

                            if (task.IsClosed())
                            {
                                this.logger.LogInformation("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                    "Notifying the thread to trigger a new rebalance immediately.", task.id);

                                throw new TaskMigratedException(task);
                            }

                            List<ConsumeResult<byte[], byte[]>> remaining = task.Update(partition, records.GetRecords(partition));
                            if (remaining.Any())
                            {
                                RestoreConsumer.Pause(new[] { partition });
                                standbyRecords.Add(partition, remaining);
                            }
                        }
                    }
                }
                catch (InvalidOffsetException recoverableException)
                {
                    this.logger.LogWarning(recoverableException, "Updating StandbyTasks failed. Deleting StandbyTasks stores to recreate from scratch.");

                    HashSet<TopicPartition> partitions = recoverableException.Partitions();
                    foreach (TopicPartition partition in partitions)
                    {
                        StandbyTask task = this.TaskManager.StandbyTask(partition);

                        if (task.IsClosed())
                        {
                            this.logger.LogInformation($"Standby task {task.id} is already closed, probably because it got " +
                                $"unexpectedly migrated to another thread already. " +
                                "Notifying the thread to trigger a new rebalance immediately.");

                            throw new TaskMigratedException(task);
                        }

                        this.logger.LogInformation($"Reinitializing StandbyTask {task} from changelogs " +
                            $"{recoverableException.Partitions().ToJoinedString()}");

                        task.ReinitializeStateStoresForPartitions(recoverableException.Partitions().ToList());
                    }

                    RestoreConsumer.SeekToBeginning(partitions);
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
            var previous = now;
            now = SystemClock.AsEpochMilliseconds;

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
            this.logger.LogInformation("Informed to shut down");
            var oldState = this.State.CurrentState;
            this.State.SetState(StreamThreadStates.PENDING_SHUTDOWN);

            if (oldState == StreamThreadStates.CREATED)
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
            this.State.SetState(StreamThreadStates.PENDING_SHUTDOWN);

            this.logger.LogInformation("Shutting down");

            try
            {
                this.TaskManager.Shutdown(cleanRun);
            }
            catch (Exception e)
            {
                this.logger.LogError(e, "Failed to close task manager due to the following LogError");
            }

            try
            {
                Consumer.Close();
            }
            catch (Exception e)
            {
                this.logger.LogError(e, "Failed to close consumer due to the following LogError");
            }

            try
            {
                RestoreConsumer.Close();
            }
            catch (Exception e)
            {
                this.logger.LogError(e, "Failed to close restore consumer due to the following LogError");
            }

            this.State.SetState(StreamThreadStates.DEAD);
            this.logger.LogInformation("Shutdown complete");
        }

        public void ClearStandbyRecords()
        {
            standbyRecords.Clear();
        }

        public Dictionary<TaskId, StreamTask> Tasks()
        {
            return this.TaskManager.ActiveTasks();
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
            return indent + "\tStreamsThread threadId: " + this.Thread.Name + "\n" + this.TaskManager.ToString(indent);
        }

        // the following are for testing only
        public void SetNow(long now)
        {
            this.now = now;
        }

        public void Start()
        {
            this.Thread.Start();
        }

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    this.producer?.Dispose();
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
    }
}