using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Kafka.Streams.Processor.Internals
{
    public class StreamThread : IThread<StreamThreadStates>
    {
        private readonly object stateLock = new object();
        private static ILogger<StreamThread> log;
        private readonly ITime time;
        private readonly string logPrefix;
        private readonly TimeSpan pollTime;
        private readonly long commitTimeMs;
        private readonly int maxPollTimeMs;
        private readonly string originalReset;
        private readonly TaskManager taskManager;
        public int assignmentErrorCode { get; set; }
        private readonly StreamsMetricsImpl streamsMetrics;
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

        public StreamThread(
            ILogger<StreamThread> logger,
            ITime time,
            StreamsConfig config,
            IProducer<byte[], byte[]> producer,
            IConsumer<byte[], byte[]> restoreConsumer,
            IConsumer<byte[], byte[]> consumer,
            string originalReset,
            TaskManager taskManager,
            StreamsMetricsImpl streamsMetrics,
            InternalTopologyBuilder builder,
            string threadClientId,
            LogContext logContext,
            int assignmentErrorCode)
            : this(threadClientId)
        {
            this.stateLock = new object();
            this.standbyRecords = new Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>>();

            //this.streamsMetrics = streamsMetrics;
            //this.commitSensor = ThreadMetrics.commitSensor(streamsMetrics);
            //this.pollSensor = ThreadMetrics.pollSensor(streamsMetrics);
            //this.processSensor = ThreadMetrics.processSensor(streamsMetrics);
            //this.punctuateSensor = ThreadMetrics.punctuateSensor(streamsMetrics);

            // The following sensors are created here but their references are not stored in this object, since within
            // this object they are not recorded. The sensors are created here so that the stream threads starts with all
            // its metrics initialised. Otherwise, those sensors would have been created during processing, which could
            // lead to missing metrics. For instance, if no task were created, the metrics for created and closed
            // tasks would never be.Added to the metrics.
            //ThreadMetrics.createTaskSensor(streamsMetrics);
            //ThreadMetrics.closeTaskSensor(streamsMetrics);
            //ThreadMetrics.skipRecordSensor(streamsMetrics);
            //ThreadMetrics.commitOverTasksSensor(streamsMetrics);

            this.time = time;
            this.builder = builder;
            //this.logPrefix = logContext.logPrefix();
            StreamThread.log = logger;
            this.rebalanceListener = new RebalanceListener(time, taskManager, this, StreamThread.log);
            this.taskManager = taskManager;
            this.producer = producer;
            this.restoreConsumer = restoreConsumer;
            this.consumer = consumer;
            this.originalReset = originalReset;
            this.assignmentErrorCode = assignmentErrorCode;

            this.pollTime = TimeSpan.FromMilliseconds(config.getLong("poll.ms").Value);
            int dummyThreadIdx = 1;
            
            // this.maxPollTimeMs = new InternalConsumerConfig(config.GetMainConsumerConfigs("dummyGroupId", "dummyClientId", dummyThreadIdx))
                  // .getInt(StreamsConfigPropertyNames.MAX_POLL_INTERVAL_MS_CONFIG);
            
            this.commitTimeMs = config.getLong(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG) ?? 30000L;

            this.numIterations = 1;
        }

        public StreamThread(StreamThreadState state, StreamStateListener stateListener)
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

        public StreamThread(string threadClientId)
        {
            ThreadClientId = threadClientId;
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
            ILogger<StreamThread> log = StreamThread.log;

           // log.LogInformation("Creating restore consumer client");
            Dictionary<string, string> restoreConsumerConfigs = config.GetRestoreConsumerConfigs(getRestoreConsumerClientId(threadClientId));

            IConsumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(restoreConsumerConfigs);
            TimeSpan pollTime = TimeSpan.FromMilliseconds(config.getLong(StreamsConfigPropertyNames.POLL_MS_CONFIG) ?? 100L);
            StoreChangelogReader changelogReader = new StoreChangelogReader(restoreConsumer, pollTime, userStateRestoreListener, logContext);

            IProducer<byte[], byte[]> threadProducer = null;
            bool eosEnabled = StreamsConfigPropertyNames.ExactlyOnce.Equals(config.getString(StreamsConfigPropertyNames.PROCESSING_GUARANTEE_CONFIG));
            if (!eosEnabled)
            {
                Dictionary<string, string> producerConfigs = config.getProducerConfigs(getThreadProducerClientId(threadClientId));
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
            string applicationId = config.getString(StreamsConfigPropertyNames.ApplicationId) ?? throw new ArgumentNullException(StreamsConfigPropertyNames.ApplicationId);

            Dictionary<string, string> consumerConfigs = config.GetMainConsumerConfigs(applicationId, getConsumerClientId(threadClientId), threadId);
            // consumerConfigs.Add(InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);

            int assignmentErrorCode = 0;

            consumerConfigs.Add(InternalConfig.ASSIGNMENT_ERROR_CODE, assignmentErrorCode.ToString());
            string originalReset = "";

            if (!builder.latestResetTopicsPattern().IsMatch("") || !builder.earliestResetTopicsPattern().IsMatch(""))
            {
                originalReset = consumerConfigs["AutoOffsetReset"];
                consumerConfigs.Add("AutoOffsetReset", "none");
            }

            IConsumer<byte[], byte[]> consumer = clientSupplier.getConsumer(consumerConfigs);
            taskManager.setConsumer(consumer);

            return new StreamThread(
                log,
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

        public void updateThreadMetadata(Dictionary<TaskId, StreamTask> activeTasks,
                                          Dictionary<TaskId, StandbyTask> standbyTasks)
        {
            HashSet<string> producerClientIds = new HashSet<string>();
            HashSet<TaskMetadata> activeTasksMetadata = new HashSet<TaskMetadata>();
            foreach (var task in activeTasks)
            {
                activeTasksMetadata.Add(new TaskMetadata(task.Key.ToString(), task.Value.partitions));
                producerClientIds.Add(getTaskProducerClientId(this.Thread.Name, task.Key));
            }

            HashSet<TaskMetadata> standbyTasksMetadata = new HashSet<TaskMetadata>();
            foreach (var task in standbyTasks)
            {
                standbyTasksMetadata.Add(new TaskMetadata(task.Key.ToString(), task.Value.partitions));
            }

            string adminClientId = threadMetadata.adminClientId;

            threadMetadata = new ThreadMetadata(
                this.Thread.Name,
                this.State.CurrentState.ToString(),
                getConsumerClientId(this.Thread.Name),
                getRestoreConsumerClientId(this.Thread.Name),
                producer == null ? producerClientIds : new HashSet<string> { getThreadProducerClientId(this.Thread.Name) },
                adminClientId,
                activeTasksMetadata,
                standbyTasksMetadata);
        }

        // package-private for testing only
        StreamThread updateThreadMetadata(string adminClientId)
        {
            threadMetadata = new ThreadMetadata(
                this.Thread.Name,
                this.State.ToString(),
                getConsumerClientId(this.Thread.Name),
                getRestoreConsumerClientId(this.Thread.Name),
                producer == null ? new HashSet<string>() : new HashSet<string> { getThreadProducerClientId(this.Thread.Name) },
                adminClientId,
                new HashSet<TaskMetadata>(),
                new HashSet<TaskMetadata>());

            return this;
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
                return this.State.isRunning();
            }
        }

        /**
         * Execute the stream processors
         *
         * @throws KafkaException   for any Kafka-related exceptions
         * @throws RuntimeException for any other non-Kafka exceptions
         */

        public void run()
        {
            log.LogInformation("Starting");
            if (!this.State.setState(StreamThreadStates.STARTING))
            {
                log.LogInformation("StreamThread already shutdown. Not running");

                return;
            }

            bool cleanRun = false;
            try
            {
                runLoop();
                cleanRun = true;
            }
            catch (KafkaException e)
            {
                log.LogError("Encountered the following unexpected Kafka exception during processing, " +
                    "this usually indicate Streams internal errors:", e);
                throw e;
            }
            catch (Exception e)
            {
                // we have caught all Kafka related exceptions, and other runtime exceptions
                // should be due to user application errors
                log.LogError("Encountered the following LogError during processing:", e);
                throw e;
            }
            finally
            {
                completeShutdown(cleanRun);
            }
        }

        public void setRebalanceException(Exception rebalanceException)
        {
            this.rebalanceException = rebalanceException;
        }

        /**
         * Main event loop for polling, and processing records through topologies.
         *
         * @throws IllegalStateException If store gets registered after initialized is already finished
         * @throws StreamsException      if the store's change log does not contain the partition
         */
        private void runLoop()
        {
            consumer.Subscribe(builder.sourceTopicPattern().ToString());//, rebalanceListener);

            while (isRunning())
            {
                try
                {
                    runOnce();
                    if (assignmentErrorCode == (int)StreamsPartitionAssignor.Error.VERSION_PROBING)
                    {
                        log.LogInformation("Version probing detected. Triggering new rebalance.");
                        enforceRebalance();
                    }
                }
                catch (TaskMigratedException ignoreAndRejoinGroup)
                {
                    log.LogWarning("Detected task {} that got migrated to another thread. " +
                            "This implies that this thread missed a rebalance and dropped out of the consumer group. " +
                            "Will try to rejoin the consumer group. Below is the detailed description of the task:\n{}",
                        ignoreAndRejoinGroup.MigratedTask.id, ignoreAndRejoinGroup.MigratedTask.ToString(">"));

                    enforceRebalance();
                }
            }
        }

        private void enforceRebalance()
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
        void runOnce()
        {
            ConsumerRecords<byte[], byte[]> records;
            now = time.milliseconds();

            if (this.State.CurrentState == StreamThreadStates.PARTITIONS_ASSIGNED)
            {
                // try to fetch some records with zero poll millis
                // to unblock the restoration as soon as possible
                records = pollRequests(TimeSpan.Zero);
            }
            else if (this.State.CurrentState == StreamThreadStates.PARTITIONS_REVOKED)
            {
                // try to fetch some records with normal poll time
                // in order to wait long enough to get the join response
                records = pollRequests(pollTime);
            }
            else if (this.State.CurrentState == StreamThreadStates.RUNNING || this.State.CurrentState == StreamThreadStates.STARTING)
            {
                // try to fetch some records with normal poll time
                // in order to get long polling
                records = pollRequests(pollTime);
            }
            else
            {
                // any other state should not happen
                log.LogError("Unexpected state {} during normal iteration", this.State.CurrentState);
                throw new StreamsException(logPrefix + "Unexpected state " + this.State.CurrentState + " during normal iteration");
            }

            // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
            // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
            // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
            // could affect the task manager state beyond this point within #runOnce().
            if (!isRunning())
            {
                log.LogDebug("State already transits to {}, skipping the run once call after poll request", this.State.CurrentState);
                return;
            }

            long pollLatency = advanceNowAndComputeLatency();

            if (records != null && records.Any())
            {
                pollSensor.record(pollLatency, now);
                addRecordsToTasks(records);
            }

            // only try to initialize the assigned tasks
            // if the state is still in PARTITION_ASSIGNED after the poll call
            if (this.State.CurrentState == StreamThreadStates.PARTITIONS_ASSIGNED)
            {
                if (taskManager.updateNewAndRestoringTasks())
                {
                    this.State.setState(StreamThreadStates.RUNNING);
                }
            }

            advanceNowAndComputeLatency();

            // TODO: we will process some tasks even if the state is not RUNNING, i.e. some other
            // tasks are still being restored.
            if (taskManager.hasActiveRunningTasks())
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
                        processed = taskManager.process(now);

                        if (processed > 0)
                        {
                            long processLatency = advanceNowAndComputeLatency();
                            processSensor.record(processLatency / (double)processed, now);

                            // commit any tasks that have requested a commit
                            int committed = taskManager.maybeCommitActiveTasksPerUserRequested();

                            if (committed > 0)
                            {
                                long commitLatency = advanceNowAndComputeLatency();
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

                    if (maybePunctuate() || maybeCommit())
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
            maybeUpdateStandbyTasks();

            maybeCommit();
        }

        /**
         * Get the next batch of records by polling.
         *
         * @param pollTime how long to block in Consumer#poll
         * @return Next batch of records or null if no records available.
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private ConsumerRecords<byte[], byte[]> pollRequests(TimeSpan pollTime)
        {
            ConsumerRecords<byte[], byte[]> records = null;

            lastPollMs = now;

            try
            {
                records = consumer.Poll(pollTime);
            }
            catch (InvalidOffsetException e)
            {
                resetInvalidOffsets(e);
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

        private void resetInvalidOffsets(InvalidOffsetException e)
        {
            HashSet<TopicPartition> partitions = e.partitions();
            HashSet<string> loggedTopics = new HashSet<string>();
            HashSet<TopicPartition> seekToBeginning = new HashSet<TopicPartition>();
            HashSet<TopicPartition> seekToEnd = new HashSet<TopicPartition>();

            foreach (TopicPartition partition in partitions)
            {
                if (builder.earliestResetTopicsPattern().IsMatch(partition.Topic))
                {
                    addToResetList(partition, seekToBeginning, "Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
                }
                else if (builder.latestResetTopicsPattern().IsMatch(partition.Topic))
                {
                    addToResetList(partition, seekToEnd, "Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
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
                        addToResetList(partition, seekToBeginning, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                    }
                    else if (originalReset.Equals("latest"))
                    {
                        addToResetList(partition, seekToEnd, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
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

        private void addToResetList(TopicPartition partition, HashSet<TopicPartition> partitions, string logMessage, string resetPolicy, HashSet<string> loggedTopics)
        {
            string topic = partition.Topic;
            if (loggedTopics.Add(topic))
            {
                log.LogInformation(logMessage, topic, resetPolicy);
            }

            partitions.Add(partition);
        }

        /**
         * Take records and add them to each respective task
         *
         * @param records Records, can be null
         */
        private void addRecordsToTasks(ConsumerRecords<byte[], byte[]> records)
        {

            foreach (TopicPartition partition in records.Partitions)
            {
                StreamTask task = taskManager.activeTask(partition);

                if (task == null)
                {
                    log.LogError(
                        "Unable to locate active task for received-record partition {}. Current tasks: {}",
                        partition,
                        taskManager.ToString(">")
                    );
                    throw new NullReferenceException("Task was unexpectedly missing for partition " + partition);
                }
                else if (task.isClosed())
                {
                    log.LogInformation("Stream task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                 "Notifying the thread to trigger a new rebalance immediately.", task.id);
                    throw new TaskMigratedException(task);
                }

                task.addRecords(partition, records.GetRecords(partition));
            }
        }

        /**
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        private bool maybePunctuate()
        {
            int punctuated = taskManager.punctuate();
            if (punctuated > 0)
            {
                long punctuateLatency = advanceNowAndComputeLatency();
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
        bool maybeCommit()
        {
            int committed = 0;

            if (now - lastCommitMs > commitTimeMs)
            {
                if (log.IsEnabled(LogLevel.Trace))
                {
                    log.LogTrace("Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                        taskManager.activeTaskIds(), taskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);
                }

                committed += taskManager.commitAll();
                if (committed > 0)
                {
                    long intervalCommitLatency = advanceNowAndComputeLatency();
                    commitSensor.record(intervalCommitLatency / (double)committed, now);

                    // try to purge the committed records for repartition topics if possible
                    taskManager.maybePurgeCommitedRecords();

                    if (log.IsEnabled(LogLevel.Debug))
                    {
                        log.LogDebug("Committed all active tasks {} and standby tasks {} in {}ms",
                            taskManager.activeTaskIds(), taskManager.standbyTaskIds(), intervalCommitLatency);
                    }
                }

                lastCommitMs = now;
                processStandbyRecords = true;
            }
            else
            {
                int commitPerRequested = taskManager.maybeCommitActiveTasksPerUserRequested();
                if (commitPerRequested > 0)
                {
                    long requestCommitLatency = advanceNowAndComputeLatency();
                    commitSensor.record(requestCommitLatency / (double)committed, now);
                    committed += commitPerRequested;
                }
            }

            return committed > 0;
        }

        private void maybeUpdateStandbyTasks()
        {
            if (this.State.CurrentState == StreamThreadStates.RUNNING && taskManager.hasStandbyRunningTasks())
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
                                StandbyTask task = taskManager.standbyTask(partition);

                                if (task.isClosed())
                                {
                                    log.LogInformation("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
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

                        if (log.IsEnabled(LogLevel.Debug))
                        {
                            log.LogDebug($"Updated standby tasks {taskManager.standbyTaskIds()} in {time.milliseconds() - now} ms");
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
                            StandbyTask task = taskManager.standbyTask(partition);

                            if (task == null)
                            {
                                throw new StreamsException(logPrefix + "Missing standby task for partition " + partition);
                            }

                            if (task.isClosed())
                            {
                                log.LogInformation("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
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
                    log.LogWarning("Updating StandbyTasks failed. Deleting StandbyTasks stores to recreate from scratch.", recoverableException);
                    HashSet<TopicPartition> partitions = recoverableException.partitions();
                    foreach (TopicPartition partition in partitions)
                    {
                        StandbyTask task = taskManager.standbyTask(partition);

                        if (task.isClosed())
                        {
                            log.LogInformation("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                "Notifying the thread to trigger a new rebalance immediately.", task.id);
                            throw new TaskMigratedException(task);
                        }

                        log.LogInformation("Reinitializing StandbyTask {} from changelogs {}", task, recoverableException.partitions());
                        task.reinitializeStateStoresForPartitions(recoverableException.partitions().ToList());
                    }

                    restoreConsumer.SeekToBeginning(partitions);
                }

                // update now if the standby restoration indeed executed
                advanceNowAndComputeLatency();
            }
        }

        /**
         * Compute the latency based on the current marked timestamp, and update the marked timestamp
         * with the current system timestamp.
         *
         * @return latency
         */
        private long advanceNowAndComputeLatency()
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
        public void shutdown()
        {
            log.LogInformation("Informed to shut down");
            var oldState = this.State.CurrentState;
            this.State.setState(StreamThreadStates.PENDING_SHUTDOWN);

            if (oldState == StreamThreadStates.CREATED)
            {
                // The thread may not have been started. Take responsibility for shutting down
                completeShutdown(true);
            }
        }

        private void completeShutdown(bool cleanRun)
        {
            // set the state to pending shutdown first as it may be called due to LogError;
            // its state may already be PENDING_SHUTDOWN so it will return false but we
            // intentionally do not check the returned flag
            this.State.setState(StreamThreadStates.PENDING_SHUTDOWN);

            log.LogInformation("Shutting down");

            try
            {
                taskManager.shutdown(cleanRun);
            }
            catch (Exception e)
            {
                log.LogError("Failed to close task manager due to the following LogError:", e);
            }
            try
            {
                consumer.Close();
            }
            catch (Exception e)
            {
                log.LogError("Failed to close consumer due to the following LogError:", e);
            }
            try
            {
                restoreConsumer.Close();
            }
            catch (Exception e)
            {
                log.LogError("Failed to close restore consumer due to the following LogError:", e);
            }

            streamsMetrics.removeAllThreadLevelSensors();

            this.State.setState(StreamThreadStates.DEAD);
            log.LogInformation("Shutdown complete");
        }

        public void clearStandbyRecords()
        {
            standbyRecords.Clear();
        }

        public Dictionary<TaskId, StreamTask> tasks()
        {
            return taskManager.activeTasks();
        }

        /**
         * Produces a string representation containing useful information about a StreamThread.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the StreamThread instance.
         */

        public string toString()
        {
            return toString("");
        }

        /**
         * Produces a string representation containing useful information about a StreamThread, starting with the given indent.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the StreamThread instance.
         */
        public string toString(string indent)
        {
            return indent + "\tStreamsThread threadId: " + this.Thread.Name + "\n" + taskManager.ToString(indent);
        }

        public Dictionary<MetricName, IMetric> producerMetrics()
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
                foreach (StreamTask task in taskManager.activeTasks().Values)
                {
                    //Dictionary<MetricName, IMetric> taskProducerMetrics = task.getProducer().metrics;
                    //result.putAll(taskProducerMetrics);
                }
            }
            return result;
        }

        public Dictionary<MetricName, IMetric> consumerMetrics()
        {
            // Dictionary<MetricName, IMetric> consumerMetrics = consumer.metrics();
            // Dictionary<MetricName, IMetric> restoreConsumerMetrics = restoreConsumer.metrics();
            Dictionary<MetricName, IMetric> result = new Dictionary<MetricName, IMetric>();

            // result.putAll(consumerMetrics);
            // result.putAll(restoreConsumerMetrics);
            return result;
        }

        public Dictionary<MetricName, IMetric> adminClientMetrics()
        {
            //            Dictionary<MetricName, IMetric> adminClientMetrics = taskManager.getAdminClient().metrics();
            Dictionary<MetricName, IMetric> result = new Dictionary<MetricName, IMetric>();
            //          result.putAll(adminClientMetrics);
            return result;
        }

        // the following are for testing only
        void setNow(long now)
        {
            this.now = now;
        }

        int currentNumIterations()
        {
            return numIterations;
        }
    }
}