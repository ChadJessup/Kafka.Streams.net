﻿using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Kafka.Streams.Processors;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Processor.Internals
{
    public class TaskCreator : AbstractTaskCreator<StreamTask>
    {
        private ThreadCache cache;
        private IKafkaClientSupplier clientSupplier;
        private string threadClientId;
        private IProducer<byte[], byte[]> threadProducer;
        private Sensor createTaskSensor;

        public TaskCreator(
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StreamsMetricsImpl streamsMetrics,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            ThreadCache cache,
            ITime time,
            IKafkaClientSupplier clientSupplier,
            IProducer<byte[], byte[]> threadProducer,
            string threadClientId,
            ILogger log)
            : base(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                storeChangelogReader,
                time,
                log)
        {
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadProducer = threadProducer;
            this.threadClientId = threadClientId;
            // createTaskSensor = ThreadMetrics.createTaskSensor(streamsMetrics);
        }

        public override StreamTask createTask(
            IConsumer<byte[], byte[]> consumer,
            TaskId taskId,
            HashSet<TopicPartition> partitions)
        {
            createTaskSensor.record();

            return new StreamTask(
                taskId,
                new List<TopicPartition>(partitions),
                builder.build(taskId.topicGroupId),
                consumer,
                storeChangelogReader,
                config,
                streamsMetrics,
                stateDirectory,
                cache,
                time,
                new BasicProducerSupplier(taskId, threadClientId, threadProducer, config, applicationId, clientSupplier));
        }

        public override void close()
        {
            if (threadProducer != null)
            {
                try
                {
                    threadProducer?.Dispose();
                }
                catch (Throwable e)
                {
                    log.LogError("Failed to close producer due to the following error:", e);
                }
            }
        }

        private ITime time;
        private ILogger log;
        private string logPrefix;
        private object stateLock;
        private TimeSpan pollTime;
        private long commitTimeMs;
        private int maxPollTimeMs;
        private string originalReset;
        private TaskManager taskManager;
        private int assignmentErrorCode = 0;

        private StreamsMetricsImpl streamsMetrics;
        private Sensor commitSensor;
        private Sensor pollSensor;
        private Sensor punctuateSensor;
        private Sensor processSensor;

        private long now;
        private long lastPollMs;
        private long lastCommitMs;
        private int numIterations;
        private Throwable rebalanceException = null;
        private bool processStandbyRecords = false;
        private volatile StreamThreadStates state = StreamThreadStates.CREATED;
        private volatile ThreadMetadata threadMetadata;
        private IStateListener stateListener;
        private Dictionary<TopicPartition, List<ConsumeResult<byte[], byte[]>>> standbyRecords;

        // package-private for testing
        ConsumerRebalanceListener rebalanceListener;
        IProducer<byte[], byte[]> producer;
        IConsumer<byte[], byte[]> restoreConsumer;
        IConsumer<byte[], byte[]> consumer;
        InternalTopologyBuilder builder;

        public static StreamThread create(
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
            int threadIdx)
        {
            string threadClientId = clientId + "-StreamThread-" + threadIdx;

            string logPrefix = string.Format("stream-thread [%s] ", threadClientId);
            LogContext logContext = new LogContext(logPrefix);
            ILogger log = logContext.logger<StreamThread>();

            log.LogInformation("Creating restore consumer client");
            Dictionary<string, object> restoreConsumerConfigs = config.GetRestoreConsumerConfigs(getRestoreConsumerClientId(threadClientId));
            IConsumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(restoreConsumerConfigs);
            TimeSpan pollTime = TimeSpan.FromMilliseconds(config.getLong(StreamsConfig.POLL_MS_CONFIG));
            StoreChangelogReader changelogReader = new StoreChangelogReader(restoreConsumer, pollTime, userStateRestoreListener, logContext);

            IProducer<byte[], byte[]> threadProducer = null;
            bool eosEnabled = StreamsConfig.EXACTLY_ONCE.Equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
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
            Dictionary<string, object> consumerConfigs = config.GetMainConsumerConfigs(applicationId, getConsumerClientId(threadClientId), threadIdx);
            //consumerConfigs.Add(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
            int assignmentErrorCode = 0;

            //consumerConfigs.Add(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE, assignmentErrorCode);
            string originalReset = null;

            if (!builder.latestResetTopicsPattern().IsMatch("") || !builder.earliestResetTopicsPattern().IsMatch(""))
            {
                originalReset = (string)consumerConfigs["auto.offset.reset"];
                consumerConfigs.Add("auto.offset.reset", "none");
            }

            IConsumer<byte[], byte[]> consumer = clientSupplier.getConsumer(consumerConfigs);
            taskManager.setConsumer(consumer);

            return new StreamThread(
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

        public StreamThread(
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
            : base(threadClientId)
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
            this.log = logContext.logger(typeof(StreamThread));
            this.rebalanceListener = new RebalanceListener(time, taskManager, this, this.log);
            this.taskManager = taskManager;
            this.producer = producer;
            this.restoreConsumer = restoreConsumer;
            this.consumer = consumer;
            this.originalReset = originalReset;
            this.assignmentErrorCode = assignmentErrorCode;

            this.pollTime = TimeSpan.FromMilliseconds(config.getLong("poll.ms").Value);
            int dummyThreadIdx = 1;
            //this.maxPollTimeMs = new InternalConsumerConfig(config.GetMainConsumerConfigs("dummyGroupId", "dummyClientId", dummyThreadIdx))
              //      .getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
            //this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);

            this.numIterations = 1;
        }

        private static string getTaskProducerClientId(string threadClientId, TaskId taskId)
        {
            return threadClientId + "-" + taskId + "-producer";
        }

        private static string getThreadProducerClientId(string threadClientId)
        {
            return threadClientId + "-producer";
        }

        private static string getConsumerClientId(string threadClientId)
        {
            return threadClientId + "-consumer";
        }

        private static string getRestoreConsumerClientId(string threadClientId)
        {
            return threadClientId + "-restore-consumer";
        }

        // currently admin client is shared among all threads
        public static string getSharedAdminClientId(string clientId)
        {
            return clientId + "-admin";
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
            if (setState(StreamThreadStates.STARTING) == null)
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
                log.LogError("Encountered the following error during processing:", e);
                throw e;
            }
            finally
            {

                completeShutdown(cleanRun);
            }
        }

        private void setRebalanceException(Throwable rebalanceException)
        {
            this.rebalanceException = rebalanceException;
        }

        /**
         * Main event loop for polling, and processing records through topologies.
         *
         * @throws InvalidOperationException If store gets registered after initialized is already finished
         * @throws StreamsException      if the store's change log does not contain the partition
         */
        private void runLoop()
        {
            consumer.Subscribe(builder.sourceTopicPattern(), rebalanceListener);

            while (isRunning())
            {
                try
                {

                    runOnce();
                    if (assignmentErrorCode == StreamsPartitionAssignor.Error.VERSION_PROBING.code())
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
                        ignoreAndRejoinGroup.migratedTask().id(), ignoreAndRejoinGroup.migratedTask().ToString(">"));

                    enforceRebalance();
                }
            }
        }

        private void enforceRebalance()
        {
            consumer.Unsubscribe();
            consumer.Subscribe(builder.sourceTopicPattern(), rebalanceListener);
        }

        /**
         * @throws InvalidOperationException If store gets registered after initialized is already finished
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

            if (state == StreamThreadStates.PARTITIONS_ASSIGNED)
            {
                // try to fetch some records with zero poll millis
                // to unblock the restoration as soon as possible
                records = pollRequests(Duration.ZERO);
            }
            else if (state == StreamThreadStates.PARTITIONS_REVOKED)
            {
                // try to fetch some records with normal poll time
                // in order to wait long enough to get the join response
                records = pollRequests(pollTime);
            }
            else if (state == StreamThreadStates.RUNNING || state == StreamThreadStates.STARTING)
            {
                // try to fetch some records with normal poll time
                // in order to get long polling
                records = pollRequests(pollTime);
            }
            else
            {

                // any other state should not happen
                log.LogError("Unexpected state {} during normal iteration", state);
                throw new StreamsException(logPrefix + "Unexpected state " + state + " during normal iteration");
            }

            // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
            // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
            // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
            // could affect the task manager state beyond this point within #runOnce().
            if (!isRunning())
            {
                log.LogDebug("State already transits to {}, skipping the run once call after poll request", state);
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
            if (state == State.PARTITIONS_ASSIGNED)
            {
                if (taskManager.updateNewAndRestoringTasks())
                {
                    setState(State.RUNNING);
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

                records = consumer.poll(pollTime);
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
                            "policy via StreamsBuilder#stream(..., Consumed.with(AutoOffsetReset)) or StreamsBuilder#table(..., Consumed.with(AutoOffsetReset))";
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
         * Take records and.Add them to each respective task
         *
         * @param records Records, can be null
         */
        private void addRecordsToTasks(ConsumerRecords<byte[], byte[]> records)
        {

            foreach (TopicPartition partition in records.partitions)
            {
                StreamTask task = taskManager.activeTask(partition);

                if (task == null)
                {
                    log.LogError(
                        "Unable to locate active task for received-record partition {}. Current tasks: {}",
                        partition,
                        taskManager.ToString(">")
                    );
                    throw new ArgumentNullException("Task was unexpectedly missing for partition " + partition);
                }
                else if (task.isClosed())
                {
                    log.LogInformation("Stream task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                 "Notifying the thread to trigger a new rebalance immediately.", task.id);
                    throw new TaskMigratedException(task);
                }

                task.addRecords(partition, records.records(partition));
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
                log.LogTrace("Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                    taskManager.activeTaskIds(), taskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);

                committed += taskManager.commitAll();
                if (committed > 0)
                {
                    long intervalCommitLatency = advanceNowAndComputeLatency();
                    commitSensor.record(intervalCommitLatency / (double)committed, now);

                    // try to purge the committed records for repartition topics if possible
                    taskManager.maybePurgeCommitedRecords();

                    log.LogDebug("Committed all active tasks {} and standby tasks {} in {}ms",
                        taskManager.activeTaskIds(), taskManager.standbyTaskIds(), intervalCommitLatency);
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
            if (state == State.RUNNING && taskManager.hasStandbyRunningTasks())
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

                        log.LogDebug("Updated standby tasks {} in {}ms", taskManager.standbyTaskIds(), time.milliseconds() - now);
                    }

                    processStandbyRecords = false;
                }

                try
                {

                    // poll(0): Since this is during the normal processing, not during restoration.
                    // We can afford to have slower restore (because we don't wait inside poll for results).
                    // Instead, we want to proceed to the next iteration to call the main consumer#poll()
                    // as soon as possible so as to not be kicked out of the group.
                    ConsumerRecords<byte[], byte[]> records = restoreConsumer.Poll(Duration.ZERO);

                    if (records.Any())
                    {
                        foreach (TopicPartition partition in records.partitions)
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

                            List<ConsumeResult<byte[], byte[]>> remaining = task.update(partition, records.records(partition));
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
                    HashSet<TopicPartition> partitions = recoverableException.partitions;
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
                        //task.reinitializeStateStoresForPartitions(recoverableException.partitions());
                    }
                    //restoreConsumer.seekToBeginning(partitions);
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
            GlobalStreamThreadStates oldState = setState(GlobalStreamThreadStates.PENDING_SHUTDOWN);
            if (oldState == GlobalStreamThreadStates.CREATED)
            {
                // The thread may not have been started. Take responsibility for shutting down
                completeShutdown(true);
            }
        }

        private void completeShutdown(bool cleanRun)
        {
            // set the state to pending shutdown first as it may be called due to error;
            // its state may already be PENDING_SHUTDOWN so it will return false but we
            // intentionally do not check the returned flag
            setState(State.PENDING_SHUTDOWN);

            log.LogInformation("Shutting down");

            try
            {

                taskManager.shutdown(cleanRun);
            }
            catch (Throwable e)
            {
                log.LogError("Failed to close task manager due to the following error:", e);
            }
            try
            {
                consumer.Close();
            }
            catch (Throwable e)
            {
                log.LogError("Failed to close consumer due to the following error:", e);
            }
            try
            {
                restoreConsumer.Close();
            }
            catch (Throwable e)
            {
                log.LogError("Failed to close restore consumer due to the following error:", e);
            }
            streamsMetrics.removeAllThreadLevelSensors();

            setState(State.DEAD);
            log.LogInformation("Shutdown complete");
        }

        private void clearStandbyRecords()
        {
            standbyRecords.Clear();
        }

        // package-private for testing only
        StreamThread updateThreadMetadata(string adminClientId)
        {

            threadMetadata = new ThreadMetadata(
                this.getName(),
                this.state().name,
                getConsumerClientId(this.getName()),
                getRestoreConsumerClientId(this.getName()),
                producer == null ? Collections.emptySet() : Collections.singleton(getThreadProducerClientId(this.getName())),
                adminClientId,
                Collections.emptySet(),
                Collections.emptySet());

            return this;
        }

        private void updateThreadMetadata(Dictionary<TaskId, StreamTask> activeTasks,
                                          Dictionary<TaskId, StandbyTask> standbyTasks)
        {
            HashSet<string> producerClientIds = new HashSet<string>();
            HashSet<TaskMetadata> activeTasksMetadata = new HashSet<TaskMetadata>();
            foreach (KeyValuePair<TaskId, StreamTask> task in activeTasks)
            {
                activeTasksMetadata.Add(new TaskMetadata(task.Key.ToString(), task.Value.partitions));
                producerClientIds.Add(getTaskProducerClientId(getName(), task.Key));
            }
            HashSet<TaskMetadata> standbyTasksMetadata = new HashSet<TaskMetadata>();
            foreach (KeyValuePair<TaskId, StandbyTask> task in standbyTasks)
            {
                standbyTasksMetadata.Add(new TaskMetadata(task.Key.ToString(), task.Value.partitions()));
            }

            string adminClientId = threadMetadata.adminClientId();
            threadMetadata = new ThreadMetadata(
                this.getName(),
                this.state().name,
                getConsumerClientId(this.getName()),
                getRestoreConsumerClientId(this.getName()),
                producer == null ? producerClientIds : Collections.singleton(getThreadProducerClientId(this.getName())),
                adminClientId,
                activeTasksMetadata,
                standbyTasksMetadata);
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

        public string ToString()
        {
            return ToString("");
        }

        /**
         * Produces a string representation containing useful information about a StreamThread, starting with the given indent.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the StreamThread instance.
         */
        public string ToString(string indent)
        {
            return indent + "\tStreamsThread threadId: " + getName() + "\n" + taskManager.ToString(indent);
        }

        public Dictionary<MetricName, IMetric> producerMetrics()
        {
            //LinkedHashMap<MetricName, IMetric> result = new LinkedHashMap<>();
            if (producer != null)
            {
                //Dictionary<MetricName, IMetric> producerMetrics = producer.metrics;
                //if (producerMetrics != null)
                //{
                //    result.putAll(producerMetrics);
                //}
            }
            else
            {

                // When EOS is turned on, each task will have its own producer client
                // and the producer object passed in here will be null. We would then iterate through
                // all the active tasks and.Add their metrics to the output metrics map.
                foreach (StreamTask task in taskManager.activeTasks().Values)
                {
                    Dictionary<MetricName, IMetric> taskProducerMetrics = task.getProducer().metrics;
                    result.putAll(taskProducerMetrics);
                }
            }
            return result;
        }

        public Dictionary<MetricName, IMetric> consumerMetrics()
        {
            Dictionary<MetricName, IMetric> consumerMetrics = consumer.metrics;
            Dictionary<MetricName, IMetric> restoreConsumerMetrics = restoreConsumer.metrics;
            LinkedHashMap<MetricName, IMetric> result = new LinkedHashMap<>();
            result.putAll(consumerMetrics);
            result.putAll(restoreConsumerMetrics);
            return result;
        }

        public Dictionary<MetricName, IMetric> adminClientMetrics()
        {
            Dictionary<MetricName, IMetric> adminClientMetrics = taskManager.getAdminClient().metrics;
            LinkedHashMap<MetricName, IMetric> result = new LinkedHashMap<>();
            result.putAll(adminClientMetrics);
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