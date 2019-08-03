/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
using Kafka.Common.KafkaException;
using Kafka.Common.Metric;
using Kafka.Common.MetricName;
using Kafka.Common.TopicPartition;
using Kafka.Common.metrics.Metrics;
using Kafka.Common.metrics.Sensor;
using Kafka.Common.serialization.ByteArrayDeserializer;
using Kafka.Common.Utils.LogContext;
using Kafka.Common.Utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TaskMetadata;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singleton;

public class StreamThread : Thread {

    /**
     * Stream thread states are the possible states that a stream thread can be in.
     * A thread must only be in one state at a time
     * The expected state transitions with the following defined states is:
     *
     * <pre>
     *                +-------------+
     *          +<--- | Created (0) |
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +<--- | Starting (1)|
     *          |     +-----+-------+
     *          |           |
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +<--- | Partitions  |
     *          |     | Revoked (2) | <----+
     *          |     +-----+-------+      |
     *          |           |              |
     *          |           v              |
     *          |     +-----+-------+      |
     *          |     | Partitions  |      |
     *          +<--- | Assigned (3)| ---->+
     *          |     +-----+-------+      |
     *          |           |              |
     *          |           v              |
     *          |     +-----+-------+      |
     *          |     | Running (4) | ---->+
     *          |     +-----+-------+
     *          |           |
     *          |           v
     *          |     +-----+-------+
     *          +---> | Pending     |
     *                | Shutdown (5)|
     *                +-----+-------+
     *                      |
     *                      v
     *                +-----+-------+
     *                | Dead (6)    |
     *                +-------------+
     * </pre>
     *
     * Note the following:
     * <ul>
     *     <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
     *     <li>
     *         State PENDING_SHUTDOWN may want to transit to some other states other than DEAD,
     *         in the corner case when the shutdown is triggered while the thread is still in the rebalance loop.
     *         In this case we will forbid the transition but will not treat as an error.
     *     </li>
     *     <li>
     *         State PARTITIONS_REVOKED may want transit to itself indefinitely, in the corner case when
     *         the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
     *         Also during streams instance start up PARTITIONS_REVOKED may want to transit to itself as well.
     *         In this case we will forbid the transition but will not treat as an error.
     *     </li>
     * </ul>
     */
    public enum State implements ThreadStateTransitionValidator {
        CREATED(1, 5), STARTING(2, 5), PARTITIONS_REVOKED(3, 5), PARTITIONS_ASSIGNED(2, 4, 5), RUNNING(2, 5), PENDING_SHUTDOWN(6), DEAD;

        private Set<Integer> validTransitions = new HashSet<>();

        State(Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public bool isRunning() {
            return equals(RUNNING) || equals(STARTING) || equals(PARTITIONS_REVOKED) || equals(PARTITIONS_ASSIGNED);
        }

        @Override
        public bool isValidTransition(ThreadStateTransitionValidator newState) {
            State tmpState = (State) newState;
            return validTransitions.contains(tmpState.ordinal());
        }
    }

    /**
     * Listen to state change events
     */
    public interface StateListener {

        /**
         * Called when state changes
         *
         * @param thread   thread changing state
         * @param newState current state
         * @param oldState previous state
         */
        void onChange(Thread thread, ThreadStateTransitionValidator newState, ThreadStateTransitionValidator oldState);
    }

    /**
     * Set the {@link StreamThread.StateListener} to be notified when state changes. Note this API is internal to
     * Kafka Streams and is not intended to be used by an external application.
     */
    public void setStateListener(StreamThread.StateListener listener) {
        stateListener = listener;
    }

    /**
     * @return The state this instance is in
     */
    public State state() {
        // we do not need to use the state lock since the variable is volatile
        return state;
    }

    /**
     * Sets the state
     *
     * @param newState New state
     * @return The state prior to the call to setState, or null if the transition is invalid
     */
    State setState(State newState) {
        State oldState;

        synchronized (stateLock) {
            oldState = state;

            if (state == State.PENDING_SHUTDOWN && newState != State.DEAD) {
                log.debug("Ignoring request to transit from PENDING_SHUTDOWN to {}: " +
                              "only DEAD state is a valid next state", newState);
                // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                // refused but we do not throw exception here
                return null;
            } else if (state == State.DEAD) {
                log.debug("Ignoring request to transit from DEAD to {}: " +
                              "no valid next state after DEAD", newState);
                // when the state is already in NOT_RUNNING, all its transitions
                // will be refused but we do not throw exception here
                return null;
            } else if (state == State.PARTITIONS_REVOKED && newState == State.PARTITIONS_REVOKED) {
                log.debug("Ignoring request to transit from PARTITIONS_REVOKED to PARTITIONS_REVOKED: " +
                              "self transition is not allowed");
                // when the state is already in PARTITIONS_REVOKED, its transition to itself will be
                // refused but we do not throw exception here
                return null;
            } else if (!state.isValidTransition(newState)) {
                log.error("Unexpected state transition from {} to {}", oldState, newState);
                throw new StreamsException(logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
            } else {
                log.info("State transition from {} to {}", oldState, newState);
            }

            state = newState;
            if (newState == State.RUNNING) {
                updateThreadMetadata(taskManager.activeTasks(), taskManager.standbyTasks());
            } else {
                updateThreadMetadata(Collections.emptyMap(), Collections.emptyMap());
            }
        }

        if (stateListener != null) {
            stateListener.onChange(this, state, oldState);
        }

        return oldState;
    }

    public bool isRunningAndNotRebalancing() {
        // we do not need to grab stateLock since it is a single read
        return state == State.RUNNING;
    }

    public bool isRunning() {
        synchronized (stateLock) {
            return state.isRunning();
        }
    }

    static class RebalanceListener implements ConsumerRebalanceListener {
        private Time time;
        private TaskManager taskManager;
        private StreamThread streamThread;
        private Logger log;

        RebalanceListener(Time time,
                          TaskManager taskManager,
                          StreamThread streamThread,
                          Logger log) {
            this.time = time;
            this.taskManager = taskManager;
            this.streamThread = streamThread;
            this.log = log;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assignment) {
            log.debug("at state {}: partitions {} assigned at the end of consumer rebalance.\n" +
                    "\tcurrent suspended active tasks: {}\n" +
                    "\tcurrent suspended standby tasks: {}\n",
                streamThread.state,
                assignment,
                taskManager.suspendedActiveTaskIds(),
                taskManager.suspendedStandbyTaskIds());

            if (streamThread.assignmentErrorCode.get() == StreamsPartitionAssignor.Error.INCOMPLETE_SOURCE_TOPIC_METADATA.code()) {
                log.error("Received error code {} - shutdown", streamThread.assignmentErrorCode.get());
                streamThread.shutdown();
                return;
            }
            long start = time.milliseconds();
            try {
                if (streamThread.setState(State.PARTITIONS_ASSIGNED) == null) {
                    log.debug(
                        "Skipping task creation in rebalance because we are already in {} state.",
                        streamThread.state()
                    );
                } else if (streamThread.assignmentErrorCode.get() != StreamsPartitionAssignor.Error.NONE.code()) {
                    log.debug(
                        "Encountered assignment error during partition assignment: {}. Skipping task initialization",
                        streamThread.assignmentErrorCode
                    );
                } else {
                    log.debug("Creating tasks based on assignment.");
                    taskManager.createTasks(assignment);
                }
            } catch (Throwable t) {
                log.error(
                    "Error caught during partition assignment, " +
                        "will abort the current process and re-throw at the end of rebalance", t);
                streamThread.setRebalanceException(t);
            } finally {
                log.info("partition assignment took {} ms.\n" +
                        "\tcurrent active tasks: {}\n" +
                        "\tcurrent standby tasks: {}\n" +
                        "\tprevious active tasks: {}\n",
                    time.milliseconds() - start,
                    taskManager.activeTaskIds(),
                    taskManager.standbyTaskIds(),
                    taskManager.prevActiveTaskIds());
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> assignment) {
            log.debug("at state {}: partitions {} revoked at the beginning of consumer rebalance.\n" +
                    "\tcurrent assigned active tasks: {}\n" +
                    "\tcurrent assigned standby tasks: {}\n",
                streamThread.state,
                assignment,
                taskManager.activeTaskIds(),
                taskManager.standbyTaskIds());

            if (streamThread.setState(State.PARTITIONS_REVOKED) != null) {
                long start = time.milliseconds();
                try {
                    // suspend active tasks
                    if (streamThread.assignmentErrorCode.get() == StreamsPartitionAssignor.Error.VERSION_PROBING.code()) {
                        streamThread.assignmentErrorCode.set(StreamsPartitionAssignor.Error.NONE.code());
                    } else {
                        taskManager.suspendTasksAndState();
                    }
                } catch (Throwable t) {
                    log.error(
                        "Error caught during partition revocation, " +
                            "will abort the current process and re-throw at the end of rebalance: {}",
                        t
                    );
                    streamThread.setRebalanceException(t);
                } finally {
                    streamThread.clearStandbyRecords();

                    log.info("partition revocation took {} ms.\n" +
                            "\tsuspended active tasks: {}\n" +
                            "\tsuspended standby tasks: {}",
                        time.milliseconds() - start,
                        taskManager.suspendedActiveTaskIds(),
                        taskManager.suspendedStandbyTaskIds());
                }
            }
        }
    }

    static abstract class AbstractTaskCreator<T : Task> {
        string applicationId;
        InternalTopologyBuilder builder;
        StreamsConfig config;
        StreamsMetricsImpl streamsMetrics;
        StateDirectory stateDirectory;
        ChangelogReader storeChangelogReader;
        Time time;
        Logger log;


        AbstractTaskCreator(InternalTopologyBuilder builder,
                            StreamsConfig config,
                            StreamsMetricsImpl streamsMetrics,
                            StateDirectory stateDirectory,
                            ChangelogReader storeChangelogReader,
                            Time time,
                            Logger log) {
            this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
            this.builder = builder;
            this.config = config;
            this.streamsMetrics = streamsMetrics;
            this.stateDirectory = stateDirectory;
            this.storeChangelogReader = storeChangelogReader;
            this.time = time;
            this.log = log;
        }

        public InternalTopologyBuilder builder() {
            return builder;
        }

        public StateDirectory stateDirectory() {
            return stateDirectory;
        }

        Collection<T> createTasks(Consumer<byte[], byte[]> consumer,
                                  Dictionary<TaskId, Set<TopicPartition>> tasksToBeCreated) {
            List<T> createdTasks = new ArrayList<>();
            for (Map.Entry<TaskId, Set<TopicPartition>> newTaskAndPartitions : tasksToBeCreated.entrySet()) {
                TaskId taskId = newTaskAndPartitions.getKey();
                Set<TopicPartition> partitions = newTaskAndPartitions.getValue();
                T task = createTask(consumer, taskId, partitions);
                if (task != null) {
                    log.trace("Created task {} with assigned partitions {}", taskId, partitions);
                    createdTasks.add(task);
                }

            }
            return createdTasks;
        }

        abstract T createTask(Consumer<byte[], byte[]> consumer, TaskId id, Set<TopicPartition> partitions);

        public void close() {}
    }

    static class TaskCreator : AbstractTaskCreator<StreamTask> {
        private ThreadCache cache;
        private KafkaClientSupplier clientSupplier;
        private string threadClientId;
        private Producer<byte[], byte[]> threadProducer;
        private Sensor createTaskSensor;

        TaskCreator(InternalTopologyBuilder builder,
                    StreamsConfig config,
                    StreamsMetricsImpl streamsMetrics,
                    StateDirectory stateDirectory,
                    ChangelogReader storeChangelogReader,
                    ThreadCache cache,
                    Time time,
                    KafkaClientSupplier clientSupplier,
                    Producer<byte[], byte[]> threadProducer,
                    string threadClientId,
                    Logger log) {
            super(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                storeChangelogReader,
                time,
                log);
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadProducer = threadProducer;
            this.threadClientId = threadClientId;
            createTaskSensor = ThreadMetrics.createTaskSensor(streamsMetrics);
        }

        @Override
        StreamTask createTask(Consumer<byte[], byte[]> consumer,
                              TaskId taskId,
                              Set<TopicPartition> partitions) {
            createTaskSensor.record();

            return new StreamTask(
                taskId,
                partitions,
                builder.build(taskId.topicGroupId),
                consumer,
                storeChangelogReader,
                config,
                streamsMetrics,
                stateDirectory,
                cache,
                time,
                () -> createProducer(taskId));
        }

        private Producer<byte[], byte[]> createProducer(TaskId id) {
            // eos
            if (threadProducer == null) {
                Dictionary<string, object> producerConfigs = config.getProducerConfigs(getTaskProducerClientId(threadClientId, id));
                log.info("Creating producer client for task {}", id);
                producerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, applicationId + "-" + id);
                return clientSupplier.getProducer(producerConfigs);
            }

            return threadProducer;
        }

        @Override
        public void close() {
            if (threadProducer != null) {
                try {
                    threadProducer.close();
                } catch (Throwable e) {
                    log.error("Failed to close producer due to the following error:", e);
                }
            }
        }
    }

    static class StandbyTaskCreator : AbstractTaskCreator<StandbyTask> {
        private Sensor createTaskSensor;

        StandbyTaskCreator(InternalTopologyBuilder builder,
                           StreamsConfig config,
                           StreamsMetricsImpl streamsMetrics,
                           StateDirectory stateDirectory,
                           ChangelogReader storeChangelogReader,
                           Time time,
                           Logger log) {
            super(
                builder,
                config,
                streamsMetrics,
                stateDirectory,
                storeChangelogReader,
                time,
                log);
            createTaskSensor = ThreadMetrics.createTaskSensor(streamsMetrics);
        }

        @Override
        StandbyTask createTask(Consumer<byte[], byte[]> consumer,
                               TaskId taskId,
                               Set<TopicPartition> partitions) {
            createTaskSensor.record();

            ProcessorTopology topology = builder.build(taskId.topicGroupId);

            if (!topology.stateStores().isEmpty() && !topology.storeToChangelogTopic().isEmpty()) {
                return new StandbyTask(
                    taskId,
                    partitions,
                    topology,
                    consumer,
                    storeChangelogReader,
                    config,
                    streamsMetrics,
                    stateDirectory);
            } else {
                log.trace(
                    "Skipped standby task {} with assigned partitions {} " +
                        "since it does not have any state stores to materialize",
                    taskId, partitions
                );
                return null;
            }
        }
    }

    private Time time;
    private Logger log;
    private string logPrefix;
    private object stateLock;
    private Duration pollTime;
    private long commitTimeMs;
    private int maxPollTimeMs;
    private string originalReset;
    private TaskManager taskManager;
    private AtomicInteger assignmentErrorCode;

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
    private volatile State state = State.CREATED;
    private volatile ThreadMetadata threadMetadata;
    private StreamThread.StateListener stateListener;
    private Dictionary<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> standbyRecords;

    // package-private for testing
    ConsumerRebalanceListener rebalanceListener;
    Producer<byte[], byte[]> producer;
    Consumer<byte[], byte[]> restoreConsumer;
    Consumer<byte[], byte[]> consumer;
    InternalTopologyBuilder builder;

    public static StreamThread create(InternalTopologyBuilder builder,
                                      StreamsConfig config,
                                      KafkaClientSupplier clientSupplier,
                                      Admin adminClient,
                                      UUID processId,
                                      string clientId,
                                      Metrics metrics,
                                      Time time,
                                      StreamsMetadataState streamsMetadataState,
                                      long cacheSizeBytes,
                                      StateDirectory stateDirectory,
                                      StateRestoreListener userStateRestoreListener,
                                      int threadIdx) {
        string threadClientId = clientId + "-StreamThread-" + threadIdx;

        string logPrefix = string.format("stream-thread [%s] ", threadClientId);
        LogContext logContext = new LogContext(logPrefix);
        Logger log = logContext.logger(StreamThread.class);

        log.info("Creating restore consumer client");
        Dictionary<string, object> restoreConsumerConfigs = config.getRestoreConsumerConfigs(getRestoreConsumerClientId(threadClientId));
        Consumer<byte[], byte[]> restoreConsumer = clientSupplier.getRestoreConsumer(restoreConsumerConfigs);
        Duration pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
        StoreChangelogReader changelogReader = new StoreChangelogReader(restoreConsumer, pollTime, userStateRestoreListener, logContext);

        Producer<byte[], byte[]> threadProducer = null;
        bool eosEnabled = StreamsConfig.EXACTLY_ONCE.Equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        if (!eosEnabled) {
            Dictionary<string, object> producerConfigs = config.getProducerConfigs(getThreadProducerClientId(threadClientId));
            log.info("Creating shared producer client");
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

        log.info("Creating consumer client");
        string applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
        Dictionary<string, object> consumerConfigs = config.getMainConsumerConfigs(applicationId, getConsumerClientId(threadClientId), threadIdx);
        consumerConfigs.put(StreamsConfig.InternalConfig.TASK_MANAGER_FOR_PARTITION_ASSIGNOR, taskManager);
        AtomicInteger assignmentErrorCode = new AtomicInteger();
        consumerConfigs.put(StreamsConfig.InternalConfig.ASSIGNMENT_ERROR_CODE, assignmentErrorCode);
        string originalReset = null;
        if (!builder.latestResetTopicsPattern().pattern().Equals("") || !builder.earliestResetTopicsPattern().pattern().Equals("")) {
            originalReset = (string) consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        }

        Consumer<byte[], byte[]> consumer = clientSupplier.getConsumer(consumerConfigs);
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

    public StreamThread(Time time,
                        StreamsConfig config,
                        Producer<byte[], byte[]> producer,
                        Consumer<byte[], byte[]> restoreConsumer,
                        Consumer<byte[], byte[]> consumer,
                        string originalReset,
                        TaskManager taskManager,
                        StreamsMetricsImpl streamsMetrics,
                        InternalTopologyBuilder builder,
                        string threadClientId,
                        LogContext logContext,
                        AtomicInteger assignmentErrorCode) {
        super(threadClientId);

        this.stateLock = new Object();
        this.standbyRecords = new HashMap<>();

        this.streamsMetrics = streamsMetrics;
        this.commitSensor = ThreadMetrics.commitSensor(streamsMetrics);
        this.pollSensor = ThreadMetrics.pollSensor(streamsMetrics);
        this.processSensor = ThreadMetrics.processSensor(streamsMetrics);
        this.punctuateSensor = ThreadMetrics.punctuateSensor(streamsMetrics);

        // The following sensors are created here but their references are not stored in this object, since within
        // this object they are not recorded. The sensors are created here so that the stream threads starts with all
        // its metrics initialised. Otherwise, those sensors would have been created during processing, which could
        // lead to missing metrics. For instance, if no task were created, the metrics for created and closed
        // tasks would never be added to the metrics.
        ThreadMetrics.createTaskSensor(streamsMetrics);
        ThreadMetrics.closeTaskSensor(streamsMetrics);
        ThreadMetrics.skipRecordSensor(streamsMetrics);
        ThreadMetrics.commitOverTasksSensor(streamsMetrics);

        this.time = time;
        this.builder = builder;
        this.logPrefix = logContext.logPrefix();
        this.log = logContext.logger(StreamThread.class);
        this.rebalanceListener = new RebalanceListener(time, taskManager, this, this.log);
        this.taskManager = taskManager;
        this.producer = producer;
        this.restoreConsumer = restoreConsumer;
        this.consumer = consumer;
        this.originalReset = originalReset;
        this.assignmentErrorCode = assignmentErrorCode;

        this.pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
        int dummyThreadIdx = 1;
        this.maxPollTimeMs = new InternalConsumerConfig(config.getMainConsumerConfigs("dummyGroupId", "dummyClientId", dummyThreadIdx))
                .getInt(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
        this.commitTimeMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);

        this.numIterations = 1;
    }

    private static class InternalConsumerConfig : ConsumerConfig {
        private InternalConsumerConfig(Dictionary<string, object> props) {
            super(ConsumerConfig.addDeserializerToConfig(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()), false);
        }
    }

    private static string getTaskProducerClientId(string threadClientId, TaskId taskId) {
        return threadClientId + "-" + taskId + "-producer";
    }

    private static string getThreadProducerClientId(string threadClientId) {
        return threadClientId + "-producer";
    }

    private static string getConsumerClientId(string threadClientId) {
        return threadClientId + "-consumer";
    }

    private static string getRestoreConsumerClientId(string threadClientId) {
        return threadClientId + "-restore-consumer";
    }

    // currently admin client is shared among all threads
    public static string getSharedAdminClientId(string clientId) {
        return clientId + "-admin";
    }

    /**
     * Execute the stream processors
     *
     * @throws KafkaException   for any Kafka-related exceptions
     * @throws RuntimeException for any other non-Kafka exceptions
     */
    @Override
    public void run() {
        log.info("Starting");
        if (setState(State.STARTING) == null) {
            log.info("StreamThread already shutdown. Not running");
            return;
        }
        bool cleanRun = false;
        try {
            runLoop();
            cleanRun = true;
        } catch (KafkaException e) {
            log.error("Encountered the following unexpected Kafka exception during processing, " +
                "this usually indicate Streams internal errors:", e);
            throw e;
        } catch (Exception e) {
            // we have caught all Kafka related exceptions, and other runtime exceptions
            // should be due to user application errors
            log.error("Encountered the following error during processing:", e);
            throw e;
        } finally {
            completeShutdown(cleanRun);
        }
    }

    private void setRebalanceException(Throwable rebalanceException) {
        this.rebalanceException = rebalanceException;
    }

    /**
     * Main event loop for polling, and processing records through topologies.
     *
     * @throws InvalidOperationException If store gets registered after initialized is already finished
     * @throws StreamsException      if the store's change log does not contain the partition
     */
    private void runLoop() {
        consumer.subscribe(builder.sourceTopicPattern(), rebalanceListener);

        while (isRunning()) {
            try {
                runOnce();
                if (assignmentErrorCode.get() == StreamsPartitionAssignor.Error.VERSION_PROBING.code()) {
                    log.info("Version probing detected. Triggering new rebalance.");
                    enforceRebalance();
                }
            } catch (TaskMigratedException ignoreAndRejoinGroup) {
                log.warn("Detected task {} that got migrated to another thread. " +
                        "This implies that this thread missed a rebalance and dropped out of the consumer group. " +
                        "Will try to rejoin the consumer group. Below is the detailed description of the task:\n{}",
                    ignoreAndRejoinGroup.migratedTask().id(), ignoreAndRejoinGroup.migratedTask().toString(">"));

                enforceRebalance();
            }
        }
    }

    private void enforceRebalance() {
        consumer.unsubscribe();
        consumer.subscribe(builder.sourceTopicPattern(), rebalanceListener);
    }

    /**
     * @throws InvalidOperationException If store gets registered after initialized is already finished
     * @throws StreamsException      If the store's change log does not contain the partition
     * @throws TaskMigratedException If another thread wrote to the changelog topic that is currently restored
     *                               or if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // Visible for testing
    void runOnce() {
        ConsumerRecords<byte[], byte[]> records;
        now = time.milliseconds();

        if (state == State.PARTITIONS_ASSIGNED) {
            // try to fetch some records with zero poll millis
            // to unblock the restoration as soon as possible
            records = pollRequests(Duration.ZERO);
        } else if (state == State.PARTITIONS_REVOKED) {
            // try to fetch some records with normal poll time
            // in order to wait long enough to get the join response
            records = pollRequests(pollTime);
        } else if (state == State.RUNNING || state == State.STARTING) {
            // try to fetch some records with normal poll time
            // in order to get long polling
            records = pollRequests(pollTime);
        } else {
            // any other state should not happen
            log.error("Unexpected state {} during normal iteration", state);
            throw new StreamsException(logPrefix + "Unexpected state " + state + " during normal iteration");
        }

        // Shutdown hook could potentially be triggered and transit the thread state to PENDING_SHUTDOWN during #pollRequests().
        // The task manager internal states could be uninitialized if the state transition happens during #onPartitionsAssigned().
        // Should only proceed when the thread is still running after #pollRequests(), because no external state mutation
        // could affect the task manager state beyond this point within #runOnce().
        if (!isRunning()) {
            log.debug("State already transits to {}, skipping the run once call after poll request", state);
            return;
        }

        long pollLatency = advanceNowAndComputeLatency();

        if (records != null && !records.isEmpty()) {
            pollSensor.record(pollLatency, now);
            addRecordsToTasks(records);
        }

        // only try to initialize the assigned tasks
        // if the state is still in PARTITION_ASSIGNED after the poll call
        if (state == State.PARTITIONS_ASSIGNED) {
            if (taskManager.updateNewAndRestoringTasks()) {
                setState(State.RUNNING);
            }
        }

        advanceNowAndComputeLatency();

        // TODO: we will process some tasks even if the state is not RUNNING, i.e. some other
        // tasks are still being restored.
        if (taskManager.hasActiveRunningTasks()) {
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

            do {
                for (int i = 0; i < numIterations; i++) {
                    processed = taskManager.process(now);

                    if (processed > 0) {
                        long processLatency = advanceNowAndComputeLatency();
                        processSensor.record(processLatency / (double) processed, now);

                        // commit any tasks that have requested a commit
                        int committed = taskManager.maybeCommitActiveTasksPerUserRequested();

                        if (committed > 0) {
                            long commitLatency = advanceNowAndComputeLatency();
                            commitSensor.record(commitLatency / (double) committed, now);
                        }
                    } else {
                        // if there is no records to be processed, exit immediately
                        break;
                    }
                }

                timeSinceLastPoll = Math.max(now - lastPollMs, 0);

                if (maybePunctuate() || maybeCommit()) {
                    numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                } else if (timeSinceLastPoll > maxPollTimeMs / 2) {
                    numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                    break;
                } else if (processed > 0) {
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
    private ConsumerRecords<byte[], byte[]> pollRequests(Duration pollTime) {
        ConsumerRecords<byte[], byte[]> records = null;

        lastPollMs = now;

        try {
            records = consumer.poll(pollTime);
        } catch (InvalidOffsetException e) {
            resetInvalidOffsets(e);
        }

        if (rebalanceException != null) {
            if (rebalanceException is TaskMigratedException) {
                throw (TaskMigratedException) rebalanceException;
            } else {
                throw new StreamsException(logPrefix + "Failed to rebalance.", rebalanceException);
            }
        }

        return records;
    }

    private void resetInvalidOffsets(InvalidOffsetException e) {
        Set<TopicPartition> partitions = e.partitions();
        Set<string> loggedTopics = new HashSet<>();
        Set<TopicPartition> seekToBeginning = new HashSet<>();
        Set<TopicPartition> seekToEnd = new HashSet<>();

        for (TopicPartition partition : partitions) {
            if (builder.earliestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToBeginning, "Setting topic '{}' to consume from {} offset", "earliest", loggedTopics);
            } else if (builder.latestResetTopicsPattern().matcher(partition.topic()).matches()) {
                addToResetList(partition, seekToEnd, "Setting topic '{}' to consume from {} offset", "latest", loggedTopics);
            } else {
                if (originalReset == null || (!originalReset.Equals("earliest") && !originalReset.Equals("latest"))) {
                    string errorMessage = "No valid committed offset found for input topic %s (partition %s) and no valid reset policy configured." +
                        " You need to set configuration parameter \"auto.offset.reset\" or specify a topic specific reset " +
                        "policy via StreamsBuilder#stream(..., Consumed.with(Topology.AutoOffsetReset)) or StreamsBuilder#table(..., Consumed.with(Topology.AutoOffsetReset))";
                    throw new StreamsException(string.format(errorMessage, partition.topic(), partition.partition()), e);
                }

                if (originalReset.Equals("earliest")) {
                    addToResetList(partition, seekToBeginning, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "earliest", loggedTopics);
                } else if (originalReset.Equals("latest")) {
                    addToResetList(partition, seekToEnd, "No custom setting defined for topic '{}' using original config '{}' for offset reset", "latest", loggedTopics);
                }
            }
        }

        if (!seekToBeginning.isEmpty()) {
            consumer.seekToBeginning(seekToBeginning);
        }
        if (!seekToEnd.isEmpty()) {
            consumer.seekToEnd(seekToEnd);
        }
    }

    private void addToResetList(TopicPartition partition, Set<TopicPartition> partitions, string logMessage, string resetPolicy, Set<string> loggedTopics) {
        string topic = partition.topic();
        if (loggedTopics.add(topic)) {
            log.info(logMessage, topic, resetPolicy);
        }
        partitions.add(partition);
    }

    /**
     * Take records and add them to each respective task
     *
     * @param records Records, can be null
     */
    private void addRecordsToTasks(ConsumerRecords<byte[], byte[]> records) {

        for (TopicPartition partition : records.partitions()) {
            StreamTask task = taskManager.activeTask(partition);

            if (task == null) {
                log.error(
                    "Unable to locate active task for received-record partition {}. Current tasks: {}",
                    partition,
                    taskManager.toString(">")
                );
                throw new NullPointerException("Task was unexpectedly missing for partition " + partition);
            } else if (task.isClosed()) {
                log.info("Stream task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                             "Notifying the thread to trigger a new rebalance immediately.", task.id());
                throw new TaskMigratedException(task);
            }

            task.addRecords(partition, records.records(partition));
        }
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    private bool maybePunctuate() {
        int punctuated = taskManager.punctuate();
        if (punctuated > 0) {
            long punctuateLatency = advanceNowAndComputeLatency();
            punctuateSensor.record(punctuateLatency / (double) punctuated, now);
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
    bool maybeCommit() {
        int committed = 0;

        if (now - lastCommitMs > commitTimeMs) {
            if (log.isTraceEnabled()) {
                log.trace("Committing all active tasks {} and standby tasks {} since {}ms has elapsed (commit interval is {}ms)",
                    taskManager.activeTaskIds(), taskManager.standbyTaskIds(), now - lastCommitMs, commitTimeMs);
            }

            committed += taskManager.commitAll();
            if (committed > 0) {
                long intervalCommitLatency = advanceNowAndComputeLatency();
                commitSensor.record(intervalCommitLatency / (double) committed, now);

                // try to purge the committed records for repartition topics if possible
                taskManager.maybePurgeCommitedRecords();

                if (log.isDebugEnabled()) {
                    log.debug("Committed all active tasks {} and standby tasks {} in {}ms",
                        taskManager.activeTaskIds(), taskManager.standbyTaskIds(), intervalCommitLatency);
                }
            }

            lastCommitMs = now;
            processStandbyRecords = true;
        } else {
            int commitPerRequested = taskManager.maybeCommitActiveTasksPerUserRequested();
            if (commitPerRequested > 0) {
                long requestCommitLatency = advanceNowAndComputeLatency();
                commitSensor.record(requestCommitLatency / (double) committed, now);
                committed += commitPerRequested;
            }
        }

        return committed > 0;
    }

    private void maybeUpdateStandbyTasks() {
        if (state == State.RUNNING && taskManager.hasStandbyRunningTasks()) {
            if (processStandbyRecords) {
                if (!standbyRecords.isEmpty()) {
                    Dictionary<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> remainingStandbyRecords = new HashMap<>();

                    for (Map.Entry<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> entry : standbyRecords.entrySet()) {
                        TopicPartition partition = entry.getKey();
                        List<ConsumerRecord<byte[], byte[]>> remaining = entry.getValue();
                        if (remaining != null) {
                            StandbyTask task = taskManager.standbyTask(partition);

                            if (task.isClosed()) {
                                log.info("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                    "Notifying the thread to trigger a new rebalance immediately.", task.id());
                                throw new TaskMigratedException(task);
                            }

                            remaining = task.update(partition, remaining);
                            if (!remaining.isEmpty()) {
                                remainingStandbyRecords.put(partition, remaining);
                            } else {
                                restoreConsumer.resume(singleton(partition));
                            }
                        }
                    }

                    standbyRecords = remainingStandbyRecords;

                    if (log.isDebugEnabled()) {
                        log.debug("Updated standby tasks {} in {}ms", taskManager.standbyTaskIds(), time.milliseconds() - now);
                    }
                }
                processStandbyRecords = false;
            }

            try {
                // poll(0): Since this is during the normal processing, not during restoration.
                // We can afford to have slower restore (because we don't wait inside poll for results).
                // Instead, we want to proceed to the next iteration to call the main consumer#poll()
                // as soon as possible so as to not be kicked out of the group.
                ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(Duration.ZERO);

                if (!records.isEmpty()) {
                    for (TopicPartition partition : records.partitions()) {
                        StandbyTask task = taskManager.standbyTask(partition);

                        if (task == null) {
                            throw new StreamsException(logPrefix + "Missing standby task for partition " + partition);
                        }

                        if (task.isClosed()) {
                            log.info("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                                "Notifying the thread to trigger a new rebalance immediately.", task.id());
                            throw new TaskMigratedException(task);
                        }

                        List<ConsumerRecord<byte[], byte[]>> remaining = task.update(partition, records.records(partition));
                        if (!remaining.isEmpty()) {
                            restoreConsumer.pause(singleton(partition));
                            standbyRecords.put(partition, remaining);
                        }
                    }
                }
            } catch (InvalidOffsetException recoverableException) {
                log.warn("Updating StandbyTasks failed. Deleting StandbyTasks stores to recreate from scratch.", recoverableException);
                Set<TopicPartition> partitions = recoverableException.partitions();
                for (TopicPartition partition : partitions) {
                    StandbyTask task = taskManager.standbyTask(partition);

                    if (task.isClosed()) {
                        log.info("Standby task {} is already closed, probably because it got unexpectedly migrated to another thread already. " +
                            "Notifying the thread to trigger a new rebalance immediately.", task.id());
                        throw new TaskMigratedException(task);
                    }

                    log.info("Reinitializing StandbyTask {} from changelogs {}", task, recoverableException.partitions());
                    task.reinitializeStateStoresForPartitions(recoverableException.partitions());
                }
                restoreConsumer.seekToBeginning(partitions);
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
    private long advanceNowAndComputeLatency() {
        long previous = now;
        now = time.milliseconds();

        return Math.max(now - previous, 0);
    }

    /**
     * Shutdown this stream thread.
     * <p>
     * Note that there is nothing to prevent this function from being called multiple times
     * (e.g., in testing), hence the state is set only the first time
     */
    public void shutdown() {
        log.info("Informed to shut down");
        State oldState = setState(State.PENDING_SHUTDOWN);
        if (oldState == State.CREATED) {
            // The thread may not have been started. Take responsibility for shutting down
            completeShutdown(true);
        }
    }

    private void completeShutdown(bool cleanRun) {
        // set the state to pending shutdown first as it may be called due to error;
        // its state may already be PENDING_SHUTDOWN so it will return false but we
        // intentionally do not check the returned flag
        setState(State.PENDING_SHUTDOWN);

        log.info("Shutting down");

        try {
            taskManager.shutdown(cleanRun);
        } catch (Throwable e) {
            log.error("Failed to close task manager due to the following error:", e);
        }
        try {
            consumer.close();
        } catch (Throwable e) {
            log.error("Failed to close consumer due to the following error:", e);
        }
        try {
            restoreConsumer.close();
        } catch (Throwable e) {
            log.error("Failed to close restore consumer due to the following error:", e);
        }
        streamsMetrics.removeAllThreadLevelSensors();

        setState(State.DEAD);
        log.info("Shutdown complete");
    }

    private void clearStandbyRecords() {
        standbyRecords.clear();
    }

    /**
     * Return information about the current {@link StreamThread}.
     *
     * @return {@link ThreadMetadata}.
     */
    public ThreadMetadata threadMetadata() {
        return threadMetadata;
    }

    // package-private for testing only
    StreamThread updateThreadMetadata(string adminClientId) {

        threadMetadata = new ThreadMetadata(
            this.getName(),
            this.state().name(),
            getConsumerClientId(this.getName()),
            getRestoreConsumerClientId(this.getName()),
            producer == null ? Collections.emptySet() : Collections.singleton(getThreadProducerClientId(this.getName())),
            adminClientId,
            Collections.emptySet(),
            Collections.emptySet());

        return this;
    }

    private void updateThreadMetadata(Dictionary<TaskId, StreamTask> activeTasks,
                                      Dictionary<TaskId, StandbyTask> standbyTasks) {
        Set<string> producerClientIds = new HashSet<>();
        Set<TaskMetadata> activeTasksMetadata = new HashSet<>();
        for (Map.Entry<TaskId, StreamTask> task : activeTasks.entrySet()) {
            activeTasksMetadata.add(new TaskMetadata(task.getKey().toString(), task.getValue().partitions()));
            producerClientIds.add(getTaskProducerClientId(getName(), task.getKey()));
        }
        Set<TaskMetadata> standbyTasksMetadata = new HashSet<>();
        for (Map.Entry<TaskId, StandbyTask> task : standbyTasks.entrySet()) {
            standbyTasksMetadata.add(new TaskMetadata(task.getKey().toString(), task.getValue().partitions()));
        }

        string adminClientId = threadMetadata.adminClientId();
        threadMetadata = new ThreadMetadata(
            this.getName(),
            this.state().name(),
            getConsumerClientId(this.getName()),
            getRestoreConsumerClientId(this.getName()),
            producer == null ? producerClientIds : Collections.singleton(getThreadProducerClientId(this.getName())),
            adminClientId,
            activeTasksMetadata,
            standbyTasksMetadata);
    }

    public Dictionary<TaskId, StreamTask> tasks() {
        return taskManager.activeTasks();
    }

    /**
     * Produces a string representation containing useful information about a StreamThread.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamThread instance.
     */
    @Override
    public string toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about a StreamThread, starting with the given indent.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamThread instance.
     */
    public string toString(string indent) {
        return indent + "\tStreamsThread threadId: " + getName() + "\n" + taskManager.toString(indent);
    }

    public Dictionary<MetricName, Metric> producerMetrics() {
        LinkedHashMap<MetricName, Metric> result = new LinkedHashMap<>();
        if (producer != null) {
            Dictionary<MetricName, ? : Metric> producerMetrics = producer.metrics();
            if (producerMetrics != null) {
                result.putAll(producerMetrics);
            }
        } else {
            // When EOS is turned on, each task will have its own producer client
            // and the producer object passed in here will be null. We would then iterate through
            // all the active tasks and add their metrics to the output metrics map.
            for (StreamTask task: taskManager.activeTasks().values()) {
                Dictionary<MetricName, ? : Metric> taskProducerMetrics = task.getProducer().metrics();
                result.putAll(taskProducerMetrics);
            }
        }
        return result;
    }

    public Dictionary<MetricName, Metric> consumerMetrics() {
        Dictionary<MetricName, ? : Metric> consumerMetrics = consumer.metrics();
        Dictionary<MetricName, ? : Metric> restoreConsumerMetrics = restoreConsumer.metrics();
        LinkedHashMap<MetricName, Metric> result = new LinkedHashMap<>();
        result.putAll(consumerMetrics);
        result.putAll(restoreConsumerMetrics);
        return result;
    }

    public Dictionary<MetricName, Metric> adminClientMetrics() {
        Dictionary<MetricName, ? : Metric> adminClientMetrics = taskManager.getAdminClient().metrics();
        LinkedHashMap<MetricName, Metric> result = new LinkedHashMap<>();
        result.putAll(adminClientMetrics);
        return result;
    }

    // the following are for testing only
    void setNow(long now) {
        this.now = now;
    }

    TaskManager taskManager() {
        return taskManager;
    }

    Dictionary<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> standbyRecords() {
        return standbyRecords;
    }

    int currentNumIterations() {
        return numIterations;
    }

    public StreamThread.StateListener stateListener() {
        return stateListener;
    }
}
