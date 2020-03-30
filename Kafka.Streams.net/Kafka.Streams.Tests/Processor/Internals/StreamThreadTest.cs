/*






 *

 *





 */































































































public class StreamThreadTest {

    private readonly string clientId = "clientId";
    private readonly string applicationId = "stream-thread-test";
    private readonly int threadIdx = 1;
    private MockTime mockTime = new MockTime();
    private Metrics metrics = new Metrics();
    private MockClientSupplier clientSupplier = new MockClientSupplier();
    private InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());
    private StreamsConfig config = new StreamsConfig(configProps(false));
    private readonly string stateDir = TestUtils.tempDirectory().getPath();
    private StateDirectory stateDirectory = new StateDirectory(config, mockTime, true);
    private ConsumedInternal<object, object> consumed = new ConsumedInternal<>();

    private UUID processId = UUID.randomUUID();
    private InternalTopologyBuilder internalTopologyBuilder;
    private StreamsMetadataState streamsMetadataState;

    
    public void SetUp() {
        processId = UUID.randomUUID();

        internalTopologyBuilder = InternalStreamsBuilderTest.internalTopologyBuilder(internalStreamsBuilder);
        internalTopologyBuilder.setApplicationId(applicationId);
        streamsMetadataState = new StreamsMetadataState(internalTopologyBuilder, StreamsMetadataState.UNKNOWN_HOST);
    }

    private readonly string topic1 = "topic1";
    private readonly string topic2 = "topic2";

    private TopicPartition t1p1 = new TopicPartition(topic1, 1);
    private TopicPartition t1p2 = new TopicPartition(topic1, 2);
    private TopicPartition t2p1 = new TopicPartition(topic2, 1);

    // task0 is unused
    private TaskId task1 = new TaskId(0, 1);
    private TaskId task2 = new TaskId(0, 2);
    private TaskId task3 = new TaskId(1, 1);

    private Properties ConfigProps(bool enableEoS) {
        return mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:2171"),
            mkEntry(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3"),
            mkEntry(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.getName()),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath()),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, enableEoS ? StreamsConfig.EXACTLY_ONCE : StreamsConfig.AT_LEAST_ONCE)
        ));
    }

    [Xunit.Fact]
    public void TestPartitionAssignmentChangeForSingleGroup() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        StreamThread thread = CreateStreamThread(clientId, config, false);

        StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);
        Assert.Equal(thread.state(), StreamThread.State.CREATED);

        ConsumerRebalanceListener rebalanceListener = thread.rebalanceListener;

        List<TopicPartition> revokedPartitions;
        List<TopicPartition> assignedPartitions;

        // revoke nothing
        thread.setState(StreamThread.State.STARTING);
        revokedPartitions = Collections.emptyList();
        rebalanceListener.onPartitionsRevoked(revokedPartitions);

        Assert.Equal(thread.state(), StreamThread.State.PARTITIONS_REVOKED);

        // assign single partition
        assignedPartitions = singletonList(t1p1);
        thread.taskManager().setAssignmentMetadata(Collections.emptyMap(), Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce();
        Assert.Equal(thread.state(), StreamThread.State.RUNNING);
        Assert.Equal(4, stateListener.numChanges);
        Assert.Equal(StreamThread.State.PARTITIONS_ASSIGNED, stateListener.oldState);

        thread.shutdown();
        assertSame(StreamThread.State.PENDING_SHUTDOWN, thread.state());
    }

    [Xunit.Fact]
    public void TestStateChangeStartClose() {// throws Exception
        StreamThread thread = CreateStreamThread(clientId, config, false);

        StateListenerStub stateListener = new StateListenerStub();
        thread.setStateListener(stateListener);

        thread.start();
        TestUtils.waitForCondition(
            () => thread.state() == StreamThread.State.STARTING,
            10 * 1000,
            "Thread never started.");

        thread.shutdown();
        TestUtils.waitForCondition(
            () => thread.state() == StreamThread.State.DEAD,
            10 * 1000,
            "Thread never shut down.");

        thread.shutdown();
        Assert.Equal(thread.state(), StreamThread.State.DEAD);
    }

    private Cluster CreateCluster() {
        Node node = new Node(0, "localhost", 8121);
        return new Cluster(
            "mockClusterId",
            singletonList(node),
            Collections.emptySet(),
            Collections.emptySet(),
            Collections.emptySet(),
            node
        );
    }

    private StreamThread CreateStreamThread( string clientId,
                                            StreamsConfig config,
                                            bool eosEnabled) {
        if (eosEnabled) {
            clientSupplier.setApplicationIdForProducer(applicationId);
        }

        clientSupplier.setClusterForAdminClient(CreateCluster());

        return StreamThread.create(
            internalTopologyBuilder,
            config,
            clientSupplier,
            clientSupplier.getAdminClient(config.getAdminConfigs(clientId)),
            processId,
            clientId,
            metrics,
            mockTime,
            streamsMetadataState,
            0,
            stateDirectory,
            new MockStateRestoreListener(),
            threadIdx);
    }

    [Xunit.Fact]
    public void TestMetricsCreatedAtStartup() {
        StreamThread thread = CreateStreamThread(clientId, config, false);
        string defaultGroupName = "stream-metrics";
        Dictionary<string, string> defaultTags = Collections.singletonMap("client-id", thread.getName());
        string descriptionIsNotVerified = "";

        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "poll-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "process-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "punctuate-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "task-created-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "task-created-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "task-closed-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "task-closed-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "skipped-records-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "skipped-records-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));

        string taskGroupName = "stream-task-metrics";
        Dictionary<string, string> taskTags =
            mkMap(mkEntry("task-id", "all"), mkEntry("client-id", thread.getName()));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-latency-avg", taskGroupName, descriptionIsNotVerified, taskTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-latency-max", taskGroupName, descriptionIsNotVerified, taskTags)));
        assertNotNull(metrics.metrics().get(metrics.metricName(
            "commit-rate", taskGroupName, descriptionIsNotVerified, taskTags)));

        JmxReporter reporter = new JmxReporter("kafka.streams");
        metrics.addReporter(reporter);
        Assert.Equal(clientId + "-StreamThread-1", thread.getName());
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=%s,client-id=%s",
                   defaultGroupName, 
                   thread.getName())));
        Assert.True(reporter.containsMbean("kafka.streams:type=stream-task-metrics,client-id=" + thread.getName() + ",task-id=all"));
    }

    [Xunit.Fact]
    public void ShouldNotCommitBeforeTheCommitInterval() {
        long commitInterval = 1000L;
        Properties props = ConfigProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.toString(commitInterval));

        StreamsConfig config = new StreamsConfig(props);
        Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
        TaskManager taskManager = MockTaskManagerCommit(consumer, 1, 1);

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""),
            new AtomicInteger()
        );
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        mockTime.sleep(commitInterval - 10L);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        EasyMock.verify(taskManager);
    }

    [Xunit.Fact]
    public void ShouldRespectNumIterationsInMainLoop() {
        MockProcessor mockProcessor = new MockProcessor(PunctuationType.WALL_CLOCK_TIME, 10L);
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
        internalTopologyBuilder.addProcessor("processor1", () => mockProcessor, "source1");
        internalTopologyBuilder.addProcessor("processor2", () => new MockProcessor(PunctuationType.STREAM_TIME, 10L), "source1");

        Properties properties = new Properties();
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig(applicationId,
            "localhost:2171",
            Serdes.ByteArraySerde.getName(),
            Serdes.ByteArraySerde.getName(),
            properties));
        StreamThread thread = CreateStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        HashSet<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
        thread.taskManager().setAssignmentMetadata(
            Collections.singletonMap(
                new TaskId(0, t1p1.partition()),
                assignedPartitions),
            Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce();

        // processed one record, punctuated after the first record, and hence num.iterations is still 1
        long offset = -1;
        addRecord(mockConsumer, ++offset, 0L);
        thread.runOnce();

        Assert.Equal(thread.currentNumIterations(), (1));

        // processed one more record without punctuation, and bump num.iterations to 2
        addRecord(mockConsumer, ++offset, 1L);
        thread.runOnce();

        Assert.Equal(thread.currentNumIterations(), (2));

        // processed zero records, early exit and iterations stays as 2
        thread.runOnce();
        Assert.Equal(thread.currentNumIterations(), (2));

        // system time based punctutation halves to 1
        mockTime.sleep(11L);

        thread.runOnce();
        Assert.Equal(thread.currentNumIterations(), (1));

        // processed two records, bumping up iterations to 2
        addRecord(mockConsumer, ++offset, 5L);
        addRecord(mockConsumer, ++offset, 6L);
        thread.runOnce();

        Assert.Equal(thread.currentNumIterations(), (2));

        // stream time based punctutation halves to 1
        addRecord(mockConsumer, ++offset, 11L);
        thread.runOnce();

        Assert.Equal(thread.currentNumIterations(), (1));

        // processed three records, bumping up iterations to 3 (1 + 2)
        addRecord(mockConsumer, ++offset, 12L);
        addRecord(mockConsumer, ++offset, 13L);
        addRecord(mockConsumer, ++offset, 14L);
        thread.runOnce();

        Assert.Equal(thread.currentNumIterations(), (3));

        mockProcessor.requestCommit();
        addRecord(mockConsumer, ++offset, 15L);
        thread.runOnce();

        // user requested commit should not impact on iteration adjustment
        Assert.Equal(thread.currentNumIterations(), (3));

        // time based commit, halves iterations to 3 / 2 = 1
        mockTime.sleep(90L);
        thread.runOnce();

        Assert.Equal(thread.currentNumIterations(), (1));

    }

    [Xunit.Fact]
    public void ShouldNotCauseExceptionIfNothingCommitted() {
        long commitInterval = 1000L;
        Properties props = ConfigProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.toString(commitInterval));

        StreamsConfig config = new StreamsConfig(props);
        Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
        TaskManager taskManager = MockTaskManagerCommit(consumer, 1, 0);

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""),
            new AtomicInteger()
        );
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        mockTime.sleep(commitInterval - 10L);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        EasyMock.verify(taskManager);
    }

    [Xunit.Fact]
    public void ShouldCommitAfterTheCommitInterval() {
        long commitInterval = 1000L;
        Properties props = ConfigProps(false);
        props.setProperty(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.toString(commitInterval));

        StreamsConfig config = new StreamsConfig(props);
        Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
        TaskManager taskManager = MockTaskManagerCommit(consumer, 2, 1);

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""),
            new AtomicInteger()
        );
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();
        mockTime.sleep(commitInterval + 1);
        thread.setNow(mockTime.milliseconds());
        thread.maybeCommit();

        EasyMock.verify(taskManager);
    }

    private TaskManager MockTaskManagerCommit(Consumer<byte[], byte[]> consumer,
                                              int numberOfCommits,
                                              int commits) {
        TaskManager taskManager = EasyMock.createNiceMock(TaskManager);
        EasyMock.expect(taskManager.commitAll()).andReturn(commits).times(numberOfCommits);
        EasyMock.replay(taskManager, consumer);
        return taskManager;
    }

    [Xunit.Fact]
    public void ShouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
        internalStreamsBuilder.buildAndOptimizeTopology();

        StreamThread thread = CreateStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.emptyList());

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        Dictionary<TopicPartition, long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);
        thread.rebalanceListener.onPartitionsAssigned(new HashSet<>(assignedPartitions));

        Assert.Equal(1, clientSupplier.producers.Count);
        Producer globalProducer = clientSupplier.producers.get(0);
        foreach (Task task in thread.tasks().values()) {
            assertSame(globalProducer, ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer());
        }
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    [Xunit.Fact]
    public void ShouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        StreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.emptyList());

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        Dictionary<TopicPartition, long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);
        thread.rebalanceListener.onPartitionsAssigned(new HashSet<>(assignedPartitions));

        thread.runOnce();

        Assert.Equal(thread.tasks().Count, clientSupplier.producers.Count);
        assertSame(clientSupplier.consumer, thread.consumer);
        assertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
    }

    [Xunit.Fact]
    public void ShouldCloseAllTaskProducersOnCloseIfEosEnabled() {
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        StreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.emptyList());

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        assignedPartitions.add(t1p2);
        activeTasks.put(task1, Collections.singleton(t1p1));
        activeTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());
        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        Dictionary<TopicPartition, long> beginOffsets = new HashMap<>();
        beginOffsets.put(t1p1, 0L);
        beginOffsets.put(t1p2, 0L);
        mockConsumer.updateBeginningOffsets(beginOffsets);

        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.shutdown();
        thread.run();

        foreach (Task task in thread.tasks().values()) {
            Assert.True(((MockProducer) ((RecordCollectorImpl) ((StreamTask) task).recordCollector()).producer()).closed());
        }
    }

    [Xunit.Fact]
    public void ShouldShutdownTaskManagerOnClose() {
        Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
        TaskManager taskManager = EasyMock.createNiceMock(TaskManager);
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""),
            new AtomicInteger()
        ).updateThreadMetadata(getSharedAdminClientId(clientId));
        thread.setStateListener(
            (t, newState, oldState) => {
                if (oldState == StreamThread.State.CREATED && newState == StreamThread.State.STARTING) {
                    thread.shutdown();
                }
            });
        thread.run();
        EasyMock.verify(taskManager);
    }

    [Xunit.Fact]
    public void ShouldShutdownTaskManagerOnCloseWithoutStart() {
        Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
        TaskManager taskManager = EasyMock.createNiceMock(TaskManager);
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""),
            new AtomicInteger()
        ).updateThreadMetadata(getSharedAdminClientId(clientId));
        thread.shutdown();
        EasyMock.verify(taskManager);
    }

    [Xunit.Fact]
    public void ShouldNotThrowWhenPendingShutdownInRunOnce() {
        MockRunOnce(true);
    }

    [Xunit.Fact]
    public void ShouldNotThrowWithoutPendingShutdownInRunOnce() {
        // A reference test to verify that without intermediate shutdown the runOnce should pass
        // without any exception.
        MockRunOnce(false);
    }

    private void MockRunOnce(bool shutdownOnPoll) {
        Collection<TopicPartition> assignedPartitions = Collections.singletonList(t1p1);
        class MockStreamThreadConsumer<K, V> : MockConsumer<K, V> {

            private StreamThread streamThread;

            private MockStreamThreadConsumer(OffsetResetStrategy offsetResetStrategy) {
                super(offsetResetStrategy);
            }

            
            public synchronized ConsumerRecords<K, V> poll(Duration timeout) {
                assertNotNull(streamThread);
                if (shutdownOnPoll) {
                    streamThread.shutdown();
                }
                streamThread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
                return base.poll(timeout);
            }

            private void SetStreamThread(StreamThread streamThread) {
                this.streamThread = streamThread;
            }
        }

    readonly MockStreamThreadConsumer<byte[], byte[]> mockStreamThreadConsumer =
            new MockStreamThreadConsumer<>(OffsetResetStrategy.EARLIEST);

        TaskManager taskManager = new TaskManager(new MockChangelogReader(),
                                                        processId,
                                                        "log-prefix",
                                                        mockStreamThreadConsumer,
                                                        streamsMetadataState,
                                                        null,
                                                        null,
                                                        null,
                                                        new AssignedStreamsTasks(new LogContext()),
                                                        new AssignedStandbyTasks(new LogContext()));
        taskManager.setConsumer(mockStreamThreadConsumer);
        taskManager.setAssignmentMetadata(Collections.emptyMap(), Collections.emptyMap());

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            mockStreamThreadConsumer,
            mockStreamThreadConsumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""),
            new AtomicInteger()
        ).updateThreadMetadata(getSharedAdminClientId(clientId));

        mockStreamThreadConsumer.setStreamThread(thread);
        mockStreamThreadConsumer.assign(assignedPartitions);
        mockStreamThreadConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

        addRecord(mockStreamThreadConsumer, 1L, 0L);
        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);
        thread.runOnce();
    }

    [Xunit.Fact]
    public void ShouldOnlyShutdownOnce() {
        Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
        TaskManager taskManager = EasyMock.createNiceMock(TaskManager);
        taskManager.shutdown(true);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
            mockTime,
            config,
            null,
            consumer,
            consumer,
            null,
            taskManager,
            streamsMetrics,
            internalTopologyBuilder,
            clientId,
            new LogContext(""),
            new AtomicInteger()
        ).updateThreadMetadata(getSharedAdminClientId(clientId));
        thread.shutdown();
        // Execute the run method. Verification of the mock will check that shutdown was only done once
        thread.run();
        EasyMock.verify(taskManager);
    }

    [Xunit.Fact]
    public void ShouldNotNullPointerWhenStandbyTasksAssignedAndNoStateStoresForTopology() {
        internalTopologyBuilder.addSource(null, "name", null, null, null, "topic");
        internalTopologyBuilder.addSink("out", "output", null, null, null, "name");

        StreamThread thread = createStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(Collections.emptyList());

        Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(Collections.emptyMap(), standbyTasks);
        thread.taskManager().createTasks(Collections.emptyList());

        thread.rebalanceListener.onPartitionsAssigned(Collections.emptyList());
    }

    [Xunit.Fact]
    public void ShouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerWasFencedWhileProcessing() {// throws Exception
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);
        internalTopologyBuilder.addSink("sink", "dummyTopic", null, null, null, "source");

        StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        MockConsumer<byte[], byte[]> consumer = clientSupplier.consumer;

        consumer.updatePartitions(topic1, singletonList(new PartitionInfo(topic1, 1, null, null, null)));

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce();
        Assert.Equal(thread.tasks().Count, (1));
        MockProducer producer = clientSupplier.producers.get(0);

        // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
        consumer.updateBeginningOffsets(Collections.singletonMap(assignedPartitions.iterator().next(), 0L));
        consumer.unsubscribe();
        consumer.assign(new HashSet<>(assignedPartitions));

        consumer.addRecord(new ConsumeResult<>(topic1, 1, 0, new byte[0], new byte[0]));
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1);
        thread.runOnce();
        Assert.Equal(producer.history().Count, (1));

        Assert.False(producer.transactionCommitted());
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        TestUtils.waitForCondition(
            () => producer.commitCount() == 1,
            "StreamsThread did not commit transaction.");

        producer.fenceProducer();
        mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        consumer.addRecord(new ConsumeResult<>(topic1, 1, 1, new byte[0], new byte[0]));
        try {
            thread.runOnce();
            Assert.True(false, "Should have thrown TaskMigratedException");
        } catch (TaskMigratedException expected) { /* ignore */ }
        TestUtils.waitForCondition(
            () => thread.tasks().isEmpty(),
            "StreamsThread did not remove fenced zombie task.");

        Assert.Equal(producer.commitCount(), (1L));
    }

    [Xunit.Fact]
    public void ShouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedInCommitTransactionWhenSuspendingTaks() {
        StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        internalTopologyBuilder.addSource(null, "name", null, null, null, topic1);
        internalTopologyBuilder.addSink("out", "output", null, null, null, "name");

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce();

        Assert.Equal(thread.tasks().Count, (1));

        clientSupplier.producers.get(0).fenceProducer();
        thread.rebalanceListener.onPartitionsRevoked(null);
        Assert.True(clientSupplier.producers.get(0).transactionInFlight());
        Assert.False(clientSupplier.producers.get(0).transactionCommitted());
        Assert.True(clientSupplier.producers.get(0).closed());
        Assert.True(thread.tasks().isEmpty());
    }

    [Xunit.Fact]
    public void ShouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedInCloseTransactionWhenSuspendingTasks() {
        StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        internalTopologyBuilder.addSource(null, "name", null, null, null, topic1);
        internalTopologyBuilder.addSink("out", "output", null, null, null, "name");

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce();

        Assert.Equal(thread.tasks().Count, (1));

        clientSupplier.producers.get(0).fenceProducerOnClose();
        thread.rebalanceListener.onPartitionsRevoked(null);

        Assert.False(clientSupplier.producers.get(0).transactionInFlight());
        Assert.True(clientSupplier.producers.get(0).transactionCommitted());
        Assert.False(clientSupplier.producers.get(0).closed());
        Assert.True(thread.tasks().isEmpty());
    }

    private static class StateListenerStub : StreamThread.StateListener {
        int numChanges = 0;
        ThreadStateTransitionValidator oldState = null;
        ThreadStateTransitionValidator newState = null;

        
        public void OnChange(Thread thread,
                             ThreadStateTransitionValidator newState,
                             ThreadStateTransitionValidator oldState) {
            ++numChanges;
            if (this.newState != null) {
                if (this.newState != oldState) {
                    throw new RuntimeException("State mismatch " + oldState + " different from " + this.newState);
                }
            }
            this.oldState = oldState;
            this.newState = newState;
        }
    }

    [Xunit.Fact]
    public void ShouldReturnActiveTaskMetadataWhileRunningState() {
        internalTopologyBuilder.addSource(null, "source", null, null, null, topic1);

        StreamThread thread = createStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(assignedPartitions);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce();

        ThreadMetadata threadMetadata = thread.threadMetadata();
        Assert.Equal(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        Assert.True(threadMetadata.activeTasks().Contains(new TaskMetadata(task1.toString(), Utils.mkSet(t1p1))));
        Assert.True(threadMetadata.standbyTasks().isEmpty());
    }

    [Xunit.Fact]
    public void ShouldReturnStandbyTaskMetadataWhileRunningState() {
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
            .groupByKey().count(Materialized.As("count-one"));

        internalStreamsBuilder.buildAndOptimizeTopology();
        StreamThread thread = createStreamThread(clientId, config, false);
        MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions(
            "stream-thread-test-count-one-changelog",
            singletonList(
                new PartitionInfo("stream-thread-test-count-one-changelog",
                0,
                null,
                new Node[0],
                new Node[0])
            )
        );

        HashDictionary<TopicPartition, long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
        restoreConsumer.updateEndOffsets(offsets);
        restoreConsumer.updateBeginningOffsets(offsets);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(Collections.emptyMap(), standbyTasks);

        thread.rebalanceListener.onPartitionsAssigned(Collections.emptyList());

        thread.runOnce();

        ThreadMetadata threadMetadata = thread.threadMetadata();
        Assert.Equal(StreamThread.State.RUNNING.name(), threadMetadata.threadState());
        Assert.True(threadMetadata.standbyTasks().Contains(new TaskMetadata(task1.toString(), Utils.mkSet(t1p1))));
        Assert.True(threadMetadata.activeTasks().isEmpty());
    }

    
    [Xunit.Fact]
    public void ShouldUpdateStandbyTask() {// throws Exception
        string storeName1 = "count-one";
        string storeName2 = "table-two";
        string changelogName1 = applicationId + "-" + storeName1 + "-changelog";
        string changelogName2 = applicationId + "-" + storeName2 + "-changelog";
        TopicPartition partition1 = new TopicPartition(changelogName1, 1);
        TopicPartition partition2 = new TopicPartition(changelogName2, 1);
        internalStreamsBuilder
            .stream(Collections.singleton(topic1), consumed)
            .groupByKey()
            .count(Materialized.As(storeName1));
        MaterializedInternal<object, object, KeyValueStore<Bytes, byte[]>> materialized
            = new MaterializedInternal<>(Materialized.As(storeName2), internalStreamsBuilder, "");
        internalStreamsBuilder.table(topic2, new ConsumedInternal<>(), materialized);

        internalStreamsBuilder.buildAndOptimizeTopology();
        StreamThread thread = createStreamThread(clientId, config, false);
        MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions(changelogName1,
            singletonList(
                new PartitionInfo(
                    changelogName1,
                    1,
                    null,
                    new Node[0],
                    new Node[0]
                )
            )
        );

        restoreConsumer.assign(Utils.mkSet(partition1, partition2));
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition1, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(partition1, 0L));
        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition2, 10L));
        restoreConsumer.updateBeginningOffsets(Collections.singletonMap(partition2, 0L));
        // let the store1 be restored from 0 to 10; store2 be restored from 5 (checkpointed) to 10
        OffsetCheckpoint checkpoint
            = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(task3), CHECKPOINT_FILE_NAME));
        checkpoint.write(Collections.singletonMap(partition2, 5L));

        for (long i = 0L; i < 10L; i++) {
            restoreConsumer.addRecord(new ConsumeResult<>(
                changelogName1,
                1,
                i,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
            restoreConsumer.addRecord(new ConsumeResult<>(
                changelogName2,
                1,
                i,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
        }

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(null);

        Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        standbyTasks.put(task1, Collections.singleton(t1p1));
        standbyTasks.put(task3, Collections.singleton(t2p1));

        thread.taskManager().setAssignmentMetadata(Collections.emptyMap(), standbyTasks);

        thread.rebalanceListener.onPartitionsAssigned(Collections.emptyList());

        thread.runOnce();

        StandbyTask standbyTask1 = thread.taskManager().standbyTask(partition1);
        StandbyTask standbyTask2 = thread.taskManager().standbyTask(partition2);
        KeyValueStore<object, long> store1 = (KeyValueStore<object, long>) standbyTask1.getStore(storeName1);
        KeyValueStore<object, long> store2 = (KeyValueStore<object, long>) standbyTask2.getStore(storeName2);

        Assert.Equal(10L, store1.approximateNumEntries());
        Assert.Equal(5L, store2.approximateNumEntries());
        Assert.Equal(0, thread.standbyRecords().Count);
    }

    [Xunit.Fact]
    public void ShouldCreateStandbyTask() {
        setupInternalTopologyWithoutState();
        internalTopologyBuilder.addStateStore(new MockKeyValueStoreBuilder("myStore", true), "processor1");

        StandbyTask standbyTask = createStandbyTask();

        Assert.Equal(standbyTask, not(nullValue()));
    }

    [Xunit.Fact]
    public void ShouldNotCreateStandbyTaskWithoutStateStores() {
        setupInternalTopologyWithoutState();

        StandbyTask standbyTask = createStandbyTask();

        Assert.Equal(standbyTask, nullValue());
    }

    [Xunit.Fact]
    public void ShouldNotCreateStandbyTaskIfStateStoresHaveLoggingDisabled() {
        setupInternalTopologyWithoutState();
        StoreBuilder storeBuilder = new MockKeyValueStoreBuilder("myStore", true);
        storeBuilder.withLoggingDisabled();
        internalTopologyBuilder.addStateStore(storeBuilder, "processor1");

        StandbyTask standbyTask = createStandbyTask();

        Assert.Equal(standbyTask, nullValue());
    }

    private void SetupInternalTopologyWithoutState() {
        MockProcessor mockProcessor = new MockProcessor();
        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);
        internalTopologyBuilder.addProcessor("processor1", () => mockProcessor, "source1");
    }

    private StandbyTask CreateStandbyTask() {
        LogContext logContext = new LogContext("test");
        Logger log = logContext.logger(StreamThreadTest);
        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread.StandbyTaskCreator standbyTaskCreator = new StreamThread.StandbyTaskCreator(
            internalTopologyBuilder,
            config,
            streamsMetrics,
            stateDirectory,
            new MockChangelogReader(),
            mockTime,
            log);
        return standbyTaskCreator.createTask(
            new MockConsumer<>(OffsetResetStrategy.EARLIEST),
            new TaskId(1, 2),
            Collections.emptySet());
    }

    [Xunit.Fact]
    public void ShouldPunctuateActiveTask() {
        List<long> punctuatedStreamTime = new ArrayList<>();
        List<long> punctuatedWallClockTime = new ArrayList<>();
        ProcessorSupplier<object, object> punctuateProcessor = () => new Processor<object, object>() {
            
            public void init(ProcessorContext context) {
                context.schedule(Duration.ofMillis(100L), PunctuationType.STREAM_TIME, punctuatedStreamTime::add);
                context.schedule(Duration.ofMillis(100L), PunctuationType.WALL_CLOCK_TIME, punctuatedWallClockTime::add);
            }

            
            public void process(object key,
                                object value) {}

            
            public void close() {}
        };

        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed).process(punctuateProcessor);
        internalStreamsBuilder.buildAndOptimizeTopology();

        StreamThread thread = createStreamThread(clientId, config, false);

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(null);
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));

        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());

        clientSupplier.consumer.assign(assignedPartitions);
        clientSupplier.consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        thread.runOnce();

        Assert.Equal(0, punctuatedStreamTime.Count);
        Assert.Equal(0, punctuatedWallClockTime.Count);

        mockTime.sleep(100L);
        for (long i = 0L; i < 10L; i++) {
            clientSupplier.consumer.addRecord(new ConsumeResult<>(
                topic1,
                1,
                i,
                i * 100L,
                TimestampType.CreateTime,
                ConsumeResult.NULL_CHECKSUM,
                ("K" + i).getBytes().Length,
                ("V" + i).getBytes().Length,
                ("K" + i).getBytes(),
                ("V" + i).getBytes()));
        }

        thread.runOnce();

        Assert.Equal(1, punctuatedStreamTime.Count);
        Assert.Equal(1, punctuatedWallClockTime.Count);

        mockTime.sleep(100L);

        thread.runOnce();

        // we should skip stream time punctuation, only trigger wall-clock time punctuation
        Assert.Equal(1, punctuatedStreamTime.Count);
        Assert.Equal(2, punctuatedWallClockTime.Count);
    }

    [Xunit.Fact]
    public void ShouldAlwaysUpdateTasksMetadataAfterChangingState() {
        StreamThread thread = createStreamThread(clientId, config, false);
        ThreadMetadata metadata = thread.threadMetadata();
        Assert.Equal(StreamThread.State.CREATED.name(), metadata.threadState());

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);
        thread.setState(StreamThread.State.PARTITIONS_ASSIGNED);
        thread.setState(StreamThread.State.RUNNING);
        metadata = thread.threadMetadata();
        Assert.Equal(StreamThread.State.RUNNING.name(), metadata.threadState());
    }

    [Xunit.Fact]
    public void ShouldAlwaysReturnEmptyTasksMetadataWhileRebalancingStateAndTasksNotRunning() {
        internalStreamsBuilder.stream(Collections.singleton(topic1), consumed)
            .groupByKey().count(Materialized.As("count-one"));
        internalStreamsBuilder.buildAndOptimizeTopology();

        StreamThread thread = createStreamThread(clientId, config, false);
        MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
            asList(
                new PartitionInfo("stream-thread-test-count-one-changelog",
                    0,
                    null,
                    new Node[0],
                    new Node[0]),
                new PartitionInfo("stream-thread-test-count-one-changelog",
                    1,
                    null,
                    new Node[0],
                    new Node[0])
            ));
        HashDictionary<TopicPartition, long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 0), 0L);
        offsets.put(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
        restoreConsumer.updateEndOffsets(offsets);
        restoreConsumer.updateBeginningOffsets(offsets);

        clientSupplier.consumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

        List<TopicPartition> assignedPartitions = new ArrayList<>();

        thread.setState(StreamThread.State.STARTING);
        thread.rebalanceListener.onPartitionsRevoked(assignedPartitions);
        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_REVOKED);

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks = new HashMap<>();

        // assign single partition
        assignedPartitions.add(t1p1);
        activeTasks.put(task1, Collections.singleton(t1p1));
        standbyTasks.put(task2, Collections.singleton(t1p2));

        thread.taskManager().setAssignmentMetadata(activeTasks, standbyTasks);

        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);

        assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThread.State.PARTITIONS_ASSIGNED);
    }

    [Xunit.Fact]
    public void ShouldRecoverFromInvalidOffsetExceptionOnRestoreAndFinishRestore() {// throws Exception
        internalStreamsBuilder.stream(Collections.singleton("topic"), consumed)
            .groupByKey().count(Materialized.As("count"));
        internalStreamsBuilder.buildAndOptimizeTopology();

        StreamThread thread = createStreamThread("clientId", config, false);
        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        MockConsumer<byte[], byte[]> mockRestoreConsumer = (MockConsumer<byte[], byte[]>) thread.restoreConsumer;

        TopicPartition topicPartition = new TopicPartition("topic", 0);
        HashSet<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);

        Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
        activeTasks.put(new TaskId(0, 0), topicPartitionSet);
        thread.taskManager().setAssignmentMetadata(activeTasks, Collections.emptyMap());

        mockConsumer.updatePartitions(
            "topic",
            singletonList(
                new PartitionInfo(
                    "topic",
                    0,
                    null,
                    new Node[0],
                    new Node[0]
                )
            )
        );
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));

        mockRestoreConsumer.updatePartitions(
            "stream-thread-test-count-changelog",
            singletonList(
                new PartitionInfo(
                    "stream-thread-test-count-changelog",
                    0,
                    null,
                    new Node[0],
                    new Node[0]
                )
            )
        );

        TopicPartition changelogPartition = new TopicPartition("stream-thread-test-count-changelog", 0);
        HashSet<TopicPartition> changelogPartitionSet = Collections.singleton(changelogPartition);
        mockRestoreConsumer.updateBeginningOffsets(Collections.singletonMap(changelogPartition, 0L));
        mockRestoreConsumer.updateEndOffsets(Collections.singletonMap(changelogPartition, 2L));

        mockConsumer.schedulePollTask(() => {
            thread.setState(StreamThread.State.PARTITIONS_REVOKED);
            thread.rebalanceListener.onPartitionsAssigned(topicPartitionSet);
        });

        try {
            thread.start();

            TestUtils.waitForCondition(
                () => mockRestoreConsumer.assignment().Count == 1,
                "Never restore first record");

            mockRestoreConsumer.addRecord(new ConsumeResult<>(
                "stream-thread-test-count-changelog",
                0,
                0L,
                "K1".getBytes(),
                "V1".getBytes()));

            TestUtils.waitForCondition(
                () => mockRestoreConsumer.position(changelogPartition) == 1L,
                "Never restore first record");

            mockRestoreConsumer.setException(new InvalidOffsetException("Try Again!") {
                
                public HashSet<TopicPartition> partitions() {
                    return changelogPartitionSet;
                }
            });

            mockRestoreConsumer.addRecord(new ConsumeResult<>(
                "stream-thread-test-count-changelog",
                0,
                0L,
                "K1".getBytes(),
                "V1".getBytes()));
            mockRestoreConsumer.addRecord(new ConsumeResult<>(
                "stream-thread-test-count-changelog",
                0,
                1L,
                "K2".getBytes(),
                "V2".getBytes()));

            TestUtils.waitForCondition(
                () => {
                    mockRestoreConsumer.assign(changelogPartitionSet);
                    return mockRestoreConsumer.position(changelogPartition) == 2L;
                },
                "Never finished restore");
        } finally {
            thread.shutdown();
            thread.join(10000);
        }
    }

    [Xunit.Fact]
    public void ShouldRecordSkippedMetricForDeserializationException() {
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        Properties config = configProps(false);
        config.setProperty(
            StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().getClass().getName());
        StreamThread thread = createStreamThread(clientId, new StreamsConfig(config), false);

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        HashSet<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
        thread.taskManager().setAssignmentMetadata(
            Collections.singletonMap(
                new TaskId(0, t1p1.partition()),
                assignedPartitions),
            Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce();

        MetricName skippedTotalMetric = metrics.metricName(
            "skipped-records-total",
            "stream-metrics",
            Collections.singletonMap("client-id", thread.getName()));
        MetricName skippedRateMetric = metrics.metricName(
            "skipped-records-rate",
            "stream-metrics",
            Collections.singletonMap("client-id", thread.getName()));
        Assert.Equal(0.0, metrics.metric(skippedTotalMetric).metricValue());
        Assert.Equal(0.0, metrics.metric(skippedRateMetric).metricValue());

        long offset = -1;
        mockConsumer.addRecord(new ConsumeResult<>(
            t1p1.topic(),
            t1p1.partition(),
            ++offset, -1,
            TimestampType.CreateTime,
            ConsumeResult.NULL_CHECKSUM,
            -1,
            -1,
            new byte[0],
            "I am not an integer.".getBytes()));
        mockConsumer.addRecord(new ConsumeResult<>(
            t1p1.topic(),
            t1p1.partition(),
            ++offset,
            -1,
            TimestampType.CreateTime,
            ConsumeResult.NULL_CHECKSUM,
            -1,
            -1,
            new byte[0],
            "I am not an integer.".getBytes()));
        thread.runOnce();
        Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
        Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

        LogCaptureAppender.Unregister(appender);
        List<string> strings = appender.getMessages();
        Assert.True(strings.Contains("task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[0]"));
        Assert.True(strings.Contains("task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[1]"));
    }

    [Xunit.Fact]
    public void ShouldReportSkippedRecordsForInvalidTimestamps() {
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        internalTopologyBuilder.addSource(null, "source1", null, null, null, topic1);

        Properties config = configProps(false);
        config.setProperty(
            StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            LogAndSkipOnInvalidTimestamp.getName());
        StreamThread thread = createStreamThread(clientId, new StreamsConfig(config), false);

        thread.setState(StreamThread.State.STARTING);
        thread.setState(StreamThread.State.PARTITIONS_REVOKED);

        HashSet<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
        thread.taskManager().setAssignmentMetadata(
            Collections.singletonMap(
                new TaskId(0, t1p1.partition()),
                assignedPartitions),
            Collections.emptyMap());

        MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.consumer;
        mockConsumer.assign(Collections.singleton(t1p1));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        thread.rebalanceListener.onPartitionsAssigned(assignedPartitions);
        thread.runOnce();

        MetricName skippedTotalMetric = metrics.metricName(
            "skipped-records-total",
            "stream-metrics",
            Collections.singletonMap("client-id", thread.getName()));
        MetricName skippedRateMetric = metrics.metricName(
            "skipped-records-rate",
            "stream-metrics",
            Collections.singletonMap("client-id", thread.getName()));
        Assert.Equal(0.0, metrics.metric(skippedTotalMetric).metricValue());
        Assert.Equal(0.0, metrics.metric(skippedRateMetric).metricValue());

        long offset = -1;
        addRecord(mockConsumer, ++offset);
        addRecord(mockConsumer, ++offset);
        thread.runOnce();
        Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
        Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

        addRecord(mockConsumer, ++offset);
        addRecord(mockConsumer, ++offset);
        addRecord(mockConsumer, ++offset);
        addRecord(mockConsumer, ++offset);
        thread.runOnce();
        Assert.Equal(6.0, metrics.metric(skippedTotalMetric).metricValue());
        Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

        addRecord(mockConsumer, ++offset, 1L);
        addRecord(mockConsumer, ++offset, 1L);
        thread.runOnce();
        Assert.Equal(6.0, metrics.metric(skippedTotalMetric).metricValue());
        Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

        LogCaptureAppender.Unregister(appender);
        List<string> strings = appender.getMessages();
        Assert.True(strings.Contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[0] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        Assert.True(strings.Contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[1] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        Assert.True(strings.Contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[2] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        Assert.True(strings.Contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[3] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        Assert.True(strings.Contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[4] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
        Assert.True(strings.Contains(
            "task [0_1] Skipping record due to negative extracted timestamp. " +
                "topic=[topic1] partition=[1] offset=[5] extractedTimestamp=[-1] " +
                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        ));
    }

    private void AssertThreadMetadataHasEmptyTasksWithState(ThreadMetadata metadata,
                                                            StreamThread.State state) {
        Assert.Equal(state.name(), metadata.threadState());
        Assert.True(metadata.activeTasks().isEmpty());
        Assert.True(metadata.standbyTasks().isEmpty());
    }

    [Xunit.Fact]
    // TODO: Need to add a test case covering EOS when we create a mock taskManager class
    public void ProducerMetricsVerificationWithoutEOS() {
        MockProducer<byte[], byte[]> producer = new MockProducer<>();
        Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
        TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 0);

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
                mockTime,
                config,
                producer,
                consumer,
                consumer,
                null,
                taskManager,
                streamsMetrics,
                internalTopologyBuilder,
                clientId,
                new LogContext(""),
                new AtomicInteger()
                );
        MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        Metric testMetric = new KafkaMetric(
            new object(),
            testMetricName,
            (Measurable) (config, now) => 0,
            null,
            new MockTime());
        producer.setMockMetrics(testMetricName, testMetric);
        Dictionary<MetricName, Metric> producerMetrics = thread.producerMetrics();
        Assert.Equal(testMetricName, producerMetrics.get(testMetricName).metricName());
    }

    [Xunit.Fact]
    public void AdminClientMetricsVerification() {
        Node broker1 = new Node(0, "dummyHost-1", 1234);
        Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = asList(broker1, broker2);

        MockAdminClient adminClient = new MockAdminClient(cluster, broker1, null);

        MockProducer<byte[], byte[]> producer = new MockProducer<>();
        Consumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
        TaskManager taskManager = EasyMock.createNiceMock(TaskManager);

        StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        StreamThread thread = new StreamThread(
                mockTime,
                config,
                producer,
                consumer,
                consumer,
                null,
                taskManager,
                streamsMetrics,
                internalTopologyBuilder,
                clientId,
                new LogContext(""),
                new AtomicInteger()
                );
        MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        Metric testMetric = new KafkaMetric(
            new object(),
            testMetricName,
            (Measurable) (config, now) => 0,
            null,
            new MockTime());

        EasyMock.expect(taskManager.getAdminClient()).andReturn(adminClient);
        EasyMock.expectLastCall();
        EasyMock.replay(taskManager, consumer);

        adminClient.setMockMetrics(testMetricName, testMetric);
        Dictionary<MetricName, Metric> adminClientMetrics = thread.adminClientMetrics();
        Assert.Equal(testMetricName, adminClientMetrics.get(testMetricName).metricName());
    }

    private void AddRecord(MockConsumer<byte[], byte[]> mockConsumer,
                           long offset) {
        addRecord(mockConsumer, offset, -1L);
    }

    private void AddRecord(MockConsumer<byte[], byte[]> mockConsumer,
                           long offset,
                           long timestamp) {
        mockConsumer.addRecord(new ConsumeResult<>(
            t1p1.topic(),
            t1p1.partition(),
            offset,
            timestamp,
            TimestampType.CreateTime,
            ConsumeResult.NULL_CHECKSUM,
            -1,
            -1,
            new byte[0],
            new byte[0]));
    }
}
