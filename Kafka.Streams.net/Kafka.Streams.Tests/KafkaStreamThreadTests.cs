using Confluent.Kafka;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.KafkaStream;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using NodaTime;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class KafkaStreamThreadTests
    {
        private readonly int threadIdx = 1;
        private readonly string clientId = "clientId";
        private readonly string applicationId = "stream-thread-test";
        private readonly MockTime mockTime = new MockTime();
        private readonly MockClientSupplier clientSupplier = new MockClientSupplier();
        private readonly InternalStreamsBuilder internalStreamsBuilder;

        private StreamsConfig Config { get; set; }
        private readonly string stateDir = TestUtils.GetTempDirectory();
        private readonly StateDirectory stateDirectory;
        private readonly ConsumedInternal<object, object> consumed = new ConsumedInternal<object, object>();

        private Guid processId = Guid.NewGuid();
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly StreamsMetadataState streamsMetadataState;

        private const string topic1 = "topic1";
        private const string topic2 = "topic2";

        private readonly TopicPartition t1p1 = new TopicPartition(topic1, 1);
        private readonly TopicPartition t1p2 = new TopicPartition(topic1, 2);
        private readonly TopicPartition t2p1 = new TopicPartition(topic2, 1);

        private StreamsBuilder streamsBuilder;

        // task0 is unused
        private readonly TaskId task1 = new TaskId(0, 1);
        private readonly TaskId task2 = new TaskId(0, 2);
        private readonly TaskId task3 = new TaskId(1, 1);

        public KafkaStreamThreadTests()
        {
            this.processId = Guid.NewGuid();

            this.streamsBuilder = new StreamsBuilder();
            var services = this.streamsBuilder.Services;
            this.stateDirectory = services.GetRequiredService<StateDirectory>();

            this.internalStreamsBuilder = this.streamsBuilder.InternalStreamsBuilder;
            this.internalTopologyBuilder = this.streamsBuilder.InternalTopologyBuilder;
            this.internalTopologyBuilder.SetApplicationId(this.applicationId);
            this.streamsMetadataState = this.streamsBuilder.Services.GetRequiredService<StreamsMetadataState>();
        }

        [Fact]
        public void TestPartitionAssignmentChangeForSingleGroup()
        {
            var sp = new ServiceCollection();
            var mockMockConsumer = new Mock<IConsumer<byte[], byte[]>>()
                .SetupAllProperties();

            var mockClientSupplier = new Mock<IKafkaClientSupplier>();
            mockClientSupplier
                .Setup(cs => cs.GetConsumer(It.IsAny<ConsumerConfig>(), It.IsAny<IConsumerRebalanceListener>()))
                    .Returns(new MockConsumer<byte[], byte[]>(mockMockConsumer.Object));
            mockClientSupplier
                .Setup(cs => cs.GetRestoreConsumer(It.IsAny<ConsumerConfig>()))
                    .Returns(new MockRestoreConsumer(mockMockConsumer.Object));

            mockClientSupplier.SetupAllProperties();

            sp.AddSingleton<IKafkaClientSupplier>(mockClientSupplier.Object);
            this.streamsBuilder = new StreamsBuilder(sp);

            internalTopologyBuilder.AddSource<string, string>(
                offsetReset: null,
                name: "source1",
                timestampExtractor: null,
                keyDeserializer: null,
                valDeserializer: null,
                topics: new[] { topic1 });

            var thread = CreateStreamThread(clientId, Config, false);

            var stateListener = new StateListenerStub();
            thread.SetStateListener(stateListener);
            Assert.Equal(KafkaStreamThreadStates.CREATED, thread.State.CurrentState);

            IConsumerRebalanceListener rebalanceListener = thread.RebalanceListener;

            List<TopicPartitionOffset> revokedPartitions;
            var assignedPartitions = new List<TopicPartition> { t1p1 };

            // revoke nothing
            thread.State.SetState(KafkaStreamThreadStates.STARTING);
            revokedPartitions = new List<TopicPartitionOffset>();
            rebalanceListener.OnPartitionsRevoked(thread.Consumer, revokedPartitions);

            Assert.Equal(KafkaStreamThreadStates.PARTITIONS_REVOKED, thread.State.CurrentState);

            // assign single partition
            thread.TaskManager.SetAssignmentMetadata(
                activeTasks: new Dictionary<TaskId, HashSet<TopicPartition>>(),
                standbyTasks: new Dictionary<TaskId, HashSet<TopicPartition>>());

            var mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
            mockConsumer.Assign(assignedPartitions);
            mockConsumer.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { { t1p1, 0L } });

            rebalanceListener.OnPartitionsAssigned(thread.Consumer, assignedPartitions);
            
            thread.RunOnce();
            Assert.Equal(KafkaStreamThreadStates.RUNNING, thread.State.CurrentState);
            
            Assert.Equal(4, stateListener.NumChanges);
            Assert.Equal(KafkaStreamThreadStates.PARTITIONS_ASSIGNED, stateListener.OldState);

            thread.Shutdown();
            Assert.Equal(KafkaStreamThreadStates.PENDING_SHUTDOWN, thread.State.CurrentState);
        }

        [Fact]
        public void TestStateChangeStartClose()
        {
            var thread = CreateStreamThread(clientId, Config, false);

            StateListenerStub stateListener = new StateListenerStub();
            thread.SetStateListener(stateListener);

            thread.Start();
            TestUtils.waitForCondition(
                () => thread.State.CurrentState == KafkaStreamThreadStates.STARTING,
                TimeSpan.FromSeconds(10),
                "Thread never started.");

            thread.Shutdown();
            TestUtils.waitForCondition(
                () => thread.State.CurrentState == KafkaStreamThreadStates.DEAD,
                TimeSpan.FromSeconds(10),
                "Thread never shut down.");

            thread.Shutdown();
            Assert.Equal(KafkaStreamThreadStates.DEAD, thread.State.CurrentState);
        }

        //private Cluster createCluster()
        //{
        //Node node = new Node(0, "localhost", 8121);
        //return new Cluster(
        //"mockClusterId",
        //singletonList(node),
        //Collections.emptySet(),
        //Collections.emptySet(),
        //Collections.emptySet(),
        //node
        //);
        //}

        private IKafkaStreamThread CreateStreamThread(string clientId, StreamsConfig config, bool eosEnabled)
        {
            if (eosEnabled)
            {
                // clientSupplier.SetApplicationIdForProducer(applicationId);
            }

            //clientSupplier.SetClusterForAdminClient(createCluster());

            var thread = this.streamsBuilder.Services.GetRequiredService<IKafkaStreamThread>();

            return thread;
        }

        // [Fact]
        // public void testMetricsCreatedAtStartup()
        // {
        // KafkaStreamThread thread = createStreamThread(clientId, config, false);
        // string defaultGroupName = "stream-metrics";
        // var defaultTags = Collections.singletonMap("client-id", thread.getName());
        // string descriptionIsNotVerified = "";
        // 
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "commit-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "commit-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "commit-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "commit-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "poll-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "poll-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "poll-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "poll-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "process-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "process-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "process-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "process-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "punctuate-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "punctuate-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "punctuate-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "punctuate-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "task-created-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "task-created-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "task-closed-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "task-closed-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "skipped-records-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "skipped-records-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // 
        // string taskGroupName = "stream-task-metrics";
        // var taskTags =
        //     mkMap(mkEntry("task-id", "all"), mkEntry("client-id", thread.getName()));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "commit-latency-avg", taskGroupName, descriptionIsNotVerified, taskTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "commit-latency-max", taskGroupName, descriptionIsNotVerified, taskTags)));
        // Assert.NotNull(metrics.metrics().get(metrics.metricName(
        //     "commit-rate", taskGroupName, descriptionIsNotVerified, taskTags)));
        // 
        // JmxReporter reporter = new JmxReporter("kafka.streams");
        // metrics.addReporter(reporter);
        // Assert.Equal(clientId + "-KafkaStreamThread-1", thread.getName());
        // Assert.True(reporter.containsMbean(string.Format("kafka.streams:type=%s,client-id=%s",
        //            defaultGroupName,
        //            thread.getName())));
        // Assert.True(reporter.containsMbean("kafka.streams:type=stream-task-metrics,client-id=" + thread.getName() + ",task-id=all"));
        //}

        //[Fact]
        //public void shouldNotCommitBeforeTheCommitInterval()
        //{
        //    long commitInterval = 1000L;
        //    var props = configProps(false);
        //    props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, stateDir);
        //    props.Set(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG, commitInterval.ToString());

        //    StreamsConfig config = new StreamsConfig(props);
        //    var consumer = new Mock<IConsumer<byte[], byte[]>>();
        //    TaskManager taskManager = mockTaskManagerCommit(consumer.Object, 1, 1);

        //    //var streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //    var thread = new KafkaStreamThread(
        //        mockTime,
        //        config,
        //                null,
        //        consumer,
        //        consumer,
        //                null,
        //        taskManager,
        //        //streamsMetrics,
        //        internalTopologyBuilder,
        //        clientId,
        //                new LogContext(""),
        //                new AtomicInteger()
        //            );

        //    thread.setNow(mockTime.milliseconds());
        //    thread.maybeCommit();
        //    mockTime.sleep(commitInterval - 10L);
        //    thread.setNow(mockTime.milliseconds());
        //    thread.maybeCommit();

        //    EasyMock.verify(taskManager);
        //}

        //        [Fact]
        //        public void shouldRespectNumIterationsInMainLoop()
        //        {
        //            var mockProcessor = new MockProcessor(PunctuationType.WALL_CLOCK_TIME, 10L);
        //            internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);
        //            internalTopologyBuilder.AddProcessor<string, string>("processor1", () => mockProcessor, "source1");
        //            internalTopologyBuilder.AddProcessor<string, string>("processor2", () => new MockProcessor(PunctuationType.STREAM_TIME, 10L), "source1");
        //
        //            var properties = new Properties();
        //            properties.Add(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG, 100L);
        //            StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig(applicationId,
        //                "localhost:2171",
        //                Serdes.ByteArray().GetType().FullName,
        //                Serdes.ByteArray().GetType().FullName,
        //                properties));
        //            KafkaStreamThread thread = createStreamThread(clientId, config, false);
        //
        //            thread.State.SetState(KafkaStreamThreadStates.STARTING);
        //            thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);
        //
        //            var assignedPartitions = new HashSet<TopicPartition> { t1p1 };
        //            thread.TaskManager.SetAssignmentMetadata(
        //                new Dictionary<TaskId, HashSet<TopicPartition>>
        //                {
        //                    { new TaskId(0, this.t1p1.Partition), assignedPartitions }
        //                },
        //                    new Dictionary<TaskId, HashSet<TopicPartition>>());
        //
        //            var mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //            mockConsumer.Assign(new[] { t1p1 });
        //            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //            thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);
        //            thread.RunOnce();
        //
        //            // processed one record, punctuated after the first record, and hence num.iterations is still 1
        //            long offset = -1;
        //            addRecord(mockConsumer, ++offset, 0L);
        //            thread.RunOnce();
        //
        //            Assert.Equal(1, thread.currentNumIterations());
        //
        //            // processed one more record without punctuation, and bump num.iterations to 2
        //            addRecord(mockConsumer, ++offset, 1L);
        //            thread.RunOnce();
        //
        //            Assert.Equal(2, thread.currentNumIterations());
        //
        //            // processed zero records, early exit and iterations stays as 2
        //            thread.RunOnce();
        //            Assert.Equal(2, thread.currentNumIterations());
        //
        //            // system time based punctutation halves to 1
        //            mockTime.sleep(11L);
        //
        //            thread.RunOnce();
        //            Assert.Equal(1, thread.currentNumIterations());
        //
        //            // processed two records, bumping up iterations to 2
        //            addRecord(mockConsumer, ++offset, 5L);
        //            addRecord(mockConsumer, ++offset, 6L);
        //            thread.RunOnce();
        //
        //            Assert.Equal(2, thread.currentNumIterations());
        //
        //            // stream time based punctutation halves to 1
        //            addRecord(mockConsumer, ++offset, 11L);
        //            thread.RunOnce();
        //
        //            Assert.Equal(1, thread.currentNumIterations());
        //
        //            // processed three records, bumping up iterations to 3 (1 + 2)
        //            addRecord(mockConsumer, ++offset, 12L);
        //            addRecord(mockConsumer, ++offset, 13L);
        //            addRecord(mockConsumer, ++offset, 14L);
        //            thread.RunOnce();
        //
        //            Assert.Equal(3, thread.currentNumIterations());
        //
        //            mockProcessor.requestCommit();
        //            addRecord(mockConsumer, ++offset, 15L);
        //            thread.RunOnce();
        //
        //            // user requested commit should not impact on iteration adjustment
        //            Assert.Equal(3, thread.currentNumIterations());
        //
        //            // time based commit, halves iterations to 3 / 2 = 1
        //            mockTime.sleep(90L);
        //            thread.RunOnce();
        //
        //            Assert.Equal(1, thread.currentNumIterations());
        //        }
        /*
                [Fact]
                public void shouldNotCauseExceptionIfNothingCommitted()
                {
                    long commitInterval = 1000L;
                    var props = ConfigProps(false);
                    props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, stateDir);
                    props.Set(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG, commitInterval.ToString());

                    StreamsConfig config = new StreamsConfig(props);
                    IConsumer<byte[], byte[]> consumer = EasyMock.createNiceMock(typeof(IConsumer<byte[], byte[]>));
                    TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 0);

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
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

                [Fact]
                public void shouldCommitAfterTheCommitInterval()
                {
                    long commitInterval = 1000L;
                    var props = ConfigProps(false);
                    props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, stateDir);
                    props.Set(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG, commitInterval.ToString());

                    StreamsConfig config = new StreamsConfig(props);
                    IConsumer<byte[], byte[]> consumer = EasyMock.createNiceMock(typeof(IConsumer<byte[], byte[]>).FullName);
                    TaskManager taskManager = mockTaskManagerCommit(consumer, 2, 1);

                    var streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
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

                private TaskManager mockTaskManagerCommit(IConsumer<byte[], byte[]> consumer,
                                                          int numberOfCommits,
                                                          int commits)
                {
                    TaskManager taskManager = EasyMock.createNiceMock(typeof(TaskManager).FullName);
                    EasyMock.expect(taskManager.commitAll()).andReturn(commits).times(numberOfCommits);
                    EasyMock.replay(taskManager, consumer);
                    return taskManager;
                }

                [Fact]
                public void shouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled()
                {
                    internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);
                    internalStreamsBuilder.BuildAndOptimizeTopology();

                    KafkaStreamThread thread = CreateStreamThread(clientId, config, false);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, new List<TopicPartitionOffset>());

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
                    var assignedPartitions = new HashSet<TopicPartition>
                    {

                        // assign single partition
                        t1p1,
                        t1p2
                    };

                    activeTasks.Add(task1, new HashSet<TopicPartition> { t1p1 });
                    activeTasks.Add(task2, new HashSet<TopicPartition> { t1p2 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    Dictionary<TopicPartition, long> beginOffsets = new Dictionary<TopicPartition, long>();
                    beginOffsets.Add(t1p1, 0L);
                    beginOffsets.Add(t1p2, 0L);
                    mockConsumer.UpdateBeginningOffsets(beginOffsets);
                    thread.RebalanceListener.OnPartitionsAssigned(null, new List<TopicPartition>(assignedPartitions));

                    Assert.Equal(1, clientSupplier.producers.size());
                    var globalProducer = clientSupplier.producers.get(0);

                    foreach (StreamTask task in thread.Tasks().Values)
                    {
                        Assert.Same(globalProducer, ((RecordCollectorImpl)((StreamTask)task).recordCollector()).producer());
                    }
                    Assert.Same(clientSupplier.Consumer, thread.Consumer);
                    Assert.Same(clientSupplier.RestoreConsumer, thread.RestoreConsumer);
                }

                [Fact]
                public void shouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable()
                {
                    internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);

                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, new List<TopicPartitionOffset>());

                    Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
                    List<TopicPartition> assignedPartitions = new List<TopicPartition>
                    {
                        // assign single partition
                        t1p1,
                        t1p2
                    };

                    activeTasks.Add(task1, new HashSet<TopicPartition> { t1p1 });
                    activeTasks.Add(task2, new HashSet<TopicPartition> { t1p2 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    Dictionary<TopicPartition, long> beginOffsets = new Dictionary<TopicPartition, long>();
                    beginOffsets.Add(t1p1, 0L);
                    beginOffsets.Add(t1p2, 0L);
                    mockConsumer.UpdateBeginningOffsets(beginOffsets);
                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);

                    thread.RunOnce();

                    Assert.Equal(thread.Tasks().Count, clientSupplier.producers.size());
                    Assert.Same(clientSupplier.Consumer, thread.Consumer);
                    Assert.Same(clientSupplier.RestoreConsumer, thread.RestoreConsumer);
                }

                [Fact]
                public void shouldCloseAllTaskProducersOnCloseIfEosEnabled()
                {
                    internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);

                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, new List<TopicPartitionOffset>());

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
                    List<TopicPartition> assignedPartitions = new List<TopicPartition>
                    {
                        // assign single partition
                        t1p1,
                        t1p2
                    };

                    activeTasks.Add(task1, new HashSet<TopicPartition> { t1p1 });
                    activeTasks.Add(task2, new HashSet<TopicPartition> { t1p2 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());
                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    var beginOffsets = new Dictionary<TopicPartition, long>();
                    beginOffsets.Add(t1p1, 0L);
                    beginOffsets.Add(t1p2, 0L);
                    mockConsumer.UpdateBeginningOffsets(beginOffsets);

                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);

                    thread.Shutdown();
                    thread.Run();

                    foreach (var task in thread.Tasks().Values)
                    {
                        Assert.True(((MockProducer)((RecordCollectorImpl)((StreamTask)task).recordCollector()).producer()).closed());
                    }
                }

                [Fact]
                public void shouldShutdownTaskManagerOnClose()
                {
                    var consumer = new Mock<IConsumer<byte[], byte[]>>();
                    var taskManager = new Mock<TaskManager>();
                    taskManager.Object.Shutdown(true);
                    // EasyMock.expectLastCall();
                    // EasyMock.replay(taskManager, consumer);

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                        mockTime,
                        null, null,
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
                    thread.SetStateListener(
                        (t, newState, oldState) =>
                        {
                            if (oldState == KafkaStreamThreadStates.CREATED && newState == KafkaStreamThreadStates.STARTING)
                            {
                                thread.Shutdown();
                            }
                        });

                    thread.Run();
                    taskManager.Verify();
                }

                [Fact]
                public void shouldShutdownTaskManagerOnCloseWithoutStart()
                {
                    var consumer = new Mock<IConsumer<byte[], byte[]>>();
                    var taskManager = new Mock<TaskManager>();
                    taskManager.Object.Shutdown(true);
                    taskManager.VerifyNoOtherCalls();

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
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

                    thread.Shutdown();
                    taskManager.Verify();
                }

                [Fact]
                public void shouldNotThrowWhenPendingShutdownInRunOnce()
                {
                    mockRunOnce(true);
                }

                [Fact]
                public void shouldNotThrowWithoutPendingShutdownInRunOnce()
                {
                    // A reference test to verify that without intermediate shutdown the runOnce should pass
                    // without any exception.
                    mockRunOnce(false);
                }

                private void mockRunOnce(bool shutdownOnPoll)
                {
                    var assignedPartitions = Collections.singletonList(t1p1);

                    MockStreamThreadConsumer<byte[], byte[]> mockStreamThreadConsumer =
                        new MockStreamThreadConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);

                    TaskManager taskManager = new TaskManager(
                        null, null,
                        new MockChangelogReader(),
                        processId,
                        "log-prefix",
                        mockStreamThreadConsumer,
                        streamsMetadataState,
                        null,
                        null,
                        null,
                        new AssignedStreamsTasks(null),
                        new AssignedStandbyTasks(null));

                    taskManager.SetConsumer(mockStreamThreadConsumer);
                    taskManager.SetAssignmentMetadata(new Dictionary<TaskId, HashSet<TopicPartition>>(), new Dictionary<TaskId, HashSet<TopicPartition>>());

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
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

                    mockStreamThreadConsumer.SetStreamThread(thread);
                    mockStreamThreadConsumer.Assign(assignedPartitions);
                    mockStreamThreadConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

                    addRecord(mockStreamThreadConsumer, 1L, 0L);
                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);
                    thread.RunOnce();
                }

                [Fact]
                public void shouldOnlyShutdownOnce()
                {
                    var consumer = new Mock<IConsumer<byte[], byte[]>>();
                    var taskManager = new Mock<TaskManager>();
                    taskManager.Object.Shutdown(clean: true);
                    taskManager.VerifyNoOtherCalls();

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
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
                    thread.Shutdown();
                    // Execute the run method. Verification of the mock will check that shutdown was only done once
                    thread.Run();
                    taskManager.Verify();
                }

                [Fact]
                public void shouldNotNullPointerWhenStandbyTasksAssignedAndNoStateStoresForTopology()
                {
                    internalTopologyBuilder.AddSource<string, string>(null, "name", null, null, null, "topic");
                    internalTopologyBuilder.AddSink<string, string>("out", "output", null, null, null, "name");

                    KafkaStreamThread thread = CreateStreamThread(clientId, config, false);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, new List<TopicPartitionOffset>());

                    var standbyTasks = new Dictionary<TaskId, HashSet<TopicPartition>>
                    {
                        // assign single partition
                        { task1, new HashSet<TopicPartition> { t1p1 } },
                    };

                    thread.TaskManager.SetAssignmentMetadata(new Dictionary<TaskId, HashSet<TopicPartition>>(), standbyTasks);
                    thread.TaskManager.createTasks(new List<TopicPartition>());

                    thread.RebalanceListener.OnPartitionsAssigned(null, new List<TopicPartition>());
                }

                [Fact]
                public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerWasFencedWhileProcessing()
                {
                    internalTopologyBuilder.AddSource<string, string>(null, "source", null, null, null, topic1);
                    internalTopologyBuilder.AddSink<string, string>("sink", "dummyTopic", null, null, null, "source");

                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

                    MockConsumer<byte[], byte[]> consumer = clientSupplier.Consumer;

                    consumer.UpdatePartitions(topic1, singletonList(new PartitionInfo(topic1, 1, null, null, null)));

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    Dictionary<TaskId, List<TopicPartition>> activeTasks = new Dictionary<TaskId, List<TopicPartition>>();
                    List<TopicPartition> assignedPartitions = new List<TopicPartition>();

                    // assign single partition
                    assignedPartitions.Add(t1p1);
                    activeTasks.Add(task1, new List<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);

                    thread.RunOnce();
                    Assert.Equal(1, thread.Tasks().Count);
                    MockProducer producer = clientSupplier.producers.get(0);

                    // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
                    consumer.UpdateBeginningOffsets(Collections.singletonMap(assignedPartitions.iterator().next(), 0L));
                    consumer.Unsubscribe();
                    consumer.Assign(assignedPartitions);

                    consumer.AddRecord(new ConsumerRecord<>(topic1, 1, 0, Array.Empty<byte>(), Array.Empty<byte>()));
                    mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1);
                    thread.RunOnce();
                    Assert.Equal(1, producer.history().size());

                    Assert.False(producer.transactionCommitted());
                    mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
                    TestUtils.waitForCondition(
                        () => producer.commitCount() == 1,
                        "StreamsThread did not commit transaction.");

                    producer.fenceProducer();
                    mockTime.sleep(config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
                    consumer.AddRecord(new ConsumerRecord<>(topic1, 1, 1, Array.Empty<byte>(), Array.Empty<byte>()));
                    try
                    {
                        thread.RunOnce();
                        //fail("Should have thrown TaskMigratedException");
                    }
                    catch (TaskMigratedException expected) { }
                    TestUtils.waitForCondition(
                        () => !thread.Tasks().Any(),
                                "StreamsThread did not remove fenced zombie task.");

                    Assert.Equal(1L, producer.commitCount());
                }

                [Fact]
                public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedInCommitTransactionWhenSuspendingTaks()
                {
                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

                    internalTopologyBuilder.AddSource<string, string>(null, "name", null, null, null, topic1);
                    internalTopologyBuilder.AddSink<string, string>("out", "output", null, null, null, "name");

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
                    List<TopicPartition> assignedPartitions = new List<TopicPartition>
                    {
                        // assign single partition
                        t1p1
                    };

                    activeTasks.Add(task1, new HashSet<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);

                    thread.RunOnce();

                    Assert.Equal(1, thread.Tasks().Count);

                    clientSupplier.producers.get(0).fenceProducer();
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);
                    Assert.True(clientSupplier.producers.get(0).transactionInFlight());
                    Assert.False(clientSupplier.producers.get(0).transactionCommitted());
                    Assert.True(clientSupplier.producers.get(0).closed());
                    Assert.False(thread.Tasks().Any());
                }

                [Fact]
                public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedInCloseTransactionWhenSuspendingTasks()
                {
                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

                    internalTopologyBuilder.AddSource<string, string>(null, "name", null, null, null, topic1);
                    internalTopologyBuilder.AddSink<string, string>("out", "output", null, null, null, "name");

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
                    List<TopicPartition> assignedPartitions = new List<TopicPartition>
                    {

                        // assign single partition
                        t1p1
                    };

                    activeTasks.Add(task1, new HashSet<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);

                    thread.RunOnce();

                    Assert.Equal(1, thread.Tasks().Count);

                    clientSupplier.producers.get(0).fenceProducerOnClose();
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    Assert.False(clientSupplier.producers.get(0).transactionInFlight());
                    Assert.True(clientSupplier.producers.get(0).transactionCommitted());
                    Assert.False(clientSupplier.producers.get(0).closed());
                    Assert.False(thread.Tasks().Any());
                }

                [Fact]
                public void shouldReturnActiveTaskMetadataWhileRunningState()
                {
                    internalTopologyBuilder.AddSource<string, string>(null, "source", null, null, null, topic1);

                    KafkaStreamThread thread = CreateStreamThread(clientId, config, false);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
                    List<TopicPartition> assignedPartitions = new List<TopicPartition>
                    {
                        // assign single partition
                        t1p1
                    };

                    activeTasks.Add(task1, new HashSet<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);

                    thread.RunOnce();

                    ThreadMetadata threadMetadata = thread.ThreadMetadata;
                    Assert.Equal(KafkaStreamThreadStates.RUNNING.ToString(), threadMetadata.ThreadState);
                    Assert.Contains(new TaskMetadata(task1.ToString(), Utils.mkSet(t1p1)), threadMetadata.ActiveTasks);
                    Assert.False(threadMetadata.standbyTasks.Any());
                }

                [Fact]
                public void shouldReturnStandbyTaskMetadataWhileRunningState()
                {
                    internalStreamsBuilder.Stream(new[] { topic1 }, consumed)
                        .groupByKey().count(Materialized<object, long, IKeyValueStore<Bytes, byte[]>>.As("count-one"));

                    internalStreamsBuilder.BuildAndOptimizeTopology();
                    KafkaStreamThread thread = CreateStreamThread(clientId, config, false);
                    MockConsumer<byte[], byte[]> RestoreConsumer = clientSupplier.RestoreConsumer;
                    RestoreConsumer.UpdatePartitions(
                        "stream-thread-test-count-one-changelog",
                        singletonList(
                            new PartitionInfo("stream-thread-test-count-one-changelog",
                            0,
                            null,
                            Array.Empty<Node>(),
                            Array.Empty<Node>())
                        )
                    );

                    Dictionary<TopicPartition, long> offsets = new Dictionary<TopicPartition, long>();
                    offsets.Add(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
                    RestoreConsumer.UpdateEndOffsets(offsets);
                    RestoreConsumer.UpdateBeginningOffsets(offsets);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    var standbyTasks = new Dictionary<TaskId, HashSet<TopicPartition>>
                    {

                        // assign single partition
                        { task1, new HashSet<TopicPartition> { t1p1 } }
                    };

                    thread.TaskManager.SetAssignmentMetadata(new Dictionary<TaskId, HashSet<TopicPartition>>(), standbyTasks);

                    thread.RebalanceListener.OnPartitionsAssigned(null, new List<TopicPartition>());

                    thread.RunOnce();

                    ThreadMetadata threadMetadata = thread.ThreadMetadata;
                    Assert.Equal(KafkaStreamThreadStates.RUNNING.ToString(), threadMetadata.ThreadState);
                    Assert.Contains(new TaskMetadata(task1.ToString(), Utils.mkSet(t1p1)), threadMetadata.standbyTasks);
                    Assert.False(threadMetadata.ActiveTasks.Any());
                }

                [Fact]
                public void shouldUpdateStandbyTask()
                {
                    string storeName1 = "count-one";
                    string storeName2 = "table-two";
                    string changelogName1 = applicationId + "-" + storeName1 + "-changelog";
                    string changelogName2 = applicationId + "-" + storeName2 + "-changelog";
                    TopicPartition partition1 = new TopicPartition(changelogName1, 1);
                    TopicPartition partition2 = new TopicPartition(changelogName2, 1);
                    internalStreamsBuilder
                        .Stream(new[] { topic1 }, consumed)
                        .groupByKey()
                        .count(Materialized<object, long, IKeyValueStore<Bytes, byte[]>>.As(storeName1));

                    MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>> materialized
                        = new MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>>(Materialized<object, object, IKeyValueStore<Bytes, byte[]>>.As(storeName2), internalStreamsBuilder, "");
                    internalStreamsBuilder.table(topic2, new ConsumedInternal<>(), materialized);

                    internalStreamsBuilder.BuildAndOptimizeTopology();
                    KafkaStreamThread thread = CreateStreamThread(clientId, config, false);
                    MockConsumer<byte[], byte[]> RestoreConsumer = clientSupplier.RestoreConsumer;
                    RestoreConsumer.updatePartitions(changelogName1,
                        singletonList(
                            new PartitionInfo(
                                changelogName1,
                                1,
                                null,
                                Array.Empty<Node>(),
                                Array.Empty<Node>()
                            )
                        )
                    );

                    RestoreConsumer.Assign(new[] { partition1, partition2 });
                    RestoreConsumer.UpdateEndOffsets(Collections.singletonMap(partition1, 10L));
                    RestoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(partition1, 0L));
                    RestoreConsumer.UpdateEndOffsets(Collections.singletonMap(partition2, 10L));
                    RestoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(partition2, 0L));
                    // let the store1 be restored from 0 to 10; store2 be restored from 5 (checkpointed) to 10
                    OffsetCheckpoint checkpoint
                        = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(task3), CHECKPOINT_FILE_NAME));
                    checkpoint.write(Collections.singletonMap(partition2, 5L));

                    for (long i = 0L; i < 10L; i++)
                    {
                        RestoreConsumer.AddRecord(new ConsumerRecord(
                            changelogName1,
                            1,
                            i,
                            ("K" + i).getBytes(),
                            ("V" + i).getBytes()));
                        RestoreConsumer.AddRecord(new ConsumerRecord(
                            changelogName2,
                            1,
                            i,
                            ("K" + i).getBytes(),
                            ("V" + i).getBytes()));
                    }

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    var standbyTasks = new Dictionary<TaskId, HashSet<TopicPartition>>
                    {
                        // assign single partition
                        { task1, new HashSet<TopicPartition> { t1p1 } },
                        { task3, new HashSet<TopicPartition> { t2p1 } }
                    };

                    thread.TaskManager.SetAssignmentMetadata(new Dictionary<TaskId, HashSet<TopicPartition>>(), standbyTasks);

                    thread.RebalanceListener.OnPartitionsAssigned(null, new List<TopicPartition>());

                    thread.RunOnce();

                    StandbyTask standbyTask1 = thread.TaskManager.StandbyTask(partition1);
                    StandbyTask standbyTask2 = thread.TaskManager.StandbyTask(partition2);
                    IKeyValueStore<object, long> store1 = (IKeyValueStore<object, long>)standbyTask1.getStore(storeName1);
                    IKeyValueStore<object, long> store2 = (IKeyValueStore<object, long>)standbyTask2.getStore(storeName2);

                    Assert.Equal(10L, store1.approximateNumEntries);
                    Assert.Equal(5L, store2.approximateNumEntries);
                    Assert.Equal(0, thread.standbyRecords.Count);
                }

                [Fact]
                public void shouldCreateStandbyTask()
                {
                    setupInternalTopologyWithoutState();
                    internalTopologyBuilder.AddStateStore(new MockKeyValueStoreBuilder("myStore", true), "processor1");

                    StandbyTask standbyTask = createStandbyTask();

                    Assert.NotNull(standbyTask);
                }

                [Fact]
                public void shouldNotCreateStandbyTaskWithoutStateStores()
                {
                    setupInternalTopologyWithoutState();

                    StandbyTask standbyTask = createStandbyTask();

                    Assert.NotNull(standbyTask);
                }

                [Fact]
                public void shouldNotCreateStandbyTaskIfStateStoresHaveLoggingDisabled()
                {
                    setupInternalTopologyWithoutState();
                    var storeBuilder = new MockKeyValueStoreBuilder("myStore", true);
                    storeBuilder.WithLoggingDisabled();
                    internalTopologyBuilder.addStateStore(storeBuilder, "processor1");

                    StandbyTask standbyTask = createStandbyTask();

                    Assert.NotNull(standbyTask);
                }

                private void setupInternalTopologyWithoutState()
                {
                    var mockProcessor = new MockProcessor();
                    internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);
                    internalTopologyBuilder.AddProcessor<string, string>("processor1", () => mockProcessor, "source1");
                }

                private StandbyTask createStandbyTask()
                {
                    LogContext logContext = new LogContext("test");
                    ILogger log = logContext.logger(typeof(StreamThreadTest));
                    //StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    StandbyTaskCreator standbyTaskCreator = new StandbyTaskCreator(
                        null,
                        null,
                        internalTopologyBuilder,
                        config,
                        //streamsMetrics,
                        stateDirectory,
                        new MockChangelogReader(),
                        null);//mockTime);

                    return standbyTaskCreator.createTask(
                        null,
                        new MockConsumer<byte[], byte[]>(null, null),//OffsetResetStrategy.EARLIEST),
                        new TaskId(1, 2),
                        new HashSet<TopicPartition>());
                }

                [Fact]
                public void shouldPunctuateActiveTask()
                {
                    List<long> punctuatedStreamTime = new List<long>();
                    List<long> punctuatedWallClockTime = new List<long>();
                    //    IProcessorSupplier<object, object> punctuateProcessor = () => new IProcessor<object, object>()
                    //    {
                    //    //@Override
                    //    public void init(IProcessorContext context)
                    //    {
                    //        context.schedule(Duration.ofMillis(100L), PunctuationType.STREAM_TIME, punctuatedStreamTime::add);
                    //        context.schedule(Duration.ofMillis(100L), PunctuationType.WALL_CLOCK_TIME, punctuatedWallClockTime::add);
                    //    }

                    //    public void process(object key,
                    //                        object value)
                    //    { }

                    //    public void close() { }
                    //};

                    internalStreamsBuilder.Stream(new[] { topic1 }, consumed).process(punctuateProcessor);
                    internalStreamsBuilder.BuildAndOptimizeTopology();

                    KafkaStreamThread thread = CreateStreamThread(clientId, config, false);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);
                    List<TopicPartition> assignedPartitions = new List<TopicPartition>();

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();

                    // assign single partition
                    assignedPartitions.Add(t1p1);
                    activeTasks.Add(task1, new HashSet<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    clientSupplier.Consumer.Assign(assignedPartitions);
                    clientSupplier.Consumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);

                    thread.RunOnce();

                    Assert.Equal(0, punctuatedStreamTime.Count);
                    Assert.Equal(0, punctuatedWallClockTime.Count);

                    mockTime.sleep(100L);
                    for (long i = 0L; i < 10L; i++)
                    {
                        clientSupplier.Consumer.AddRecord(new ConsumerRecord<>(
                            topic1,
                            1,
                            i,
                            i * 100L,
                            TimestampType.CreateTime,
                            ConsumerRecord.NULL_CHECKSUM,
                            ("K" + i).getBytes().length,
                            ("V" + i).getBytes().length,
                            ("K" + i).getBytes(),
                            ("V" + i).getBytes()));
                    }

                    thread.RunOnce();

                    Assert.Equal(1, punctuatedStreamTime.Count);
                    Assert.Equal(1, punctuatedWallClockTime.Count);

                    mockTime.sleep(100L);

                    thread.RunOnce();

                    // we should skip stream time punctuation, only trigger wall-clock time punctuation
                    Assert.Equal(1, punctuatedStreamTime.Count);
                    Assert.Equal(2, punctuatedWallClockTime.Count);
                }

                [Fact]
                public void shouldAlwaysUpdateTasksMetadataAfterChangingState()
                {
                    KafkaStreamThread thread = CreateStreamThread(clientId, config, false);
                    ThreadMetadata metadata = thread.ThreadMetadata;
                    Assert.Equal(KafkaStreamThreadStates.CREATED.ToString(), metadata.ThreadState);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);
                    thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_ASSIGNED);
                    thread.State.SetState(KafkaStreamThreadStates.RUNNING);
                    metadata = thread.ThreadMetadata;
                    Assert.Equal(KafkaStreamThreadStates.RUNNING.ToString(), metadata.ThreadState);
                }

                [Fact]
                public void shouldAlwaysReturnEmptyTasksMetadataWhileRebalancingStateAndTasksNotRunning()
                {
                    internalStreamsBuilder.Stream(new[] { topic1 }, consumed)
                        .groupByKey().count(Materialized<object, long>.As("count-one"));
                    internalStreamsBuilder.BuildAndOptimizeTopology();

                    KafkaStreamThread thread = CreateStreamThread(clientId, config, false);
                    MockConsumer<byte[], byte[]> RestoreConsumer = clientSupplier.GetRestoreConsumer();
                    RestoreConsumer.UpdatePartitions("stream-thread-test-count-one-changelog",
                        asList(
                            new PartitionInfo("stream-thread-test-count-one-changelog",
                                0,
                                null,
                                Array.Empty<Node>(),
                                Array.Empty<Node>()),
                            new PartitionInfo("stream-thread-test-count-one-changelog",
                                1,
                                null,
                                Array.Empty<Node>(),
                                Array.Empty<Node>())
                        ));

                    Dictionary<TopicPartition, long> offsets = new Dictionary<TopicPartition, long>
                    {
                        { new TopicPartition("stream-thread-test-count-one-changelog", 0), 0L },
                        { new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L }
                    };

                    RestoreConsumer.updateEndOffsets(offsets);
                    RestoreConsumer.UpdateBeginningOffsets(offsets);

                    clientSupplier.GetConsumer().UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

                    var assignedPartitions = new List<TopicPartitionOffset>();

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, assignedPartitions);
                    assertThreadMetadataHasEmptyTasksWithState(thread.ThreadMetadata, KafkaStreamThreadStates.PARTITIONS_REVOKED);

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
                    var standbyTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();

                    // assign single partition
                    assignedPartitions.Add(t1p1);
                    activeTasks.Add(task1, new HashSet<TopicPartition> { t1p1 });
                    standbyTasks.Add(task2, new HashSet<TopicPartition> { t1p2 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, standbyTasks);

                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);

                    assertThreadMetadataHasEmptyTasksWithState(thread.ThreadMetadata, KafkaStreamThreadStates.PARTITIONS_ASSIGNED);
                }

                [Fact]
                public void shouldRecoverFromInvalidOffsetExceptionOnRestoreAndFinishRestore()
                {
                    internalStreamsBuilder.Stream(new[] { "topic" }, consumed)
                            .groupByKey().count(Materialized<object, long, IKeyValueStore<Bytes, byte[]>>.As("count"));

                    internalStreamsBuilder.BuildAndOptimizeTopology();

                    KafkaStreamThread thread = CreateStreamThread("clientId", config, false);
                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    MockConsumer<byte[], byte[]> mockRestoreConsumer = (MockConsumer<byte[], byte[]>)thread.RestoreConsumer;

                    TopicPartition topicPartition = new TopicPartition("topic", 0);
                    List<TopicPartition> topicPartitionSet = new List<TopicPartition> { topicPartition };

                    Dictionary<TaskId, List<TopicPartition>> activeTasks = new HashMap<>();
                    activeTasks.Add(new TaskId(0, 0), topicPartitionSet);
                    thread.TaskManager.SetAssignmentMetadata(activeTasks, Collections.emptyMap());

                    mockConsumer.UpdatePartitions(
                                "topic",
                                singletonList(
                                    new PartitionInfo(
                                        "topic",
                                        0,
                                        null,
                                        Array.Empty<Node>(),
                                        Array.Empty<Node>()
                                    )
                                )
                            );
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));

                    mockRestoreConsumer.updatePartitions(
                        "stream-thread-test-count-changelog",
                        singletonList(
                            new PartitionInfo(
                                "stream-thread-test-count-changelog",
                                0,
                                null,
                                Array.Empty<Node>(),
                                Array.Empty<Node>()
                            )
                        )
                    );

                    TopicPartition changelogPartition = new TopicPartition("stream-thread-test-count-changelog", 0);
                    List<TopicPartition> changelogPartitionSet = new List<TopicPartition> { changelogPartition };
                    mockRestoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(changelogPartition, 0L));
                    mockRestoreConsumer.updateEndOffsets(Collections.singletonMap(changelogPartition, 2L));

                    mockConsumer.schedulePollTask(() =>
                    {
                        thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);
                        thread.RebalanceListener.OnPartitionsAssigned(topicPartitionSet);
                    });

                    try
                    {
                        thread.Start();

                        TestUtils.waitForCondition(
                            () => mockRestoreConsumer.Assignment.Count == 1,
                            "Never restore first record");

                        mockRestoreConsumer.addRecord(new ConsumerRecord<>(
                            "stream-thread-test-count-changelog",
                            0,
                            0L,
                            "K1".getBytes(),
                            "V1".getBytes()));

                        TestUtils.waitForCondition(
                            () => mockRestoreConsumer.position(changelogPartition) == 1L,
                            "Never restore first record");

                        //mockRestoreConsumer.SetException(new InvalidOffsetException("Try Again!"));
                        //            {
                        //            public override List<TopicPartition> partitions()
                        //    {
                        //        return changelogPartitionSet;
                        //    }
                        //});

                        mockRestoreConsumer.addRecord(new ConsumerRecord<>(
                            "stream-thread-test-count-changelog",
                            0,
                            0L,
                            "K1".getBytes(),
                            "V1".getBytes()));

                        mockRestoreConsumer.addRecord(new ConsumerRecord<>(
                            "stream-thread-test-count-changelog",
                            0,
                            1L,
                            "K2".getBytes(),
                            "V2".getBytes()));

                        TestUtils.waitForCondition(
                            () =>
                            {
                                mockRestoreConsumer.Assign(changelogPartitionSet);
                                return mockRestoreConsumer.position(changelogPartition) == 2L;
                            },
                            "Never finished restore");
                    }
                    finally
                    {
                        thread.Shutdown();
                        thread.Thread.Join(10000);
                    }
                }

                [Fact]
                public void shouldRecordSkippedMetricForDeserializationException()
                {
                    LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

                    internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);

                    var config = ConfigProps(false);
                    config.setProperty(
                        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                        LogAndContinueExceptionHandler.getName());
                    config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().getClass().getName());
                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(config), false);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);

                    List<TopicPartition> assignedPartitions = new List<TopicPartition> { t1p1 };
                    thread.TaskManager.SetAssignmentMetadata(
                        Collections.singletonMap(
                            new TaskId(0, t1p1.Partition),
                            assignedPartitions),
                        Collections.emptyMap());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(new[] { t1p1 });
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);
                    thread.RunOnce();

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
                    mockConsumer.addRecord(new ConsumerRecord<>(
                        t1p1.Topic,
                        t1p1.Partition,
                        ++offset, -1,
                        TimestampType.CreateTime,
                        ConsumerRecord.NULL_CHECKSUM,
                        -1,
                        -1,
                        Array.Empty<byte>(),
                        "I am not an integer.".getBytes()));
                    mockConsumer.addRecord(new ConsumerRecord<>(
                        t1p1.Topic,
                        t1p1.Partition,
                        ++offset,
                        -1,
                        TimestampType.CreateTime,
                        ConsumerRecord.NULL_CHECKSUM,
                        -1,
                        -1,
                        Array.Empty<byte>(),
                        "I am not an integer.".getBytes()));
                    thread.RunOnce();
                    Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
                    Assert.NotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());

                    LogCaptureAppender.unregister(appender);
                    List<string> strings = appender.getMessages();
                    Assert.Contains("task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[0]", strings);
                    Assert.Contains("task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[1]", strings);
                }

                [Fact]
                public void shouldReportSkippedRecordsForInvalidTimestamps()
                {
                    LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

                    internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);

                    var config = ConfigProps(false);
                    config.setProperty(
                        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                        LogAndSkipOnInvalidTimestamp.getName());
                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(config), false);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);

                    List<TopicPartition> assignedPartitions = new List<TopicPartition> { t1p1 };
                    thread.TaskManager.SetAssignmentMetadata(
                        Collections.singletonMap(
                                new TaskId(0, t1p1.Partition),
                                assignedPartitions),
                            new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(new[] { t1p1 });
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null, assignedPartitions);
                    thread.RunOnce();

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
                    thread.RunOnce();
                    Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
                    Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

                    addRecord(mockConsumer, ++offset);
                    addRecord(mockConsumer, ++offset);
                    addRecord(mockConsumer, ++offset);
                    addRecord(mockConsumer, ++offset);
                    thread.RunOnce();
                    Assert.Equal(6.0, metrics.metric(skippedTotalMetric).metricValue());
                    Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

                    addRecord(mockConsumer, ++offset, 1L);
                    addRecord(mockConsumer, ++offset, 1L);
                    thread.RunOnce();
                    Assert.Equal(6.0, metrics.metric(skippedTotalMetric).metricValue());
                    Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

                    LogCaptureAppender.unregister(appender);
                    List<string> strings = appender.getMessages();
                    Assert.Contains("task [0_1] Skipping record due to negative extracted timestamp. " +
                                "topic=[topic1] partition=[1] offset=[0] extractedTimestamp=[-1] " +
                                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[1] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[2] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[3] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[4] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[5] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                }

                private void assertThreadMetadataHasEmptyTasksWithState(ThreadMetadata metadata,
                                                                        KafkaStreamThreadStates state)
                {
                    Assert.Equal(state.ToString(), metadata.ThreadState);
                    Assert.False(metadata.ActiveTasks.Any());
                    Assert.False(metadata.standbyTasks.Any());
                }

                [Fact]
                // TODO: Need to add a test case covering EOS when we create a mock taskManager class
                public void producerMetricsVerificationWithoutEOS()
                {
                    MockProducer<byte[], byte[]> producer = new MockProducer<>();
                    IConsumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
                    TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 0);

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
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
                        (Measurable)(config, now)=> 0,
                        null,
                        new MockTime());
                    producer.setMockMetrics(testMetricName, testMetric);
                    Dictionary<MetricName, Metric> producerMetrics = thread.producerMetrics();
                    Assert.Equal(testMetricName, producerMetrics.get(testMetricName).metricName());
                }

                [Fact]
                public void adminClientMetricsVerification()
                {
                    Node broker1 = new Node(0, "dummyHost-1", 1234);
                    Node broker2 = new Node(1, "dummyHost-2", 1234);
                    List<Node> cluster = asList(broker1, broker2);

                    MockAdminClient adminClient = new MockAdminClient(cluster, broker1, null);

                    MockProducer<byte[], byte[]> producer = new MockProducer<>();
                    IConsumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
                    TaskManager taskManager = EasyMock.createNiceMock(TaskManager);

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
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
                        (Measurable)(config, now)=> 0,
                        null,
                        new MockTime());

                    EasyMock.expect(taskManager.getAdminClient()).andReturn(adminClient);
                    EasyMock.expectLastCall();
                    EasyMock.replay(taskManager, consumer);

                    adminClient.setMockMetrics(testMetricName, testMetric);
                    Dictionary<MetricName, Metric> adminClientMetrics = thread.adminClientMetrics();
                    Assert.Equal(testMetricName, adminClientMetrics.get(testMetricName).metricName());
                }

                private void addRecord(MockConsumer<byte[], byte[]> mockConsumer,
                                       long offset)
                {
                    addRecord(mockConsumer, offset, -1L);
                }

                private void addRecord(MockConsumer<byte[], byte[]> mockConsumer,
                                       long offset,
                                       long timestamp)
                {
                    mockConsumer.addRecord(new ConsumerRecord<>(
                        t1p1.Topic,
                        t1p1.Partition,
                        offset,
                        timestamp,
                        TimestampType.CreateTime,
                        ConsumerRecord.NULL_CHECKSUM,
                        -1,
                        -1,
                        Array.Empty<byte>(),
                        Array.Empty<byte>()));
                }
        */
    }
}
