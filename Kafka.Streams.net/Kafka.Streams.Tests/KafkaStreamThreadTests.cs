using Confluent.Kafka;
using Kafka.Streams.Clients;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Threads.Stream;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using Moq;
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

        private readonly StreamsConfig config;
        private readonly string stateDir = TestUtils.GetTempDirectory().FullName;
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

        // Task0 is unused
        private readonly TaskId Task1 = new TaskId(0, 1);
        private readonly TaskId Task2 = new TaskId(0, 2);
        private readonly TaskId Task3 = new TaskId(1, 1);

        public KafkaStreamThreadTests()
        {
            this.processId = Guid.NewGuid();

            this.config = StreamsTestConfigs.GetStandardConfig(nameof(KafkaStreamThreadTests));
            var sc = new ServiceCollection().AddSingleton(this.config);

            this.streamsBuilder = new StreamsBuilder(sc);
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
            sp.AddSingleton(this.config);

            var mockConsumer = new Mock<IConsumer<byte[], byte[]>>().SetupAllProperties().Object;

            Mock<IKafkaClientSupplier> mockClientSupplier = TestUtils.GetMockClientSupplier(
                mockConsumer: new MockConsumer<byte[], byte[]>(mockConsumer),
                mockRestoreConsumer: new MockRestoreConsumer(mockConsumer));

            sp.AddSingleton(mockClientSupplier.GetType());
            this.streamsBuilder = new StreamsBuilder(sp);

            internalTopologyBuilder.AddSource<string, string>(
                offsetReset: null,
                name: "source1",
                timestampExtractor: null,
                keyDeserializer: null,
                valDeserializer: null,
                topics: new[] { topic1 });

            var thread = TestUtils.CreateStreamThread(this.streamsBuilder, clientId, false);

            var stateListener = new StateListenerStub();
            thread.SetStateListener(stateListener);
            Assert.Equal(StreamThreadStates.CREATED, thread.State.CurrentState);

            IConsumerRebalanceListener rebalanceListener = thread.RebalanceListener;

            List<TopicPartitionOffset> revokedPartitions;
            var assignedPartitions = new List<TopicPartition> { t1p1 };

            // revoke nothing
            thread.State.SetState(StreamThreadStates.STARTING);
            revokedPartitions = new List<TopicPartitionOffset>();
            rebalanceListener.OnPartitionsRevoked(thread.Consumer, revokedPartitions);

            Assert.Equal(StreamThreadStates.PARTITIONS_REVOKED, thread.State.CurrentState);

            // assign single partition
            thread.TaskManager.SetAssignmentMetadata(
                activeTasks: new Dictionary<TaskId, HashSet<TopicPartition>>(),
                standbyTasks: new Dictionary<TaskId, HashSet<TopicPartition>>());

            var consumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
            consumer.Assign(assignedPartitions);
            consumer.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { { t1p1, 0L } });

            rebalanceListener.OnPartitionsAssigned(thread.Consumer, assignedPartitions);

            thread.RunOnce();
            Assert.Equal(StreamThreadStates.RUNNING, thread.State.CurrentState);

            Assert.Equal(4, stateListener.NumChanges);
            Assert.Equal(StreamThreadStates.PARTITIONS_ASSIGNED, stateListener.OldState);

            thread.Shutdown();
            Assert.Equal(StreamThreadStates.PENDING_SHUTDOWN, thread.State.CurrentState);
        }

        [Fact]
        public void TestStateChangeStartClose()
        {
            var thread = TestUtils.CreateStreamThread(this.streamsBuilder, clientId, false);

            var stateListener = new StateListenerStub();
            thread.SetStateListener(stateListener);

            thread.Start();
            TestUtils.WaitForCondition(
                () => thread.State.CurrentState == StreamThreadStates.STARTING,
                TimeSpan.FromSeconds(10),
                "Thread never started.");

            thread.Shutdown();
            TestUtils.WaitForCondition(
                () => thread.State.CurrentState == StreamThreadStates.DEAD,
                TimeSpan.FromSeconds(10),
                "Thread never shut down.");

            thread.Shutdown();
            Assert.Equal(StreamThreadStates.DEAD, thread.State.CurrentState);
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

        // [Fact]
        // public void testMetricsCreatedAtStartup()
        // {
        // KafkaStreamThread thread = createStreamThread(clientId, config, false);
        // string defaultGroupName = "stream-metrics";
        // var defaultTags = Collections.singletonMap("client-id", thread.getName());
        // string descriptionIsNotVerified = "";
        // 
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "commit-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "commit-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "commit-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "commit-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "poll-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "poll-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "poll-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "poll-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "process-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "process-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "process-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "process-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "punctuate-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "punctuate-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "punctuate-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "punctuate-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "Task-created-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "Task-created-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "Task-closed-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "Task-closed-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "skipped-records-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "skipped-records-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        // 
        // string TaskGroupName = "stream-Task-metrics";
        // var TaskTags =
        //     mkMap(mkEntry("Task-id", "all"), mkEntry("client-id", thread.getName()));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "commit-latency-avg", TaskGroupName, descriptionIsNotVerified, TaskTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "commit-latency-max", TaskGroupName, descriptionIsNotVerified, TaskTags)));
        // Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //     "commit-rate", TaskGroupName, descriptionIsNotVerified, TaskTags)));
        // 
        // JmxReporter reporter = new JmxReporter("kafka.streams");
        // metrics.addReporter(reporter);
        // Assert.Equal(clientId + "-KafkaStreamThread-1", thread.getName());
        // Assert.True(reporter.ContainsMbean(string.Format("kafka.streams:type=%s,client-id=%s",
        //            defaultGroupName,
        //            thread.getName())));
        // Assert.True(reporter.ContainsMbean("kafka.streams:type=stream-Task-metrics,client-id=" + thread.getName() + ",Task-id=all"));
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
        //    TaskManager TaskManager = mockTaskManagerCommit(consumer.Object, 1, 1);

        //    //var streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //    var thread = new KafkaStreamThread(
        //        mockTime,
        //        config,
        //                null,
        //        consumer,
        //        consumer,
        //                null,
        //        TaskManager,
        //        //streamsMetrics,
        //        internalTopologyBuilder,
        //        clientId,
        //                new LogContext(""),
        //                new AtomicInteger()
        //            );

        //    thread.setNow(mockTime.NowAsEpochMilliseconds;);
        //    thread.maybeCommit();
        //    mockTime.sleep(commitInterval - 10L);
        //    thread.setNow(mockTime.NowAsEpochMilliseconds;);
        //    thread.maybeCommit();

        //    EasyMock.verify(TaskManager);
        //}

        //        [Fact]
        //        public void shouldRespectNumIterationsInMainLoop()
        //        {
        //            var mockProcessor = new MockProcessor(PunctuationType.WALL_CLOCK_TIME, 10L);
        //            internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);
        //            internalTopologyBuilder.AddProcessor<string, string>("processor1", () => mockProcessor, "source1");
        //            internalTopologyBuilder.AddProcessor<string, string>("processor2", () => new MockProcessor(PunctuationType.STREAM_TIME, 10L), "source1");
        //
        //            var properties = new StreamsConfig();
        //            properties.Add(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG, 100L);
        //            StreamsConfig config = new StreamsConfig(StreamsTestConfigs.GetStandardConfig(applicationId,
        //                "localhost:2171",
        //                Serdes.ByteArray().GetType().FullName,
        //                Serdes.ByteArray().GetType().FullName,
        //                properties));
        //            KafkaStreamThread thread = createStreamThread(clientId, config, false);
        //
        //            thread.State.SetState(KafkaStreamThreadStates.STARTING);
        //            thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);
        //
        //            varassignedPartitions = new HashSet<TopicPartition> { t1p1 };
        //            thread.TaskManager.SetAssignmentMetadata(
        //                new Dictionary<TaskId, HashSet<TopicPartition>>
        //                {
        //                    { new TaskId(0, this.t1p1.Partition),assignedPartitions }
        //                },
        //                    new Dictionary<TaskId, HashSet<TopicPartition>>());
        //
        //            var mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //            mockConsumer.Assign(new[] { t1p1 });
        //            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //            thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);
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
        //            // processed zero records, early exit and iterations stays.As 2
        //            thread.RunOnce();
        //            Assert.Equal(2, thread.currentNumIterations());
        //
        //            // system time .Ased punctutation halves to 1
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
        //            // stream time .Ased punctutation halves to 1
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
        //            // time .Ased commit, halves iterations to 3 / 2 = 1
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
                    TaskManager TaskManager = mockTaskManagerCommit(consumer, 1, 0);

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                        mockTime,
                        config,
                                null,
                        consumer,
                        consumer,
                                null,
                        TaskManager,
                        streamsMetrics,
                        internalTopologyBuilder,
                        clientId,
                                new LogContext(""),
                                new AtomicInteger()
                            );
                    thread.setNow(mockTime.NowAsEpochMilliseconds;);
                    thread.maybeCommit();
                    mockTime.sleep(commitInterval - 10L);
                    thread.setNow(mockTime.NowAsEpochMilliseconds;);
                    thread.maybeCommit();

                    EasyMock.verify(TaskManager);
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
                    TaskManager TaskManager = mockTaskManagerCommit(consumer, 2, 1);

                    var streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                        mockTime,
                        config,
                                null,
                        consumer,
                        consumer,
                                null,
                        TaskManager,
                        streamsMetrics,
                        internalTopologyBuilder,
                        clientId,
                                new LogContext(""),
                                new AtomicInteger()
                            );
                    thread.setNow(mockTime.NowAsEpochMilliseconds;);
                    thread.maybeCommit();
                    mockTime.sleep(commitInterval + 1);
                    thread.setNow(mockTime.NowAsEpochMilliseconds;);
                    thread.maybeCommit();

                    EasyMock.verify(TaskManager);
                }

                private TaskManager mockTaskManagerCommit(IConsumer<byte[], byte[]> consumer,
                                                          int numberOfCommits,
                                                          int commits)
                {
                    TaskManager TaskManager = EasyMock.createNiceMock(typeof(TaskManager).FullName);
                    EasyMock.expect(TaskManager.commitAll()).andReturn(commits).times(numberOfCommits);
                    EasyMock.replay(TaskManager, consumer);
                    return TaskManager;
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
                    varassignedPartitions = new HashSet<TopicPartition>
                    {

                        //.Assign single partition
                        t1p1,
                        t1p2
                    };

                    activeTasks.Add(Task1, new HashSet<TopicPartition> { t1p1 });
                    activeTasks.Add(Task2, new HashSet<TopicPartition> { t1p2 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    Dictionary<TopicPartition, long> beginOffsets = new Dictionary<TopicPartition, long>();
                    beginOffsets.Add(t1p1, 0L);
                    beginOffsets.Add(t1p2, 0L);
                    mockConsumer.UpdateBeginningOffsets(beginOffsets);
                    thread.RebalanceListener.OnPartitionsAssigned(null, new List<TopicPartition>assignedPartitions));

                    Assert.Equal(1, clientSupplier.producers.Count);
                    var globalProducer = clientSupplier.producers.Get(0);

                    foreach (StreamTask Task in thread.Tasks().Values)
                    {
                        Assert.Same(globalProducer, ((RecordCollectorImpl)((StreamTask)Task).recordCollector()).producer());
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
                    List<TopicPartition>assignedPartitions = new List<TopicPartition>
                    {
                        //.Assign single partition
                        t1p1,
                        t1p2
                    };

                    activeTasks.Add(Task1, new HashSet<TopicPartition> { t1p1 });
                    activeTasks.Add(Task2, new HashSet<TopicPartition> { t1p2 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    Dictionary<TopicPartition, long> beginOffsets = new Dictionary<TopicPartition, long>();
                    beginOffsets.Add(t1p1, 0L);
                    beginOffsets.Add(t1p2, 0L);
                    mockConsumer.UpdateBeginningOffsets(beginOffsets);
                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);

                    thread.RunOnce();

                    Assert.Equal(thread.Tasks().Count, clientSupplier.producers.Count);
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
                    List<TopicPartition>assignedPartitions = new List<TopicPartition>
                    {
                        //.Assign single partition
                        t1p1,
                        t1p2
                    };

                    activeTasks.Add(Task1, new HashSet<TopicPartition> { t1p1 });
                    activeTasks.Add(Task2, new HashSet<TopicPartition> { t1p2 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());
                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    var beginOffsets = new Dictionary<TopicPartition, long>();
                    beginOffsets.Add(t1p1, 0L);
                    beginOffsets.Add(t1p2, 0L);
                    mockConsumer.UpdateBeginningOffsets(beginOffsets);

                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);

                    thread.Shutdown();
                    thread.Run();

                    foreach (var Task in thread.Tasks().Values)
                    {
                        Assert.True(((MockProducer)((RecordCollectorImpl)((StreamTask)Task).recordCollector()).producer()).closed());
                    }
                }

                [Fact]
                public void shouldShutdownTaskManagerOnClose()
                {
                    var consumer = new Mock<IConsumer<byte[], byte[]>>();
                    var TaskManager = new Mock<TaskManager>();
                    TaskManager.Object.Shutdown(true);
                    // EasyMock.expect.AstCall();
                    // EasyMock.replay(TaskManager, consumer);

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                        mockTime,
                        null, null,
                        config,
                        null,
                        consumer,
                        consumer,
                        null,
                        TaskManager,
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
                    TaskManager.Verify();
                }

                [Fact]
                public void shouldShutdownTaskManagerOnCloseWithoutStart()
                {
                    var consumer = new Mock<IConsumer<byte[], byte[]>>();
                    var TaskManager = new Mock<TaskManager>();
                    TaskManager.Object.Shutdown(true);
                    TaskManager.VerifyNoOtherCalls();

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                        mockTime,
                        config,
                                null,
                        consumer,
                        consumer,
                                null,
                        TaskManager,
                        streamsMetrics,
                        internalTopologyBuilder,
                        clientId,
                                new LogContext(""),
                                new AtomicInteger()
                            ).updateThreadMetadata(getSharedAdminClientId(clientId));

                    thread.Shutdown();
                    TaskManager.Verify();
                }

                [Fact]
                public void shouldNotThrowWhenPendingShutdownInRunOnce()
                {
                    mockRunOnce(true);
                }

                [Fact]
                public void shouldNotThrowWithoutPendingShutdownInRunOnce()
                {
                    // A reference test to verify that without intermediate shutdown the runOnce should .Ass
                    // without any exception.
                    mockRunOnce(false);
                }

                private void mockRunOnce(bool shutdownOnPoll)
                {
                    varassignedPartitions = Collections.singletonList(t1p1);

                    MockStreamThreadConsumer<byte[], byte[]> mockStreamThreadConsumer =
                        new MockStreamThreadConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);

                    TaskManager TaskManager = new TaskManager(
                        null, null,
                        new MockChangelogReader(),
                        processId,
                        "this.logger-prefix",
                        mockStreamThreadConsumer,
                        streamsMetadataState,
                        null,
                        null,
                        null,
                        new AssignedStreamsTasks(null),
                        new AssignedStandbyTasks(null));

                    TaskManager.SetConsumer(mockStreamThreadConsumer);
                    TaskManager.SetAssignmentMetadata(new Dictionary<TaskId, HashSet<TopicPartition>>(), new Dictionary<TaskId, HashSet<TopicPartition>>());

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                        mockTime,
                        config,
                                null,
                        mockStreamThreadConsumer,
                        mockStreamThreadConsumer,
                                null,
                        TaskManager,
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
                    var TaskManager = new Mock<TaskManager>();
                    TaskManager.Object.Shutdown(clean: true);
                    TaskManager.VerifyNoOtherCalls();

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                        mockTime,
                        config,
                                null,
                        consumer,
                        consumer,
                                null,
                        TaskManager,
                        streamsMetrics,
                        internalTopologyBuilder,
                        clientId,
                                new LogContext(""),
                                new AtomicInteger()
                            ).updateThreadMetadata(getSharedAdminClientId(clientId));
                    thread.Shutdown();
                    // Execute the run method. Verification of the mock will check that shutdown .As only done once
                    thread.Run();
                    TaskManager.Verify();
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
                        //.Assign single partition
                        { Task1, new HashSet<TopicPartition> { t1p1 } },
                    };

                    thread.TaskManager.SetAssignmentMetadata(new Dictionary<TaskId, HashSet<TopicPartition>>(), standbyTasks);
                    thread.TaskManager.createTasks(new List<TopicPartition>());

                    thread.RebalanceListener.OnPartitionsAssigned(null, new List<TopicPartition>());
                }

                [Fact]
                public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducer.AsFencedWhileProcessing()
                {
                    internalTopologyBuilder.AddSource<string, string>(null, "source", null, null, null, topic1);
                    internalTopologyBuilder.AddSink<string, string>("sink", "dummyTopic", null, null, null, "source");

                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

                    MockConsumer<byte[], byte[]> consumer = clientSupplier.Consumer;

                    consumer.UpdatePartitions(topic1, singletonList(new PartitionInfo(topic1, 1, null, null, null)));

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    Dictionary<TaskId, List<TopicPartition>> activeTasks = new Dictionary<TaskId, List<TopicPartition>>();
                    List<TopicPartition>assignedPartitions = new List<TopicPartition>();

                    //.Assign single partition
                   assignedPartitions.Add(t1p1);
                    activeTasks.Add(Task1, new List<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);

                    thread.RunOnce();
                    Assert.Equal(1, thread.Tasks().Count);
                    MockProducer producer = clientSupplier.producers.Get(0);

                    // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
                    consumer.UpdateBeginningOffsets(Collections.singletonMapassignedPartitions.iterator().MoveNext(), 0L));
                    consumer.Unsubscribe();
                    consumer.Assign(assignedPartitions);

                    consumer.AddRecord(new ConsumeResult<>(topic1, 1, 0, Array.Empty<byte>(), Array.Empty<byte>()));
                    mockTime.sleep(config.GetLong(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG) + 1);
                    thread.RunOnce();
                    Assert.Equal(1, producer.history().Count);

                    Assert.False(producer.transactionCommitted());
                    mockTime.sleep(config.GetLong(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG) + 1L);
                    TestUtils.WaitForCondition(
                        () => producer.commitCount() == 1,
                        "StreamsThread did not commit transaction.");

                    producer.fenceProducer();
                    mockTime.sleep(config.GetLong(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG) + 1L);
                    consumer.AddRecord(new ConsumeResult<>(topic1, 1, 1, Array.Empty<byte>(), Array.Empty<byte>()));
                    try
                    {
                        thread.RunOnce();
                        //Assert.False(true, "Should have thrown TaskMigratedException");
                    }
                    catch (TaskMigratedException expected) { }
                    TestUtils.WaitForCondition(
                        () => !thread.Tasks().Any(),
                                "StreamsThread did not remove fenced zombie Task.");

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
                    List<TopicPartition>assignedPartitions = new List<TopicPartition>
                    {
                        //.Assign single partition
                        t1p1
                    };

                    activeTasks.Add(Task1, new HashSet<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);

                    thread.RunOnce();

                    Assert.Equal(1, thread.Tasks().Count);

                    clientSupplier.producers.Get(0).fenceProducer();
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);
                    Assert.True(clientSupplier.producers.Get(0).transactionInFlight());
                    Assert.False(clientSupplier.producers.Get(0).transactionCommitted());
                    Assert.True(clientSupplier.producers.Get(0).closed());
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
                    List<TopicPartition>assignedPartitions = new List<TopicPartition>
                    {

                        //.Assign single partition
                        t1p1
                    };

                    activeTasks.Add(Task1, new HashSet<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);

                    thread.RunOnce();

                    Assert.Equal(1, thread.Tasks().Count);

                    clientSupplier.producers.Get(0).fenceProducerOnClose();
                    thread.RebalanceListener.OnPartitionsRevoked(null, null);

                    Assert.False(clientSupplier.producers.Get(0).transactionInFlight());
                    Assert.True(clientSupplier.producers.Get(0).transactionCommitted());
                    Assert.False(clientSupplier.producers.Get(0).closed());
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
                    List<TopicPartition>assignedPartitions = new List<TopicPartition>
                    {
                        //.Assign single partition
                        t1p1
                    };

                    activeTasks.Add(Task1, new HashSet<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(assignedPartitions);
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);

                    thread.RunOnce();

                    ThreadMetadata threadMetadata = thread.ThreadMetadata;
                    Assert.Equal(KafkaStreamThreadStates.RUNNING.ToString(), threadMetadata.ThreadState);
                    Assert.Contains(new TaskMetadata(Task1.ToString(), Utils.mkSet(t1p1)), threadMetadata.ActiveTasks);
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

                        //.Assign single partition
                        { Task1, new HashSet<TopicPartition> { t1p1 } }
                    };

                    thread.TaskManager.SetAssignmentMetadata(new Dictionary<TaskId, HashSet<TopicPartition>>(), standbyTasks);

                    thread.RebalanceListener.OnPartitionsAssigned(null, new List<TopicPartition>());

                    thread.RunOnce();

                    ThreadMetadata threadMetadata = thread.ThreadMetadata;
                    Assert.Equal(KafkaStreamThreadStates.RUNNING.ToString(), threadMetadata.ThreadState);
                    Assert.Contains(new TaskMetadata(Task1.ToString(), Utils.mkSet(t1p1)), threadMetadata.standbyTasks);
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
                    internalStreamsBuilder.Table(topic2, new ConsumedInternal<>(), materialized);

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
                        = new OffsetCheckpoint(new File(stateDirectory.directoryForTask(Task3), CHECKPOINT_FILE_NAME));
                    checkpoint.write(Collections.singletonMap(partition2, 5L));

                    for (long i = 0L; i < 10L; i++)
                    {
                        RestoreConsumer.AddRecord(new ConsumeResult(
                            changelogName1,
                            1,
                            i,
                            ("K" + i).getBytes(),
                            ("V" + i).getBytes()));
                        RestoreConsumer.AddRecord(new ConsumeResult(
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
                        //.Assign single partition
                        { Task1, new HashSet<TopicPartition> { t1p1 } },
                        { Task3, new HashSet<TopicPartition> { t2p1 } }
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
                    ILogger this.logger = logContext.logger(typeof(StreamThreadTest));
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
                    //    //
                    //    public void init(IProcessorContext context)
                    //    {
                    //        context.schedule(TimeSpan.FromMilliseconds(100L), PunctuationType.STREAM_TIME, punctuatedStreamTime::add);
                    //        context.schedule(TimeSpan.FromMilliseconds(100L), PunctuationType.WALL_CLOCK_TIME, punctuatedWallClockTime::add);
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
                    List<TopicPartition>assignedPartitions = new List<TopicPartition>();

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();

                    //.Assign single partition
                   assignedPartitions.Add(t1p1);
                    activeTasks.Add(Task1, new HashSet<TopicPartition> { t1p1 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

                    clientSupplier.Consumer.Assign(assignedPartitions);
                    clientSupplier.Consumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);

                    thread.RunOnce();

                    Assert.Equal(0, punctuatedStreamTime.Count);
                    Assert.Equal(0, punctuatedWallClockTime.Count);

                    mockTime.sleep(100L);
                    for (long i = 0L; i < 10L; i++)
                    {
                        clientSupplier.Consumer.AddRecord(new ConsumeResult<>(
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

                    varassignedPartitions = new List<TopicPartitionOffset>();

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.RebalanceListener.OnPartitionsRevoked(null,assignedPartitions);
                   .AssertThreadMetadata.AsEmptyTasksWithState(thread.ThreadMetadata, KafkaStreamThreadStates.PARTITIONS_REVOKED);

                    var activeTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
                    var standbyTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();

                    //.Assign single partition
                   assignedPartitions.Add(t1p1);
                    activeTasks.Add(Task1, new HashSet<TopicPartition> { t1p1 });
                    standbyTasks.Add(Task2, new HashSet<TopicPartition> { t1p2 });

                    thread.TaskManager.SetAssignmentMetadata(activeTasks, standbyTasks);

                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);

                   .AssertThreadMetadata.AsEmptyTasksWithState(thread.ThreadMetadata, KafkaStreamThreadStates.PARTITIONS_ASSIGNED);
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

                        TestUtils.WaitForCondition(
                            () => mockRestoreConsumer.Assignment.Count == 1,
                            "Never restore first record");

                        mockRestoreConsumer.addRecord(new ConsumeResult<>(
                            "stream-thread-test-count-changelog",
                            0,
                            0L,
                            "K1".getBytes(),
                            "V1".getBytes()));

                        TestUtils.WaitForCondition(
                            () => mockRestoreConsumer.position(changelogPartition) == 1L,
                            "Never restore first record");

                        //mockRestoreConsumer.SetException(new InvalidOffsetException("Try Again!"));
                        //            {
                        //            public override List<TopicPartition> partitions()
                        //    {
                        //        return changelogPartitionSet;
                        //    }
                        //});

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

                        TestUtils.WaitForCondition(
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
                    config.Set(
                        StreamsConfigPropertyNames.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                        LogAndContinueExceptionHandler.getName());
                    config.Set(StreamsConfigPropertyNames.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Int().GetType().FullName);
                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(config), false);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);

                    List<TopicPartition>assignedPartitions = new List<TopicPartition> { t1p1 };
                    thread.TaskManager.SetAssignmentMetadata(
                        Collections.singletonMap(
                            new TaskId(0, t1p1.Partition),
                           assignedPartitions),
                        Collections.emptyMap());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(new[] { t1p1 });
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);
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
                    mockConsumer.addRecord(new ConsumeResult<>(
                        t1p1.Topic,
                        t1p1.Partition,
                        ++offset, -1,
                        TimestampType.CreateTime,
                        ConsumeResult.NULL_CHECKSUM,
                        -1,
                        -1,
                        Array.Empty<byte>(),
                        "I am not an integer.".getBytes()));
                    mockConsumer.addRecord(new ConsumeResult<>(
                        t1p1.Topic,
                        t1p1.Partition,
                        ++offset,
                        -1,
                        TimestampType.CreateTime,
                        ConsumeResult.NULL_CHECKSUM,
                        -1,
                        -1,
                        Array.Empty<byte>(),
                        "I am not an integer.".getBytes()));
                    thread.RunOnce();
                    Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
                    Assert.NotEquals(0.0, metrics.metric(skippedRateMetric).metricValue());

                    LogCaptureAppender.unregister(appender);
                    List<string> strings = appender.getMessages();
                    Assert.Contains("Task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[0]", strings);
                    Assert.Contains("Task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[1]", strings);
                }

                [Fact]
                public void shouldReportSkippedRecordsForInvalidTimestamps()
                {
                    LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

                    internalTopologyBuilder.AddSource<string, string>(null, "source1", null, null, null, topic1);

                    var config = ConfigProps(false);
                    config.Set(
                        StreamsConfigPropertyNames.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                        LogAndSkipOnInvalidTimestamp.getName());
                    KafkaStreamThread thread = CreateStreamThread(clientId, new StreamsConfig(config), false);

                    thread.State.SetState(KafkaStreamThreadStates.STARTING);
                    thread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED);

                    List<TopicPartition>assignedPartitions = new List<TopicPartition> { t1p1 };
                    thread.TaskManager.SetAssignmentMetadata(
                        Collections.singletonMap(
                                new TaskId(0, t1p1.Partition),
                               assignedPartitions),
                            new Dictionary<TaskId, HashSet<TopicPartition>>());

                    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
                    mockConsumer.Assign(new[] { t1p1 });
                    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
                    thread.RebalanceListener.OnPartitionsAssigned(null,assignedPartitions);
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
                    Assert.Contains("Task [0_1] Skipping record due to negative extracted timestamp. " +
                                "topic=[topic1] partition=[1] offset=[0] extractedTimestamp=[-1] " +
                                "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("Task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[1] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("Task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[2] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("Task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[3] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("Task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[4] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                    Assert.Contains("Task [0_1] Skipping record due to negative extracted timestamp. " +
                            "topic=[topic1] partition=[1] offset=[5] extractedTimestamp=[-1] " +
                            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        , strings);
                }

                private void assertThreadMetadata.AsEmptyTasksWithState(ThreadMetadata metadata,
                                                                        KafkaStreamThreadStates state)
                {
                    Assert.Equal(state.ToString(), metadata.ThreadState);
                    Assert.False(metadata.ActiveTasks.Any());
                    Assert.False(metadata.standbyTasks.Any());
                }

                [Fact]
                // TODO: Need to add a test case covering EOS when we Create a mock TaskManager class
                public void producerMetricsVerificationWithoutEOS()
                {
                    MockProducer<byte[], byte[]> producer = new MockProducer<>();
                    IConsumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
                    TaskManager TaskManager = mockTaskManagerCommit(consumer, 1, 0);

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                            mockTime,
                            config,
                            producer,
                            consumer,
                            consumer,
                                    null,
                            TaskManager,
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
                        (M.Asurable)(config, now)=> 0,
                        null,
                        new MockTime());
                    producer.setMockMetrics(testMetricName, testMetric);
                    Dictionary<MetricName, Metric> producerMetrics = thread.producerMetrics();
                    Assert.Equal(testMetricName, producerMetrics.Get(testMetricName).metricName());
                }

                [Fact]
                public void adminClientMetricsVerification()
                {
                    Node broker1 = new Node(0, "dummyHost-1", 1234);
                    Node broker2 = new Node(1, "dummyHost-2", 1234);
                    List<Node> cluster =asList(broker1, broker2);

                    MockAdminClient adminClient = new MockAdminClient(cluster, broker1, null);

                    MockProducer<byte[], byte[]> producer = new MockProducer<>();
                    IConsumer<byte[], byte[]> consumer = EasyMock.createNiceMock(Consumer);
                    TaskManager TaskManager = EasyMock.createNiceMock(TaskManager);

                    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
                    KafkaStreamThread thread = new KafkaStreamThread(
                            mockTime,
                            config,
                            producer,
                            consumer,
                            consumer,
                                    null,
                            TaskManager,
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
                        (M.Asurable)(config, now)=> 0,
                        null,
                        new MockTime());

                    EasyMock.expect(TaskManager.getAdminClient()).andReturn(adminClient);
                    EasyMock.expect.AstCall();
                    EasyMock.replay(TaskManager, consumer);

                    adminClient.setMockMetrics(testMetricName, testMetric);
                    Dictionary<MetricName, Metric> adminClientMetrics = thread.adminClientMetrics();
                    Assert.Equal(testMetricName, adminClientMetrics.Get(testMetricName).metricName());
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
                    mockConsumer.addRecord(new ConsumeResult<>(
                        t1p1.Topic,
                        t1p1.Partition,
                        offset,
                        timestamp,
                        TimestampType.CreateTime,
                        ConsumeResult.NULL_CHECKSUM,
                        -1,
                        -1,
                        Array.Empty<byte>(),
                        Array.Empty<byte>()));
                }
        */
    }
}
