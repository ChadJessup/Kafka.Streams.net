﻿using Confluent.Kafka;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Threads.Stream;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class StreamThreadTests
    {
        private readonly string clientId = "clientId";
        private readonly string applicationId = "stream-thread-test";
        private readonly int threadIdx = 1;
        private readonly MockTime mockTime = new MockTime();
        //private  Metrics metrics = new Metrics();
        private readonly MockClientSupplier clientSupplier = new MockClientSupplier();
        private readonly InternalStreamsBuilder? internalStreamsBuilder = null;
        private readonly StreamsConfig config;
        private readonly DirectoryInfo stateDir = TestUtils.GetTempDirectory();
        private StateDirectory? stateDirectory = null;
        private readonly ConsumedInternal<object, object> consumed = new ConsumedInternal<object, object>();

        private Guid processId = Guid.NewGuid();
        //private StreamsBuilder streamsBuilder;
        //private InternalTopologyBuilder internalTopologyBuilder;
        //private StreamsMetadataState streamsMetadataState;

        public StreamThreadTests()
        {
            this.processId = Guid.NewGuid();
            this.config = StreamsTestConfigs.GetStandardConfig();

            // internalTopologyBuilder = this.streamsBuilder.InternalTopologyBuilder;
            // internalTopologyBuilder.SetApplicationId(applicationId);
            // streamsMetadataState = new StreamsMetadataState(this.streamsBuilder.Topology, this.config);
        }

        private const string topic1 = "topic1";
        private const string topic2 = "topic2";

        private readonly TopicPartition t1p1 = new TopicPartition(topic1, 1);
        private readonly TopicPartition t1p2 = new TopicPartition(topic1, 2);
        private readonly TopicPartition t2p1 = new TopicPartition(topic2, 1);

        // Task0 is unused
        private readonly TaskId Task1 = new TaskId(0, 1);
        private readonly TaskId Task2 = new TaskId(0, 2);
        private readonly TaskId Task3 = new TaskId(1, 1);


        [Fact]
        public void TestStateChangeStartClose() //// throws Exception
        {
            var streamsBuilder = TestUtils.GetStreamsBuilder(this.config);
            var thread = TestUtils.CreateStreamThread(streamsBuilder, this.clientId, false);

            var stateListener = new StateListenerStub();
            thread.SetStateListener(stateListener);

            thread.Start();
            TestUtils.WaitForCondition(
                () => thread.State.CurrentState == StreamThreadStates.STARTING,
                10 * 1000,
                "Thread never started.");

            thread.Shutdown();
            TestUtils.WaitForCondition(
                () => thread.State.CurrentState == StreamThreadStates.DEAD,
                TimeSpan.FromMilliseconds(10 * 1000),
                "Thread never shut down.");

            thread.Shutdown();
            Assert.Equal(StreamThreadStates.DEAD, thread.State.CurrentState);
        }

        //        private Cluster createCluster()
        //        {
        //            Node node = new Node(0, "localhost", 8121);
        //            return new Cluster(
        //                "mockClusterId",
        //                Collections.singletonList(node),
        //                Collections.emptySet(),
        //                Collections.emptySet(),
        //                Collections.emptySet(),
        //                node
        //            );
        //        }

        //        //    [Fact]
        //        //public void testMetricsCreatedAtStartup()
        //        //{
        //        //    StreamThread thread = createStreamThread(clientId, config, false);
        //        //    string defaultGroupName = "stream-metrics";
        //        //    Dictionary<string, string> defaultTags = Collections.singletonMap("client-id", thread.getName());
        //        //    string descriptionIsNotVerified = "";

        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "commit-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "commit-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "commit-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "commit-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "poll-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "poll-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "poll-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "poll-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "process-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "process-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "process-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "process-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "punctuate-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "punctuate-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "punctuate-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "punctuate-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "Task-created-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "Task-created-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "Task-closed-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "Task-closed-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "skipped-records-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "skipped-records-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));

        //        //    string TaskGroupName = "stream-Task-metrics";
        //        //    Dictionary<string, string> TaskTags =
        //        //       mkMap(mkEntry("Task-id", "All"), mkEntry("client-id", thread.getName()));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "commit-latency-avg", TaskGroupName, descriptionIsNotVerified, TaskTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "commit-latency-max", TaskGroupName, descriptionIsNotVerified, TaskTags)));
        //        //    Assert.NotNull(metrics.metrics().Get(metrics.metricName(
        //        //        "commit-rate", TaskGroupName, descriptionIsNotVerified, TaskTags)));

        //        //    JmxReporter reporter = new JmxReporter("kafka.streams");
        //        //    metrics.addReporter(reporter);
        //        //    Assert.Equal(clientId + "-StreamThread-1", thread.getName());
        //        //    Assert.True(reporter.ContainsMbean(string.Format("kafka.streams:type=%s,client-id=%s",
        //        //               defaultGroupName,
        //        //               thread.getName())));
        //        //    Assert.True(reporter.ContainsMbean("kafka.streams:type=stream-Task-metrics,client-id=" + thread.getName() + ",Task-id=All"));
        //        //}

        [Fact]
        public void ShouldNotCommitBeforeTheCommitInterval()
        {
            var commitInterval = 1000L;
            var props = StreamsTestConfigs.GetStandardConfig();
            var mockTaskManager = TestUtils.GetMockTaskManagerCommit(1);
            var sc = new ServiceCollection()
                .AddSingleton(mockTaskManager.Object)
                .AddSingleton(props);

            props.StateStoreDirectory = this.stateDir;
            props.CommitIntervalMs = commitInterval;
            var streamsBuilder = TestUtils.GetStreamsBuilder(sc);
            this.stateDirectory = new StateDirectory(streamsBuilder.Context.CreateLogger<StateDirectory>(), streamsBuilder.Context.StreamsConfig);
            var consumer = new Mock<IConsumer<byte[], byte[]>>();

            IStreamThread thread = TestUtils.CreateStreamThread(streamsBuilder);

            thread.SetNow(this.mockTime.UtcNow);
            thread.MaybeCommit();
            this.mockTime.Sleep(commitInterval - 10L);
            thread.SetNow(this.mockTime.UtcNow);
            thread.MaybeCommit();
        }

        //        [Fact]
        //        public void shouldRespectNumIterationsInMainLoop()
        //        {
        //            MockProcessor mockProcessor = new MockProcessor(PunctuationType.WALL_CLOCK_TIME, 10L);
        //            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);
        //            internalTopologyBuilder.AddProcessor("processor1", () => mockProcessor, "source1");
        //            internalTopologyBuilder.AddProcessor("processor2", () => new MockProcessor(PunctuationType.STREAM_TIME, 10L), "source1");

        //           StreamsConfig properties = new StreamsConfig();
        //            properties.Add(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        //            StreamsConfig config = new StreamsConfig(StreamsTestConfigs.GetStandardConfig(applicationId,
        //               "localhost:2171",
        //               Serdes.ByteArraySerde.getName(),
        //                    Serdes.ByteArraySerde.getName(),
        //                    properties));
        //            StreamThread thread = createStreamThread(clientId, config, false);

        //            thread.SetState(StreamThreadStates.STARTING);
        //            thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);

        //            List<TopicPartition>assignedPartitions = Collections.singleton(t1p1);
        //            thread.TaskManager.SetAssignmentMetadata(
        //                Collections.singletonMap(
        //                            new TaskId(0, t1p1.Partition),
        //                           assignedPartitions),
        //                        Collections.emptyMap());

        //            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //            mockConsumer.Assign(Collections.singleton(t1p1));
        //            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //            thread.RebalanceListener.onPartitionsAssignedassignedPartitions);
        //            thread.RunOnce();

        //            // processed one record, punctuated after the first record, and hence num.iterations is still 1
        //            long offset = -1;
        //            addRecord(mockConsumer, ++offset, 0L);
        //            thread.RunOnce();

        //            Assert.Equal(thread.currentNumIterations(), (1));

        //            // processed one more record without punctuation, and bump num.iterations to 2
        //            addRecord(mockConsumer, ++offset, 1L);
        //            thread.RunOnce();

        //            Assert.Equal(thread.currentNumIterations(), (2));

        //            // processed zero records, early exit and iterations stays.As 2
        //            thread.RunOnce();
        //            Assert.Equal(thread.currentNumIterations(), (2));

        //            // system time .Ased punctutation halves to 1
        //            mockTime.Sleep(11L);

        //            thread.RunOnce();
        //            Assert.Equal(thread.currentNumIterations(), (1));

        //            // processed two records, bumping up iterations to 2
        //            addRecord(mockConsumer, ++offset, 5L);
        //            addRecord(mockConsumer, ++offset, 6L);
        //            thread.RunOnce();

        //            Assert.Equal(thread.currentNumIterations(), (2));

        //            // stream time .Ased punctutation halves to 1
        //            addRecord(mockConsumer, ++offset, 11L);
        //            thread.RunOnce();

        //            Assert.Equal(thread.currentNumIterations(), (1));

        //            // processed three records, bumping up iterations to 3 (1 + 2)
        //            addRecord(mockConsumer, ++offset, 12L);
        //            addRecord(mockConsumer, ++offset, 13L);
        //            addRecord(mockConsumer, ++offset, 14L);
        //            thread.RunOnce();

        //            Assert.Equal(thread.currentNumIterations(), (3));

        //            mockProcessor.requestCommit();
        //            addRecord(mockConsumer, ++offset, 15L);
        //            thread.RunOnce();

        //            // user requested commit should not impact on iteration adjustment
        //            Assert.Equal(thread.currentNumIterations(), (3));

        //            // time .Ased commit, halves iterations to 3 / 2 = 1
        //            mockTime.Sleep(90L);
        //            thread.RunOnce();

        //            Assert.Equal(thread.currentNumIterations(), (1));

        //        }

        //        [Fact]
        //        public void shouldNotCauseExceptionIfNothingCommitted()
        //        {
        //            long commitInterval = 1000L;
        //           StreamsConfig props = configProps(false);
        //            props.Set(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        //            props.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.ToString(commitInterval));

        //            StreamsConfig config = new StreamsConfig(props);
        //            IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer);
        //            TaskManager TaskManager = mockTaskManagerCommit(consumer, 1, 0);

        //            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //            StreamThread thread = new StreamThread(
        //               mockTime,
        //               config,
        //                       null,
        //               consumer,
        //               consumer,
        //                       null,
        //               TaskManager,
        //               streamsMetrics,
        //               internalTopologyBuilder,
        //               clientId,
        //                       new LogContext(""),
        //                       new int()
        //                   );
        //            thread.SetNow(mockTime.NowAsEpochMilliseconds);
        //            thread.maybeCommit();
        //            mockTime.Sleep(commitInterval - 10L);
        //            thread.SetNow(mockTime.NowAsEpochMilliseconds);
        //            thread.maybeCommit();

        //            EasyMock.verify(TaskManager);
        //        }

        //        [Fact]
        //        public void shouldCommitAfterTheCommitInterval()
        //        {
        //            long commitInterval = 1000L;
        //           StreamsConfig props = configProps(false);
        //            props.Set(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        //            props.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.ToString(commitInterval));

        //            StreamsConfig config = new StreamsConfig(props);
        //            IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer);
        //            TaskManager TaskManager = mockTaskManagerCommit(consumer, 2, 1);

        //            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //            StreamThread thread = new StreamThread(
        //               mockTime,
        //               config,
        //                       null,
        //               consumer,
        //               consumer,
        //                       null,
        //               TaskManager,
        //               streamsMetrics,
        //               internalTopologyBuilder,
        //               clientId,
        //                       new LogContext(""),
        //                       new int()
        //                   );
        //            thread.SetNow(mockTime.NowAsEpochMilliseconds);
        //            thread.maybeCommit();
        //            mockTime.Sleep(commitInterval + 1);
        //            thread.SetNow(mockTime.NowAsEpochMilliseconds);
        //            thread.maybeCommit();

        //            EasyMock.verify(TaskManager);
        //        }

        //        [Fact]
        //        public void shouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled()
        //        {
        //            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);
        //            internalStreamsBuilder.BuildAndOptimizeTopology();

        //            StreamThread thread = createStreamThread(clientId, config, false);

        //            thread.State.SetState(StreamThreadStates.STARTING);
        //            thread.RebalanceListener.OnPartitionsRevoked(null, new List<TopicPartitionOffset>());

        //            var ActiveTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();
        //            List<TopicPartition>assignedPartitions = new List<TopicPartition>
        //            {
        //                //.Assign single partition
        //                t1p1,
        //                t1p2
        //            };

        //            ActiveTasks.Add(Task1, Collections.singleton(t1p1));
        //            ActiveTasks.Add(Task2, Collections.singleton(t1p2));

        //            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, new Dictionary<TaskId, HashSet<TopicPartition>>());

        //            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //            mockConsumer.Assign(assignedPartitions);
        //            var beginOffsets = new Dictionary<TopicPartition, long>();
        //            beginOffsets.Add(t1p1, 0L);
        //            beginOffsets.Add(t1p2, 0L);
        //            mockConsumer.UpdateBeginningOffsets(beginOffsets);
        //            thread.RebalanceListener.OnPartitionsAssigned(null, new HashSet<TopicPartition>assignedPartitions));

        //            Assert.Equal(1, clientSupplier.producers.Count);
        //            Producer globalProducer = clientSupplier.producers.Get(0);
        //            foreach (Task Task in thread.Tasks().values())
        //            {
        //               .AssertSame(globalProducer, ((RecordCollector)((StreamTask)Task).recordCollector()).producer());
        //            }
        //           .AssertSame(clientSupplier.Consumer, thread.Consumer);
        //           .AssertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
        //        }

        //        [Fact]
        //        public void shouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable()
        //        {
        //            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

        //            StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        //            thread.SetState(StreamThreadStates.STARTING);
        //            thread.RebalanceListener.OnPartitionsRevoked(Collections.emptyList());

        //            Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();
        //            List<TopicPartition>assignedPartitions = new List<>();

        //            //.Assign single partition
        //           assignedPartitions.Add(t1p1);
        //           assignedPartitions.Add(t1p2);
        //            ActiveTasks.Add(Task1, Collections.singleton(t1p1));
        //            ActiveTasks.Add(Task2, Collections.singleton(t1p2));

        //            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

        //            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //            mockConsumer.Assign(assignedPartitions);
        //            Dictionary<TopicPartition, long> beginOffsets = new HashMap<>();
        //            beginOffsets.Add(t1p1, 0L);
        //            beginOffsets.Add(t1p2, 0L);
        //            mockConsumer.UpdateBeginningOffsets(beginOffsets);
        //            thread.RebalanceListener.OnPartitionsAssigned(new HashSet<>assignedPartitions));

        //            thread.RunOnce();

        //            Assert.Equal(thread.Tasks().Count, clientSupplier.producers.Count);
        //           .AssertSame(clientSupplier.Consumer, thread.Consumer);
        //           .AssertSame(clientSupplier.restoreConsumer, thread.restoreConsumer);
        //        }

        //        [Fact]
        //        public void shouldCloseAllTaskProducersOnCloseIfEosEnabled()
        //        {
        //            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

        //            StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        //            thread.SetState(StreamThreadStates.STARTING);
        //            thread.RebalanceListener.OnPartitionsRevoked(Collections.emptyList());

        //            Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();
        //            List<TopicPartition>assignedPartitions = new List<>();

        //            //.Assign single partition
        //           assignedPartitions.Add(t1p1);
        //           assignedPartitions.Add(t1p2);
        //            ActiveTasks.Add(Task1, Collections.singleton(t1p1));
        //            ActiveTasks.Add(Task2, Collections.singleton(t1p2));

        //            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());
        //            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //            mockConsumer.Assign(assignedPartitions);
        //            Dictionary<TopicPartition, long> beginOffsets = new HashMap<>();
        //            beginOffsets.Add(t1p1, 0L);
        //            beginOffsets.Add(t1p2, 0L);
        //            mockConsumer.UpdateBeginningOffsets(beginOffsets);

        //            thread.RebalanceListener.onPartitionsAssignedassignedPartitions);

        //            thread.Shutdown();
        //            thread.run();

        //            foreach (Task Task in thread.Tasks().values())
        //            {
        //                Assert.True(((MockProducer)((RecordCollector)((StreamTask)Task).recordCollector()).producer()).closed());
        //            }
        //        }

        //        [Fact]
        //        public void shouldShutdownTaskManagerOnClose()
        //        {
        //            IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer);
        //            TaskManager TaskManager = Mock.Of<TaskManager);
        //            TaskManager.Shutdown(true);
        //            EasyMock.expect.AstCall();
        //            EasyMock.replay(TaskManager, consumer);

        //            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //            StreamThread thread = new StreamThread(
        //               mockTime,
        //               config,
        //                       null,
        //               consumer,
        //               consumer,
        //                       null,
        //               TaskManager,
        //               streamsMetrics,
        //               internalTopologyBuilder,
        //               clientId,
        //                       new LogContext(""),
        //                       new int()
        //                   ).UpdateThreadMetadata(getSharedAdminClientId(clientId));
        //            thread.SetStateListener(
        //                (t, newState, oldState) =>
        //                {
        //                    if (oldState == StreamThreadStates.CREATED && newState == StreamThreadStates.STARTING)
        //                    {
        //                        thread.Shutdown();
        //                    }
        //                });
        //            thread.run();
        //            EasyMock.verify(TaskManager);
        //        }

        //        [Fact]
        //        public void shouldShutdownTaskManagerOnCloseWithoutStart()
        //        {
        //            IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer);
        //            TaskManager TaskManager = Mock.Of<TaskManager);
        //            TaskManager.Shutdown(true);
        //            EasyMock.expect.AstCall();
        //            EasyMock.replay(TaskManager, consumer);

        //            StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //            StreamThread thread = new StreamThread(
        //               mockTime,
        //               config,
        //                       null,
        //               consumer,
        //               consumer,
        //                       null,
        //               TaskManager,
        //               streamsMetrics,
        //               internalTopologyBuilder,
        //               clientId,
        //                       new LogContext(""),
        //                       new int()
        //                   ).UpdateThreadMetadata(getSharedAdminClientId(clientId));
        //            thread.Shutdown();
        //            EasyMock.verify(TaskManager);
        //        }

        //        [Fact]
        //        public void shouldNotThrowWhenPendingShutdownInRunOnce()
        //        {
        //            mockRunOnce(true);
        //        }

        //        [Fact]
        //        public void shouldNotThrowWithoutPendingShutdownInRunOnce()
        //        {
        //            // A reference test to verify that without intermediate Shutdown the RunOnce should .Ass
        //            // without any exception.
        //            mockRunOnce(false);
        //        }

        //        private void mockRunOnce(bool shutdownOnPoll)
        //        {
        //            Collection<TopicPartition>assignedPartitions = Collections.singletonList(t1p1);
        //        class MockStreamThreadConsumer<K, V> : MockConsumer<K, V> {

        //            private StreamThread streamThread;

        //        private StreamThreadTests(OffsetResetStrategy offsetResetStrategy)
        //        {
        //            super(offsetResetStrategy);
        //        }

        //
        //            public ConsumeResult<K, V> poll(TimeSpan timeout)
        //{
        //            Assert.NotNull(streamThread);
        //            if (shutdownOnPoll)
        //            {
        //                streamThread.Shutdown();
        //            }
        //            streamThread.RebalanceListener.onPartitionsAssignedassignedPartitions);
        //            return base.poll(timeout);
        //        }

        //        private void setStreamThread(StreamThread streamThread)
        //        {
        //            this.streamThread = streamThread;
        //        }
        //    }

        //    MockStreamThreadConsumer<byte[], byte[]> mockStreamThreadConsumer =
        //       new MockStreamThreadConsumer<>(OffsetResetStrategy.EARLIEST);

        //    TaskManager TaskManager = new TaskManager(new MockChangelogReader(),
        //                                                           processId,
        //                                                           "this.logger-prefix",
        //                                                           mockStreamThreadConsumer,
        //                                                           streamsMetadataState,
        //                                                           null,
        //                                                           null,
        //                                                           null,
        //                                                           new AssignedStreamsTasks(new LogContext()),
        //                                                           new AssignedStandbyTasks(new LogContext()));
        //    TaskManager.SetConsumer(mockStreamThreadConsumer);
        //        TaskManager.SetAssignmentMetadata(Collections.emptyMap(), Collections.emptyMap());

        //         StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //    StreamThread thread = new StreamThread(
        //       mockTime,
        //       config,
        //               null,
        //       mockStreamThreadConsumer,
        //       mockStreamThreadConsumer,
        //               null,
        //       TaskManager,
        //       streamsMetrics,
        //       internalTopologyBuilder,
        //       clientId,
        //               new LogContext(""),
        //               new int()
        //           ).UpdateThreadMetadata(getSharedAdminClientId(clientId));

        //    mockStreamThreadConsumer.setStreamThread(thread);
        //        mockStreamThreadConsumer.Assign(assignedPartitions);
        //        mockStreamThreadConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

        //        addRecord(mockStreamThreadConsumer, 1L, 0L);
        //    thread.SetState(StreamThreadStates.STARTING);
        //        thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);
        //        thread.RunOnce();
        //    }

        //[Fact]
        //public void shouldOnlyShutdownOnce()
        //{
        //    IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer);
        //    TaskManager TaskManager = Mock.Of<TaskManager);
        //    TaskManager.Shutdown(true);
        //    EasyMock.expect.AstCall();
        //    EasyMock.replay(TaskManager, consumer);

        //    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //    StreamThread thread = new StreamThread(
        //       mockTime,
        //       config,
        //               null,
        //       consumer,
        //       consumer,
        //               null,
        //       TaskManager,
        //       streamsMetrics,
        //       internalTopologyBuilder,
        //       clientId,
        //               new LogContext(""),
        //               new int()
        //           ).UpdateThreadMetadata(getSharedAdminClientId(clientId));
        //    thread.Shutdown();
        //    // Execute the run method. Verification of the mock will check that Shutdown .As only done once
        //    thread.run();
        //    EasyMock.verify(TaskManager);
        //}

        //[Fact]
        //public void shouldNotNullPointerWhenStandbyTasksAssignedAndNoStateStoresForTopology()
        //{
        //    internalTopologyBuilder.AddSource(null, "Name", null, null, null, "topic");
        //    internalTopologyBuilder.AddSink("out", "output", null, null, null, "Name");

        //    StreamThread thread = createStreamThread(clientId, config, false);

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.RebalanceListener.OnPartitionsRevoked(Collections.emptyList());

        //    Dictionary<TaskId, List<TopicPartition>> StandbyTasks = new HashMap<>();

        //    //.Assign single partition
        //    StandbyTasks.Add(Task1, Collections.singleton(t1p1));

        //    thread.TaskManager.SetAssignmentMetadata(Collections.emptyMap(), StandbyTasks);
        //    thread.TaskManager.CreateTasks(Collections.emptyList());

        //    thread.RebalanceListener.OnPartitionsAssigned(Collections.emptyList());
        //}

        //[Fact]
        //public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducer.AsFencedWhileProcessing()// throws Exception

        //{
        //    internalTopologyBuilder.AddSource(null, "source", null, null, null, topic1);
        //    internalTopologyBuilder.AddSink("sink", "dummyTopic", null, null, null, "source");

        //    StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        //         MockConsumer<byte[], byte[]> consumer = clientSupplier.Consumer;

        //consumer.updatePartitions(topic1, Collections.singletonList(new PartitionInfo(topic1, 1, null, null, null)));

        //        thread.SetState(StreamThreadStates.STARTING);
        //        thread.RebalanceListener.OnPartitionsRevoked(null);

        //         Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();
        //List<TopicPartition>assignedPartitions = new List<>();

        //// assign single partition
        //assignedPartitions.Add(t1p1);
        //        ActiveTasks.Add(Task1, Collections.singleton(t1p1));

        //        thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

        //MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //mockConsumer.Assign(assignedPartitions);
        //        mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //        thread.RebalanceListener.onPartitionsAssignedassignedPartitions);

        //        thread.RunOnce();
        //        Assert.Equal(thread.Tasks().Count, (1));
        //         MockProducer producer = clientSupplier.producers.Get(0);

        //// change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
        //consumer.UpdateBeginningOffsets(Collections.singletonMapassignedPartitions.iterator().MoveNext(), 0L));
        //        consumer.Unsubscribe();
        //        consumer.Assign(new HashSet<>assignedPartitions));

        //        consumer.AddRecord(new ConsumeResult<>(topic1, 1, 0, new byte[0], new byte[0]));
        //        mockTime.Sleep(config.GetLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1);
        //        thread.RunOnce();
        //        Assert.Equal(producer.history().Count, (1));

        //       Assert.False(producer.transactionCommitted());
        //mockTime.Sleep(config.GetLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        //        TestUtils.WaitForCondition(
        //            () => producer.commitCount() == 1,
        //            "StreamsThread did not commit transaction.");

        //        producer.fenceProducer();
        //        mockTime.Sleep(config.GetLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
        //        consumer.AddRecord(new ConsumeResult<>(topic1, 1, 1, new byte[0], new byte[0]));
        //        try {
        //            thread.RunOnce();
        //            Assert.False(true, "Should have thrown TaskMigratedException");
        //        } catch (TaskMigratedException expected) { /* ignore */ }
        //        TestUtils.WaitForCondition(
        //            () => thread.Tasks().IsEmpty(),
        //            "StreamsThread did not remove fenced zombie Task.");

        //        Assert.Equal(producer.commitCount(), (1L));
        //    }

        //    [Fact]
        //public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedInCommitTransactionWhenSuspendingTaks()
        //{
        //    StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        //    internalTopologyBuilder.AddSource(null, "Name", null, null, null, topic1);
        //    internalTopologyBuilder.AddSink("out", "output", null, null, null, "Name");

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.RebalanceListener.OnPartitionsRevoked(null);

        //    Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();
        //    List<TopicPartition>assignedPartitions = new List<>();

        //    //.Assign single partition
        //   assignedPartitions.Add(t1p1);
        //    ActiveTasks.Add(Task1, Collections.singleton(t1p1));

        //    thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

        //    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //    mockConsumer.Assign(assignedPartitions);
        //    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //    thread.RebalanceListener.onPartitionsAssignedassignedPartitions);

        //    thread.RunOnce();

        //    Assert.Equal(thread.Tasks().Count, (1));

        //    clientSupplier.producers.Get(0).fenceProducer();
        //    thread.RebalanceListener.OnPartitionsRevoked(null);
        //    Assert.True(clientSupplier.producers.Get(0).transactionInFlight());
        //   Assert.False(clientSupplier.producers.Get(0).transactionCommitted());
        //    Assert.True(clientSupplier.producers.Get(0).closed());
        //    Assert.True(thread.Tasks().IsEmpty());
        //}

        //[Fact]
        //public void shouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedInCloseTransactionWhenSuspendingTasks()
        //{
        //    StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

        //    internalTopologyBuilder.AddSource(null, "Name", null, null, null, topic1);
        //    internalTopologyBuilder.AddSink("out", "output", null, null, null, "Name");

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.RebalanceListener.OnPartitionsRevoked(null);

        //    Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();
        //    List<TopicPartition>assignedPartitions = new List<>();

        //    //.Assign single partition
        //   assignedPartitions.Add(t1p1);
        //    ActiveTasks.Add(Task1, Collections.singleton(t1p1));

        //    thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

        //    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //    mockConsumer.Assign(assignedPartitions);
        //    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //    thread.RebalanceListener.onPartitionsAssignedassignedPartitions);

        //    thread.RunOnce();

        //    Assert.Equal(thread.Tasks().Count, (1));

        //    clientSupplier.producers.Get(0).fenceProducerOnClose();
        //    thread.RebalanceListener.OnPartitionsRevoked(null);

        //   Assert.False(clientSupplier.producers.Get(0).transactionInFlight());
        //    Assert.True(clientSupplier.producers.Get(0).transactionCommitted());
        //   Assert.False(clientSupplier.producers.Get(0).closed());
        //    Assert.True(thread.Tasks().IsEmpty());
        //}

        //private static class StateListenerStub : StreamThread.StateListener
        //{
        //        int numChanges = 0;
        //    ThreadStateTransitionValidator oldState = null;
        //    ThreadStateTransitionValidator newState = null;

        //
        //        public void onChange(Thread thread,
        //                              ThreadStateTransitionValidator newState,
        //                              ThreadStateTransitionValidator oldState)
        //{
        //    ++numChanges;
        //    if (this.newState != null)
        //    {
        //        if (this.newState != oldState)
        //        {
        //            throw new RuntimeException("State mismatch " + oldState + " different from " + this.newState);
        //        }
        //    }
        //    this.oldState = oldState;
        //    this.newState = newState;
        //}
        //    }

        //    [Fact]
        //public void shouldReturnActiveTaskMetadataWhileRunningState()
        //{
        //    internalTopologyBuilder.AddSource(null, "source", null, null, null, topic1);

        //    StreamThread thread = createStreamThread(clientId, config, false);

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.RebalanceListener.OnPartitionsRevoked(null);

        //    Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();
        //    List<TopicPartition>assignedPartitions = new List<>();

        //    //.Assign single partition
        //   assignedPartitions.Add(t1p1);
        //    ActiveTasks.Add(Task1, Collections.singleton(t1p1));

        //    thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

        //    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //    mockConsumer.Assign(assignedPartitions);
        //    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //    thread.RebalanceListener.onPartitionsAssignedassignedPartitions);

        //    thread.RunOnce();

        //    ThreadMetadata threadMetadata = thread.threadMetadata();
        //    Assert.Equal(StreamThreadStates.RUNNING.Name(), threadMetadata.ThreadState);
        //    Assert.True(threadMetadata.ActiveTasks.Contains(new TaskMetadata(Task1.ToString(), Utils.mkSet(t1p1))));
        //    Assert.True(threadMetadata.StandbyTasks.IsEmpty());
        //}

        //[Fact]
        //public void shouldReturnStandbyTaskMetadataWhileRunningState()
        //{
        //    internalStreamsBuilder.Stream(Collections.singleton(topic1), consumed)
        //        .GroupByKey().Count(Materialized.As ("count-one"));

        //    internalStreamsBuilder.BuildAndOptimizeTopology();
        //    StreamThread thread = createStreamThread(clientId, config, false);
        //    MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        //    restoreConsumer.updatePartitions(
        //        "stream-thread-test-count-one-changelog",
        //        Collections.singletonList(
        //            new PartitionInfo("stream-thread-test-count-one-changelog",
        //            0,
        //            null,
        //            new Node[0],
        //            new Node[0])
        //        )
        //    );

        //    Dictionary<TopicPartition, long> offsets = new HashMap<>();
        //    offsets.Add(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
        //    restoreConsumer.updateEndOffsets(offsets);
        //    restoreConsumer.UpdateBeginningOffsets(offsets);

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.RebalanceListener.OnPartitionsRevoked(null);

        //    Dictionary<TaskId, List<TopicPartition>> StandbyTasks = new HashMap<>();

        //    //.Assign single partition
        //    StandbyTasks.Add(Task1, Collections.singleton(t1p1));

        //    thread.TaskManager.SetAssignmentMetadata(Collections.emptyMap(), StandbyTasks);

        //    thread.RebalanceListener.OnPartitionsAssigned(Collections.emptyList());

        //    thread.RunOnce();

        //    ThreadMetadata threadMetadata = thread.threadMetadata();
        //    Assert.Equal(StreamThreadStates.RUNNING.Name(), threadMetadata.ThreadState);
        //    Assert.True(threadMetadata.StandbyTasks.Contains(new TaskMetadata(Task1.ToString(), Utils.mkSet(t1p1))));
        //    Assert.True(threadMetadata.ActiveTasks.IsEmpty());
        //}

        //
        //    [Fact]
        //public void shouldUpdateStandbyTask()// throws Exception

        //{
        //     string storeName1 = "count-one";
        //     string storeName2 = "table-two";
        //     string changelogName1 = applicationId + "-" + storeName1 + "-changelog";
        //     string changelogName2 = applicationId + "-" + storeName2 + "-changelog";
        //    TopicPartition partition1 = new TopicPartition(changelogName1, 1);
        //TopicPartition partition2 = new TopicPartition(changelogName2, 1);
        //internalStreamsBuilder
        //    .Stream(Collections.singleton(topic1), consumed)
        //            .GroupByKey()
        //            .Count(Materialized.As(storeName1));
        //MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>> materialized

        //   = new MaterializedInternal<>(Materialized.As (storeName2), internalStreamsBuilder, "");
        //internalStreamsBuilder.Table(topic2, new ConsumedInternal<>(), materialized);

        //        internalStreamsBuilder.BuildAndOptimizeTopology();
        //         StreamThread thread = createStreamThread(clientId, config, false);
        //MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        //restoreConsumer.updatePartitions(changelogName1,
        //    Collections.singletonList(
        //                new PartitionInfo(
        //                    changelogName1,
        //                    1,
        //                    null,
        //                    new Node[0],
        //                    new Node[0]
        //                )
        //            )
        //        );

        //        restoreConsumer.Assign(Utils.mkSet(partition1, partition2));
        //        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition1, 10L));
        //        restoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(partition1, 0L));
        //        restoreConsumer.updateEndOffsets(Collections.singletonMap(partition2, 10L));
        //        restoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(partition2, 0L));
        //        // let the store1 be restored from 0 to 10; store2 be restored from 5 (checkpointed) to 10
        //         OffsetCheckpoint checkpoint
        //            = new OffsetCheckpoint(new FileInfo(stateDirectory.DirectoryForTask(Task3), CHECKPOINT_FILE_NAME));
        //checkpoint.write(Collections.singletonMap(partition2, 5L));

        //        for (long i = 0L; i< 10L; i++) {
        //            restoreConsumer.AddRecord(new ConsumeResult<>(
        //                changelogName1,
        //                1,
        //                i,
        //                ("K" + i).getBytes(),
        //                ("V" + i).getBytes()));
        //            restoreConsumer.AddRecord(new ConsumeResult<>(
        //                changelogName2,
        //                1,
        //                i,
        //                ("K" + i).getBytes(),
        //                ("V" + i).getBytes()));
        //        }

        //        thread.SetState(StreamThreadStates.STARTING);
        //        thread.RebalanceListener.OnPartitionsRevoked(null);

        //         Dictionary<TaskId, List<TopicPartition>> StandbyTasks = new HashMap<>();

        ////.Assign single partition
        //StandbyTasks.Add(Task1, Collections.singleton(t1p1));
        //        StandbyTasks.Add(Task3, Collections.singleton(t2p1));

        //        thread.TaskManager.SetAssignmentMetadata(Collections.emptyMap(), StandbyTasks);

        //thread.RebalanceListener.OnPartitionsAssigned(Collections.emptyList());

        //        thread.RunOnce();

        //         StandbyTask standbyTask1 = thread.TaskManager.standbyTask(partition1);
        //StandbyTask standbyTask2 = thread.TaskManager.standbyTask(partition2);
        //IKeyValueStore<object, long> store1 = (IKeyValueStore<object, long>)standbyTask1.GetStore(storeName1);
        //IKeyValueStore<object, long> store2 = (IKeyValueStore<object, long>)standbyTask2.GetStore(storeName2);

        //Assert.Equal(10L, store1.approximateNumEntries);
        //Assert.Equal(5L, store2.approximateNumEntries);
        //Assert.Equal(0, thread.standbyRecords().Count);
        //    }

        //    [Fact]
        //public void shouldCreateStandbyTask()
        //{
        //    setupInternalTopologyWithoutState();
        //    internalTopologyBuilder.AddStateStore(new MockKeyValueStoreBuilder("myStore", true), "processor1");

        //    StandbyTask standbyTask = createStandbyTask();

        //    Assert.Equal(standbyTask, not(nullValue()));
        //}

        //[Fact]
        //public void shouldNotCreateStandbyTaskWithoutStateStores()
        //{
        //    setupInternalTopologyWithoutState();

        //    StandbyTask standbyTask = createStandbyTask();

        //    Assert.Equal(standbyTask, nullValue());
        //}

        //[Fact]
        //public void shouldNotCreateStandbyTaskIfStateStoresHaveLoggingDisabled()
        //{
        //    setupInternalTopologyWithoutState();
        //    IStoreBuilder storeBuilder = new MockKeyValueStoreBuilder("myStore", true);
        //    storeBuilder.WithLoggingDisabled();
        //    internalTopologyBuilder.AddStateStore(storeBuilder, "processor1");

        //    StandbyTask standbyTask = createStandbyTask();

        //    Assert.Equal(standbyTask, nullValue());
        //}

        //private void setupInternalTopologyWithoutState()
        //{
        //    MockProcessor mockProcessor = new MockProcessor();
        //    internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);
        //    internalTopologyBuilder.AddProcessor("processor1", () => mockProcessor, "source1");
        //}

        //private StandbyTask createStandbyTask()
        //{
        //    LogContext logContext = new LogContext("test");
        //    Logger this.logger = logContext.logger(StreamThreadTest);
        //    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //    StreamThread.StandbyTaskCreator standbyTaskCreator = new StreamThread.StandbyTaskCreator(
        //       internalTopologyBuilder,
        //       config,
        //       streamsMetrics,
        //       stateDirectory,
        //       new MockChangelogReader(),
        //       mockTime,
        //       this.logger);
        //    return standbyTaskCreator.createTask(
        //        new MockConsumer<>(OffsetResetStrategy.EARLIEST),
        //        new TaskId(1, 2),
        //        Collections.emptySet());
        //}

        //[Fact]
        //public void shouldPunctuateActiveTask()
        //{
        //    List<long> punctuatedStreamTime = new List<>();
        //    List<long> punctuatedWallClockTime = new List<>();
        //    ProcessorSupplier<object, object> punctuateProcessor = () => new Processor<object, object>() {
        //
        //            public void Init(IProcessorContext context)
        //    {
        //        context.schedule(TimeSpan.FromMilliseconds(100L), PunctuationType.STREAM_TIME, punctuatedStreamTime::add);
        //        context.schedule(TimeSpan.FromMilliseconds(100L), PunctuationType.WALL_CLOCK_TIME, punctuatedWallClockTime::add);
        //    }

        //
        //            public void process(object key,
        //                                 object value)
        //    { }

        //
        //            public void Close() { }
        //};

        //internalStreamsBuilder.Stream(Collections.singleton(topic1), consumed).Process(punctuateProcessor);
        //internalStreamsBuilder.BuildAndOptimizeTopology();

        //         StreamThread thread = createStreamThread(clientId, config, false);

        //thread.SetState(StreamThreadStates.STARTING);
        //        thread.RebalanceListener.OnPartitionsRevoked(null);
        //         List<TopicPartition>assignedPartitions = new List<>();

        //Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();

        //// assign single partition
        // assignedPartitions.Add(t1p1);
        //        ActiveTasks.Add(Task1, Collections.singleton(t1p1));

        //        thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

        //clientSupplier.Consumer.Assign(assignedPartitions);
        //        clientSupplier.Consumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //        thread.RebalanceListener.onPartitionsAssignedassignedPartitions);

        //        thread.RunOnce();

        //        Assert.Equal(0, punctuatedStreamTime.Count);
        //Assert.Equal(0, punctuatedWallClockTime.Count);

        //mockTime.Sleep(100L);
        //        for (long i = 0L; i< 10L; i++) {
        //            clientSupplier.Consumer.AddRecord(new ConsumeResult<>(
        //                topic1,
        //                1,
        //                i,
        //                i* 100L,
        //                TimestampType.CreateTime,
        //                ConsumeResult.NULL_CHECKSUM,
        //                ("K" + i).getBytes().Length,
        //                ("V" + i).getBytes().Length,
        //                ("K" + i).getBytes(),
        //                ("V" + i).getBytes()));
        //        }

        //        thread.RunOnce();

        //        Assert.Equal(1, punctuatedStreamTime.Count);
        //Assert.Equal(1, punctuatedWallClockTime.Count);

        //mockTime.Sleep(100L);

        //        thread.RunOnce();

        //        // we should skip stream time punctuation, only trigger wall-clock time punctuation
        //        Assert.Equal(1, punctuatedStreamTime.Count);
        //Assert.Equal(2, punctuatedWallClockTime.Count);
        //    }

        //    [Fact]
        //public void shouldAlwaysUpdateTasksMetadataAfterChangingState()
        //{
        //    StreamThread thread = createStreamThread(clientId, config, false);
        //    ThreadMetadata metadata = thread.threadMetadata();
        //    Assert.Equal(StreamThreadStates.CREATED.Name(), metadata.ThreadState);

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);
        //    thread.SetState(StreamThreadStates.PARTITIONS_ASSIGNED);
        //    thread.SetState(StreamThreadStates.RUNNING);
        //    metadata = thread.threadMetadata();
        //    Assert.Equal(StreamThreadStates.RUNNING.Name(), metadata.ThreadState);
        //}

        //[Fact]
        //public void shouldAlwaysReturnEmptyTasksMetadataWhileRebalancingStateAndTasksNotRunning()
        //{
        //    internalStreamsBuilder.Stream(Collections.singleton(topic1), consumed)
        //        .GroupByKey().Count(Materialized.As ("count-one"));
        //    internalStreamsBuilder.BuildAndOptimizeTopology();

        //    StreamThread thread = createStreamThread(clientId, config, false);
        //    MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
        //    restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
        //       Arrays.asList(
        //            new PartitionInfo("stream-thread-test-count-one-changelog",
        //                0,
        //                null,
        //                new Node[0],
        //                new Node[0]),
        //            new PartitionInfo("stream-thread-test-count-one-changelog",
        //                1,
        //                null,
        //                new Node[0],
        //                new Node[0])
        //        ));
        //    Dictionary<TopicPartition, long> offsets = new HashMap<>();
        //    offsets.Add(new TopicPartition("stream-thread-test-count-one-changelog", 0), 0L);
        //    offsets.Add(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
        //    restoreConsumer.updateEndOffsets(offsets);
        //    restoreConsumer.UpdateBeginningOffsets(offsets);

        //    clientSupplier.Consumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

        //    List<TopicPartition>assignedPartitions = new List<>();

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.RebalanceListener.onPartitionsRevokedassignedPartitions);
        //   .AssertThreadMetadata.AsEmptyTasksWithState(thread.threadMetadata(), StreamThreadStates.PARTITIONS_REVOKED);

        //    Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();
        //    Dictionary<TaskId, List<TopicPartition>> StandbyTasks = new HashMap<>();

        //    //.Assign single partition
        //   assignedPartitions.Add(t1p1);
        //    ActiveTasks.Add(Task1, Collections.singleton(t1p1));
        //    StandbyTasks.Add(Task2, Collections.singleton(t1p2));

        //    thread.TaskManager.SetAssignmentMetadata(ActiveTasks, StandbyTasks);

        //    thread.RebalanceListener.onPartitionsAssignedassignedPartitions);

        //   .AssertThreadMetadata.AsEmptyTasksWithState(thread.threadMetadata(), StreamThreadStates.PARTITIONS_ASSIGNED);
        //}

        //[Fact]
        //public void shouldRecoverFromInvalidOffsetExceptionOnRestoreAndFinishRestore()// throws Exception

        //{
        //    internalStreamsBuilder.Stream(Collections.singleton("topic"), consumed)
        //            .GroupByKey().Count(Materialized.As("count"));
        //    internalStreamsBuilder.BuildAndOptimizeTopology();

        //    StreamThread thread = createStreamThread("clientId", config, false);
        //    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>) thread.Consumer;
        //    MockConsumer<byte[], byte[]> mockRestoreConsumer = (MockConsumer<byte[], byte[]>) thread.restoreConsumer;

        //    TopicPartition topicPartition = new TopicPartition("topic", 0);
        //List<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);

        //Dictionary<TaskId, List<TopicPartition>> ActiveTasks = new HashMap<>();
        //ActiveTasks.Add(new TaskId(0, 0), topicPartitionSet);
        //        thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

        //mockConsumer.updatePartitions(
        //            "topic",
        //            Collections.singletonList(
        //                new PartitionInfo(
        //                    "topic",
        //                    0,
        //                    null,
        //                    new Node[0],
        //                    new Node[0]
        //                )
        //            )
        //        );
        //        mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));

        //        mockRestoreConsumer.updatePartitions(
        //            "stream-thread-test-count-changelog",
        //            Collections.singletonList(
        //                new PartitionInfo(
        //                    "stream-thread-test-count-changelog",
        //                    0,
        //                    null,
        //                    new Node[0],
        //                    new Node[0]
        //                )
        //            )
        //        );

        //         TopicPartition changelogPartition = new TopicPartition("stream-thread-test-count-changelog", 0);
        //List<TopicPartition> changelogPartitionSet = Collections.singleton(changelogPartition);
        //mockRestoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(changelogPartition, 0L));
        //        mockRestoreConsumer.updateEndOffsets(Collections.singletonMap(changelogPartition, 2L));

        //        mockConsumer.schedulePollTask(() => {
        //            thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);
        //            thread.RebalanceListener.OnPartitionsAssigned(topicPartitionSet);
        //        });

        //        try {
        //            thread.Start();

        //            TestUtils.WaitForCondition(
        //                () => mockRestoreConsumer.Assignment().Count == 1,
        //                "Never restore first record");

        //            mockRestoreConsumer.AddRecord(new ConsumeResult<>(
        //                "stream-thread-test-count-changelog",
        //                0,
        //                0L,
        //                "K1".getBytes(),
        //                "V1".getBytes()));

        //            TestUtils.WaitForCondition(
        //                () => mockRestoreConsumer.position(changelogPartition) == 1L,
        //                "Never restore first record");

        //            mockRestoreConsumer.setException(new InvalidOffsetException("Try Again!")
        //{
        //
        //                public List<TopicPartition> partitions()
        //    {
        //        return changelogPartitionSet;
        //    }
        //});

        //            mockRestoreConsumer.AddRecord(new ConsumeResult<>(
        //                "stream-thread-test-count-changelog",
        //                0,
        //                0L,
        //                "K1".getBytes(),
        //                "V1".getBytes()));
        //            mockRestoreConsumer.AddRecord(new ConsumeResult<>(
        //                "stream-thread-test-count-changelog",
        //                0,
        //                1L,
        //                "K2".getBytes(),
        //                "V2".getBytes()));

        //            TestUtils.WaitForCondition(
        //                () => {
        //                    mockRestoreConsumer.Assign(changelogPartitionSet);
        //                    return mockRestoreConsumer.position(changelogPartition) == 2L;
        //                },
        //                "Never finished restore");
        //        } finally {
        //            thread.Shutdown();
        //            thread.Join(10000);
        //        }
        //    }

        //    [Fact]
        //public void shouldRecordSkippedMetricForDeserializationException()
        //{
        //    LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        //    internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

        //   StreamsConfig config = configProps(false);
        //    config.Set(
        //        StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        //        LogAndContinueExceptionHandler.getName());
        //    config.Set(StreamsConfig.DefaultValueSerdeClassConfig, Serdes.Int().GetType().FullName);
        //    StreamThread thread = createStreamThread(clientId, new StreamsConfig(config), false);

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);

        //    List<TopicPartition>assignedPartitions = Collections.singleton(t1p1);
        //    thread.TaskManager.SetAssignmentMetadata(
        //        Collections.singletonMap(
        //                    new TaskId(0, t1p1.Partition),
        //                   assignedPartitions),
        //                Collections.emptyMap());

        //    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //    mockConsumer.Assign(Collections.singleton(t1p1));
        //    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //    thread.RebalanceListener.onPartitionsAssignedassignedPartitions);
        //    thread.RunOnce();

        //    MetricName skippedTotalMetric = metrics.metricName(
        //       "skipped-records-total",
        //       "stream-metrics",
        //       Collections.singletonMap("client-id", thread.getName()));
        //    MetricName skippedRateMetric = metrics.metricName(
        //       "skipped-records-rate",
        //       "stream-metrics",
        //       Collections.singletonMap("client-id", thread.getName()));
        //    Assert.Equal(0.0, metrics.metric(skippedTotalMetric).metricValue());
        //    Assert.Equal(0.0, metrics.metric(skippedRateMetric).metricValue());

        //    long offset = -1;
        //    mockConsumer.AddRecord(new ConsumeResult<>(
        //        t1p1.Topic,
        //                t1p1.Partition,
        //                ++offset, -1,
        //                TimestampType.CreateTime,
        //                ConsumeResult.NULL_CHECKSUM,
        //                -1,
        //                -1,
        //                new byte[0],
        //                "I am not an integer.".getBytes()));
        //    mockConsumer.AddRecord(new ConsumeResult<>(
        //        t1p1.Topic,
        //        t1p1.Partition,
        //        ++offset,
        //        -1,
        //        TimestampType.CreateTime,
        //        ConsumeResult.NULL_CHECKSUM,
        //        -1,
        //        -1,
        //        new byte[0],
        //        "I am not an integer.".getBytes()));
        //    thread.RunOnce();
        //    Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
        //   Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

        //    LogCaptureAppender.unregister(appender);
        //    List<string> strings = appender.getMessages();
        //    Assert.True(strings.Contains("Task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[0]"));
        //    Assert.True(strings.Contains("Task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[1]"));
        //}

        //[Fact]
        //public void shouldReportSkippedRecordsForInvalidTimestamps()
        //{
        //    LogCaptureAppender appender = LogCaptureAppender.createAndRegister();

        //    internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

        //   StreamsConfig config = configProps(false);
        //    config.Set(
        //        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        //        LogAndSkipOnInvalidTimestamp.getName());
        //    StreamThread thread = createStreamThread(clientId, new StreamsConfig(config), false);

        //    thread.SetState(StreamThreadStates.STARTING);
        //    thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);

        //    List<TopicPartition>assignedPartitions = Collections.singleton(t1p1);
        //    thread.TaskManager.SetAssignmentMetadata(
        //        Collections.singletonMap(
        //                    new TaskId(0, t1p1.Partition),
        //                   assignedPartitions),
        //                Collections.emptyMap());

        //    MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.Consumer;
        //    mockConsumer.Assign(Collections.singleton(t1p1));
        //    mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
        //    thread.RebalanceListener.onPartitionsAssignedassignedPartitions);
        //    thread.RunOnce();

        //    MetricName skippedTotalMetric = metrics.metricName(
        //       "skipped-records-total",
        //       "stream-metrics",
        //       Collections.singletonMap("client-id", thread.getName()));
        //    MetricName skippedRateMetric = metrics.metricName(
        //       "skipped-records-rate",
        //       "stream-metrics",
        //       Collections.singletonMap("client-id", thread.getName()));
        //    Assert.Equal(0.0, metrics.metric(skippedTotalMetric).metricValue());
        //    Assert.Equal(0.0, metrics.metric(skippedRateMetric).metricValue());

        //    long offset = -1;
        //    addRecord(mockConsumer, ++offset);
        //    addRecord(mockConsumer, ++offset);
        //    thread.RunOnce();
        //    Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
        //   Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

        //    addRecord(mockConsumer, ++offset);
        //    addRecord(mockConsumer, ++offset);
        //    addRecord(mockConsumer, ++offset);
        //    addRecord(mockConsumer, ++offset);
        //    thread.RunOnce();
        //    Assert.Equal(6.0, metrics.metric(skippedTotalMetric).metricValue());
        //   Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

        //    addRecord(mockConsumer, ++offset, 1L);
        //    addRecord(mockConsumer, ++offset, 1L);
        //    thread.RunOnce();
        //    Assert.Equal(6.0, metrics.metric(skippedTotalMetric).metricValue());
        //   Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

        //    LogCaptureAppender.unregister(appender);
        //    List<string> strings = appender.getMessages();
        //    Assert.True(strings.Contains(
        //                "Task [0_1] Skipping record due to negative extracted timestamp. " +
        //                    "topic=[topic1] partition=[1] offset=[0] extractedTimestamp=[-1] " +
        //                    "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        //            ));
        //    Assert.True(strings.Contains(
        //        "Task [0_1] Skipping record due to negative extracted timestamp. " +
        //            "topic=[topic1] partition=[1] offset=[1] extractedTimestamp=[-1] " +
        //            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        //    ));
        //    Assert.True(strings.Contains(
        //        "Task [0_1] Skipping record due to negative extracted timestamp. " +
        //            "topic=[topic1] partition=[1] offset=[2] extractedTimestamp=[-1] " +
        //            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        //    ));
        //    Assert.True(strings.Contains(
        //        "Task [0_1] Skipping record due to negative extracted timestamp. " +
        //            "topic=[topic1] partition=[1] offset=[3] extractedTimestamp=[-1] " +
        //            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        //    ));
        //    Assert.True(strings.Contains(
        //        "Task [0_1] Skipping record due to negative extracted timestamp. " +
        //            "topic=[topic1] partition=[1] offset=[4] extractedTimestamp=[-1] " +
        //            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        //    ));
        //    Assert.True(strings.Contains(
        //        "Task [0_1] Skipping record due to negative extracted timestamp. " +
        //            "topic=[topic1] partition=[1] offset=[5] extractedTimestamp=[-1] " +
        //            "extractor=[org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp]"
        //    ));
        //}

        //private void assertThreadMetadata.AsEmptyTasksWithState(ThreadMetadata metadata,
        //                                                         StreamThreadStates state)
        //{
        //    Assert.Equal(state.Name(), metadata.ThreadState);
        //    Assert.True(metadata.ActiveTasks.IsEmpty());
        //    Assert.True(metadata.StandbyTasks.IsEmpty());
        //}

        //[Fact]
        //// TODO: Need to add a test case covering EOS when we Create a mock TaskManager class
        //public void producerMetricsVerificationWithoutEOS()
        //{
        //    MockProducer<byte[], byte[]> producer = new MockProducer<>();
        //    IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer);
        //    TaskManager TaskManager = mockTaskManagerCommit(consumer, 1, 0);

        //    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //    StreamThread thread = new StreamThread(
        //           mockTime,
        //           config,
        //           producer,
        //           consumer,
        //           consumer,
        //                   null,
        //           TaskManager,
        //           streamsMetrics,
        //           internalTopologyBuilder,
        //           clientId,
        //                   new LogContext(""),
        //                   new int()
        //                   );
        //    MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        //    Metric testMetric = new KafkaMetric(
        //       new object(),
        //       testMetricName,
        //       (M.Asurable)(config, now)=> 0,
        //       null,
        //       new MockTime());
        //    producer.setMockMetrics(testMetricName, testMetric);
        //    Dictionary<MetricName, Metric> producerMetrics = thread.producerMetrics();
        //    Assert.Equal(testMetricName, producerMetrics.Get(testMetricName).metricName());
        //}

        //[Fact]
        //public void adminClientMetricsVerification()
        //{
        //    Node broker1 = new Node(0, "dummyHost-1", 1234);
        //    Node broker2 = new Node(1, "dummyHost-2", 1234);
        //    List<Node> cluster =asList(broker1, broker2);

        //    MockAdminClient adminClient = new MockAdminClient(cluster, broker1, null);

        //    MockProducer<byte[], byte[]> producer = new MockProducer<>();
        //    IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer);
        //    TaskManager TaskManager = Mock.Of<TaskManager);

        //    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //    StreamThread thread = new StreamThread(
        //           mockTime,
        //           config,
        //           producer,
        //           consumer,
        //           consumer,
        //                   null,
        //           TaskManager,
        //           streamsMetrics,
        //           internalTopologyBuilder,
        //           clientId,
        //                   new LogContext(""),
        //                   new int()
        //                   );
        //    MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        //    Metric testMetric = new KafkaMetric(
        //       new object(),
        //       testMetricName,
        //       (M.Asurable)(config, now)=> 0,
        //       null,
        //       new MockTime());

        //    EasyMock.expect(TaskManager.GetAdminClient()).andReturn(adminClient);
        //    EasyMock.expect.AstCall();
        //    EasyMock.replay(TaskManager, consumer);

        //    adminClient.setMockMetrics(testMetricName, testMetric);
        //    Dictionary<MetricName, Metric> adminClientMetrics = thread.adminClientMetrics();
        //    Assert.Equal(testMetricName, adminClientMetrics.Get(testMetricName).metricName());
        //}

        //private void addRecord(MockConsumer<byte[], byte[]> mockConsumer,
        //                        long offset)
        //{
        //    addRecord(mockConsumer, offset, -1L);
        //}

        //private void addRecord(MockConsumer<byte[], byte[]> mockConsumer,
        //                        long offset,
        //                        long timestamp)
        //{
        //    mockConsumer.AddRecord(new ConsumeResult<>(
        //        t1p1.Topic,
        //        t1p1.Partition,
        //        offset,
        //        timestamp,
        //        TimestampType.CreateTime,
        //        ConsumeResult.NULL_CHECKSUM,
        //        -1,
        //        -1,
        //        new byte[0],
        //        new byte[0]));
        //}
    }
}