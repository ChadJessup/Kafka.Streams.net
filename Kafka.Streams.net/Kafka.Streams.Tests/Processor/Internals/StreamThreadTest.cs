using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Threads;
using Kafka.Streams.Threads.Stream;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class StreamThreadTest
    {
        private readonly string clientId = "clientId";
        private readonly string applicationId = "stream-thread-test";
        private readonly int threadIdx = 1;
        private MockTime mockTime = new MockTime();
        private MockClientSupplier clientSupplier = new MockClientSupplier();
        private InternalStreamsBuilder internalStreamsBuilder = new InternalStreamsBuilder(new InternalTopologyBuilder());
        private StreamsConfig config = new StreamsConfig(configProps(false));
        private readonly string stateDir = TestUtils.GetTempDirectory().FullName;
        private StateDirectory stateDirectory = new StateDirectory(config, mockTime, true);
        private ConsumedInternal<object, object> consumed = new ConsumedInternal<>();

        private Guid processId = Guid.NewGuid();
        private InternalTopologyBuilder internalTopologyBuilder;
        private StreamsMetadataState streamsMetadataState;


        public StreamThreadTest()
        {
            processId = Guid.NewGuid();

            internalTopologyBuilder = InternalStreamsBuilderTest.InternalTopologyBuilder(internalStreamsBuilder);
            internalTopologyBuilder.SetApplicationId(applicationId);
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

        private StreamsConfig ConfigProps(bool enableEoS)
        {
            var config = new StreamsConfig
            {
                ApplicationId = applicationId,
                BootstrapServers = "localhost:2171",
                DefaultTimestampExtractorType = typeof(MockTimestampExtractor),
                StateStoreDirectory = TestUtils.GetTempDirectory().FullName,
            };
            
        config.Set(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIGConfig, "3");
            config.Set(StreamsConfig.ProcessingGuaranteeConfig, enableEoS ? StreamsConfig.ExactlyOnceConfig : StreamsConfig.AtLeastOnceConfig)

            return config;
        }

        [Fact]
        public void TestPartitionAssignmentChangeForSingleGroup()
        {
            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

            StreamThread thread = CreateStreamThread(clientId, config, false);

            StateListenerStub stateListener = new StateListenerStub();
            thread.SetStateListener(stateListener);
            Assert.Equal(thread.state(), StreamThreadStates.CREATED);

            ConsumerRebalanceListener RebalanceListener = thread.RebalanceListener;

            List<TopicPartition> revokedPartitions;
            List<TopicPartition> assignedPartitions;

            // revoke nothing
            thread.SetState(StreamThreadStates.STARTING);
            revokedPartitions = Collections.emptyList();
            RebalanceListener.OnPartitionsRevoked(revokedPartitions);

            Assert.Equal(thread.state(), StreamThreadStates.PARTITIONS_REVOKED);

            // assign single partition
            assignedPartitions = Collections.singletonList(t1p1);
            thread.TaskManager.SetAssignmentMetadata(Collections.emptyMap(), Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(assignedPartitions);
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            RebalanceListener.OnPartitionsAssigned(assignedPartitions);
            thread.RunOnce();
            Assert.Equal(thread.state(), StreamThreadStates.RUNNING);
            Assert.Equal(4, stateListener.numChanges);
            Assert.Equal(StreamThreadStates.PARTITIONS_ASSIGNED, stateListener.oldState);

            thread.Shutdown();
            Assert.Same(StreamThreadStates.PENDING_SHUTDOWN, thread.state());
        }

        [Fact]
        public void TestStateChangeStartClose()
        {// throws Exception
            StreamThread thread = CreateStreamThread(clientId, config, false);

            StateListenerStub stateListener = new StateListenerStub();
            thread.SetStateListener(stateListener);

            thread.Start();
            TestUtils.WaitForCondition(
                () => thread.state() == StreamThreadStates.STARTING,
                10 * 1000,
                "Thread never started.");

            thread.Shutdown();
            TestUtils.WaitForCondition(
                () => thread.state() == StreamThreadStates.DEAD,
                10 * 1000,
                "Thread never shut down.");

            thread.Shutdown();
            Assert.Equal(thread.state(), StreamThreadStates.DEAD);
        }

        private Cluster CreateCluster()
        {
            Node node = new Node(0, "localhost", 8121);
            return new Cluster(
                "mockClusterId",
                Collections.singletonList(node),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                node
            );
        }

        private StreamThread CreateStreamThread(string clientId,
                                                StreamsConfig config,
                                                bool eosEnabled)
        {
            if (eosEnabled)
            {
                clientSupplier.setApplicationIdForProducer(applicationId);
            }

            clientSupplier.setClusterForAdminClient(CreateCluster());

            return StreamThread.Create(
                internalTopologyBuilder,
                config,
                clientSupplier,
                clientSupplier.GetAdminClient(config.getAdminConfigs(clientId)),
                processId,
                clientId,
                //metrics,
                mockTime,
                streamsMetadataState,
                0,
                stateDirectory,
                new MockStateRestoreListener(),
                threadIdx);
        }

        [Fact]
        public void TestMetricsCreatedAtStartup()
        {
            StreamThread thread = CreateStreamThread(clientId, config, false);
            string defaultGroupName = "stream-metrics";
            Dictionary<string, string> defaultTags = Collections.singletonMap("client-id", thread.getName());
            string descriptionIsNotVerified = "";

            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "commit-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "commit-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "commit-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "commit-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "poll-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "poll-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "poll-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "poll-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "process-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "process-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "process-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "process-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "punctuate-latency-avg", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "punctuate-latency-max", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "punctuate-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "punctuate-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "task-created-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "task-created-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "task-closed-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "task-closed-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "skipped-records-rate", defaultGroupName, descriptionIsNotVerified, defaultTags)));
            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
                "skipped-records-total", defaultGroupName, descriptionIsNotVerified, defaultTags)));

            string taskGroupName = "stream-task-metrics";
            Dictionary<string, string> taskTags =
                mkMap(mkEntry("task-id", "All"), mkEntry("client-id", thread.getName()));
            //            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
            //                "commit-latency-avg", taskGroupName, descriptionIsNotVerified, taskTags)));
            //            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
            //                "commit-latency-max", taskGroupName, descriptionIsNotVerified, taskTags)));
            //            Assert.NotNull(metrics.metrics().Get(metrics.metricName(
            //                "commit-rate", taskGroupName, descriptionIsNotVerified, taskTags)));

            //            JmxReporter reporter = new JmxReporter("kafka.streams");
            //            metrics.addReporter(reporter);
            Assert.Equal(clientId + "-StreamThread-1", thread.getName());
            Assert.True(reporter.containsMbean(string.format("kafka.streams:type=%s,client-id=%s",
                       defaultGroupName,
                       thread.getName())));
            //            Assert.True(reporter.containsMbean("kafka.streams:type=stream-task-metrics,client-id=" + thread.getName() + ",task-id=All"));
        }

        [Fact]
        public void ShouldNotCommitBeforeTheCommitInterval()
        {
            long commitInterval = 1000L;
            StreamsConfig props = ConfigProps(false);
            props.Set(StreamsConfig.STATE_DIR_CONFIG, stateDir);
            props.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.ToString(commitInterval));

            StreamsConfig config = new StreamsConfig(props);
            IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer>();
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
                new int()
            );
            thread.SetNow(mockTime.NowAsEpochMilliseconds);
            thread.MaybeCommit();
            mockTime.Sleep(commitInterval - 10L);
            thread.SetNow(mockTime.NowAsEpochMilliseconds);
            thread.MaybeCommit();

            EasyMock.verify(taskManager);
        }

        [Fact]
        public void ShouldRespectNumIterationsInMainLoop()
        {
            MockProcessor mockProcessor = new MockProcessor(PunctuationType.WALL_CLOCK_TIME, 10L);
            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);
            internalTopologyBuilder.AddProcessor("processor1", () => mockProcessor, "source1");
            internalTopologyBuilder.AddProcessor("processor2", () => new MockProcessor(PunctuationType.STREAM_TIME, 10L), "source1");

            StreamsConfig properties = new StreamsConfig();
            properties.Put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
            StreamsConfig config = new StreamsConfig(StreamsTestUtils.getStreamsConfig(applicationId,
                "localhost:2171",
                Serdes.ByteArraySerde.getName(),
                Serdes.ByteArraySerde.getName(),
                properties));
            StreamThread thread = CreateStreamThread(clientId, config, false);

            thread.SetState(StreamThreadStates.STARTING);
            thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);

            HashSet<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
            thread.TaskManager.SetAssignmentMetadata(
                Collections.singletonMap(
                    new TaskId(0, t1p1.Partition),
                    assignedPartitions),
                Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(Collections.singleton(t1p1));
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);
            thread.RunOnce();

            // processed one record, punctuated after the first record, and hence num.iterations is still 1
            long offset = -1;
            addRecord(mockConsumer, ++offset, 0L);
            thread.RunOnce();

            Assert.Equal(thread.currentNumIterations(), 1);

            // processed one more record without punctuation, and bump num.iterations to 2
            addRecord(mockConsumer, ++offset, 1L);
            thread.RunOnce();

            Assert.Equal(thread.currentNumIterations(), 2);

            // processed zero records, early exit and iterations stays as 2
            thread.RunOnce();
            Assert.Equal(thread.currentNumIterations(), 2);

            // system time based punctutation halves to 1
            mockTime.Sleep(11L);

            thread.RunOnce();
            Assert.Equal(thread.currentNumIterations(), 1);

            // processed two records, bumping up iterations to 2
            addRecord(mockConsumer, ++offset, 5L);
            addRecord(mockConsumer, ++offset, 6L);
            thread.RunOnce();

            Assert.Equal(thread.currentNumIterations(), 2);

            // stream time based punctutation halves to 1
            addRecord(mockConsumer, ++offset, 11L);
            thread.RunOnce();

            Assert.Equal(thread.currentNumIterations(), 1);

            // processed three records, bumping up iterations to 3 (1 + 2)
            addRecord(mockConsumer, ++offset, 12L);
            addRecord(mockConsumer, ++offset, 13L);
            addRecord(mockConsumer, ++offset, 14L);
            thread.RunOnce();

            Assert.Equal(thread.currentNumIterations(), 3);

            mockProcessor.requestCommit();
            addRecord(mockConsumer, ++offset, 15L);
            thread.RunOnce();

            // user requested commit should not impact on iteration adjustment
            Assert.Equal(thread.currentNumIterations(), 3);

            // time based commit, halves iterations to 3 / 2 = 1
            mockTime.Sleep(90L);
            thread.RunOnce();

            Assert.Equal(thread.currentNumIterations(), 1);

        }

        [Fact]
        public void ShouldNotCauseExceptionIfNothingCommitted()
        {
            long commitInterval = 1000L;
            StreamsConfig props = ConfigProps(false);
            props.Set(StreamsConfig.STATE_DIR_CONFIG, stateDir);
            props.Set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, long.ToString(commitInterval));

            StreamsConfig config = new StreamsConfig(props);
            IConsumer<byte[], byte[]> consumer = Mock.Of<IConsumer<byte[], byte[]>>();
            TaskManager taskManager = MockTaskManagerCommit(consumer, 1, 0);

            //StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
            StreamThread thread = new StreamThread(
                mockTime,
                config,
                null,
                consumer,
                consumer,
                null,
                taskManager,
                internalTopologyBuilder,
                clientId,
                new LogContext(""),
                0);

            thread.SetNow(mockTime.NowAsEpochMilliseconds);
            thread.MaybeCommit();
            mockTime.Sleep(commitInterval - 10L);
            thread.SetNow(mockTime.NowAsEpochMilliseconds);
            thread.MaybeCommit();

            //EasyMock.verify(taskManager);
        }

        [Fact]
        public void ShouldCommitAfterTheCommitInterval()
        {
            long commitInterval = 1000L;
            StreamsConfig props = ConfigProps(false);
            props.StateStoreDirectory = stateDir;
            props.CommitIntervalMs = commitInterval;

            StreamsConfig config = new StreamsConfig(props);
            IConsumer<byte[], byte[]> consumer = Mock.Of<IConsumer<byte[], byte[]>>();
            TaskManager taskManager = MockTaskManagerCommit(consumer, 2, 1);

            // StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
            StreamThread thread = new StreamThread(
                mockTime,
                config,
                null,
                consumer,
                consumer,
                null,
                taskManager,
                //streamsMetrics,
                //internalTopologyBuilder,
                clientId,
                new LogContext(""),
                new int()
            );
            thread.SetNow(mockTime.UtcNow);
            thread.MaybeCommit();
            mockTime.Sleep(commitInterval + 1);
            thread.SetNow(mockTime.NowAsEpochMilliseconds);
            thread.MaybeCommit();

            //EasyMock.verify(taskManager);
        }

        private TaskManager MockTaskManagerCommit(
            IConsumer<byte[], byte[]> consumer,
            int numberOfCommits,
            int commits)
        {
            TaskManager taskManager = Mock.Of<TaskManager>();
            // EasyMock.expect(taskManager.CommitAll()).andReturn(commits).times(numberOfCommits);
            // EasyMock.replay(taskManager, consumer);
            return taskManager;
        }

        [Fact]
        public void ShouldInjectSharedProducerForAllTasksUsingClientSupplierOnCreateIfEosDisabled()
        {
            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);
            internalStreamsBuilder.BuildAndOptimizeTopology();

            StreamThread thread = CreateStreamThread(clientId, config, false);

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(Collections.emptyList());

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            assignedPartitions.Add(t1p2);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));
            ActiveTasks.Put(task2, Collections.singleton(t1p2));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(assignedPartitions);
            Dictionary<TopicPartition, long> beginOffsets = new Dictionary<TopicPartition, long>();
            beginOffsets.Put(t1p1, 0L);
            beginOffsets.Put(t1p2, 0L);
            mockConsumer.UpdateBeginningOffsets(beginOffsets);
            thread.RebalanceListener.OnPartitionsAssigned(new HashSet<>(assignedPartitions));

            Assert.Equal(1, clientSupplier.Producers.Count);
            var globalProducer = clientSupplier.Producers[0];

            foreach (Task task in thread.Tasks())
            {
                Assert.Same(globalProducer, ((RecordCollector)((StreamTask)task).recordCollector()).producer());
            }
            Assert.Same(clientSupplier.Consumer, thread.Consumer);
            Assert.Same(clientSupplier.RestoreConsumer, thread.RestoreConsumer);
        }

        [Fact]
        public void ShouldInjectProducerPerTaskUsingClientSupplierOnCreateIfEosEnable()
        {
            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

            StreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(Collections.emptyList());

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            assignedPartitions.Add(t1p2);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));
            ActiveTasks.Put(task2, Collections.singleton(t1p2));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(assignedPartitions);
            Dictionary<TopicPartition, long> beginOffsets = new HashMap<>();
            beginOffsets.Put(t1p1, 0L);
            beginOffsets.Put(t1p2, 0L);
            mockConsumer.UpdateBeginningOffsets(beginOffsets);
            thread.RebalanceListener.OnPartitionsAssigned(new HashSet<>(assignedPartitions));

            thread.RunOnce();

            Assert.Equal(thread.Tasks().Count, clientSupplier.producers.Count);
            Assert.Same(clientSupplier.consumer, thread.consumer);
            Assert.Same(clientSupplier.restoreConsumer, thread.restoreConsumer);
        }

        [Fact]
        public void ShouldCloseAllTaskProducersOnCloseIfEosEnabled()
        {
            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

            StreamThread thread = CreateStreamThread(clientId, new StreamsConfig(ConfigProps(true)), true);

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(Collections.emptyList());

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            assignedPartitions.Add(t1p2);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));
            ActiveTasks.Put(task2, Collections.singleton(t1p2));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());
            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(assignedPartitions);
            Dictionary<TopicPartition, long> beginOffsets = new HashMap<>();
            beginOffsets.Put(t1p1, 0L);
            beginOffsets.Put(t1p2, 0L);
            mockConsumer.UpdateBeginningOffsets(beginOffsets);

            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);

            thread.Shutdown();
            thread.Run();

            foreach (Task task in thread.Tasks().Values)
            {
                Assert.True(((MockProducer)((RecordCollector)((StreamTask)task).recordCollector()).producer()).closed());
            }
        }

        [Fact]
        public void ShouldShutdownTaskManagerOnClose()
        {
            IConsumer<byte[], byte[]> consumer = Mock.Of<IConsumer<byte[], byte[]>>();
            TaskManager taskManager = Mock.Of<TaskManager>();
            taskManager.Shutdown(true);
            // EasyMock.expectLastCall();
            // EasyMock.replay(taskManager, consumer);

            //StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
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
                0
            ).UpdateThreadMetadata(getSharedAdminClientId(clientId));

            thread.SetStateListener(
                (t, newState, oldState) =>
                {
                    if (oldState == StreamThreadStates.CREATED && newState == StreamThreadStates.STARTING)
                    {
                        thread.Shutdown();
                    }
                });

            thread.Run();
            //EasyMock.verify(taskManager);
        }

        [Fact]
        public void ShouldShutdownTaskManagerOnCloseWithoutStart()
        {
            IConsumer<byte[], byte[]> consumer = Mock.Of<IConsumer<byte[], byte[]>>();
            TaskManager taskManager = Mock.Of<TaskManager>();
            taskManager.Shutdown(true);
            //EasyMock.expectLastCall();
            //EasyMock.replay(taskManager, consumer);

            //StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
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
                0)
                .UpdateThreadMetadata(getSharedAdminClientId(clientId));
            thread.Shutdown();
            //EasyMock.verify(taskManager);
        }

        [Fact]
        public void ShouldNotThrowWhenPendingShutdownInRunOnce()
        {
            MockRunOnce(true);
        }

        [Fact]
        public void ShouldNotThrowWithoutPendingShutdownInRunOnce()
        {
            // A reference test to verify that without intermediate Shutdown the RunOnce should pass
            // without any exception.
            MockRunOnce(false);
            //}

            //        private void MockRunOnce(bool shutdownOnPoll)
            //        {
            //            Collection<TopicPartition> assignedPartitions = Collections.singletonList(t1p1);
            //        class MockStreamThreadConsumer<K, V> : MockConsumer<K, V>
            //        {
            //
            //            private StreamThread streamThread;
            //
            //            private MockStreamThreadConsumer(AutoOffsetReset offsetResetStrategy)
            //                : base(offsetResetStrategy)
            //            {
            //            }
            //
            //
            //            public ConsumeResult<K, V> poll(TimeSpan timeout)
            //            {
            //                Assert.NotNull(streamThread);
            //                if (shutdownOnPoll)
            //                {
            //                    streamThread.Shutdown();
            //                }
            //                streamThread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);
            //                return base.Poll(timeout);
            //            }
            //
            //            private void SetStreamThread(StreamThread streamThread)
            //            {
            //                this.streamThread = streamThread;
            //            }
            //        }
            //
            //        readonly MockStreamThreadConsumer<byte[], byte[]> mockStreamThreadConsumer =
            //                new MockStreamThreadConsumer<>(OffsetResetStrategy.EARLIEST);
            //
            //        TaskManager taskManager = new TaskManager(
            //            new MockChangelogReader(),
            //            processId,
            //            "log-prefix",
            //            mockStreamThreadConsumer,
            //            streamsMetadataState,
            //            null,
            //            null,
            //            null,
            //            new AssignedStreamsTasks(new LogContext()),
            //            new AssignedStandbyTasks(new LogContext()));
            //        taskManager.SetConsumer(mockStreamThreadConsumer);
            //        taskManager.SetAssignmentMetadata(Collections.emptyMap(), Collections.emptyMap());
            //
            //        // StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
            //        StreamThread thread = new StreamThread(
            //            mockTime,
            //            config,
            //            null,
            //            mockStreamThreadConsumer,
            //            mockStreamThreadConsumer,
            //            null,
            //            taskManager,
            //            //streamsMetrics,
            //            internalTopologyBuilder,
            //            clientId,
            //            new LogContext(""),
            //            0)
            //            .UpdateThreadMetadata(getSharedAdminClientId(clientId));

            mockStreamThreadConsumer.SetStreamThread(thread);
            mockStreamThreadConsumer.Assign(assignedPartitions);
            mockStreamThreadConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

            addRecord(mockStreamThreadConsumer, 1L, 0L);
            thread.SetState(StreamThreadStates.STARTING);
            thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);
            thread.RunOnce();
        }

        [Fact]
        public void ShouldOnlyShutdownOnce()
        {
            IConsumer<byte[], byte[]> consumer = Mock.Of<IConsumer<byte[], byte[]>>();
            TaskManager taskManager = Mock.Of<TaskManager>();
            taskManager.Shutdown(true);
            //EasyMock.expectLastCall();
            //EasyMock.replay(taskManager, consumer);

            //StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
            StreamThread thread = new StreamThread(
                config,
                null,
                consumer,
                consumer,
                null,
                taskManager,
                //streamsMetrics,
                internalTopologyBuilder,
                clientId,
                new LogContext(""),
                new int()
            ).UpdateThreadMetadata(GetSharedAdminClientId(clientId));
            thread.Shutdown();
            // Execute the run method. Verification of the mock will check that Shutdown was only done once
            thread.Run();
            // EasyMock.verify(taskManager);
        }

        [Fact]
        public void ShouldNotNullPointerWhenStandbyTasksAssignedAndNoStateStoresForTopology()
        {
            internalTopologyBuilder.AddSource(null, "Name", null, null, null, "topic");
            internalTopologyBuilder.AddSink("out", "output", null, null, null, "Name");

            StreamThread thread = createStreamThread(clientId, config, false);

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(Collections.emptyList());

            Dictionary<TaskId, HashSet<TopicPartition>> StandbyTasks = new HashMap<>();

            // assign single partition
            StandbyTasks.Put(task1, Collections.singleton(t1p1));

            thread.TaskManager.SetAssignmentMetadata(Collections.emptyMap(), StandbyTasks);
            thread.TaskManager.CreateTasks(Collections.emptyList());

            thread.RebalanceListener.OnPartitionsAssigned(Collections.emptyList());
        }

        [Fact]
        public void ShouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerWasFencedWhileProcessing()
        {// throws Exception
            internalTopologyBuilder.AddSource(null, "source", null, null, null, topic1);
            internalTopologyBuilder.AddSink("sink", "dummyTopic", null, null, null, "source");

            StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

            MockConsumer<byte[], byte[]> consumer = clientSupplier.consumer;

            consumer.updatePartitions(topic1, Collections.singletonList(new PartitionInfo(topic1, 1, null, null, null)));

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(null);

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(assignedPartitions);
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);

            thread.RunOnce();
            Assert.Single(thread.Tasks());
            MockProducer producer = clientSupplier.producers.Get(0);

            // change consumer subscription from "pattern" to "manual" to be able to call .addRecords()
            consumer.UpdateBeginningOffsets(Collections.singletonMap(assignedPartitions.iterator().MoveNext(), 0L));
            consumer.Unsubscribe();
            consumer.Assign(new HashSet<>(assignedPartitions));

            consumer.AddRecord(new ConsumeResult<>(topic1, 1, 0, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
            mockTime.Sleep(config.GetLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1);
            thread.RunOnce();
            Assert.Equal(producer.history().Count, 1);

            Assert.False(producer.transactionCommitted());
            mockTime.Sleep(config.GetLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
            TestUtils.WaitForCondition(
                () => producer.commitCount() == 1,
                "StreamsThread did not commit transaction.");

            producer.fenceProducer();
            mockTime.Sleep(config.GetLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) + 1L);
            consumer.AddRecord(new ConsumeResult<>(topic1, 1, 1, System.Array.Empty<byte>(), System.Array.Empty<byte>()));
            try
            {
                thread.RunOnce();
                Assert.True(false, "Should have thrown TaskMigratedException");
            }
            catch (TaskMigratedException expected) { /* ignore */ }

            TestUtils.WaitForCondition(
                () => thread.Tasks().IsEmpty(),
                "StreamsThread did not remove fenced zombie task.");

            Assert.Equal(producer.commitCount(), 1L);
        }

        [Fact]
        public void ShouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedInCommitTransactionWhenSuspendingTaks()
        {
            StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

            internalTopologyBuilder.AddSource(null, "Name", null, null, null, topic1);
            internalTopologyBuilder.AddSink("out", "output", null, null, null, "Name");

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(null);

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(assignedPartitions);
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);

            thread.RunOnce();

            Assert.Single(thread.Tasks());

            clientSupplier.producers.Get(0).fenceProducer();
            thread.RebalanceListener.OnPartitionsRevoked(null);
            Assert.True(clientSupplier.producers.Get(0).transactionInFlight());
            Assert.False(clientSupplier.producers.Get(0).transactionCommitted());
            Assert.True(clientSupplier.producers.Get(0).closed());
            Assert.True(thread.Tasks().IsEmpty());
        }

        [Fact]
        public void ShouldCloseTaskAsZombieAndRemoveFromActiveTasksIfProducerGotFencedInCloseTransactionWhenSuspendingTasks()
        {
            StreamThread thread = createStreamThread(clientId, new StreamsConfig(configProps(true)), true);

            internalTopologyBuilder.AddSource(null, "Name", null, null, null, topic1);
            internalTopologyBuilder.AddSink("out", "output", null, null, null, "Name");

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(null);

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(assignedPartitions);
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);

            thread.RunOnce();

            Assert.Single(thread.Tasks());

            clientSupplier.producers.Get(0).fenceProducerOnClose();
            thread.RebalanceListener.OnPartitionsRevoked(null);

            Assert.False(clientSupplier.producers.Get(0).transactionInFlight());
            Assert.True(clientSupplier.producers.Get(0).transactionCommitted());
            Assert.False(clientSupplier.producers.Get(0).closed());
            Assert.True(thread.Tasks().IsEmpty());
        }

        private class StateListenerStub : IStateListener
        {
            int numChanges = 0;
            IThreadStateTransitionValidator oldState = null;
            IThreadStateTransitionValidator newState = null;


            public void OnChange(IThread thread,
                                 IThreadStateTransitionValidator newState,
                                 IThreadStateTransitionValidator oldState)
            {
                ++numChanges;
                if (this.newState != null)
                {
                    if (this.newState != oldState)
                    {
                        throw new RuntimeException("State mismatch " + oldState + " different from " + this.newState);
                    }
                }
                this.oldState = oldState;
                this.newState = newState;
            }
        }

        [Fact]
        public void ShouldReturnActiveTaskMetadataWhileRunningState()
        {
            internalTopologyBuilder.AddSource(null, "source", null, null, null, topic1);

            StreamThread thread = createStreamThread(clientId, config, false);

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(null);

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(assignedPartitions);
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);

            thread.RunOnce();

            ThreadMetadata threadMetadata = thread.threadMetadata();
            Assert.Equal(StreamThreadStates.RUNNING.Name(), threadMetadata.threadState());
            Assert.True(threadMetadata.ActiveTasks().Contains(new TaskMetadata(task1.ToString(), Utils.mkSet(t1p1))));
            Assert.True(threadMetadata.StandbyTasks().IsEmpty());
        }

        [Fact]
        public void ShouldReturnStandbyTaskMetadataWhileRunningState()
        {
            internalStreamsBuilder.Stream(Collections.singleton(topic1), consumed)
                .GroupByKey().Count(Materialized.As("count-one"));

            internalStreamsBuilder.BuildAndOptimizeTopology();
            StreamThread thread = createStreamThread(clientId, config, false);
            MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
            restoreConsumer.updatePartitions(
                "stream-thread-test-count-one-changelog",
                Collections.singletonList(
                    new PartitionInfo("stream-thread-test-count-one-changelog",
                    0,
                    null,
                    System.Array.Empty<Node>(),
                    System.Array.Empty<Node>())
                )
            );

            Dictionary<TopicPartition, long> offsets = new HashMap<>();
            offsets.Put(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
            restoreConsumer.UpdateEndOffsets(offsets);
            restoreConsumer.UpdateBeginningOffsets(offsets);

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(null);

            Dictionary<TaskId, HashSet<TopicPartition>> StandbyTasks = new HashMap<>();

            // assign single partition
            StandbyTasks.Put(task1, Collections.singleton(t1p1));

            thread.TaskManager.SetAssignmentMetadata(Collections.emptyMap(), StandbyTasks);

            thread.RebalanceListener.OnPartitionsAssigned(Collections.emptyList());

            thread.RunOnce();

            ThreadMetadata threadMetadata = thread.ThreadMetadata;
            Assert.Equal(StreamThreadStates.RUNNING.Name(), threadMetadata.ThreadState);
            Assert.Contains(new TaskMetadata(task1.ToString(), Utils.mkSet(t1p1)), threadMetadata.StandbyTasks);
            Assert.True(threadMetadata.ActiveTasks.Any());
        }


        [Fact]
        public void ShouldUpdateStandbyTask()
        {// throws Exception
            string storeName1 = "count-one";
            string storeName2 = "table-two";
            string changelogName1 = applicationId + "-" + storeName1 + "-changelog";
            string changelogName2 = applicationId + "-" + storeName2 + "-changelog";
            TopicPartition partition1 = new TopicPartition(changelogName1, 1);
            TopicPartition partition2 = new TopicPartition(changelogName2, 1);
            internalStreamsBuilder
                .Stream(Collections.singleton(topic1), consumed)
                .GroupByKey()
                .Count(Materialized.As(storeName1));
            MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>> materialized
                = new MaterializedInternal<>(Materialized.As(storeName2), internalStreamsBuilder, "");
            internalStreamsBuilder.table(topic2, new ConsumedInternal<>(), materialized);

            internalStreamsBuilder.BuildAndOptimizeTopology();
            StreamThread thread = createStreamThread(clientId, config, false);
            MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
            restoreConsumer.updatePartitions(changelogName1,
                Collections.singletonList(
                    new PartitionInfo(
                        changelogName1,
                        1,
                        null,
                        System.Array.Empty<Node>(),
                        System.Array.Empty<Node>()
                    )
                )
            );

            restoreConsumer.Assign(Utils.mkSet(partition1, partition2));
            restoreConsumer.UpdateEndOffsets(Collections.singletonMap(partition1, 10L));
            restoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(partition1, 0L));
            restoreConsumer.UpdateEndOffsets(Collections.singletonMap(partition2, 10L));
            restoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(partition2, 0L));
            // let the store1 be restored from 0 to 10; store2 be restored from 5 (checkpointed) to 10
            OffsetCheckpoint checkpoint
                = new OffsetCheckpoint(new FileInfo(stateDirectory.DirectoryForTask(task3), CHECKPOINT_FILE_NAME));
            checkpoint.Write(Collections.singletonMap(partition2, 5L));

            for (long i = 0L; i < 10L; i++)
            {
                restoreConsumer.AddRecord(new ConsumeResult<>(
                    changelogName1,
                    1,
                    i,
                    ("K" + i).GetBytes(),
                    ("V" + i).GetBytes()));
                restoreConsumer.AddRecord(new ConsumeResult<>(
                    changelogName2,
                    1,
                    i,
                    ("K" + i).GetBytes(),
                    ("V" + i).GetBytes()));
            }

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(null);

            Dictionary<TaskId, HashSet<TopicPartition>> StandbyTasks = new HashMap<>();

            // assign single partition
            StandbyTasks.Put(task1, Collections.singleton(t1p1));
            StandbyTasks.Put(task3, Collections.singleton(t2p1));

            thread.TaskManager.SetAssignmentMetadata(Collections.emptyMap(), StandbyTasks);

            thread.RebalanceListener.OnPartitionsAssigned(Collections.emptyList());

            thread.RunOnce();

            StandbyTask standbyTask1 = thread.TaskManager.StandbyTask(partition1);
            StandbyTask standbyTask2 = thread.TaskManager.StandbyTask(partition2);
            IKeyValueStore<object, long> store1 = (IKeyValueStore<object, long>)standbyTask1.GetStore(storeName1);
            IKeyValueStore<object, long> store2 = (IKeyValueStore<object, long>)standbyTask2.GetStore(storeName2);

            Assert.Equal(10L, store1.approximateNumEntries);
            Assert.Equal(5L, store2.approximateNumEntries);
            Assert.Equal(0, thread.StandbyRecords.Count);
        }

        [Fact]
        public void ShouldCreateStandbyTask()
        {
            setupInternalTopologyWithoutState();
            internalTopologyBuilder.AddStateStore(new MockKeyValueStoreBuilder("myStore", true), "processor1");

            StandbyTask standbyTask = createStandbyTask();

            Assert.Equal(standbyTask, not(nullValue()));
        }

        [Fact]
        public void ShouldNotCreateStandbyTaskWithoutStateStores()
        {
            setupInternalTopologyWithoutState();

            StandbyTask standbyTask = createStandbyTask();

            Assert.Equal(standbyTask, nullValue());
        }

        [Fact]
        public void ShouldNotCreateStandbyTaskIfStateStoresHaveLoggingDisabled()
        {
            setupInternalTopologyWithoutState();
            IStoreBuilder storeBuilder = new MockKeyValueStoreBuilder("myStore", true);
            storeBuilder.WithLoggingDisabled();
            internalTopologyBuilder.AddStateStore(storeBuilder, "processor1");

            StandbyTask standbyTask = createStandbyTask();

            Assert.Equal(standbyTask, nullValue());
        }

        private void SetupInternalTopologyWithoutState()
        {
            MockProcessor mockProcessor = new MockProcessor();
            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);
            internalTopologyBuilder.AddProcessor("processor1", () => mockProcessor, "source1");
        }

        private StandbyTask CreateStandbyTask()
        {
            LogContext logContext = new LogContext("test");
            ILogger<StreamThreadTest> log = logContext.logger(StreamThreadTest);
            //StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
            StandbyTaskCreator standbyTaskCreator = new StandbyTaskCreator(
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

        [Fact]
        public void ShouldPunctuateActiveTask()
        {
            List<long> punctuatedStreamTime = new List<long>();
            List<long> punctuatedWallClockTime = new List<long>();
            ProcessorSupplier<object, object> punctuateProcessor = () => new Processor<object, object>();
            //        {
            //
            //
            //            public void Init(IProcessorContext context)
            //        {
            //            context.schedule(TimeSpan.FromMilliseconds(100L), PunctuationType.STREAM_TIME, punctuatedStreamTime::add);
            //            context.schedule(TimeSpan.FromMilliseconds(100L), PunctuationType.WALL_CLOCK_TIME, punctuatedWallClockTime::add);
            //        }
            //
            //
            //        public void process(object key,
            //                            object value)
            //        { }
            //
            //
            //        public void Close() { }
            //    };

            internalStreamsBuilder.Stream(Collections.singleton(topic1), consumed).Process(punctuateProcessor);
            internalStreamsBuilder.BuildAndOptimizeTopology();

            StreamThread thread = createStreamThread(clientId, config, false);

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(null);
            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new Dictionary<TaskId, HashSet<TopicPartition>>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

            clientSupplier.consumer.Assign(assignedPartitions);
            clientSupplier.consumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);

            thread.RunOnce();

            Assert.Equal(0, punctuatedStreamTime.Count);
            Assert.Equal(0, punctuatedWallClockTime.Count);

            mockTime.Sleep(100L);
            for (long i = 0L; i < 10L; i++)
            {
                clientSupplier.consumer.AddRecord(new ConsumeResult<>(
                    topic1,
                    1,
                    i,
                    i * 100L,
                    TimestampType.CreateTime,
                    ConsumeResult.NULL_CHECKSUM,
                    ("K" + i).GetBytes().Length,
                    ("V" + i).GetBytes().Length,
                    ("K" + i).GetBytes(),
                    ("V" + i).GetBytes()));
            }

            thread.RunOnce();

            Assert.Single(punctuatedStreamTime);
            Assert.Single(punctuatedWallClockTime);

            mockTime.Sleep(100L);

            thread.RunOnce();

            // we should skip stream time punctuation, only trigger wall-clock time punctuation
            Assert.Single(punctuatedStreamTime);
            Assert.Equal(2, punctuatedWallClockTime.Count);
        }

        [Fact]
        public void ShouldAlwaysUpdateTasksMetadataAfterChangingState()
        {
            StreamThread thread = createStreamThread(clientId, config, false);
            ThreadMetadata metadata = thread.ThreadMetadata;
            Assert.Equal(StreamThreadStates.CREATED.ToString(), metadata.ThreadState);

            thread.SetState(StreamThreadStates.STARTING);
            thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);
            thread.SetState(StreamThreadStates.PARTITIONS_ASSIGNED);
            thread.SetState(StreamThreadStates.RUNNING);
            metadata = thread.ThreadMetadata;
            Assert.Equal(StreamThreadStates.RUNNING.ToString(), metadata.ThreadState);
        }

        [Fact]
        public void ShouldAlwaysReturnEmptyTasksMetadataWhileRebalancingStateAndTasksNotRunning()
        {
            internalStreamsBuilder.Stream(Collections.singleton(topic1), consumed)
                .GroupByKey().Count(Materialized.As("count-one"));
            internalStreamsBuilder.BuildAndOptimizeTopology();

            StreamThread thread = createStreamThread(clientId, config, false);
            MockConsumer<byte[], byte[]> restoreConsumer = clientSupplier.restoreConsumer;
            restoreConsumer.updatePartitions("stream-thread-test-count-one-changelog",
                Arrays.asList(
                    new PartitionInfo("stream-thread-test-count-one-changelog",
                        0,
                        null,
                        System.Array.Empty<Node>(),
                        System.Array.Empty<Node>()),
                    new PartitionInfo("stream-thread-test-count-one-changelog",
                        1,
                        null,
                        System.Array.Empty<Node>(),
                        System.Array.Empty<Node>())
                ));
            Dictionary<TopicPartition, long> offsets = new HashMap<>();
            offsets.Put(new TopicPartition("stream-thread-test-count-one-changelog", 0), 0L);
            offsets.Put(new TopicPartition("stream-thread-test-count-one-changelog", 1), 0L);
            restoreConsumer.updateEndOffsets(offsets);
            restoreConsumer.UpdateBeginningOffsets(offsets);

            clientSupplier.consumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));

            List<TopicPartition> assignedPartitions = new List<TopicPartition>();

            thread.SetState(StreamThreadStates.STARTING);
            thread.RebalanceListener.OnPartitionsRevoked(assignedPartitions);
            assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThreadStates.PARTITIONS_REVOKED);

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            Dictionary<TaskId, HashSet<TopicPartition>> StandbyTasks = new HashMap<>();

            // assign single partition
            assignedPartitions.Add(t1p1);
            ActiveTasks.Put(task1, Collections.singleton(t1p1));
            StandbyTasks.Put(task2, Collections.singleton(t1p2));

            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, StandbyTasks);

            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);

            assertThreadMetadataHasEmptyTasksWithState(thread.threadMetadata(), StreamThreadStates.PARTITIONS_ASSIGNED);
        }

        [Fact]
        public void ShouldRecoverFromInvalidOffsetExceptionOnRestoreAndFinishRestore()
        {// throws Exception
            internalStreamsBuilder.Stream(Collections.singleton("topic"), consumed)
                .GroupByKey().Count(Materialized.As("count"));
            internalStreamsBuilder.BuildAndOptimizeTopology();

            StreamThread thread = createStreamThread("clientId", config, false);
            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            MockConsumer<byte[], byte[]> mockRestoreConsumer = (MockConsumer<byte[], byte[]>)thread.restoreConsumer;

            TopicPartition topicPartition = new TopicPartition("topic", 0);
            HashSet<TopicPartition> topicPartitionSet = Collections.singleton(topicPartition);

            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            ActiveTasks.Put(new TaskId(0, 0), topicPartitionSet);
            thread.TaskManager.SetAssignmentMetadata(ActiveTasks, Collections.emptyMap());

            mockConsumer.updatePartitions(
                "topic",
                Collections.singletonList(
                    new PartitionInfo(
                        "topic",
                        0,
                        null,
                        System.Array.Empty<Node>(),
                        System.Array.Empty<Node>()
                    )
                )
            );
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));

            mockRestoreConsumer.updatePartitions(
                "stream-thread-test-count-changelog",
                Collections.singletonList(
                    new PartitionInfo(
                        "stream-thread-test-count-changelog",
                        0,
                        null,
                        System.Array.Empty<Node>(),
                        System.Array.Empty<Node>()
                    )
                )
            );

            TopicPartition changelogPartition = new TopicPartition("stream-thread-test-count-changelog", 0);
            HashSet<TopicPartition> changelogPartitionSet = Collections.singleton(changelogPartition);
            mockRestoreConsumer.UpdateBeginningOffsets(Collections.singletonMap(changelogPartition, 0L));
            mockRestoreConsumer.updateEndOffsets(Collections.singletonMap(changelogPartition, 2L));

            mockConsumer.schedulePollTask(() =>
            {
                thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);
                thread.RebalanceListener.OnPartitionsAssigned(topicPartitionSet);
            });

            try
            {
                thread.Start();

                TestUtils.WaitForCondition(
                    () => mockRestoreConsumer.Assignment.Count == 1,
                    "Never restore first record");

                mockRestoreConsumer.AddRecord(new ConsumeResult<>(
                    "stream-thread-test-count-changelog",
                    0,
                    0L,
                    "K1".GetBytes(),
                    "V1".GetBytes()));

                TestUtils.WaitForCondition(
                    () => mockRestoreConsumer.Position(changelogPartition) == 1L,
                    "Never restore first record");

                mockRestoreConsumer.SetException(new InvalidOffsetException("Try Again!"));
                //            {
                //
                //
                //                public HashSet<TopicPartition> partitions()
                //            {
                //                return changelogPartitionSet;
                //            }
                //        });

                mockRestoreConsumer.AddRecord(new ConsumeResult<>(
                    "stream-thread-test-count-changelog",
                    0,
                    0L,
                    "K1".GetBytes(),
                    "V1".GetBytes()));
                mockRestoreConsumer.AddRecord(new ConsumeResult<>(
                    "stream-thread-test-count-changelog",
                    0,
                    1L,
                    "K2".GetBytes(),
                    "V2".GetBytes()));

                TestUtils.WaitForCondition(
                    () =>
                    {
                        mockRestoreConsumer.Assign(changelogPartitionSet);
                        return mockRestoreConsumer.Position(changelogPartition) == 2L;
                    },
                    "Never finished restore");
            }
            finally
            {
                thread.Shutdown();
                thread.Join(10000);
            }
        }

        [Fact]
        public void ShouldRecordSkippedMetricForDeserializationException()
        {
            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

            StreamsConfig config = configProps(false);
            config.Set(
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.getName());
            config.DefaultValueSerdeType = Serdes.Int().GetType();

            StreamThread thread = createStreamThread(clientId, new StreamsConfig(config), false);

            thread.SetState(StreamThreadStates.STARTING);
            thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);

            HashSet<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
            thread.TaskManager.SetAssignmentMetadata(
                Collections.singletonMap(
                    new TaskId(0, t1p1.Partition),
                    assignedPartitions),
                Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(Collections.singleton(t1p1));
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);
            thread.RunOnce();

            // MetricName skippedTotalMetric = metrics.metricName(
            //     "skipped-records-total",
            //     "stream-metrics",
            //     Collections.singletonMap("client-id", thread.getName()));
            // MetricName skippedRateMetric = metrics.metricName(
            //     "skipped-records-rate",
            //     "stream-metrics",
            //     Collections.singletonMap("client-id", thread.getName()));
            // Assert.Equal(0.0, metrics.metric(skippedTotalMetric).metricValue());
            // Assert.Equal(0.0, metrics.metric(skippedRateMetric).metricValue());

            long offset = -1;
            mockConsumer.AddRecord(new ConsumeResult<>(
                t1p1.Topic,
                t1p1.Partition,
                ++offset, -1,
                TimestampType.CreateTime,
                ConsumeResult.NULL_CHECKSUM,
                -1,
                -1,
                System.Array.Empty<byte>(),
                "I am not an integer.".GetBytes()));
            mockConsumer.AddRecord(new ConsumeResult<>(
                t1p1.Topic,
                t1p1.Partition,
                ++offset,
                -1,
                TimestampType.CreateTime,
                ConsumeResult.NULL_CHECKSUM,
                -1,
                -1,
                System.Array.Empty<byte>(),
                "I am not an integer.".GetBytes()));
            thread.RunOnce();
            // Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
            // Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());
            // 
            // LogCaptureAppender.Unregister(appender);
            List<string> strings = appender.getMessages();
            Assert.Contains("task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[0]", strings);
            Assert.Contains("task [0_1] Skipping record due to deserialization error. topic=[topic1] partition=[1] offset=[1]", strings);
        }

        [Fact]
        public void ShouldReportSkippedRecordsForInvalidTimestamps()
        {
            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

            internalTopologyBuilder.AddSource(null, "source1", null, null, null, topic1);

            StreamsConfig config = configProps(false);
            config.Set(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                LogAndSkipOnInvalidTimestamp.getName());
            StreamThread thread = createStreamThread(clientId, new StreamsConfig(config), false);

            thread.SetState(StreamThreadStates.STARTING);
            thread.SetState(StreamThreadStates.PARTITIONS_REVOKED);

            HashSet<TopicPartition> assignedPartitions = Collections.singleton(t1p1);
            thread.TaskManager.SetAssignmentMetadata(
                Collections.singletonMap(
                    new TaskId(0, t1p1.Partition),
                    assignedPartitions),
                Collections.emptyMap());

            MockConsumer<byte[], byte[]> mockConsumer = (MockConsumer<byte[], byte[]>)thread.consumer;
            mockConsumer.Assign(Collections.singleton(t1p1));
            mockConsumer.UpdateBeginningOffsets(Collections.singletonMap(t1p1, 0L));
            thread.RebalanceListener.OnPartitionsAssigned(assignedPartitions);
            thread.RunOnce();

            // MetricName skippedTotalMetric = metrics.metricName(
            //     "skipped-records-total",
            //     "stream-metrics",
            //     Collections.singletonMap("client-id", thread.getName()));
            // MetricName skippedRateMetric = metrics.metricName(
            //     "skipped-records-rate",
            //     "stream-metrics",
            //     Collections.singletonMap("client-id", thread.getName()));
            // Assert.Equal(0.0, metrics.metric(skippedTotalMetric).metricValue());
            // Assert.Equal(0.0, metrics.metric(skippedRateMetric).metricValue());

            long offset = -1;
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            thread.RunOnce();
            //Assert.Equal(2.0, metrics.metric(skippedTotalMetric).metricValue());
            //Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            addRecord(mockConsumer, ++offset);
            thread.RunOnce();
            //Assert.Equal(6.0, metrics.metric(skippedTotalMetric).metricValue());
            //Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

            addRecord(mockConsumer, ++offset, 1L);
            addRecord(mockConsumer, ++offset, 1L);
            thread.RunOnce();
            //Assert.Equal(6.0, metrics.metric(skippedTotalMetric).metricValue());
            //Assert.NotEqual(0.0, metrics.metric(skippedRateMetric).metricValue());

            LogCaptureAppender.Unregister(appender);
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

        private void AssertThreadMetadataHasEmptyTasksWithState(ThreadMetadata metadata,
                                                                StreamThreadStates state)
        {
            Assert.Equal(state.Name(), metadata.threadState());
            Assert.True(metadata.ActiveTasks().IsEmpty());
            Assert.True(metadata.StandbyTasks().IsEmpty());
        }

        // [Fact]
        // // TODO: Need to add a test case covering EOS when we Create a mock taskManager class
        // public void ProducerMetricsVerificationWithoutEOS()
        // {
        //     MockProducer<byte[], byte[]> producer = new MockProducer<>();
        //     IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer>();
        //     TaskManager taskManager = mockTaskManagerCommit(consumer, 1, 0);
        // 
        //     StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //     StreamThread thread = new StreamThread(
        //             mockTime,
        //             config,
        //             producer,
        //             consumer,
        //             consumer,
        //             null,
        //             taskManager,
        //             streamsMetrics,
        //             internalTopologyBuilder,
        //             clientId,
        //             new LogContext(""),
        //             new int()
        //             );
        //     MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        //     Metric testMetric = new KafkaMetric(
        //         new object(),
        //         testMetricName,
        //         (Measurable)(config, now) => 0,
        //         null,
        //         new MockTime());
        //     producer.setMockMetrics(testMetricName, testMetric);
        //     Dictionary<MetricName, Metric> producerMetrics = thread.producerMetrics();
        //     Assert.Equal(testMetricName, producerMetrics.Get(testMetricName).metricName());
        // }

        //[Fact]
        //public void AdminClientMetricsVerification()
        //{
        //    Node broker1 = new Node(0, "dummyHost-1", 1234);
        //    Node broker2 = new Node(1, "dummyHost-2", 1234);
        //    List<Node> cluster = Arrays.asList(broker1, broker2);
        //
        //    MockAdminClient adminClient = new MockAdminClient(cluster, broker1, null);
        //
        //    MockProducer<byte[], byte[]> producer = new MockProducer<>();
        //    IConsumer<byte[], byte[]> consumer = Mock.Of<Consumer);
        //    TaskManager taskManager = Mock.Of<TaskManager);
        //
        //    StreamsMetricsImpl streamsMetrics = new StreamsMetricsImpl(metrics, clientId);
        //    StreamThread thread = new StreamThread(
        //            mockTime,
        //            config,
        //            producer,
        //            consumer,
        //            consumer,
        //            null,
        //            taskManager,
        //            streamsMetrics,
        //            internalTopologyBuilder,
        //            clientId,
        //            new LogContext(""),
        //            new int()
        //            );
        //    MetricName testMetricName = new MetricName("test_metric", "", "", new HashMap<>());
        //    Metric testMetric = new KafkaMetric(
        //        new object(),
        //        testMetricName,
        //        (Measurable)(config, now) => 0,
        //        null,
        //        new MockTime());
        //
        //    EasyMock.expect(taskManager.GetAdminClient()).andReturn(adminClient);
        //    EasyMock.expectLastCall();
        //    EasyMock.replay(taskManager, consumer);
        //
        //    adminClient.setMockMetrics(testMetricName, testMetric);
        //    Dictionary<MetricName, Metric> adminClientMetrics = thread.adminClientMetrics();
        //    Assert.Equal(testMetricName, adminClientMetrics.Get(testMetricName).metricName());
        //}

        private void AddRecord(MockConsumer<byte[], byte[]> mockConsumer,
                               long offset)
        {
            addRecord(mockConsumer, offset, -1L);
        }

        private void AddRecord(MockConsumer<byte[], byte[]> mockConsumer,
                               long offset,
                               long timestamp)
        {
            mockConsumer.AddRecord(new ConsumeResult<>(
                t1p1.Topic,
                t1p1.Partition,
                offset,
                timestamp,
                TimestampType.CreateTime,
                ConsumeResult.NULL_CHECKSUM,
                -1,
                -1,
                System.Array.Empty<byte>(),
                System.Array.Empty<byte>()));
        }
    }
}
