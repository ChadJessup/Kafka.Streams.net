using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Threads;
using Kafka.Streams.Threads.KafkaStreams;
using Kafka.Streams.Threads.Stream;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class KafkaStreamsTest
    {
        private const int NUM_BROKERS = 1;
        private const int NUM_THREADS = 1;

        private readonly IKafkaStreamsThread globalStreams;
        private readonly StreamsConfig props;

        public KafkaStreamsTest()
        {
            // throw if librdkafka won't load
            Confluent.Kafka.Library.Load();

            this.props = StreamsTestConfigs.GetStandardConfig(numberOfMockBrokers: NUM_BROKERS, numberOfThreads: NUM_THREADS);
            this.globalStreams = TestUtils.GetStreamsBuilder(this.props).BuildKafkaStreams();
        }

        public void Cleanup()
        {
            if (this.globalStreams != null)
            {
                this.globalStreams.Close();
            }
        }

        [Fact]
        public void TestOsDefaultSocketBufferSizes()
        {
            this.props.GetGlobalConsumerConfigs("").SocketReceiveBufferBytes = 0;
            this.props.GetGlobalConsumerConfigs("").SocketReceiveBufferBytes = 0;
            var streams = TestUtils.GetStreamsBuilder(this.props).BuildKafkaStreams();

            streams.Close();
        }

        [Fact] //(expected = KafkaException))
        public void TestInvalidSocketSendBufferSize()
        {
            this.props.GetGlobalConsumerConfigs("").SocketSendBufferBytes = -2;
            var streams = TestUtils.GetStreamsBuilder(this.props).BuildKafkaStreams();

            streams.Close();
        }

        [Fact] //(expected = KafkaException))
        public void TestInvalidSocketReceiveBufferSize()
        {
            this.props.GetGlobalConsumerConfigs("").SocketReceiveBufferBytes = -2;
            var streams = TestUtils.GetStreamsBuilder(this.props).BuildKafkaStreams();

            streams.Close();
        }

        [Fact]
        public void StateShouldTransitToNotRunningIfCloseRightAfterCreated()
        {
            this.globalStreams.Close();

            Assert.Equal(KafkaStreamsThreadStates.NOT_RUNNING, this.globalStreams.State.CurrentState);
        }

        [Fact]
        public void StateShouldTransitToRunningIfNonDeadThreadsBackToRunning()//// throws InterruptedException

        {
            var stateListener = new KafkaStreamsTestsStateListenerStub();
            this.globalStreams.SetStateListener(stateListener);

            Assert.Equal(0, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.CREATED, this.globalStreams.State.CurrentState);

            this.globalStreams.Start();

            TestUtils.WaitForCondition(
                () => stateListener.numChanges == 2,
                timeout: TimeSpan.FromSeconds(5.0),
                "Streams never started.");

            Assert.Equal(KafkaStreamsThreadStates.RUNNING, this.globalStreams.State.CurrentState);

            foreach (StreamThread thread in this.globalStreams.Threads)
            {
                thread.StateListener.OnChange(
                    thread,
                    StreamThreadStates.PARTITIONS_REVOKED,
                    StreamThreadStates.RUNNING);
            }

            Assert.Equal(3, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.REBALANCING, this.globalStreams.State.CurrentState);

            foreach (var thread in this.globalStreams.Threads)
            {
                thread.StateListener.OnChange(
                    thread,
                    StreamThreadStates.PARTITIONS_ASSIGNED,
                    StreamThreadStates.PARTITIONS_REVOKED);
            }

            Assert.Equal(3, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.REBALANCING, this.globalStreams.State.CurrentState);

            this.globalStreams.Threads[NUM_THREADS - 1].StateListener.OnChange(
                this.globalStreams.Threads[NUM_THREADS - 1],
                StreamThreadStates.PENDING_SHUTDOWN,
                StreamThreadStates.PARTITIONS_ASSIGNED);

            this.globalStreams.Threads[NUM_THREADS - 1].StateListener.OnChange(
                this.globalStreams.Threads[NUM_THREADS - 1],
                StreamThreadStates.DEAD,
                StreamThreadStates.PENDING_SHUTDOWN);

            Assert.Equal(3, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.REBALANCING, this.globalStreams.State.CurrentState);

            foreach (var thread in this.globalStreams.Threads)
            {
                if (thread != this.globalStreams.Threads[NUM_THREADS - 1])
                {
                    thread.StateListener.OnChange(
                        thread,
                        StreamThreadStates.RUNNING,
                        StreamThreadStates.PARTITIONS_ASSIGNED);
                }
            }

            Assert.Equal(4, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.RUNNING, this.globalStreams.State.CurrentState);

            this.globalStreams.Close();

            TestUtils.WaitForCondition(
                () => stateListener.numChanges == 6,
                "Streams never closed.");

            Assert.Equal(KafkaStreamsThreadStates.NOT_RUNNING, this.globalStreams.State.CurrentState);
        }

        //[Fact]
        //public void stateShouldTransitToErrorIfAllThreadsDead() //// throws InterruptedException
        //{
        //    StateListenerStub stateListener = new StateListenerStub();
        //    globalStreams.SetStateListener(stateListener);

        //    Assert.Equal(0, stateListener.numChanges);
        //    Assert.Equal(KafkaStreamsThreadStates.CREATED, globalStreams.State.CurrentState);

        //    globalStreams.Start();

        //    TestUtils.WaitForCondition(
        //        () => stateListener.numChanges == 2,
        //        "Streams never started.");
        //    Assert.Equal(KafkaStreamsThreadStates.RUNNING, globalStreams.State.CurrentState);

        //    foreach (var thread in globalStreams.threads)
        //    {
        //        thread.StateListener.onChange(
        //            thread,
        //            KafkaStreamThreadStates.PARTITIONS_REVOKED,
        //            KafkaStreamThreadStates.RUNNING);
        //    }

        //    Assert.Equal(3, stateListener.numChanges);
        //    Assert.Equal(KafkaStreamsThreadStates.REBALANCING, globalStreams.State.CurrentState);

        //    globalStreams.threads[NUM_THREADS - 1].StateListener.onChange(
        //        globalStreams.threads[NUM_THREADS - 1],
        //        KafkaStreamThreadStates.PENDING_SHUTDOWN,
        //        KafkaStreamThreadStates.PARTITIONS_REVOKED);

        //    globalStreams.threads[NUM_THREADS - 1].StateListener.onChange(
        //        globalStreams.threads[NUM_THREADS - 1],
        //        KafkaStreamThreadStates.DEAD,
        //        KafkaStreamThreadStates.PENDING_SHUTDOWN);

        //    Assert.Equal(3, stateListener.numChanges);
        //    Assert.Equal(KafkaStreamsThreadStates.REBALANCING, globalStreams.State.CurrentState);

        //    foreach (var thread in globalStreams.threads)
        //    {
        //        if (thread != globalStreams.threads[NUM_THREADS - 1])
        //        {
        //            thread.StateListener.onChange(
        //                thread,
        //                KafkaStreamThreadStates.PENDING_SHUTDOWN,
        //                KafkaStreamThreadStates.PARTITIONS_REVOKED);

        //            thread.StateListener.onChange(
        //                thread,
        //                KafkaStreamThreadStates.DEAD,
        //                KafkaStreamThreadStates.PENDING_SHUTDOWN);
        //        }
        //    }

        //    Assert.Equal(4, stateListener.numChanges);
        //    Assert.Equal(KafkaStreamsThreadStates.ERROR, globalStreams.State.CurrentState);

        //    globalStreams.Close();

        //    // the state should not stuck with ERROR, but transit to NOT_RUNNING in the end
        //    TestUtils.WaitForCondition(
        //        () => stateListener.numChanges == 6,
        //        "Streams never closed.");
        //    Assert.Equal(KafkaStreamsThreadStates.NOT_RUNNING, globalStreams.State.CurrentState);
        //}

        //[Fact]
        //public void shouldCleanupResourcesOnCloseWithoutPreviousStart() //// throws Exception
        //{
        //    builder.globalTable("anyTopic");
        //    List<Node> nodes = Collections.singletonList(new Node(0, "localhost", 8121));
        //    Cluster cluster = new Cluster("mockClusterId", nodes,
        //                                        Collections.emptySet(), Collections.emptySet(),
        //                                        Collections.emptySet(), nodes.Get(0));
        //    MockClientSupplier clientSupplier = new MockClientSupplier();
        //    clientSupplier.setClusterForAdminClient(cluster);
        //    var streams = new KafkaStreamsThread(builder.Build(), props, clientSupplier);
        //    streams.Close();
        //    TestUtils.WaitForCondition(
        //        () => streams.State.CurrentState == KafkaStreamsThreadStates.NOT_RUNNING,
        //        "Streams never stopped.");

        //    // Ensure that any created clients are closed
        //    Assert.True(clientSupplier.Consumer.closed());
        //    Assert.True(clientSupplier.RestoreConsumer.closed());
        //    foreach (var p in clientSupplier.Producers)
        //    {
        //        Assert.True(p.closed());
        //    }
        //}

        //[Fact]
        //public void testStateThreadClose() //// throws Exception
        //{
        //    // make sure we have the global state thread running too
        //    builder.globalTable("anyTopic");
        //    var streams = new KafkaStreamsThread(builder.Build(), props);

        //    try
        //    {
        //        java.lang.reflect.Field threadsField = streams.GetType().getDeclaredField("threads");
        //        threadsField.setAccessible(true);
        //        StreamThread[] threads = (StreamThread[])threadsField.Get(streams);

        //        Assert.Equal(NUM_THREADS, threads.Length);
        //        Assert.Equal(streams.State.CurrentState, KafkaStreamsThreadStates.CREATED);

        //        streams.Start();
        //        TestUtils.WaitForCondition(
        //            () => streams.State.CurrentState == KafkaStreamsThreadStates.RUNNING,
        //            "Streams never started.");

        //        for (int i = 0; i < NUM_THREADS; i++)
        //        {
        //            StreamThread tmpThread = threads[i];
        //            tmpThread.Shutdown();
        //            TestUtils.WaitForCondition(
        //                () => tmpThread.State.CurrentState == StreamThreadStates.DEAD,
        //                "Thread never stopped.");
        //            threads[i].Join();
        //        }
        //        TestUtils.WaitForCondition(
        //            () => streams.State.CurrentState == KafkaStreamsThreadStates.ERROR,
        //            "Streams never stopped.");
        //    }
        //    finally
        //    {
        //        streams.Close();
        //    }

        //    TestUtils.WaitForCondition(
        //        () => streams.State.CurrentState == KafkaStreamsThreadStates.NOT_RUNNING,
        //        "Streams never stopped.");

        //    java.lang.reflect.Field globalThreadField = streams.GetType().getDeclaredField("globalStreamThread");
        //    globalThreadField.setAccessible(true);
        //    GlobalStreamThread globalStreamThread = (GlobalStreamThread)globalThreadField.Get(streams);
        //   Assert.Null(globalStreamThread);
        //}

        //[Fact]
        //public void testStateGlobalThreadClose() //// throws Exception
        //{
        //    // make sure we have the global state thread running too
        //    builder.globalTable("anyTopic");
        //    var streams = new KafkaStreamsThread(builder.Build(), props);

        //    try
        //    {
        //        streams.Start();
        //        TestUtils.WaitForCondition(
        //            () => streams.State.CurrentState == KafkaStreamsThreadStates.RUNNING,
        //            errorMessage: "Streams never started.");
        //        var globalThreadField = streams.GetType().getDeclaredField("globalStreamThread");
        //        globalThreadField.setAccessible(true);
        //        GlobalStreamThread globalStreamThread = (GlobalStreamThread)globalThreadField.Get(streams);
        //        globalStreamThread.Shutdown();
        //        TestUtils.WaitForCondition(
        //            () => globalStreamThread.State.CurrentState == GlobalStreamThreadStates.DEAD,
        //            errorMessage: "Thread never stopped.");
        //        globalStreamThread.Join();
        //        Assert.Equal(streams.State.CurrentState, KafkaStreamsThreadStates.ERROR);
        //    }
        //    finally
        //    {
        //        streams.Close();
        //    }

        //    Assert.Equal(streams.State.CurrentState, KafkaStreamsThreadStates.NOT_RUNNING);
        //}

        //[Fact]
        //public void globalThreadShouldTimeoutWhenBrokerConnectionCannotBeEstablished()
        //{
        //   StreamsConfig props = new StreamsConfig();
        //    props.Set(StreamsConfig.ApplicationId, "appId");
        //    props.Set(StreamsConfig.BootstrapServers, "localhost:1");
        //    props.Set(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.getName());
        //    props.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
        //    props.Set(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        //    props.Set(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 200);

        //    // make sure we have the global state thread running too
        //    builder.globalTable("anyTopic");
        //    try
        //    {
        //        var streams = new KafkaStreamsThread(builder.Build(), props);
        //        streams.Start();
        //        Assert.False(true, "expected start() to time out and throw an exception.");
        //    }
        //    catch (StreamsException expected)
        //    {
        //        // This is a result of not being able to connect to the broker.
        //    }
        //    // There's nothing to.Assert... We're testing that this operation actually completes.
        //}

        //[Fact]
        //public void testLocalThreadCloseWithoutConnectingToBroker()
        //{
        //   StreamsConfig props = new StreamsConfig();
        //    props.Set(StreamsConfig.ApplicationId, "appId");
        //    props.Set(StreamsConfig.BootstrapServers, "localhost:1");
        //    props.Set(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.getName());
        //    props.Set(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
        //    props.Set(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        //    // make sure we have the global state thread running too
        //    builder.Table("anyTopic");
        //    try
        //    {
        //        var streams = new KafkaStreamsThread(builder.Build(), props);
        //        streams.Start();
        //    }
        //    catch { }
        //    // There's nothing to.Assert... We're testing that this operation actually completes.
        //}

        ////[Fact]
        ////public void testInitializesAndDestroysMetricsReporters()
        ////{
        ////int oldInitCount = MockMetricsReporter.INIT_COUNT.Get();

        ////try {
        //var streams = new KafkaStreamsThread(builder.Build(), props){
        ////int newInitCount = MockMetricsReporter.INIT_COUNT.Get();
        ////int initDiff = newInitCount - oldInitCount;
        ////Assert.True("some reporters should be initialized by calling on construction", initDiff > 0);

        ////streams.Start();
        ////int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.Get();
        ////streams.Close();
        ////Assert.Equal(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.Get());
        ////}
        ////}

        //[Fact]
        //public void testCloseIsIdempotent()
        //{
        //    globalStreams.Close();
        //    int closeCount = MockMetricsReporter.CLOSE_COUNT.Get();

        //    globalStreams.Close();
        //    Assert.Equal("subsequent Close() calls should do nothing",
        //        closeCount, MockMetricsReporter.CLOSE_COUNT.Get());
        //}

        //[Fact]
        //public void testCannotStartOnceClosed()
        //{
        //    globalStreams.Start();
        //    globalStreams.Close();
        //    try
        //    {
        //        globalStreams.Start();
        //        Assert.False(true, "Should have throw IllegalStateException");
        //    }
        //    catch (IllegalStateException expected)
        //    {
        //        // this is ok
        //    }
        //    finally
        //    {
        //        globalStreams.Close();
        //    }
        //}

        //[Fact]
        //public void testCannotStartTwice()
        //{
        //    globalStreams.Start();

        //    try
        //    {
        //        globalStreams.Start();
        //        Assert.False(true, "Should throw an IllegalStateException");
        //    }
        //    catch (IllegalStateException e)
        //    {
        //        // this is ok
        //    }
        //    finally
        //    {
        //        globalStreams.Close();
        //    }
        //}

        //[Fact]
        //public void shouldNotSetGlobalRestoreListenerAfterStarting()
        //{
        //    globalStreams.Start();
        //    try
        //    {
        //        globalStreams.setGlobalStateRestoreListener(new MockStateRestoreListener());
        //        Assert.False(true, "Should throw an IllegalStateException");
        //    }
        //    catch (IllegalStateException e)
        //    {
        //        // expected
        //    }
        //    finally
        //    {
        //        globalStreams.Close();
        //    }
        //}

        //[Fact]
        //public void shouldThrowExceptionSettingUncaughtExceptionHandlerNotInCreateState()
        //{
        //    globalStreams.Start();
        //    try
        //    {
        //        globalStreams.setUncaughtExceptionHandler(null);
        //        Assert.False(true, "Should throw IllegalStateException");
        //    }
        //    catch (IllegalStateException e)
        //    {
        //        // expected
        //    }
        //}

        //[Fact]
        //public void shouldThrowExceptionSettingStateListenerNotInCreateState()
        //{
        //    globalStreams.Start();
        //    try
        //    {
        //        globalStreams.SetStateListener(null);
        //        Assert.False(true, "Should throw IllegalStateException");
        //    }
        //    catch (IllegalStateException e)
        //    {
        //        // expected
        //    }
        //}

        //[Fact]
        //public void testIllegalMetricsConfig()
        //{
        //    props.Set(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "illegalConfig");

        //    try
        //    {
        //        new KafkaStreamsThread(builder.Build(), props);
        //        Assert.False(true, "Should have throw ConfigException");
        //    }
        //    catch (ConfigException expected) { /* expected */ }
        //}

        //[Fact]
        //public void testLegalMetricsConfig()
        //{
        //    //props.Set(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.INFO.ToString());
        //    //new KafkaStreamsThread(builder.Build(), props).Close();

        //    //props.Set(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.ToString());
        //    //new KafkaStreamsThread(builder.Build(), props).Close();
        //}

        //[Fact] //(expected = IllegalStateException))
        //public void shouldNotGetAllTasksWhenNotRunning()
        //{
        //    globalStreams.allMetadata();
        //}

        //[Fact] //(expected = IllegalStateException))
        //public void shouldNotGetAllTasksWithStoreWhenNotRunning()
        //{
        //    globalStreams.allMetadataForStore("store");
        //}

        // [Fact]// (expected = IllegalStateException))
        // public void ShouldNotGetTaskWithKeyAndSerializerWhenNotRunning()
        // {
        //     globalStreams.MetadataForKey("store", "key", Serdes.String().Serializer);
        // }

        //[Fact]// (expected = IllegalStateException))
        //public void shouldNotGetTaskWithKeyAndPartitionerWhenNotRunning()
        //{
        //    globalStreams.MetadataForKey("store", "key", (topic, key, value, numPartitions) => 0);
        //}

        //[Fact]
        //public void shouldReturnFalseOnCloseWhenThreadsHaventTerminated() //// throws Exception

        //{
        //    bool keepRunning = true;
        //    var streams = null;
        //    try
        //    {
        //        StreamsBuilder builder = new StreamsBuilder();
        //        CountDownLatch latch = new CountDownLatch(1);
        //        string topic = "input";
        //        CLUSTER.CreateTopics(topic);

        //        builder.Stream(topic, Consumed.With(Serdes.String(), Serdes.String()))
        //                .Foreach((key, value) =>
        //               {
        //                   try
        //                   {
        //                       latch.countDown();
        //                       while (keepRunning.Get())
        //                       {
        //                           Thread.Sleep(10);
        //                       }
        //                   }
        //                   catch (InterruptedException e)
        //                   {
        //                       // no-op
        //                   }
        //               });
        //        streams = new KafkaStreamsThread(builder.Build(), props);
        //        streams.Start();
        //        IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(topic,
        //            Collections.singletonList(KeyValuePair.Create("A", "A")),
        //            TestUtils.ProducerConfig(
        //                CLUSTER.bootstrapServers(),
        //                Serdes.String().Serializer,
        //            Serdes.String().Serializer,
        //            new StreamsConfig()),
        //        System.currentTimeMillis());

        //        Assert.True("Timed out waiting to receive single message", latch.wait(30, TimeUnit.SECONDS));
        //        Assert.False(streams.Close(TimeSpan.FromMilliseconds(10)));
        //    }
        //    finally
        //    {
        //        // stop the thread so we don't interfere with other tests etc
        //        keepRunning = false;
        //        if (streams != null)
        //        {
        //            streams.Close();
        //        }
        //    }
        //}

        [Fact]
        public void ShouldReturnThreadMetadata()
        {
            this.globalStreams.Start();
            List<ThreadMetadata> threadMetadata = this.globalStreams.LocalThreadsMetadata();
            Assert.NotNull(threadMetadata);
            Assert.Equal(2, threadMetadata.Count);
            foreach (var metadata in threadMetadata)
            {
                Assert.True(new List<string> { "RUNNING", "STARTING", "PARTITIONS_REVOKED", "PARTITIONS_ASSIGNED", "CREATED" }.Contains(metadata.ThreadState),
                    "#threadState() .As: " + metadata.ThreadState + "; expected either RUNNING, STARTING, PARTITIONS_REVOKED, PARTITIONS_ASSIGNED, or CREATED");

                Assert.Empty(metadata.StandbyTasks);
                Assert.Empty(metadata.ActiveTasks);
                string threadName = metadata.ThreadName;
                Assert.StartsWith("clientId-StreamThread-", threadName);
                Assert.Equal(threadName + "-consumer", metadata.ConsumerClientId);
                Assert.Equal(threadName + "-restore-consumer", metadata.RestoreConsumerClientId);
                Assert.Equal(new List<string> { threadName + "-producer" }, metadata.ProducerClientIds);
                Assert.Equal("clientId-admin", metadata.AdminClientId);
            }
        }

        //[Fact]
        //public void shouldAllowCleanupBeforeStartAndAfterClose()
        //{
        //    try
        //    {
        //        globalStreams.cleanUp();
        //        globalStreams.Start();
        //    }
        //    finally
        //    {
        //        globalStreams.Close();
        //    }
        //    globalStreams.cleanUp();
        //}

        //[Fact]
        //public void shouldThrowOnCleanupWhileRunning() //// throws InterruptedException
        //{
        //    globalStreams.Start();
        //    TestUtils.WaitForCondition(
        //            () => globalStreams.State.CurrentState == KafkaStreamsThreadStates.RUNNING,
        //            "Streams never started.");

        //    try
        //    {
        //        globalStreams.cleanUp();
        //        Assert.False(true, "Should have thrown IllegalStateException");
        //    }
        //    catch (IllegalStateException expected)
        //    {
        //        Assert.Equal("Cannot clean up while running.", expected.Message);
        //    }
        //}

        //[Fact]
        //public void shouldCleanupOldStateDirs() //// throws InterruptedException
        //{
        //    props.Set(StreamsConfig.StateCleanupDelayMs, "1");

        //    string topic = "topic";
        //    CLUSTER.CreateTopic(topic);
        //    StreamsBuilder builder = new StreamsBuilder();

        //    builder.Table(topic, Materialized.As("store"));

        //    try
        //    {
        //        var streams = new KafkaStreamsThread(builder.Build(), props);
        //        CountDownLatch latch = new CountDownLatch(1);
        //        streams.SetStateListener((newState, oldState) =>
        //        {
        //            if (newState == KafkaStreamsThreadStates.RUNNING && oldState == KafkaStreamsThreadStates.REBALANCING)
        //            {
        //                latch.countDown();
        //            }
        //        });
        //        string appDir = props.Get(StreamsConfig.STATE_DIR_CONFIG) + Path.DirectorySeparatorChar + props.Get(StreamsConfig.ApplicationId);
        //        File oldTaskDir = new FileInfo(appDir, "10_1");
        //        Assert.True(oldTaskDir.mkdirs());

        //        streams.Start();
        //        latch.wait(30, TimeUnit.SECONDS);
        //        verifyCleanupStateDir(appDir, oldTaskDir);
        //        Assert.True(oldTaskDir.mkdirs());
        //        verifyCleanupStateDir(appDir, oldTaskDir);
        //    }
        //    catch
        //    { }
        //}

        //[Fact]
        //public void shouldThrowOnNegativeTimeoutForClose()
        //{
        //    try
        //    {
        //        var streams = new KafkaStreamsThread(builder.Build(), props);
        //        streams.Close(TimeSpan.FromMilliseconds(-1L));
        //        Assert.False(true, "should not accept negative Close parameter");
        //    }
        //    catch (ArgumentException e)
        //    {
        //        // expected
        //    }
        //}

        //[Fact]
        //public void shouldNotBlockInCloseForZeroDuration() //// throws InterruptedException {
        //{
        //    var streams = new KafkaStreamsThread(builder.Build(), props);
        //    IThread th = new Thread(() => streams.Close(TimeSpan.FromMilliseconds(0L)));

        //    th.Start();

        //    try
        //    {
        //        th.Join(30_000L);
        //        Assert.False(th.isAlive());
        //    }
        //    finally
        //    {
        //        streams.Close();
        //    }
        //}

        //[Fact]
        //public void statelessTopologyShouldNotCreateStateDirectory() //// throws Exception {

        //{
        //    string inputTopic = testName.getMethodName() + "-input";
        //    string outputTopic = testName.getMethodName() + "-output";
        //    CLUSTER.CreateTopics(inputTopic, outputTopic);

        //    Topology topology = new Topology();
        //    topology.AddSource("source", Serdes.String().Deserializer, Serdes.String().Deserializer, inputTopic)
        //            .AddProcessor("process", () => new AbstractProcessor<string, string>()
        //            {
        //                //public void process(string key, string value)
        //                //{
        //                //    if (value.Length() % 2 == 0)
        //                //    {
        //                //        context.Forward(key, key + value);
        //                //    }
        //                //}
        //            }, "source")
        //        .AddSink("sink", outputTopic, Serdes.String(), Serdes.String(), "process");
        //    startStreamsAndCheckDirExists(topology, Collections.singleton(inputTopic), outputTopic, false);
        //}

        //[Fact]
        //public void inMemoryStatefulTopologyShouldNotCreateStateDirectory() //// throws Exception
        //{
        //    string inputTopic = testName.getMethodName() + "-input";
        //    string outputTopic = testName.getMethodName() + "-output";
        //    string globalTopicName = testName.getMethodName() + "-global";
        //    string storeName = testName.getMethodName() + "-counts";
        //    string globalStoreName = testName.getMethodName() + "-globalStore";
        //    Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, false);
        //    startStreamsAndCheckDirExists(topology, new List<(inputTopic, globalTopicName), outputTopic, false);
        //}

        //[Fact]
        //public void statefulTopologyShouldCreateStateDirectory() //// throws Exception
        //{
        //    string inputTopic = testName.getMethodName() + "-input";
        //    string outputTopic = testName.getMethodName() + "-output";
        //    string globalTopicName = testName.getMethodName() + "-global";
        //    string storeName = testName.getMethodName() + "-counts";
        //    string globalStoreName = testName.getMethodName() + "-globalStore";
        //    Topology topology = getStatefulTopology(inputTopic, outputTopic, globalTopicName, storeName, globalStoreName, true);
        //    startStreamsAndCheckDirExists(topology, new List<(inputTopic, globalTopicName), outputTopic, true);
        //}

        //private Topology getStatefulTopology(string inputTopic,
        //                                     string outputTopic,
        //                                     string globalTopicName,
        //                                     string storeName,
        //                                     string globalStoreName,
        //                                     bool isPersistentStore) //// throws Exception
        //{
        //    CLUSTER.CreateTopics(inputTopic, outputTopic, globalTopicName);
        //    IStoreBuilder<IKeyValueStore<string, long>> storeBuilder = Stores.KeyValueStoreBuilder(
        //            isPersistentStore ?
        //                Stores.PersistentKeyValueStore(storeName)
        //                : Stores.InMemoryKeyValueStore(storeName),
        //            Serdes.String(),
        //            Serdes.Long());
        //    Topology topology = new Topology();
        //    topology.AddSource("source", Serdes.String().Deserializer, Serdes.String().Deserializer, inputTopic)
        //                    .AddProcessor("process", () => new AbstractProcessor<string, string>()
        //                    {
        //                        //                                public void process(string key, string value)
        //                        //            {
        //                        //                IKeyValueStore<string, long> kvStore =
        //                        //                        (IKeyValueStore<string, long>)context.getStateStore(storeName);
        //                        //                kvStore.Put(key, 5L);
        //                        //
        //                        //                context.Forward(key, "5");
        //                        //                context.Commit();
        //                        //            }
        //                    }, "source")
        //        .AddStateStore(storeBuilder, "process")
        //        .AddSink("sink", outputTopic, Serdes.String(), Serdes.String(), "process");

        //    IStoreBuilder<IKeyValueStore<string, string>> globalStoreBuilder = Stores.KeyValueStoreBuilder(
        //            isPersistentStore ? Stores.PersistentKeyValueStore(globalStoreName) : Stores.inMemorySessionStore(globalStoreName),
        //            Serdes.String(), Serdes.String()).WithLoggingDisabled();
        //    topology.addGlobalStore(globalStoreBuilder,
        //            "global",
        //            Serdes.String().Deserializer,
        //            Serdes.String().Deserializer,
        //            globalTopicName,
        //            globalTopicName + "-processor",
        //            new MockProcessorSupplier());
        //    return topology;
        //}

        //private void startStreamsAndCheckDirExists(Topology topology,
        //                                           List<string> inputTopics,
        //                                           string outputTopic,
        //                                           bool shouldFilesExist) //// throws Exception
        //{
        //    File .AseDir = new FileInfo(TestUtils.IO_TMP_DIR + Path.DirectorySeparatorChar + "kafka-" + TestUtils.randomString(5));
        //    Path .AsePath = .AseDir.toPath();
        //    if (!.AseDir.Exists)
        //    {
        //        Files.createDirectory(.AsePath);
        //    }
        //    // changing the path of state directory to make sure that it should not c.Ash with other test Cases.
        //   StreamsConfig localProps = new StreamsConfig();
        //    localProps.PutAll(props);
        //    localProps.Put(StreamsConfig.STATE_DIR_CONFIG, .AseDir.FullName);

        //    var streams = new KafkaStreamsThread(topology, localProps);
        //    streams.Start();

        //    foreach (string topic in inputTopics)
        //    {
        //        IntegrationTestUtils.ProduceKeyValuesSynchronouslyWithTimestamp(topic,
        //                Collections.singletonList(KeyValuePair.Create("A", "A")),
        //                TestUtils.ProducerConfig(
        //                        CLUSTER.bootstrapServers(),
        //                        Serdes.String().Serializer,
        //                    Serdes.String().Serializer,
        //                    new StreamsConfig()),
        //            System.currentTimeMillis());
        //    }

        //    IntegrationTestUtils.readKeyValues(outputTopic,
        //            TestUtils.consumerConfig(
        //                    CLUSTER.bootstrapServers(),
        //                        outputTopic + "-group",
        //                        Serdes.String().Deserializer,
        //                        Serdes.String().Deserializer),
        //                5000, 1);

        //    try
        //    {
        //        List<Path> files = Files.find(.AsePath, 999, (p, bfa) => !p.Equals(.AsePath)).collect(Collectors.toList());
        //        if (shouldFilesExist && files.IsEmpty())
        //        {
        //            Assert.False(true, "Files should have existed, but it didn't: " + files);
        //        }
        //        if (!shouldFilesExist && !files.IsEmpty())
        //        {
        //            Assert.False(true, "Files should not have existed, but it did: " + files);
        //        }
        //    }
        //    catch (IOException e)
        //    {
        //        Assert.False(true, "Couldn't read the state directory : " + .AseDir.getPath());
        //    }
        //    finally
        //    {
        //        streams.Close();
        //        streams.cleanUp();
        //        Utils.Delete(.AseDir);
        //    }
        //}

        //private void verifyCleanupStateDir(string appDir,
        //                                   File oldTaskDir) //// throws InterruptedException

        //{
        //    File TaskDir = new FileInfo(appDir, "0_0");
        //    TestUtils.WaitForCondition(
        //        () => !oldTaskDir.Exists && TaskDir.Exists,
        //                "cleanup .As not successfully run");
        //    Assert.True(TaskDir.Exists);
        //}

        internal class KafkaStreamsTestsStateListenerStub : IStateListener
        {
            public int numChanges { get; private set; } = 0;

            private object? oldState;
            private object? newState;
            public ConcurrentDictionary<object, long> mapStates = new ConcurrentDictionary<object, long>();

            public void OnChange<States>(IThread<States> thread, States newState, States oldState)
                where States : Enum
            {
                if (!this.mapStates.TryGetValue(newState, out var prevCount))
                {

                }

                this.oldState = oldState;
                this.newState = newState;

                this.mapStates.AddOrUpdate(
                    key: newState,
                    addValue: 1,
                    updateValueFactory: (k, v) => v + 1);

                this.numChanges++;
            }

            public void SetThreadStates(Dictionary<long, StreamThreadState> threadStates)
            {
            }

            public void OnChange(IThread thread, object newState, object oldState)
            {
            }
        }
    }
}
