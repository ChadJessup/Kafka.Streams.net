using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValue;
using Kafka.Streams.Threads;
using Kafka.Streams.Threads.GlobalStream;
using Kafka.Streams.Threads.KafkaStream;
using Kafka.Streams.Threads.KafkaStreams;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Xunit;

namespace Kafka.Streams.Tests
{
    public class KafkaStreamsTest
    {
        private static readonly int NUM_BROKERS = 1;
        private static readonly int NUM_THREADS = 2;

        private IKafkaStreamsThread globalStreams;
        private StreamsConfig props;

        public KafkaStreamsTest()
        {
            props = new StreamsConfig();
            props.Set(StreamsConfigPropertyNames.ApplicationId, "appId");
            props.Set(StreamsConfigPropertyNames.ClientId, "clientId");
            props.Set("test.mock.num.brokers", NUM_BROKERS.ToString());
            props.Set(StreamsConfigPropertyNames.BootstrapServers, "localhost:9092");
            props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory());
            props.Set(StreamsConfigPropertyNames.NumberOfStreamThreads, NUM_THREADS.ToString());

            globalStreams = TestUtils.GetStreamsBuilder(props).BuildKafkaStreams();
        }

        public void cleanup()
        {
            if (globalStreams != null)
            {
                globalStreams.Close();
            }
        }

        [Fact]
        public void TestOsDefaultSocketBufferSizes()
        {
            props.GetGlobalConsumerConfigs("").SocketReceiveBufferBytes = 0;
            props.GetGlobalConsumerConfigs("").SocketReceiveBufferBytes = 0;
            var streams = TestUtils.GetStreamsBuilder(props).BuildKafkaStreams();

            streams.Close();
        }

        [Fact] //(expected = KafkaException.class)
        public void TestInvalidSocketSendBufferSize()
        {
            props.GetGlobalConsumerConfigs("").SocketSendBufferBytes = -2;
            var streams = TestUtils.GetStreamsBuilder(props).BuildKafkaStreams();
            streams.Close();
        }

        [Fact] //(expected = KafkaException.class)
        public void testInvalidSocketReceiveBufferSize()
        {
            props.GetGlobalConsumerConfigs("").SocketReceiveBufferBytes = -2;
            var streams = TestUtils.GetStreamsBuilder(props).BuildKafkaStreams();

            streams.Close();
        }

        [Fact]
        public void StateShouldTransitToNotRunningIfCloseRightAfterCreated()
        {
            globalStreams.Close();

            Assert.Equal(KafkaStreamsThreadStates.NOT_RUNNING, globalStreams.State.CurrentState);
        }

        [Fact]
        public void StateShouldTransitToRunningIfNonDeadThreadsBackToRunning()// throws InterruptedException
        {
            var stateListener = new KafkaStreamsTestsStateListenerStub();
            globalStreams.SetStateListener(stateListener);

            Assert.Equal(0, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.CREATED, globalStreams.State.CurrentState);

            globalStreams.Start();

            TestUtils.waitForCondition(
                () => stateListener.numChanges == 2,
                timeout: TimeSpan.FromMinutes(5.0),
                "Streams never started.");

            Assert.Equal(KafkaStreamsThreadStates.RUNNING, globalStreams.State.CurrentState);

            foreach (KafkaStreamThread thread in globalStreams.Threads)
            {
                thread.StateListener.onChange(
                    thread,
                    KafkaStreamThreadStates.PARTITIONS_REVOKED,
                    KafkaStreamThreadStates.RUNNING);
            }

            Assert.Equal(3, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.REBALANCING, globalStreams.State.CurrentState);

            foreach (var thread in globalStreams.Threads)
            {
                thread.StateListener.onChange(
                    thread,
                    KafkaStreamThreadStates.PARTITIONS_ASSIGNED,
                    KafkaStreamThreadStates.PARTITIONS_REVOKED);
            }

            Assert.Equal(3, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.REBALANCING, globalStreams.State.CurrentState);

            globalStreams.Threads[NUM_THREADS - 1].StateListener.onChange(
                globalStreams.Threads[NUM_THREADS - 1],
                KafkaStreamThreadStates.PENDING_SHUTDOWN,
                KafkaStreamThreadStates.PARTITIONS_ASSIGNED);

            globalStreams.Threads[NUM_THREADS - 1].StateListener.onChange(
                globalStreams.Threads[NUM_THREADS - 1],
                KafkaStreamThreadStates.DEAD,
                KafkaStreamThreadStates.PENDING_SHUTDOWN);

            Assert.Equal(3, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.REBALANCING, globalStreams.State.CurrentState);

            foreach (var thread in globalStreams.Threads)
            {
                if (thread != globalStreams.Threads[NUM_THREADS - 1])
                {
                    thread.StateListener.onChange(
                        thread,
                        KafkaStreamThreadStates.RUNNING,
                        KafkaStreamThreadStates.PARTITIONS_ASSIGNED);
                }
            }

            Assert.Equal(4, stateListener.numChanges);
            Assert.Equal(KafkaStreamsThreadStates.RUNNING, globalStreams.State.CurrentState);

            globalStreams.Close();

            TestUtils.waitForCondition(
                () => stateListener.numChanges == 6,
                "Streams never closed.");

            Assert.Equal(KafkaStreamsThreadStates.NOT_RUNNING, globalStreams.State.CurrentState);
        }

        //[Fact]
        //public void stateShouldTransitToErrorIfAllThreadsDead() // throws InterruptedException
        //{
        //    StateListenerStub stateListener = new StateListenerStub();
        //    globalStreams.setStateListener(stateListener);

        //    Assert.Equal(0, stateListener.numChanges);
        //    Assert.Equal(KafkaStreamsThreadStates.CREATED, globalStreams.State.CurrentState);

        //    globalStreams.start();

        //    TestUtils.waitForCondition(
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

        //    globalStreams.close();

        //    // the state should not stuck with ERROR, but transit to NOT_RUNNING in the end
        //    TestUtils.waitForCondition(
        //        () => stateListener.numChanges == 6,
        //        "Streams never closed.");
        //    Assert.Equal(KafkaStreamsThreadStates.NOT_RUNNING, globalStreams.State.CurrentState);
        //}

        //[Fact]
        //public void shouldCleanupResourcesOnCloseWithoutPreviousStart() // throws Exception
        //{
        //    builder.globalTable("anyTopic");
        //    List<Node> nodes = Collections.singletonList(new Node(0, "localhost", 8121));
        //    Cluster cluster = new Cluster("mockClusterId", nodes,
        //                                        Collections.emptySet(), Collections.emptySet(),
        //                                        Collections.emptySet(), nodes.get(0));
        //    MockClientSupplier clientSupplier = new MockClientSupplier();
        //    clientSupplier.setClusterForAdminClient(cluster);
        //    var streams = new KafkaStreams(builder.build(), props, clientSupplier);
        //    streams.close();
        //    TestUtils.waitForCondition(
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
        //public void testStateThreadClose() // throws Exception
        //{
        //    // make sure we have the global state thread running too
        //    builder.globalTable("anyTopic");
        //    var streams = new KafkaStreams(builder.build(), props);

        //    try
        //    {
        //        java.lang.reflect.Field threadsField = streams.getClass().getDeclaredField("threads");
        //        threadsField.setAccessible(true);
        //        StreamThread[] threads = (StreamThread[])threadsField.get(streams);

        //        Assert.Equal(NUM_THREADS, threads.length);
        //        Assert.Equal(streams.State.CurrentState, KafkaStreamsThreadStates.CREATED);

        //        streams.start();
        //        TestUtils.waitForCondition(
        //            () => streams.State.CurrentState == KafkaStreamsThreadStates.RUNNING,
        //            "Streams never started.");

        //        for (int i = 0; i < NUM_THREADS; i++)
        //        {
        //            StreamThread tmpThread = threads[i];
        //            tmpThread.shutdown();
        //            TestUtils.waitForCondition(
        //                () => tmpThread.State.CurrentState == StreamThread.State.DEAD,
        //                "Thread never stopped.");
        //            threads[i].join();
        //        }
        //        TestUtils.waitForCondition(
        //            () => streams.State.CurrentState == KafkaStreamsThreadStates.ERROR,
        //            "Streams never stopped.");
        //    }
        //    finally
        //    {
        //        streams.close();
        //    }

        //    TestUtils.waitForCondition(
        //        () => streams.State.CurrentState == KafkaStreamsThreadStates.NOT_RUNNING,
        //        "Streams never stopped.");

        //    java.lang.reflect.Field globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
        //    globalThreadField.setAccessible(true);
        //    GlobalStreamThread globalStreamThread = (GlobalStreamThread)globalThreadField.get(streams);
        //    assertNull(globalStreamThread);
        //}

        //[Fact]
        //public void testStateGlobalThreadClose() // throws Exception
        //{
        //    // make sure we have the global state thread running too
        //    builder.globalTable("anyTopic");
        //    var streams = new KafkaStreams(builder.build(), props);

        //    try
        //    {
        //        streams.start();
        //        TestUtils.waitForCondition(
        //            () => streams.State.CurrentState == KafkaStreamsThreadStates.RUNNING,
        //            errorMessage: "Streams never started.");
        //        var globalThreadField = streams.getClass().getDeclaredField("globalStreamThread");
        //        globalThreadField.setAccessible(true);
        //        GlobalStreamThread globalStreamThread = (GlobalStreamThread)globalThreadField.get(streams);
        //        globalStreamThread.Shutdown();
        //        TestUtils.waitForCondition(
        //            () => globalStreamThread.State.CurrentState == GlobalStreamThreadStates.DEAD,
        //            errorMessage: "Thread never stopped.");
        //        globalStreamThread.join();
        //        Assert.Equal(streams.State.CurrentState, KafkaStreamsThreadStates.ERROR);
        //    }
        //    finally
        //    {
        //        streams.close();
        //    }

        //    Assert.Equal(streams.State.CurrentState, KafkaStreamsThreadStates.NOT_RUNNING);
        //}

        //[Fact]
        //public void globalThreadShouldTimeoutWhenBrokerConnectionCannotBeEstablished()
        //{
        //    Properties props = new StreamsConfig();
        //    props.Set(StreamsConfigPropertyNames.APPLICATION_ID_CONFIG, "appId");
        //    props.Set(StreamsConfigPropertyNames.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        //    props.Set(StreamsConfigPropertyNames.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.getName());
        //    props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        //    props.Set(StreamsConfigPropertyNames.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        //    props.Set(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 200);

        //    // make sure we have the global state thread running too
        //    builder.globalTable("anyTopic");
        //    try
        //    {
        //        var streams = new KafkaStreams(builder.build(), props);
        //        streams.start();
        //        fail("expected start() to time out and throw an exception.");
        //    }
        //    catch (StreamsException expected)
        //    {
        //        // This is a result of not being able to connect to the broker.
        //    }
        //    // There's nothing to assert... We're testing that this operation actually completes.
        //}

        //[Fact]
        //public void testLocalThreadCloseWithoutConnectingToBroker()
        //{
        //    Properties props = new StreamsConfig();
        //    props.Set(StreamsConfigPropertyNames.APPLICATION_ID_CONFIG, "appId");
        //    props.Set(StreamsConfigPropertyNames.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        //    props.Set(StreamsConfigPropertyNames.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.getName());
        //    props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        //    props.Set(StreamsConfigPropertyNames.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);

        //    // make sure we have the global state thread running too
        //    builder.table("anyTopic");
        //    try
        //    {
        //        var streams = new KafkaStreams(builder.build(), props);
        //        streams.start();
        //    }
        //    catch { }
        //    // There's nothing to assert... We're testing that this operation actually completes.
        //}

        ////[Fact]
        ////public void testInitializesAndDestroysMetricsReporters()
        ////{
        ////int oldInitCount = MockMetricsReporter.INIT_COUNT.get();

        ////try (var streams = new KafkaStreams(builder.build(), props)) {
        ////int newInitCount = MockMetricsReporter.INIT_COUNT.get();
        ////int initDiff = newInitCount - oldInitCount;
        ////Assert.True("some reporters should be initialized by calling on construction", initDiff > 0);

        ////streams.start();
        ////int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
        ////streams.close();
        ////Assert.Equal(oldCloseCount + initDiff, MockMetricsReporter.CLOSE_COUNT.get());
        ////}
        ////}

        //[Fact]
        //public void testCloseIsIdempotent()
        //{
        //    globalStreams.close();
        //    int closeCount = MockMetricsReporter.CLOSE_COUNT.get();

        //    globalStreams.close();
        //    Assert.Equal("subsequent close() calls should do nothing",
        //        closeCount, MockMetricsReporter.CLOSE_COUNT.get());
        //}

        //[Fact]
        //public void testCannotStartOnceClosed()
        //{
        //    globalStreams.start();
        //    globalStreams.close();
        //    try
        //    {
        //        globalStreams.start();
        //        fail("Should have throw IllegalStateException");
        //    }
        //    catch (IllegalStateException expected)
        //    {
        //        // this is ok
        //    }
        //    finally
        //    {
        //        globalStreams.close();
        //    }
        //}

        //[Fact]
        //public void testCannotStartTwice()
        //{
        //    globalStreams.start();

        //    try
        //    {
        //        globalStreams.start();
        //        fail("Should throw an IllegalStateException");
        //    }
        //    catch (IllegalStateException e)
        //    {
        //        // this is ok
        //    }
        //    finally
        //    {
        //        globalStreams.close();
        //    }
        //}

        //[Fact]
        //public void shouldNotSetGlobalRestoreListenerAfterStarting()
        //{
        //    globalStreams.start();
        //    try
        //    {
        //        globalStreams.setGlobalStateRestoreListener(new MockStateRestoreListener());
        //        fail("Should throw an IllegalStateException");
        //    }
        //    catch (IllegalStateException e)
        //    {
        //        // expected
        //    }
        //    finally
        //    {
        //        globalStreams.close();
        //    }
        //}

        //[Fact]
        //public void shouldThrowExceptionSettingUncaughtExceptionHandlerNotInCreateState()
        //{
        //    globalStreams.start();
        //    try
        //    {
        //        globalStreams.setUncaughtExceptionHandler(null);
        //        fail("Should throw IllegalStateException");
        //    }
        //    catch (IllegalStateException e)
        //    {
        //        // expected
        //    }
        //}

        //[Fact]
        //public void shouldThrowExceptionSettingStateListenerNotInCreateState()
        //{
        //    globalStreams.start();
        //    try
        //    {
        //        globalStreams.setStateListener(null);
        //        fail("Should throw IllegalStateException");
        //    }
        //    catch (IllegalStateException e)
        //    {
        //        // expected
        //    }
        //}

        //[Fact]
        //public void testIllegalMetricsConfig()
        //{
        //    props.Set(StreamsConfigPropertyNames.METRICS_RECORDING_LEVEL_CONFIG, "illegalConfig");

        //    try
        //    {
        //        new KafkaStreams(builder.build(), props);
        //        fail("Should have throw ConfigException");
        //    }
        //    catch (ConfigException expected) { /* expected */ }
        //}

        //[Fact]
        //public void testLegalMetricsConfig()
        //{
        //    //props.Set(StreamsConfigPropertyNames.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.INFO.toString());
        //    //new KafkaStreams(builder.build(), props).close();

        //    //props.Set(StreamsConfigPropertyNames.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.toString());
        //    //new KafkaStreams(builder.build(), props).close();
        //}

        //[Fact] //(expected = IllegalStateException.class)
        //public void shouldNotGetAllTasksWhenNotRunning()
        //{
        //    globalStreams.allMetadata();
        //}

        //[Fact] //(expected = IllegalStateException.class)
        //public void shouldNotGetAllTasksWithStoreWhenNotRunning()
        //{
        //    globalStreams.allMetadataForStore("store");
        //}

        //[Fact]// (expected = IllegalStateException.class)
        //public void shouldNotGetTaskWithKeyAndSerializerWhenNotRunning()
        //{
        //    globalStreams.metadataForKey("store", "key", Serdes.String().Serializer);
        //}

        //[Fact]// (expected = IllegalStateException.class)
        //public void shouldNotGetTaskWithKeyAndPartitionerWhenNotRunning()
        //{
        //    globalStreams.metadataForKey("store", "key", (topic, key, value, numPartitions) => 0);
        //}

        //[Fact]
        //public void shouldReturnFalseOnCloseWhenThreadsHaventTerminated() // throws Exception
        //{
        //    bool keepRunning = true;
        //    var streams = null;
        //    try
        //    {
        //        StreamsBuilder builder = new StreamsBuilder();
        //        CountDownLatch latch = new CountDownLatch(1);
        //        string topic = "input";
        //        CLUSTER.createTopics(topic);

        //        builder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()))
        //                .Foreach((key, value) =>
        //               {
        //                   try
        //                   {
        //                       latch.countDown();
        //                       while (keepRunning.get())
        //                       {
        //                           Thread.sleep(10);
        //                       }
        //                   }
        //                   catch (InterruptedException e)
        //                   {
        //                       // no-op
        //                   }
        //               });
        //        streams = new KafkaStreams(builder.build(), props);
        //        streams.start();
        //        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic,
        //            Collections.singletonList(new KeyValue<>("A", "A")),
        //            TestUtils.producerConfig(
        //                CLUSTER.bootstrapServers(),
        //                StringSerializer,
        //            StringSerializer,
        //            new StreamsConfig()),
        //        System.currentTimeMillis());

        //        Assert.True("Timed out waiting to receive single message", latch.wait(30, TimeUnit.SECONDS));
        //        Assert.False(streams.close(Duration.ofMillis(10)));
        //    }
        //    finally
        //    {
        //        // stop the thread so we don't interfere with other tests etc
        //        keepRunning = false;
        //        if (streams != null)
        //        {
        //            streams.close();
        //        }
        //    }
        //}

        //[Fact]
        //public void shouldReturnThreadMetadata()
        //{
        //    globalStreams.Start();
        //    Set<ThreadMetadata> threadMetadata = globalStreams.localThreadsMetadata();
        //    Assert.NotNull(threadMetadata);
        //    Assert.Equal(2, threadMetadata.size());
        //    foreach (var metadata in threadMetadata)
        //    {
        //        Assert.True("#threadState() was: " + metadata.threadState() + "; expected either RUNNING, STARTING, PARTITIONS_REVOKED, PARTITIONS_ASSIGNED, or CREATED",
        //            new List<string> { "RUNNING", "STARTING", "PARTITIONS_REVOKED", "PARTITIONS_ASSIGNED", "CREATED" }.Contains(metadata.threadState()));
        //        Assert.Equal(0, metadata.standbyTasks().size());
        //        Assert.Equal(0, metadata.activeTasks().size());
        //        string threadName = metadata.threadName();
        //        Assert.True(threadName.StartsWith("clientId-StreamThread-"));
        //        Assert.Equal(threadName + "-consumer", metadata.consumerClientId());
        //        Assert.Equal(threadName + "-restore-consumer", metadata.restoreConsumerClientId());
        //        Assert.Equal(Collections.singleton(threadName + "-producer"), metadata.producerClientIds());
        //        Assert.Equal("clientId-admin", metadata.adminClientId());
        //    }
        //}

        //[Fact]
        //public void shouldAllowCleanupBeforeStartAndAfterClose()
        //{
        //    try
        //    {
        //        globalStreams.cleanUp();
        //        globalStreams.start();
        //    }
        //    finally
        //    {
        //        globalStreams.close();
        //    }
        //    globalStreams.cleanUp();
        //}

        //[Fact]
        //public void shouldThrowOnCleanupWhileRunning() // throws InterruptedException
        //{
        //    globalStreams.start();
        //    TestUtils.waitForCondition(
        //            () => globalStreams.State.CurrentState == KafkaStreamsThreadStates.RUNNING,
        //            "Streams never started.");

        //    try
        //    {
        //        globalStreams.cleanUp();
        //        fail("Should have thrown IllegalStateException");
        //    }
        //    catch (IllegalStateException expected)
        //    {
        //        Assert.Equal("Cannot clean up while running.", expected.getMessage());
        //    }
        //}

        //[Fact]
        //public void shouldCleanupOldStateDirs() // throws InterruptedException
        //{
        //    props.Set(StreamsConfigPropertyNames.StateCleanupDelayMs, "1");

        //    string topic = "topic";
        //    CLUSTER.createTopic(topic);
        //    StreamsBuilder builder = new StreamsBuilder();

        //    builder.table(topic, Materialized.As("store"));

        //    try
        //    {
        //        var streams = new KafkaStreams(builder.build(), props);
        //        CountDownLatch latch = new CountDownLatch(1);
        //        streams.setStateListener((newState, oldState) =>
        //        {
        //            if (newState == KafkaStreamsThreadStates.RUNNING && oldState == KafkaStreamsThreadStates.REBALANCING)
        //            {
        //                latch.countDown();
        //            }
        //        });
        //        string appDir = props.Get(StreamsConfigPropertyNames.STATE_DIR_CONFIG) + File.separator + props.getProperty(StreamsConfigPropertyNames.ApplicationId);
        //        File oldTaskDir = new File(appDir, "10_1");
        //        Assert.True(oldTaskDir.mkdirs());

        //        streams.start();
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
        //        var streams = new KafkaStreams(builder.build(), props);
        //        streams.close(Duration.ofMillis(-1L));
        //        fail("should not accept negative close parameter");
        //    }
        //    catch (IllegalArgumentException e)
        //    {
        //        // expected
        //    }
        //}

        //[Fact]
        //public void shouldNotBlockInCloseForZeroDuration() // throws InterruptedException {
        //{
        //    var streams = new KafkaStreams(builder.build(), props);
        //    IThread th = new Thread(() => streams.close(Duration.ofMillis(0L)));

        //    th.start();

        //    try
        //    {
        //        th.join(30_000L);
        //        Assert.False(th.isAlive());
        //    }
        //    finally
        //    {
        //        streams.close();
        //    }
        //}

        //[Fact]
        //public void statelessTopologyShouldNotCreateStateDirectory() // throws Exception {
        //{
        //    string inputTopic = testName.getMethodName() + "-input";
        //    string outputTopic = testName.getMethodName() + "-output";
        //    CLUSTER.createTopics(inputTopic, outputTopic);

        //    Topology topology = new Topology();
        //    topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
        //            .addProcessor("process", () => new AbstractProcessor<string, string>()
        //            {
        //                //public void process(string key, string value)
        //                //{
        //                //    if (value.length() % 2 == 0)
        //                //    {
        //                //        context().forward(key, key + value);
        //                //    }
        //                //}
        //            }, "source")
        //        .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");
        //    startStreamsAndCheckDirExists(topology, Collections.singleton(inputTopic), outputTopic, false);
        //}

        //[Fact]
        //public void inMemoryStatefulTopologyShouldNotCreateStateDirectory() // throws Exception
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
        //public void statefulTopologyShouldCreateStateDirectory() // throws Exception
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
        //                                     bool isPersistentStore) // throws Exception
        //{
        //    CLUSTER.createTopics(inputTopic, outputTopic, globalTopicName);
        //    IStoreBuilder<IKeyValueStore<string, long>> storeBuilder = Stores.keyValueStoreBuilder(
        //            isPersistentStore ?
        //                Stores.persistentKeyValueStore(storeName)
        //                : Stores.inMemoryKeyValueStore(storeName),
        //            Serdes.String(),
        //            Serdes.long());
        //    Topology topology = new Topology();
        //    topology.addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), inputTopic)
        //                    .addProcessor("process", () => new AbstractProcessor<string, string>()
        //                    {
        //                        //                                public void process(string key, string value)
        //                        //            {
        //                        //                KeyValueStore<string, long> kvStore =
        //                        //                        (KeyValueStore<string, long>)context().getStateStore(storeName);
        //                        //                kvStore.put(key, 5L);
        //                        //
        //                        //                context().forward(key, "5");
        //                        //                context().commit();
        //                        //            }
        //                    }, "source")
        //        .addStateStore(storeBuilder, "process")
        //        .addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "process");

        //    IStoreBuilder<IKeyValueStore<string, string>> globalStoreBuilder = Stores.keyValueStoreBuilder(
        //            isPersistentStore ? Stores.persistentKeyValueStore(globalStoreName) : Stores.inMemorySessionStore(globalStoreName),
        //            Serdes.String(), Serdes.String()).withLoggingDisabled();
        //    topology.addGlobalStore(globalStoreBuilder,
        //            "global",
        //            Serdes.String().deserializer(),
        //            Serdes.String().deserializer(),
        //            globalTopicName,
        //            globalTopicName + "-processor",
        //            new MockProcessorSupplier());
        //    return topology;
        //}

        //private void startStreamsAndCheckDirExists(Topology topology,
        //                                           List<string> inputTopics,
        //                                           string outputTopic,
        //                                           bool shouldFilesExist) // throws Exception
        //{
        //    File baseDir = new File(TestUtils.IO_TMP_DIR + File.separator + "kafka-" + TestUtils.randomString(5));
        //    Path basePath = baseDir.toPath();
        //    if (!baseDir.exists())
        //    {
        //        Files.createDirectory(basePath);
        //    }
        //    // changing the path of state directory to make sure that it should not clash with other test cases.
        //    Properties localProps = new StreamsConfig();
        //    localProps.putAll(props);
        //    localProps.put(StreamsConfigPropertyNames.STATE_DIR_CONFIG, baseDir.getAbsolutePath());

        //    var streams = new KafkaStreams(topology, localProps);
        //    streams.start();

        //    foreach (string topic in inputTopics)
        //    {
        //        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(topic,
        //                Collections.singletonList(new KeyValue<>("A", "A")),
        //                TestUtils.producerConfig(
        //                        CLUSTER.bootstrapServers(),
        //                        StringSerializer,
        //                    StringSerializer,
        //                    new StreamsConfig()),
        //            System.currentTimeMillis());
        //    }

        //    IntegrationTestUtils.readKeyValues(outputTopic,
        //            TestUtils.consumerConfig(
        //                    CLUSTER.bootstrapServers(),
        //                        outputTopic + "-group",
        //                        StringDeserializer,
        //                        StringDeserializer),
        //                5000, 1);

        //    try
        //    {
        //        List<Path> files = Files.find(basePath, 999, (p, bfa) => !p.equals(basePath)).collect(Collectors.toList());
        //        if (shouldFilesExist && files.isEmpty())
        //        {
        //            Assert.fail("Files should have existed, but it didn't: " + files);
        //        }
        //        if (!shouldFilesExist && !files.isEmpty())
        //        {
        //            Assert.fail("Files should not have existed, but it did: " + files);
        //        }
        //    }
        //    catch (IOException e)
        //    {
        //        Assert.fail("Couldn't read the state directory : " + baseDir.getPath());
        //    }
        //    finally
        //    {
        //        streams.close();
        //        streams.cleanUp();
        //        Utils.delete(baseDir);
        //    }
        //}

        //private void verifyCleanupStateDir(string appDir,
        //                                   File oldTaskDir) // throws InterruptedException
        //{
        //    File taskDir = new File(appDir, "0_0");
        //    TestUtils.waitForCondition(
        //        () => !oldTaskDir.exists() && taskDir.exists(),
        //                "cleanup has not successfully run");
        //    Assert.True(taskDir.exists());
        //}

        public class KafkaStreamsTestsStateListenerStub : IStateListener
        {
            public int numChanges { get; private set; } = 0;
            object? oldState;
            object? newState;
            public ConcurrentDictionary<object, long> mapStates = new ConcurrentDictionary<object, long>();

            public void onChange<States>(IThread<States> thread, States newState, States oldState)
                where States : Enum
            {

                if (!mapStates.TryGetValue(newState, out var prevCount))
                {

                }

                this.oldState = oldState;
                this.newState = newState;
                
                mapStates.AddOrUpdate(
                    key: newState,
                    addValue: 1,
                    updateValueFactory: (k, v) => v + 1);

                numChanges++;
            }

            public void SetThreadStates(Dictionary<long, KafkaStreamThreadState> threadStates)
            {
            }
        }
    }
}