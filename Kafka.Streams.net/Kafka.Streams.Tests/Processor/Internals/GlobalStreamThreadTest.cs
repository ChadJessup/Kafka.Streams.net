using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Tests.Helpers;
using Kafka.Streams.Threads.GlobalStream;
using Kafka.Streams.Topologies;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class GlobalStreamThreadTest
    {
        private InternalTopologyBuilder builder = new InternalTopologyBuilder();
        private MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
        private MockTime time = new MockTime();
        private MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
        private GlobalStreamThread globalStreamThread;
        private StreamsConfig config;

        private static string GLOBAL_STORE_TOPIC_NAME = "foo";
        private static string GLOBAL_STORE_NAME = "bar";
        private TopicPartition topicPartition = new TopicPartition(GLOBAL_STORE_TOPIC_NAME, 0);



        public void before()
        {
            MaterializedInternal<object, object, KeyValueStore<Bytes, byte[]>> materialized =
                new MaterializedInternal<>(Materialized.with(null, null),
                    new InternalNameProvider()
                    {



                    public string newProcessorName(string prefix)
            {
                return "processorName";
            }


            public string newStoreName(string prefix)
            {
                return GLOBAL_STORE_NAME;
            }
        },
                "store-"
            );

        builder.addGlobalStore(


                new TimestampedKeyValueStoreMaterializer<>(materialized).materialize().withLoggingDisabled(),
            "sourceName",
            null,
            null,
            null,
            GLOBAL_STORE_TOPIC_NAME,
            "processorName",
            new KTableSource<>(GLOBAL_STORE_NAME, GLOBAL_STORE_NAME));

        HashDictionary<string, object> properties = new HashMap<>();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "blah");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "blah");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        config = new StreamsConfig(properties);
        globalStreamThread = new GlobalStreamThread(builder.rewriteTopology(config).buildGlobalStateTopology(),
                                                    config,
                                                    mockConsumer,
                                                        new StateDirectory(config, time, true),
                                                    0,
                                                    new Metrics(),
                                                    new MockTime(),
                                                    "clientId",
                                                     stateRestoreListener);
    }

    [Xunit.Fact]
    public void shouldThrowStreamsExceptionOnStartupIfThereIsAStreamsException()
    {
        // should throw as the MockConsumer hasn't been configured and there are no
        // partitions available
        try
        {
            globalStreamThread.start();
            Assert.True(false, "Should have thrown StreamsException if start up failed");
        }
        catch (StreamsException e)
        {
            // ok
        }
        Assert.False(globalStreamThread.stillRunning());
    }


    [Xunit.Fact]
    public void shouldThrowStreamsExceptionOnStartupIfExceptionOccurred()
    {
        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer(OffsetResetStrategy.EARLIEST)
        {


            public List<PartitionInfo> partitionsFor(string topic)
        {
            throw new RuntimeException("KABOOM!");
        }
    };
    globalStreamThread = new GlobalStreamThread(builder.buildGlobalStateTopology(),
                                                config,
                                                mockConsumer,
                                                    new StateDirectory(config, time, true),
                                                    0,
                                                    new Metrics(),
                                                    new MockTime(),
                                                    "clientId",
                                                    stateRestoreListener);

        try {
            globalStreamThread.start();
            Assert.True(false, "Should have thrown StreamsException if start up failed");
} catch (StreamsException e) {
            Assert.Equal(e.getCause(), instanceOf(RuntimeException));
Assert.Equal(e.getCause().getMessage(), ("KABOOM!"));
        }
        Assert.False(globalStreamThread.stillRunning());
    }

    [Xunit.Fact]
public void shouldBeRunningAfterSuccessfulStart()
{
    initializeConsumer();
    globalStreamThread.start();
    Assert.True(globalStreamThread.stillRunning());
}

[Xunit.Fact(Timeout = 30000)
    public void shouldStopRunningWhenClosedByUser()
{// throws Exception
    initializeConsumer();
    globalStreamThread.start();
    globalStreamThread.shutdown();
    globalStreamThread.join();
    Assert.Equal(GlobalStreamThread.State.DEAD, globalStreamThread.state());
}

[Xunit.Fact]
public void shouldCloseStateStoresOnClose()
{// throws Exception
    initializeConsumer();
    globalStreamThread.start();
    StateStore globalStore = builder.globalStateStores().get(GLOBAL_STORE_NAME);
    Assert.True(globalStore.isOpen());
    globalStreamThread.shutdown();
    globalStreamThread.join();
    Assert.False(globalStore.isOpen());
}


[Xunit.Fact]
public void shouldTransitionToDeadOnClose()
{// throws Exception
    initializeConsumer();
    globalStreamThread.start();
    globalStreamThread.shutdown();
    globalStreamThread.join();

    Assert.Equal(GlobalStreamThread.State.DEAD, globalStreamThread.state());
}


[Xunit.Fact]
public void shouldStayDeadAfterTwoCloses()
{// throws Exception
    initializeConsumer();
    globalStreamThread.start();
    globalStreamThread.shutdown();
    globalStreamThread.join();
    globalStreamThread.shutdown();

    Assert.Equal(GlobalStreamThread.State.DEAD, globalStreamThread.state());
}


[Xunit.Fact]
public void shouldTransitionToRunningOnStart()
{// throws Exception
    initializeConsumer();
    globalStreamThread.start();

    TestUtils.waitForCondition(
        () => globalStreamThread.state() == RUNNING,
        10 * 1000,
        "Thread never started.");

    globalStreamThread.shutdown();
}

[Xunit.Fact]
public void shouldDieOnInvalidOffsetException()
{// throws Exception
    initializeConsumer();
    globalStreamThread.start();

    TestUtils.waitForCondition(
        () => globalStreamThread.state() == RUNNING,
        10 * 1000,
        "Thread never started.");

    mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 1L));
    mockConsumer.addRecord(new ConsumeResult<>(GLOBAL_STORE_TOPIC_NAME, 0, 0L, "K1".getBytes(), "V1".getBytes()));

    TestUtils.waitForCondition(
        () => mockConsumer.position(topicPartition) == 1L,
        10 * 1000,
        "Input record never consumed");

    mockConsumer.setException(new InvalidOffsetException("Try Again!")
    {


            public HashSet<TopicPartition> partitions()
    {
        return Collections.singleton(topicPartition);
    }
});
        // feed first record for recovery
        mockConsumer.addRecord(new ConsumeResult<>(GLOBAL_STORE_TOPIC_NAME, 0, 0L, "K1".getBytes(), "V1".getBytes()));

        TestUtils.waitForCondition(
            () => globalStreamThread.state() == DEAD,
            10 * 1000,
            "GlobalStreamThread should have died.");
    }

    private void initializeConsumer()
{
    mockConsumer.updatePartitions(
        GLOBAL_STORE_TOPIC_NAME,
        Collections.singletonList(new PartitionInfo(
            GLOBAL_STORE_TOPIC_NAME,
            0,
            null,
            new Node[0],
            new Node[0])));
    mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
    mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 0L));
    mockConsumer.assign(Collections.singleton(topicPartition));
}
}
