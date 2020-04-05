//using Confluent.Kafka;
//using Kafka.Common;
//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Tests.Helpers;
//using Kafka.Streams.Threads.GlobalStream;
//using Kafka.Streams.Topologies;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class GlobalStreamThreadTest
//    {
//        private readonly InternalTopologyBuilder builder = new InternalTopologyBuilder();
//        private readonly MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.NONE);
//        private readonly MockTime time = new MockTime();
//        private readonly MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
//        private readonly GlobalStreamThread globalStreamThread;
//        private readonly StreamsConfig config;

//        private const string GLOBAL_STORE_TOPIC_NAME = "foo";
//        private const string GLOBAL_STORE_NAME = "bar";
//        private readonly TopicPartition topicPartition = new TopicPartition(GLOBAL_STORE_TOPIC_NAME, 0);



//        public void Before()
//        {
//            MaterializedInternal<object, object, IKeyValueStore<Bytes, byte[]>> materialized =
//                new MaterializedInternal<>(Materialized.with(null, null),
//                    new InternalNameProvider()
//                    {



//                    public string newProcessorName(string prefix)
//            {
//                return "processorName";
//            }


//            public string newStoreName(string prefix)
//            {
//                return GLOBAL_STORE_NAME;
//            }
//        },
//                "store-"
//            );

//        builder.addGlobalStore(


//                new TimestampedKeyValueStoreMaterializer<>(materialized).materialize().withLoggingDisabled(),
//            "sourceName",
//            null,
//            null,
//            null,
//            GLOBAL_STORE_TOPIC_NAME,
//            "processorName",
//            new KTableSource<>(GLOBAL_STORE_NAME, GLOBAL_STORE_NAME));

//        HashDictionary<string, object> properties = new HashMap<>();
//        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "blah");
//        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "blah");
//        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().FullName);
//        config = new StreamsConfig(properties);
//        globalStreamThread = new GlobalStreamThread(builder.rewriteTopology(config).buildGlobalStateTopology(),
//                                                    config,
//                                                    mockConsumer,
//                                                        new StateDirectory(config, time, true),
//                                                    0,
//                                                    new Metrics(),
//                                                    new MockTime(),
//                                                    "clientId",
//                                                     stateRestoreListener);
//    }

//    [Xunit.Fact]
//    public void ShouldThrowStreamsExceptionOnStartupIfThereIsAStreamsException()
//    {
//        // should throw as the MockConsumer hasn't been configured and there are no
//        // partitions available
//        try
//        {
//            globalStreamThread.start();
//            Assert.True(false, "Should have thrown StreamsException if start up failed");
//        }
//        catch (StreamsException e)
//        {
//            // ok
//        }
//        Assert.False(globalStreamThread.stillRunning());
//    }


//    [Xunit.Fact]
//    public void ShouldThrowStreamsExceptionOnStartupIfExceptionOccurred()
//    {
//        MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer(OffsetResetStrategy.EARLIEST)
//        {


//            public List<PartitionInfo> partitionsFor(string topic)
//        {
//            throw new RuntimeException("KABOOM!");
//        }
//    };
//    globalStreamThread = new GlobalStreamThread(builder.buildGlobalStateTopology(),
//                                                config,
//                                                mockConsumer,
//                                                    new StateDirectory(config, time, true),
//                                                    0,
//                                                    new Metrics(),
//                                                    new MockTime(),
//                                                    "clientId",
//                                                    stateRestoreListener);

//        try {
//            globalStreamThread.start();
//            Assert.True(false, "Should have thrown StreamsException if start up failed");
//} catch (StreamsException e) {
//            Assert.Equal(e.getCause(), instanceOf(RuntimeException));
//Assert.Equal(e.getCause().getMessage(), ("KABOOM!"));
//        }
//        Assert.False(globalStreamThread.stillRunning());
//    }

//    [Xunit.Fact]
//public void ShouldBeRunningAfterSuccessfulStart()
//{
//    initializeConsumer();
//    globalStreamThread.start();
//    Assert.True(globalStreamThread.stillRunning());
//}

//[Xunit.Fact(Timeout = 30000)
//    public void ShouldStopRunningWhenClosedByUser()
//{// throws Exception
//    initializeConsumer();
//    globalStreamThread.start();
//    globalStreamThread.shutdown();
//    globalStreamThread.join();
//    Assert.Equal(GlobalStreamThread.State.DEAD, globalStreamThread.state());
//}

//[Xunit.Fact]
//public void ShouldCloseStateStoresOnClose()
//{// throws Exception
//    initializeConsumer();
//    globalStreamThread.start();
//    IStateStore globalStore = builder.globalStateStores().Get(GLOBAL_STORE_NAME);
//    Assert.True(globalStore.isOpen());
//    globalStreamThread.shutdown();
//    globalStreamThread.join();
//    Assert.False(globalStore.isOpen());
//}


//[Xunit.Fact]
//public void ShouldTransitionToDeadOnClose()
//{// throws Exception
//    initializeConsumer();
//    globalStreamThread.start();
//    globalStreamThread.shutdown();
//    globalStreamThread.join();

//    Assert.Equal(GlobalStreamThread.State.DEAD, globalStreamThread.state());
//}


//[Xunit.Fact]
//public void ShouldStayDeadAfterTwoCloses()
//{// throws Exception
//    initializeConsumer();
//    globalStreamThread.start();
//    globalStreamThread.shutdown();
//    globalStreamThread.join();
//    globalStreamThread.shutdown();

//    Assert.Equal(GlobalStreamThread.State.DEAD, globalStreamThread.state());
//}


//[Xunit.Fact]
//public void ShouldTransitionToRunningOnStart()
//{// throws Exception
//    initializeConsumer();
//    globalStreamThread.start();

//    TestUtils.WaitForCondition(
//        () => globalStreamThread.state() == RUNNING,
//        10 * 1000,
//        "Thread never started.");

//    globalStreamThread.shutdown();
//}

//[Xunit.Fact]
//public void ShouldDieOnInvalidOffsetException()
//{// throws Exception
//    initializeConsumer();
//    globalStreamThread.start();

//    TestUtils.WaitForCondition(
//        () => globalStreamThread.state() == RUNNING,
//        10 * 1000,
//        "Thread never started.");

//    mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 1L));
//    mockConsumer.addRecord(new ConsumeResult<>(GLOBAL_STORE_TOPIC_NAME, 0, 0L, "K1".getBytes(), "V1".getBytes()));

//    TestUtils.WaitForCondition(
//        () => mockConsumer.position(topicPartition) == 1L,
//        10 * 1000,
//        "Input record never consumed");

//    mockConsumer.setException(new InvalidOffsetException("Try Again!")
//    {


//            public HashSet<TopicPartition> partitions()
//    {
//        return Collections.singleton(topicPartition);
//    }
//});
//        // feed first record for recovery
//        mockConsumer.addRecord(new ConsumeResult<>(GLOBAL_STORE_TOPIC_NAME, 0, 0L, "K1".getBytes(), "V1".getBytes()));

//        TestUtils.WaitForCondition(
//            () => globalStreamThread.state() == DEAD,
//            10 * 1000,
//            "GlobalStreamThread should have died.");
//    }

//    private void InitializeConsumer()
//{
//    mockConsumer.updatePartitions(
//        GLOBAL_STORE_TOPIC_NAME,
//        Collections.singletonList(new PartitionInfo(
//            GLOBAL_STORE_TOPIC_NAME,
//            0,
//            null,
//            System.Array.Empty<Node>(),
//            System.Array.Empty<Node>())));
//    mockConsumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
//    mockConsumer.updateEndOffsets(Collections.singletonMap(topicPartition, 0L));
//    mockConsumer.assign(Collections.singleton(topicPartition));
//}
//}
