/*






 *

 *





 */





















































public class StreamThreadStateStoreProviderTest {

    private StreamTask taskOne;
    private StreamThreadStateStoreProvider provider;
    private StateDirectory stateDirectory;
    private File stateDir;
    private string topicName = "topic";
    private StreamThread threadMock;
    private Dictionary<TaskId, StreamTask> tasks;

    
    public void before() {
        TopologyWrapper topology = new TopologyWrapper();
        topology.addSource("the-source", topicName);
        topology.addProcessor("the-processor", new MockProcessorSupplier(), "the-source");
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("kv-store"),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("timestamped-kv-store"),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.windowStoreBuilder(
                Stores.inMemoryWindowStore(
                    "window-store",
                    Duration.ofMillis(10L),
                    Duration.ofMillis(2L),
                    false),
                Serdes.String(),
                Serdes.String()),
            "the-processor");
        topology.addStateStore(
            Stores.timestampedWindowStoreBuilder(
                Stores.inMemoryWindowStore(
                    "timestamped-window-store",
                    Duration.ofMillis(10L),
                    Duration.ofMillis(2L),
                    false),
                Serdes.String(),
                Serdes.String()),
            "the-processor");

        Properties properties = new Properties();
        string applicationId = "applicationId";
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        stateDir = TestUtils.tempDirectory();
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());

        StreamsConfig streamsConfig = new StreamsConfig(properties);
        MockClientSupplier clientSupplier = new MockClientSupplier();
        configureRestoreConsumer(clientSupplier, "applicationId-kv-store-changelog");
        configureRestoreConsumer(clientSupplier, "applicationId-window-store-changelog");

        ProcessorTopology processorTopology = topology.getInternalBuilder(applicationId).build();

        tasks = new HashMap<>();
        stateDirectory = new StateDirectory(streamsConfig, new MockTime(), true);

        taskOne = createStreamsTask(
            streamsConfig,
            clientSupplier,
            processorTopology,
            new TaskId(0, 0));
        taskOne.initializeStateStores();
        tasks.put(new TaskId(0, 0), taskOne);

        StreamTask taskTwo = createStreamsTask(
            streamsConfig,
            clientSupplier,
            processorTopology,
            new TaskId(0, 1));
        taskTwo.initializeStateStores();
        tasks.put(new TaskId(0, 1), taskTwo);

        threadMock = EasyMock.createNiceMock(StreamThread);
        provider = new StreamThreadStateStoreProvider(threadMock);

    }

    
    public void cleanUp(){ //throws IOException
        Utils.delete(stateDir);
    }

    [Xunit.Fact]
    public void shouldFindKeyValueStores() {
        mockThread(true);
        List<ReadOnlyKeyValueStore<string, string>> kvStores =
            provider.stores("kv-store", QueryableStoreTypes.keyValueStore());
        Assert.Equal(2, kvStores.Count);
        foreach (ReadOnlyKeyValueStore<string, string> store in kvStores) {
            Assert.Equal(store, instanceOf(ReadOnlyKeyValueStore));
            Assert.Equal(store, not(instanceOf(TimestampedKeyValueStore)));
        }
    }

    [Xunit.Fact]
    public void shouldFindTimestampedKeyValueStores() {
        mockThread(true);
        List<ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> tkvStores =
            provider.stores("timestamped-kv-store", QueryableStoreTypes.timestampedKeyValueStore());
        Assert.Equal(2, tkvStores.Count);
        foreach (ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>> store in tkvStores) {
            Assert.Equal(store, instanceOf(ReadOnlyKeyValueStore));
            Assert.Equal(store, instanceOf(TimestampedKeyValueStore));
        }
    }

    [Xunit.Fact]
    public void shouldNotFindKeyValueStoresAsTimestampedStore() {
        mockThread(true);
        List<ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> tkvStores =
            provider.stores("kv-store", QueryableStoreTypes.timestampedKeyValueStore());
        Assert.Equal(0, tkvStores.Count);
    }

    [Xunit.Fact]
    public void shouldFindTimestampedKeyValueStoresAsKeyValueStores() {
        mockThread(true);
        List<ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>>> tkvStores =
            provider.stores("timestamped-kv-store", QueryableStoreTypes.keyValueStore());
        Assert.Equal(2, tkvStores.Count);
        foreach (ReadOnlyKeyValueStore<string, ValueAndTimestamp<string>> store in tkvStores) {
            Assert.Equal(store, instanceOf(ReadOnlyKeyValueStore));
            Assert.Equal(store, not(instanceOf(TimestampedKeyValueStore)));
        }
    }

    [Xunit.Fact]
    public void shouldFindWindowStores() {
        mockThread(true);
        List<ReadOnlyWindowStore<string, string>> windowStores =
            provider.stores("window-store", windowStore());
        Assert.Equal(2, windowStores.Count);
        foreach (ReadOnlyWindowStore<string, string> store in windowStores) {
            Assert.Equal(store, instanceOf(ReadOnlyWindowStore));
            Assert.Equal(store, not(instanceOf(TimestampedWindowStore)));
        }
    }

    [Xunit.Fact]
    public void shouldFindTimestampedWindowStores() {
        mockThread(true);
        List<ReadOnlyWindowStore<string, ValueAndTimestamp<string>>> windowStores =
            provider.stores("timestamped-window-store", timestampedWindowStore());
        Assert.Equal(2, windowStores.Count);
        foreach (ReadOnlyWindowStore<string, ValueAndTimestamp<string>> store in windowStores) {
            Assert.Equal(store, instanceOf(ReadOnlyWindowStore));
            Assert.Equal(store, instanceOf(TimestampedWindowStore));
        }
    }

    [Xunit.Fact]
    public void shouldNotFindWindowStoresAsTimestampedStore() {
        mockThread(true);
        List<ReadOnlyWindowStore<string, ValueAndTimestamp<string>>> windowStores =
            provider.stores("window-store", timestampedWindowStore());
        Assert.Equal(0, windowStores.Count);
    }

    [Xunit.Fact]
    public void shouldFindTimestampedWindowStoresAsWindowStore() {
        mockThread(true);
        List<ReadOnlyWindowStore<string, ValueAndTimestamp<string>>> windowStores =
            provider.stores("timestamped-window-store", windowStore());
        Assert.Equal(2, windowStores.Count);
        foreach (ReadOnlyWindowStore<string, ValueAndTimestamp<string>> store in windowStores) {
            Assert.Equal(store, instanceOf(ReadOnlyWindowStore));
            Assert.Equal(store, not(instanceOf(TimestampedWindowStore)));
        }
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionIfKVStoreClosed() {
        mockThread(true);
        taskOne.getStore("kv-store").close();
        provider.stores("kv-store", QueryableStoreTypes.keyValueStore());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionIfTsKVStoreClosed() {
        mockThread(true);
        taskOne.getStore("timestamped-kv-store").close();
        provider.stores("timestamped-kv-store", QueryableStoreTypes.timestampedKeyValueStore());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionIfWindowStoreClosed() {
        mockThread(true);
        taskOne.getStore("window-store").close();
        provider.stores("window-store", QueryableStoreTypes.windowStore());
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionIfTsWindowStoreClosed() {
        mockThread(true);
        taskOne.getStore("timestamped-window-store").close();
        provider.stores("timestamped-window-store", QueryableStoreTypes.timestampedWindowStore());
    }

    [Xunit.Fact]
    public void shouldReturnEmptyListIfNoStoresFoundWithName() {
        mockThread(true);
        Assert.Equal(
            Collections.emptyList(),
            provider.stores("not-a-store", QueryableStoreTypes.keyValueStore()));
    }

    [Xunit.Fact]
    public void shouldReturnEmptyListIfStoreExistsButIsNotOfTypeValueStore() {
        mockThread(true);
        Assert.Equal(
            Collections.emptyList(),
            provider.stores("window-store", QueryableStoreTypes.keyValueStore())
        );
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void shouldThrowInvalidStoreExceptionIfNotAllStoresAvailable() {
        mockThread(false);
        provider.stores("kv-store", QueryableStoreTypes.keyValueStore());
    }

    private StreamTask createStreamsTask(StreamsConfig streamsConfig,
                                         MockClientSupplier clientSupplier,
                                         ProcessorTopology topology,
                                         TaskId taskId) {
        Metrics metrics = new Metrics();
        return new StreamTask(
            taskId,
            Collections.singletonList(new TopicPartition(topicName, taskId.partition)),
            topology,
            clientSupplier.consumer,
            new StoreChangelogReader(
                clientSupplier.restoreConsumer,
                Duration.ZERO,
                new MockStateRestoreListener(),
                new LogContext("test-stream-task ")),
            streamsConfig,
            new MockStreamsMetrics(metrics),
            stateDirectory,
            null,
            new MockTime(),
            () => clientSupplier.getProducer(new HashMap<>())) {
            
            protected void updateOffsetLimits() {}
        };
    }

    private void mockThread(bool initialized) {
        EasyMock.expect(threadMock.isRunningAndNotRebalancing()).andReturn(initialized);
        EasyMock.expect(threadMock.tasks()).andStubReturn(tasks);
        EasyMock.replay(threadMock);
    }

    private void configureRestoreConsumer(MockClientSupplier clientSupplier,
                                          string topic) {
        List<PartitionInfo> partitions = Array.asList(
            new PartitionInfo(topic, 0, null, null, null),
            new PartitionInfo(topic, 1, null, null, null)
        );
        clientSupplier.restoreConsumer.updatePartitions(topic, partitions);
        TopicPartition tp1 = new TopicPartition(topic, 0);
        TopicPartition tp2 = new TopicPartition(topic, 1);

        clientSupplier.restoreConsumer.assign(Array.asList(tp1, tp2));

        Dictionary<TopicPartition, long> offsets = new HashMap<>();
        offsets.put(tp1, 0L);
        offsets.put(tp2, 0L);

        clientSupplier.restoreConsumer.updateBeginningOffsets(offsets);
        clientSupplier.restoreConsumer.updateEndOffsets(offsets);
    }
}
