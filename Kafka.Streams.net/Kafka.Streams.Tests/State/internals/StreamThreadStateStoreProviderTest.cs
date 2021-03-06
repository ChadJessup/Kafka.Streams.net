//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */





















































//    public class StreamThreadStateStoreProviderTest
//    {

//        private StreamTask taskOne;
//        private StreamThreadStateStoreProvider provider;
//        private StateDirectory stateDirectory;
//        private File stateDir;
//        private readonly string topicName = "topic";
//        private StreamThread threadMock;
//        private Dictionary<TaskId, StreamTask> tasks;


//        public void Before()
//        {
//            TopologyWrapper topology = new TopologyWrapper();
//            topology.AddSource("the-source", topicName);
//            topology.AddProcessor("the-processor", new MockProcessorSupplier(), "the-source");
//            topology.AddStateStore(
//                Stores.KeyValueStoreBuilder(
//                    Stores.InMemoryKeyValueStore("kv-store"),
//                    Serdes.String(),
//                    Serdes.String()),
//                "the-processor");
//            topology.AddStateStore(
//                Stores.TimestampedKeyValueStoreBuilder(
//                    Stores.InMemoryKeyValueStore("timestamped-kv-store"),
//                    Serdes.String(),
//                    Serdes.String()),
//                "the-processor");
//            topology.AddStateStore(
//                Stores.windowStoreBuilder(
//                    Stores.inMemoryWindowStore(
//                        "window-store",
//                        TimeSpan.FromMilliseconds(10L),
//                        TimeSpan.FromMilliseconds(2L),
//                        false),
//                    Serdes.String(),
//                    Serdes.String()),
//                "the-processor");
//            topology.AddStateStore(
//                Stores.timestampedWindowStoreBuilder(
//                    Stores.inMemoryWindowStore(
//                        "timestamped-window-store",
//                        TimeSpan.FromMilliseconds(10L),
//                        TimeSpan.FromMilliseconds(2L),
//                        false),
//                    Serdes.String(),
//                    Serdes.String()),
//                "the-processor");

//            StreamsConfig properties = new StreamsConfig();
//            string applicationId = "applicationId";
//            properties.Put(StreamsConfig.ApplicationIdConfig, applicationId);
//            properties.Put(ConsumerConfig.BootstrapServersConfig, "localhost:9092");
//            stateDir = TestUtils.GetTempDirectory();
//            properties.Put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());

//            StreamsConfig streamsConfig = new StreamsConfig(properties);
//            MockClientSupplier clientSupplier = new MockClientSupplier();
//            configureRestoreConsumer(clientSupplier, "applicationId-kv-store-changelog");
//            configureRestoreConsumer(clientSupplier, "applicationId-window-store-changelog");

//            ProcessorTopology processorTopology = topology.getInternalBuilder(applicationId).Build();

//            tasks = new HashMap<>();
//            stateDirectory = new StateDirectory(streamsConfig, new MockTime(), true);

//            taskOne = CreateStreamsTask(
//                streamsConfig,
//                clientSupplier,
//                processorTopology,
//                new TaskId(0, 0));
//            taskOne.InitializeStateStores();
//            tasks.Put(new TaskId(0, 0), taskOne);

//            StreamTask taskTwo = CreateStreamsTask(
//                streamsConfig,
//                clientSupplier,
//                processorTopology,
//                new TaskId(0, 1));
//            taskTwo.InitializeStateStores();
//            tasks.Put(new TaskId(0, 1), taskTwo);

//            threadMock = Mock.Of<StreamThread);
//            provider = new StreamThreadStateStoreProvider(threadMock);

//        }


//        public void CleanUp()
//        { //throws IOException
//            Utils.Delete(stateDir);
//        }

//        [Fact]
//        public void ShouldFindKeyValueStores()
//        {
//            mockThread(true);
//            List<IReadOnlyKeyValueStore<string, string>> kvStores =
//                provider.Stores("kv-store", QueryableStoreTypes.KeyValueStore);
//            Assert.Equal(2, kvStores.Count);
//            foreach (IReadOnlyKeyValueStore<string, string> store in kvStores)
//            {
//                Assert.Equal(store, instanceOf(IReadOnlyKeyValueStore));
//                Assert.Equal(store, not(instanceOf(ITimestampedKeyValueStore)));
//            }
//        }

//        [Fact]
//        public void ShouldFindTimestampedKeyValueStores()
//        {
//            mockThread(true);
//            List<IReadOnlyKeyValueStore<string, IValueAndTimestamp<string>>> tkvStores =
//                provider.Stores("timestamped-kv-store", QueryableStoreTypes.TimestampedKeyValueStore());
//            Assert.Equal(2, tkvStores.Count);
//            foreach (IReadOnlyKeyValueStore<string, IValueAndTimestamp<string>> store in tkvStores)
//            {
//                Assert.Equal(store, instanceOf(IReadOnlyKeyValueStore));
//                Assert.Equal(store, instanceOf(ITimestampedKeyValueStore));
//            }
//        }

//        [Fact]
//        public void ShouldNotFindKeyValueStoresAsTimestampedStore()
//        {
//            mockThread(true);
//            List<IReadOnlyKeyValueStore<string, IValueAndTimestamp<string>>> tkvStores =
//                provider.Stores("kv-store", QueryableStoreTypes.TimestampedKeyValueStore());
//            Assert.Equal(0, tkvStores.Count);
//        }

//        [Fact]
//        public void ShouldFindTimestampedKeyValueStoresAsKeyValueStores()
//        {
//            mockThread(true);
//            List<IReadOnlyKeyValueStore<string, IValueAndTimestamp<string>>> tkvStores =
//                provider.Stores("timestamped-kv-store", QueryableStoreTypes.KeyValueStore);
//            Assert.Equal(2, tkvStores.Count);
//            foreach (IReadOnlyKeyValueStore<string, IValueAndTimestamp<string>> store in tkvStores)
//            {
//                Assert.Equal(store, instanceOf(IReadOnlyKeyValueStore));
//                Assert.Equal(store, not(instanceOf(ITimestampedKeyValueStore)));
//            }
//        }

//        [Fact]
//        public void ShouldFindWindowStores()
//        {
//            mockThread(true);
//            List<IReadOnlyWindowStore<string, string>> windowStores =
//                provider.Stores("window-store", windowStore());
//            Assert.Equal(2, windowStores.Count);
//            foreach (IReadOnlyWindowStore<string, string> store in windowStores)
//            {
//                Assert.Equal(store, instanceOf(IReadOnlyWindowStore));
//                Assert.Equal(store, not(instanceOf(ITimestampedWindowStore)));
//            }
//        }

//        [Fact]
//        public void ShouldFindTimestampedWindowStores()
//        {
//            mockThread(true);
//            List<IReadOnlyWindowStore<string, IValueAndTimestamp<string>>> windowStores =
//                provider.Stores("timestamped-window-store", timestampedWindowStore());
//            Assert.Equal(2, windowStores.Count);
//            foreach (IReadOnlyWindowStore<string, IValueAndTimestamp<string>> store in windowStores)
//            {
//                Assert.Equal(store, instanceOf(IReadOnlyWindowStore));
//                Assert.Equal(store, instanceOf(ITimestampedWindowStore));
//            }
//        }

//        [Fact]
//        public void ShouldNotFindWindowStoresAsTimestampedStore()
//        {
//            mockThread(true);
//            List<IReadOnlyWindowStore<string, IValueAndTimestamp<string>>> windowStores =
//                provider.Stores("window-store", timestampedWindowStore());
//            Assert.Equal(0, windowStores.Count);
//        }

//        [Fact]
//        public void ShouldFindTimestampedWindowStoresAsWindowStore()
//        {
//            mockThread(true);
//            List<IReadOnlyWindowStore<string, IValueAndTimestamp<string>>> windowStores =
//                provider.Stores("timestamped-window-store", windowStore());
//            Assert.Equal(2, windowStores.Count);
//            foreach (IReadOnlyWindowStore<string, IValueAndTimestamp<string>> store in windowStores)
//            {
//                Assert.Equal(store, instanceOf(IReadOnlyWindowStore));
//                Assert.Equal(store, not(instanceOf(ITimestampedWindowStore)));
//            }
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionIfKVStoreClosed()
//        {
//            mockThread(true);
//            taskOne.GetStore("kv-store").Close();
//            provider.Stores("kv-store", QueryableStoreTypes.KeyValueStore);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionIfTsKVStoreClosed()
//        {
//            mockThread(true);
//            taskOne.GetStore("timestamped-kv-store").Close();
//            provider.Stores("timestamped-kv-store", QueryableStoreTypes.TimestampedKeyValueStore());
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionIfWindowStoreClosed()
//        {
//            mockThread(true);
//            taskOne.GetStore("window-store").Close();
//            provider.Stores("window-store", QueryableStoreTypes.windowStore());
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionIfTsWindowStoreClosed()
//        {
//            mockThread(true);
//            taskOne.GetStore("timestamped-window-store").Close();
//            provider.Stores("timestamped-window-store", QueryableStoreTypes.timestampedWindowStore());
//        }

//        [Fact]
//        public void ShouldReturnEmptyListIfNoStoresFoundWithName()
//        {
//            mockThread(true);
//            Assert.Equal(
//                Collections.emptyList(),
//                provider.Stores("not-a-store", QueryableStoreTypes.KeyValueStore));
//        }

//        [Fact]
//        public void ShouldReturnEmptyListIfStoreExistsButIsNotOfTypeValueStore()
//        {
//            mockThread(true);
//            Assert.Equal(
//                Collections.emptyList(),
//                provider.Stores("window-store", QueryableStoreTypes.KeyValueStore)
//            );
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionIfNotAllStoresAvailable()
//        {
//            mockThread(false);
//            provider.Stores("kv-store", QueryableStoreTypes.KeyValueStore);
//        }

//        private StreamTask CreateStreamsTask(StreamsConfig streamsConfig,
//                                             MockClientSupplier clientSupplier,
//                                             ProcessorTopology topology,
//                                             TaskId taskId)
//        {
//            Metrics metrics = new Metrics();
//            return new StreamTask(
//                taskId,
//                Collections.singletonList(new TopicPartition(topicName, taskId.partition)),
//                topology,
//                clientSupplier.consumer,
//                new StoreChangelogReader(
//                    clientSupplier.restoreConsumer,
//                    TimeSpan.TimeSpan.Zero,
//                    new MockStateRestoreListener(),
//                    new LogContext("test-stream-task ")),
//                streamsConfig,
//                new MockStreamsMetrics(metrics),
//                stateDirectory,
//                null,
//                new MockTime(),
//                () => clientSupplier.getProducer(new HashMap<>()))
//            {


//            protected void updateOffsetLimits() { }
//        };
//    }

//    private void MockThread(bool initialized)
//    {
//        EasyMock.expect(threadMock.isRunningAndNotRebalancing()).andReturn(initialized);
//        EasyMock.expect(threadMock.Tasks()).andStubReturn(tasks);
//        EasyMock.replay(threadMock);
//    }

//    private void ConfigureRestoreConsumer(MockClientSupplier clientSupplier,
//                                          string topic)
//    {
//        List<PartitionInfo> partitions = Arrays.asList(
//            new PartitionInfo(topic, 0, null, null, null),
//            new PartitionInfo(topic, 1, null, null, null)
//        );
//        clientSupplier.restoreConsumer.updatePartitions(topic, partitions);
//        TopicPartition tp1 = new TopicPartition(topic, 0);
//        TopicPartition tp2 = new TopicPartition(topic, 1);

//        clientSupplier.restoreConsumer.Assign(Arrays.asList(tp1, tp2));

//        Dictionary<TopicPartition, long> offsets = new HashMap<>();
//        offsets.Put(tp1, 0L);
//        offsets.Put(tp2, 0L);

//        clientSupplier.restoreConsumer.UpdateBeginningOffsets(offsets);
//        clientSupplier.restoreConsumer.updateEndOffsets(offsets);
//    }
//}
//}
///*






//*

//*





//*/























































