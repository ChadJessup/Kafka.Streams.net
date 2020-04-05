//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    /*






//    *

//    *





//    */










































//    public class AbstractTaskTest
//    {

//        private TaskId id = new TaskId(0, 0);
//        private StateDirectory stateDirectory = EasyMock.createMock(StateDirectory);
//        private TopicPartition storeTopicPartition1 = new TopicPartition("t1", 0);
//        private TopicPartition storeTopicPartition2 = new TopicPartition("t2", 0);
//        private TopicPartition storeTopicPartition3 = new TopicPartition("t3", 0);
//        private TopicPartition storeTopicPartition4 = new TopicPartition("t4", 0);
//        private Collection<TopicPartition> storeTopicPartitions =
//            Utils.mkSet(storeTopicPartition1, storeTopicPartition2, storeTopicPartition3, storeTopicPartition4);


//        public void Before()
//        {
//            expect(stateDirectory.directoryForTask(id)).andReturn(TestUtils.GetTempDirectory());
//        }

//        [Fact]// (expected = ProcessorStateException)
//        public void ShouldThrowProcessorStateExceptionOnInitializeOffsetsWhenAuthorizationException()
//        {
//            Consumer consumer = mockConsumer(new AuthorizationException("blah"));
//            AbstractTask task = createTask(consumer, Collections.< IStateStore, string > emptyMap());
//            task.updateOffsetLimits();
//        }

//        [Fact]// (expected = ProcessorStateException)
//        public void ShouldThrowProcessorStateExceptionOnInitializeOffsetsWhenKafkaException()
//        {
//            Consumer consumer = mockConsumer(new KafkaException("blah"));
//            AbstractTask task = createTask(consumer, Collections.< IStateStore, string > emptyMap());
//            task.updateOffsetLimits();
//        }

//        [Fact]// (expected = WakeupException)
//        public void ShouldThrowWakeupExceptionOnInitializeOffsetsWhenWakeupException()
//        {
//            Consumer consumer = mockConsumer(new WakeupException());
//            AbstractTask task = createTask(consumer, Collections.< IStateStore, string > emptyMap());
//            task.updateOffsetLimits();
//        }

//        [Fact]
//        public void ShouldThrowLockExceptionIfFailedToLockStateDirectoryWhenTopologyHasStores()
//        { //throws IOException
//            Consumer consumer = EasyMock.createNiceMock(Consumer);
//            IStateStore store = EasyMock.createNiceMock(IStateStore);
//            expect(store.name()).andReturn("dummy-store-name").anyTimes();
//            EasyMock.replay(store);
//            expect(stateDirectory.Lock(id)).andReturn(false);
//            EasyMock.replay(stateDirectory);

//            AbstractTask task = createTask(consumer, Collections.singletonMap(store, "dummy"));

//            try
//            {
//                task.registerStateStores();
//                Assert.True(false, "Should have thrown LockException");
//            }
//            catch (LockException e)
//            {
//                // ok
//            }

//        }

//        [Fact]
//        public void ShouldNotAttemptToLockIfNoStores()
//        {
//            Consumer consumer = EasyMock.createNiceMock(Consumer);
//            EasyMock.replay(stateDirectory);

//            AbstractTask task = createTask(consumer, Collections.< IStateStore, string > emptyMap());

//            task.registerStateStores();

//            // should fail if lock is called
//            EasyMock.verify(stateDirectory);
//        }

//        [Fact]
//        public void ShouldDeleteAndRecreateStoreDirectoryOnReinitialize()
//        { //throws IOException
//            StreamsConfig streamsConfig = new StreamsConfig(new StreamsConfig() {
//            {
//                put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
//            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//            put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().FullName);
//        }
//    });
//        Consumer consumer = EasyMock.createNiceMock(Consumer);

//    IStateStore store1 = EasyMock.createNiceMock(IStateStore);
//    IStateStore store2 = EasyMock.createNiceMock(IStateStore);
//    IStateStore store3 = EasyMock.createNiceMock(IStateStore);
//    IStateStore store4 = EasyMock.createNiceMock(IStateStore);
//    string storeName1 = "storeName1";
//    string storeName2 = "storeName2";
//    string storeName3 = "storeName3";
//    string storeName4 = "storeName4";

//    expect(store1.name()).andReturn(storeName1).anyTimes();
//    EasyMock.replay(store1);
//        expect(store2.name()).andReturn(storeName2).anyTimes();
//    EasyMock.replay(store2);
//        expect(store3.name()).andReturn(storeName3).anyTimes();
//    EasyMock.replay(store3);
//        expect(store4.name()).andReturn(storeName4).anyTimes();
//    EasyMock.replay(store4);

//        StateDirectory stateDirectory = new StateDirectory(streamsConfig, new MockTime(), true);
//    AbstractTask task = createTask(
//        consumer,
//        new HashDictionary<IStateStore, string>() {
//                {
//                    put(store1, storeTopicPartition1.Topic);
//    put(store2, storeTopicPartition2.Topic);
//    put(store3, storeTopicPartition3.Topic);
//    put(store4, storeTopicPartition4.Topic);
//                }
//            },
//            stateDirectory);

//        string taskDir = stateDirectory.directoryForTask(task.id).FullName;
//    File storeDirectory1 = new File(taskDir
//        + Path.DirectorySeparatorChar + "rocksdb"
//        + Path.DirectorySeparatorChar + storeName1);
//    File storeDirectory2 = new File(taskDir
//        + Path.DirectorySeparatorChar + "rocksdb"
//        + Path.DirectorySeparatorChar + storeName2);
//    File storeDirectory3 = new File(taskDir
//        + Path.DirectorySeparatorChar + storeName3);
//    File storeDirectory4 = new File(taskDir
//        + Path.DirectorySeparatorChar + storeName4);
//    File testFile1 = new File(storeDirectory1.FullName + Path.DirectorySeparatorChar + "testFile");
//    File testFile2 = new File(storeDirectory2.FullName + Path.DirectorySeparatorChar + "testFile");
//    File testFile3 = new File(storeDirectory3.FullName + Path.DirectorySeparatorChar + "testFile");
//    File testFile4 = new File(storeDirectory4.FullName + Path.DirectorySeparatorChar + "testFile");

//    storeDirectory1.mkdirs();
//        storeDirectory2.mkdirs();
//        storeDirectory3.mkdirs();
//        storeDirectory4.mkdirs();

//        testFile1.createNewFile();
//        Assert.True(testFile1.Exists);
//        testFile2.createNewFile();
//        Assert.True(testFile2.Exists);
//        testFile3.createNewFile();
//        Assert.True(testFile3.Exists);
//        testFile4.createNewFile();
//        Assert.True(testFile4.Exists);

//        task.processorContext = new InternalMockProcessorContext(stateDirectory.directoryForTask(task.id), streamsConfig);

//    task.stateMgr.register(store1, new MockRestoreCallback());
//        task.stateMgr.register(store2, new MockRestoreCallback());
//        task.stateMgr.register(store3, new MockRestoreCallback());
//        task.stateMgr.register(store4, new MockRestoreCallback());

//        // only reinitialize store1 and store3 -- store2 and store4 should be untouched
//        task.reinitializeStateStoresForPartitions(Utils.mkSet(storeTopicPartition1, storeTopicPartition3));

//        Assert.False(testFile1.Exists);
//        Assert.True(testFile2.Exists);
//        Assert.False(testFile3.Exists);
//        Assert.True(testFile4.Exists);
//    }

//    private AbstractTask CreateTask(Consumer consumer,
//                                    Dictionary<IStateStore, string> stateStoresToChangelogTopics)
//    {
//        return createTask(consumer, stateStoresToChangelogTopics, stateDirectory);
//    }


//    private AbstractTask CreateTask(Consumer consumer,
//                                    Dictionary<IStateStore, string> stateStoresToChangelogTopics,
//                                    StateDirectory stateDirectory)
//    {
//        StreamsConfig properties = new StreamsConfig();
//        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
//        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummyhost:9092");
//        StreamsConfig config = new StreamsConfig(properties);

//        Dictionary<string, string> storeNamesToChangelogTopics = new HashMap<>(stateStoresToChangelogTopics.Count);
//        foreach (Map.Entry<IStateStore, string> e in stateStoresToChangelogTopics.entrySet())
//        {
//            storeNamesToChangelogTopics.put(e.getKey().name(), e.getValue());
//        }

//        return new AbstractTask(id,
//                                storeTopicPartitions,
//                                withLocalStores(new ArrayList<>(stateStoresToChangelogTopics.keySet()),
//                                                storeNamesToChangelogTopics),
//                                consumer,
//                                new StoreChangelogReader(consumer,
//                                                         Duration.TimeSpan.Zero,
//                                                         new MockStateRestoreListener(),
//                                                         new LogContext("stream-task-test ")),
//                                false,
//                                stateDirectory,
//                                config)
//        {



//            public void resume() { }


//        public void commit() { }


//        public void suspend() { }


//        public void close(bool clean, bool isZombie) { }


//        public void closeSuspended(bool clean, bool isZombie, RuntimeException e) { }


//        public bool initializeStateStores()
//        {
//            return false;
//        }


//        public void initializeTopology() { }
//    };
//    }

//    private Consumer MockConsumer(RuntimeException toThrow)
//    {
//        return new MockConsumer(OffsetResetStrategy.EARLIEST)
//        {


//            public OffsetAndMetadata committed(TopicPartition partition)
//        {
//            throw toThrow;
//        }
//    };
//    }

//}
//}
///*






//*

//*





//*/



















































//// only reinitialize store1 and store3 -- store2 and store4 should be untouched





