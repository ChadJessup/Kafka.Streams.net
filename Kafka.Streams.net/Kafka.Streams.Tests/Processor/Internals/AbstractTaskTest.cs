namespace Kafka.Streams.Tests.Processor.Internals
{
    /*






    *

    *





    */










































    public class AbstractTaskTest
    {

        private TaskId id = new TaskId(0, 0);
        private StateDirectory stateDirectory = EasyMock.createMock(StateDirectory);
        private TopicPartition storeTopicPartition1 = new TopicPartition("t1", 0);
        private TopicPartition storeTopicPartition2 = new TopicPartition("t2", 0);
        private TopicPartition storeTopicPartition3 = new TopicPartition("t3", 0);
        private TopicPartition storeTopicPartition4 = new TopicPartition("t4", 0);
        private Collection<TopicPartition> storeTopicPartitions =
            Utils.mkSet(storeTopicPartition1, storeTopicPartition2, storeTopicPartition3, storeTopicPartition4);


        public void Before()
        {
            expect(stateDirectory.directoryForTask(id)).andReturn(TestUtils.tempDirectory());
        }

        [Xunit.Fact]// (expected = ProcessorStateException)
        public void ShouldThrowProcessorStateExceptionOnInitializeOffsetsWhenAuthorizationException()
        {
            Consumer consumer = mockConsumer(new AuthorizationException("blah"));
            AbstractTask task = createTask(consumer, Collections.< StateStore, string > emptyMap());
            task.updateOffsetLimits();
        }

        [Xunit.Fact]// (expected = ProcessorStateException)
        public void ShouldThrowProcessorStateExceptionOnInitializeOffsetsWhenKafkaException()
        {
            Consumer consumer = mockConsumer(new KafkaException("blah"));
            AbstractTask task = createTask(consumer, Collections.< StateStore, string > emptyMap());
            task.updateOffsetLimits();
        }

        [Xunit.Fact]// (expected = WakeupException)
        public void ShouldThrowWakeupExceptionOnInitializeOffsetsWhenWakeupException()
        {
            Consumer consumer = mockConsumer(new WakeupException());
            AbstractTask task = createTask(consumer, Collections.< StateStore, string > emptyMap());
            task.updateOffsetLimits();
        }

        [Xunit.Fact]
        public void ShouldThrowLockExceptionIfFailedToLockStateDirectoryWhenTopologyHasStores()
        { //throws IOException
            Consumer consumer = EasyMock.createNiceMock(Consumer);
            StateStore store = EasyMock.createNiceMock(StateStore);
            expect(store.name()).andReturn("dummy-store-name").anyTimes();
            EasyMock.replay(store);
            expect(stateDirectory.Lock(id)).andReturn(false);
            EasyMock.replay(stateDirectory);

            AbstractTask task = createTask(consumer, Collections.singletonMap(store, "dummy"));

            try
            {
                task.registerStateStores();
                Assert.True(false, "Should have thrown LockException");
            }
            catch (LockException e)
            {
                // ok
            }

        }

        [Xunit.Fact]
        public void ShouldNotAttemptToLockIfNoStores()
        {
            Consumer consumer = EasyMock.createNiceMock(Consumer);
            EasyMock.replay(stateDirectory);

            AbstractTask task = createTask(consumer, Collections.< StateStore, string > emptyMap());

            task.registerStateStores();

            // should fail if lock is called
            EasyMock.verify(stateDirectory);
        }

        [Xunit.Fact]
        public void ShouldDeleteAndRecreateStoreDirectoryOnReinitialize()
        { //throws IOException
            StreamsConfig streamsConfig = new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        }
    });
        Consumer consumer = EasyMock.createNiceMock(Consumer);

    StateStore store1 = EasyMock.createNiceMock(StateStore);
    StateStore store2 = EasyMock.createNiceMock(StateStore);
    StateStore store3 = EasyMock.createNiceMock(StateStore);
    StateStore store4 = EasyMock.createNiceMock(StateStore);
    string storeName1 = "storeName1";
    string storeName2 = "storeName2";
    string storeName3 = "storeName3";
    string storeName4 = "storeName4";

    expect(store1.name()).andReturn(storeName1).anyTimes();
    EasyMock.replay(store1);
        expect(store2.name()).andReturn(storeName2).anyTimes();
    EasyMock.replay(store2);
        expect(store3.name()).andReturn(storeName3).anyTimes();
    EasyMock.replay(store3);
        expect(store4.name()).andReturn(storeName4).anyTimes();
    EasyMock.replay(store4);

        StateDirectory stateDirectory = new StateDirectory(streamsConfig, new MockTime(), true);
    AbstractTask task = createTask(
        consumer,
        new HashDictionary<StateStore, string>() {
                {
                    put(store1, storeTopicPartition1.topic());
    put(store2, storeTopicPartition2.topic());
    put(store3, storeTopicPartition3.topic());
    put(store4, storeTopicPartition4.topic());
                }
            },
            stateDirectory);

        string taskDir = stateDirectory.directoryForTask(task.id).getAbsolutePath();
    File storeDirectory1 = new File(taskDir
        + File.separator + "rocksdb"
        + File.separator + storeName1);
    File storeDirectory2 = new File(taskDir
        + File.separator + "rocksdb"
        + File.separator + storeName2);
    File storeDirectory3 = new File(taskDir
        + File.separator + storeName3);
    File storeDirectory4 = new File(taskDir
        + File.separator + storeName4);
    File testFile1 = new File(storeDirectory1.getAbsolutePath() + File.separator + "testFile");
    File testFile2 = new File(storeDirectory2.getAbsolutePath() + File.separator + "testFile");
    File testFile3 = new File(storeDirectory3.getAbsolutePath() + File.separator + "testFile");
    File testFile4 = new File(storeDirectory4.getAbsolutePath() + File.separator + "testFile");

    storeDirectory1.mkdirs();
        storeDirectory2.mkdirs();
        storeDirectory3.mkdirs();
        storeDirectory4.mkdirs();

        testFile1.createNewFile();
        Assert.True(testFile1.exists());
        testFile2.createNewFile();
        Assert.True(testFile2.exists());
        testFile3.createNewFile();
        Assert.True(testFile3.exists());
        testFile4.createNewFile();
        Assert.True(testFile4.exists());

        task.processorContext = new InternalMockProcessorContext(stateDirectory.directoryForTask(task.id), streamsConfig);

    task.stateMgr.register(store1, new MockRestoreCallback());
        task.stateMgr.register(store2, new MockRestoreCallback());
        task.stateMgr.register(store3, new MockRestoreCallback());
        task.stateMgr.register(store4, new MockRestoreCallback());

        // only reinitialize store1 and store3 -- store2 and store4 should be untouched
        task.reinitializeStateStoresForPartitions(Utils.mkSet(storeTopicPartition1, storeTopicPartition3));

        Assert.False(testFile1.exists());
        Assert.True(testFile2.exists());
        Assert.False(testFile3.exists());
        Assert.True(testFile4.exists());
    }

    private AbstractTask CreateTask(Consumer consumer,
                                    Dictionary<StateStore, string> stateStoresToChangelogTopics)
    {
        return createTask(consumer, stateStoresToChangelogTopics, stateDirectory);
    }


    private AbstractTask CreateTask(Consumer consumer,
                                    Dictionary<StateStore, string> stateStoresToChangelogTopics,
                                    StateDirectory stateDirectory)
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummyhost:9092");
        StreamsConfig config = new StreamsConfig(properties);

        Dictionary<string, string> storeNamesToChangelogTopics = new HashMap<>(stateStoresToChangelogTopics.Count);
        foreach (Map.Entry<StateStore, string> e in stateStoresToChangelogTopics.entrySet())
        {
            storeNamesToChangelogTopics.put(e.getKey().name(), e.getValue());
        }

        return new AbstractTask(id,
                                storeTopicPartitions,
                                withLocalStores(new ArrayList<>(stateStoresToChangelogTopics.keySet()),
                                                storeNamesToChangelogTopics),
                                consumer,
                                new StoreChangelogReader(consumer,
                                                         Duration.ZERO,
                                                         new MockStateRestoreListener(),
                                                         new LogContext("stream-task-test ")),
                                false,
                                stateDirectory,
                                config)
        {



            public void resume() { }


        public void commit() { }


        public void suspend() { }


        public void close(bool clean, bool isZombie) { }


        public void closeSuspended(bool clean, bool isZombie, RuntimeException e) { }


        public bool initializeStateStores()
        {
            return false;
        }


        public void initializeTopology() { }
    };
    }

    private Consumer MockConsumer(RuntimeException toThrow)
    {
        return new MockConsumer(OffsetResetStrategy.EARLIEST)
        {


            public OffsetAndMetadata committed(TopicPartition partition)
        {
            throw toThrow;
        }
    };
    }

}
}
/*






*

*





*/



















































// only reinitialize store1 and store3 -- store2 and store4 should be untouched





