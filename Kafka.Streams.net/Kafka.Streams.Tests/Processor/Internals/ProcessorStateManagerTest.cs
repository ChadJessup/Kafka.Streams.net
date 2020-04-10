///*






// *

// *





// */























































//using Confluent.Kafka;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.TimeStamped;
//using Kafka.Streams.Tasks;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class ProcessorStateManagerTest
//    {

//        private readonly HashSet<TopicPartition> noPartitions = Collections.emptySet();
//        private readonly string applicationId = "test-application";
//        private readonly string persistentStoreName = "persistentStore";
//        private readonly string nonPersistentStoreName = "nonPersistentStore";
//        private readonly string persistentStoreTopicName = ProcessorStateManager.StoreChangelogTopic(applicationId, persistentStoreName);
//        private readonly string nonPersistentStoreTopicName = ProcessorStateManager.StoreChangelogTopic(applicationId, nonPersistentStoreName);
//        private readonly MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
//        private readonly MockKeyValueStore nonPersistentStore = new MockKeyValueStore(nonPersistentStoreName, false);
//        private readonly TopicPartition persistentStorePartition = new TopicPartition(persistentStoreTopicName, 1);
//        private readonly string storeName = "mockKeyValueStore";
//        private readonly string changelogTopic = ProcessorStateManager.StoreChangelogTopic(applicationId, storeName);
//        private readonly TopicPartition changelogTopicPartition = new TopicPartition(changelogTopic, 0);
//        private readonly TaskId taskId = new TaskId(0, 1);
//        private readonly MockChangelogReader changelogReader = new MockChangelogReader();
//        private readonly MockKeyValueStore mockKeyValueStore = new MockKeyValueStore(storeName, true);
//        private readonly byte[] key = new byte[] { 0x0, 0x0, 0x0, 0x1 };
//        private readonly byte[] value = "the-value".getBytes(StandardCharsets.UTF_8);
//        private readonly ConsumeResult<byte[], byte[]> consumerRecord = new ConsumeResult<>(changelogTopic, 0, 0, key, value);
//        private LogContext logContext = new LogContext("process-state-manager-test ");

//        private File baseDir;
//        private File checkpointFile;
//        private OffsetCheckpoint checkpoint;
//        private StateDirectory stateDirectory;


//        public void Setup()
//        {
//            baseDir = TestUtils.GetTempDirectory();

//            stateDirectory = new StateDirectory(new StreamsConfig(new StreamsConfig() {
//            {
//                Put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
//        Put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
//            Put(StreamsConfig.STATE_DIR_CONFIG, baseDir.getPath());
//        }
//    }), new MockTime(), true);
//        checkpointFile = new File(stateDirectory.directoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME);
//    checkpoint = new OffsetCheckpoint(checkpointFile);
//    }

    
//    public void Cleanup()
//    { //throws IOException
//        Utils.delete(baseDir);
//    }

//    [Fact]
//    public void ShouldRestoreStoreWithBatchingRestoreSpecification()
//    {// throws Exception
//        TaskId taskId = new TaskId(0, 2);
//        MockBatchingStateRestoreListener batchingRestoreCallback = new MockBatchingStateRestoreListener();

//        KeyValuePair<byte[], byte[]> expectedKeyValue = KeyValuePair.Create(key, value);

//        MockKeyValueStore persistentStore = getPersistentStore();
//        ProcessorStateManager stateMgr = getStandByStateManager(taskId);

//        try
//        {
//            stateMgr.register(persistentStore, batchingRestoreCallback);
//            stateMgr.updateStandbyStates(
//                persistentStorePartition,
//                singletonList(consumerRecord),
//                consumerRecord.Offset
//            );
//            Assert.Equal(batchingRestoreCallback.getRestoredRecords().Count, is (1));
//            Assert.True(batchingRestoreCallback.getRestoredRecords().Contains(expectedKeyValue));
//        }
//        finally
//        {
//            stateMgr.Close(true);
//        }
//    }

//    [Fact]
//    public void ShouldRestoreStoreWithSinglePutRestoreSpecification()
//    {// throws Exception
//        TaskId taskId = new TaskId(0, 2);
//        int intKey = 1;

//        MockKeyValueStore persistentStore = getPersistentStore();
//        ProcessorStateManager stateMgr = getStandByStateManager(taskId);

//        try
//        {
//            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
//            stateMgr.updateStandbyStates(
//                persistentStorePartition,
//                singletonList(consumerRecord),
//                consumerRecord.Offset
//            );
//            Assert.Equal(persistentStore.keys.Count, (1));
//            Assert.True(persistentStore.keys.Contains(intKey));
//            Assert.Equal(9, persistentStore.values.Get(0).Length);
//        }
//        finally
//        {
//            stateMgr.Close(true);
//        }
//    }

//    [Fact]
//    public void ShouldConvertDataOnRestoreIfStoreImplementsTimestampedBytesStore()
//    {// throws Exception
//        TaskId taskId = new TaskId(0, 2);
//        int intKey = 1;

//        MockKeyValueStore persistentStore = getConverterStore();
//        ProcessorStateManager stateMgr = getStandByStateManager(taskId);

//        try
//        {
//            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
//            stateMgr.updateStandbyStates(
//                persistentStorePartition,
//                singletonList(consumerRecord),
//                consumerRecord.Offset
//            );
//            Assert.Equal(persistentStore.keys.Count, is (1));
//            Assert.True(persistentStore.keys.Contains(intKey));
//            Assert.Equal(17, persistentStore.values.Get(0).Length);
//        }
//        finally
//        {
//            stateMgr.Close(true);
//        }
//    }

//    [Fact]
//    public void TestRegisterPersistentStore()
//    { //throws IOException
//        TaskId taskId = new TaskId(0, 2);

//        MockKeyValueStore persistentStore = getPersistentStore();
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            mkMap(
//                mkEntry(persistentStoreName, persistentStoreTopicName),
//                mkEntry(nonPersistentStoreName, nonPersistentStoreName)
//            ),
//            changelogReader,
//            false,
//            logContext);

//        try
//        {
//            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
//            Assert.True(changelogReader.wasRegistered(new TopicPartition(persistentStoreTopicName, 2)));
//        }
//        finally
//        {
//            stateMgr.Close(true);
//        }
//    }

//    [Fact]
//    public void TestRegisterNonPersistentStore()
//    { //throws IOException
//        MockKeyValueStore nonPersistentStore =
//            new MockKeyValueStore(nonPersistentStoreName, false); // non Persistent store
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            new TaskId(0, 2),
//            noPartitions,
//            false,
//            stateDirectory,
//            mkMap(
//                mkEntry(persistentStoreName, persistentStoreTopicName),
//                mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)
//            ),
//            changelogReader,
//            false,
//            logContext);

//        try
//        {
//            stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
//            Assert.True(changelogReader.wasRegistered(new TopicPartition(nonPersistentStoreTopicName, 2)));
//        }
//        finally
//        {
//            stateMgr.Close(true);
//        }
//    }

//    [Fact]
//    public void TestChangeLogOffsets()
//    { //throws IOException
//        TaskId taskId = new TaskId(0, 0);
//        long storeTopic1LoadedCheckpoint = 10L;
//        string storeName1 = "store1";
//        string storeName2 = "store2";
//        string storeName3 = "store3";

//        string storeTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
//        string storeTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
//        string storeTopicName3 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName3);

//        Dictionary<string, string> storeToChangelogTopic = new HashMap<>();
//        storeToChangelogTopic.Put(storeName1, storeTopicName1);
//        storeToChangelogTopic.Put(storeName2, storeTopicName2);
//        storeToChangelogTopic.Put(storeName3, storeTopicName3);

//        OffsetCheckpoint checkpoint = new OffsetCheckpoint(
//            new File(stateDirectory.directoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME)
//        );
//        checkpoint.write(singletonMap(new TopicPartition(storeTopicName1, 0), storeTopic1LoadedCheckpoint));

//        TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
//        TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
//        TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);

//        MockKeyValueStore store1 = new MockKeyValueStore(storeName1, true);
//        MockKeyValueStore store2 = new MockKeyValueStore(storeName2, true);
//        MockKeyValueStore store3 = new MockKeyValueStore(storeName3, true);

//        // if there is a source partition, inherit the partition id
//        HashSet<TopicPartition> sourcePartitions = Utils.mkSet(new TopicPartition(storeTopicName3, 1));

//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            sourcePartitions,
//            true, // standby
//            stateDirectory,
//            storeToChangelogTopic,
//            changelogReader,
//            false,
//            logContext);

//        try
//        {
//            stateMgr.register(store1, store1.stateRestoreCallback);
//            stateMgr.register(store2, store2.stateRestoreCallback);
//            stateMgr.register(store3, store3.stateRestoreCallback);

//            Dictionary<TopicPartition, long> changeLogOffsets = stateMgr.Checkpointed();

//            Assert.Equal(3, changeLogOffsets.Count);
//            Assert.True(changeLogOffsets.containsKey(partition1));
//            Assert.True(changeLogOffsets.containsKey(partition2));
//            Assert.True(changeLogOffsets.containsKey(partition3));
//            Assert.Equal(storeTopic1LoadedCheckpoint, (long)changeLogOffsets.Get(partition1));
//            Assert.Equal(-1L, (long)changeLogOffsets.Get(partition2));
//            Assert.Equal(-1L, (long)changeLogOffsets.Get(partition3));

//        }
//        finally
//        {
//            stateMgr.Close(true);
//        }
//    }

//    [Fact]
//    public void TestGetStore()
//    { //throws IOException
//        MockKeyValueStore mockKeyValueStore = new MockKeyValueStore(nonPersistentStoreName, false);
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            new TaskId(0, 1),
//            noPartitions,
//            false,
//            stateDirectory,
//            emptyMap(),
//            changelogReader,
//            false,
//            logContext);
//        try
//        {
//            stateMgr.register(mockKeyValueStore, mockKeyValueStore.stateRestoreCallback);

//            Assert.Null(stateMgr.getStore("noSuchStore"));
//            Assert.Equal(mockKeyValueStore, stateMgr.getStore(nonPersistentStoreName));

//        }
//        finally
//        {
//            stateMgr.Close(true);
//        }
//    }

//    [Fact]
//    public void TestFlushAndClose()
//    { //throws IOException
//        checkpoint.write(emptyMap());

//        // set up ack'ed offsets
//        HashDictionary<TopicPartition, long> ackedOffsets = new HashMap<>();
//        ackedOffsets.Put(new TopicPartition(persistentStoreTopicName, 1), 123L);
//        ackedOffsets.Put(new TopicPartition(nonPersistentStoreTopicName, 1), 456L);
//        ackedOffsets.Put(new TopicPartition(ProcessorStateManager.storeChangelogTopic(applicationId, "otherTopic"), 1), 789L);

//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            mkMap(mkEntry(persistentStoreName, persistentStoreTopicName),
//                  mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)),
//            changelogReader,
//            false,
//            logContext);
//        try
//        {
//            // make sure the checkpoint file is not written yet
//            Assert.False(checkpointFile.Exists);

//            stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
//            stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
//        }
//        finally
//        {
//            // Close the state manager with the ack'ed offsets
//            stateMgr.Flush();
//            stateMgr.checkpoint(ackedOffsets);
//            stateMgr.Close(true);
//        }
//        // make sure All stores are closed, and the checkpoint file is written.
//        Assert.True(persistentStore.flushed);
//        Assert.True(persistentStore.closed);
//        Assert.True(nonPersistentStore.flushed);
//        Assert.True(nonPersistentStore.closed);
//        Assert.True(checkpointFile.Exists);

//        // make sure that Flush is called in the proper order
//        Assert.Equal(persistentStore.getLastFlushCount(), Matchers.lessThan(nonPersistentStore.getLastFlushCount()));

//        // the checkpoint file should contain an offset from the Persistent store only.
//        Dictionary<TopicPartition, long> checkpointedOffsets = checkpoint.read();
//        Assert.Equal(checkpointedOffsets, is (singletonMap(new TopicPartition(persistentStoreTopicName, 1), 124L)));
//    }

//    [Fact]
//    public void ShouldMaintainRegistrationOrderWhenReregistered()
//    { //throws IOException
//        checkpoint.write(emptyMap());

//        // set up ack'ed offsets
//        TopicPartition persistentTopicPartition = new TopicPartition(persistentStoreTopicName, 1);
//        TopicPartition nonPersistentTopicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            mkMap(mkEntry(persistentStoreName, persistentStoreTopicName),
//                  mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)),
//            changelogReader,
//            false,
//            logContext);
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
//        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
//        // de-registers the stores, but doesn't re-register them because
//        // the context isn't connected to our state manager
//        stateMgr.reinitializeStateStoresForPartitions(asList(nonPersistentTopicPartition, persistentTopicPartition),
//                                                      new MockInternalProcessorContext());
//        // register them in backward order
//        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

//        stateMgr.Flush();

//        // make sure that Flush is called in the proper order
//        Assert.True(persistentStore.flushed);
//        Assert.True(nonPersistentStore.flushed);
//        Assert.Equal(persistentStore.getLastFlushCount(), Matchers.lessThan(nonPersistentStore.getLastFlushCount()));
//    }

//    [Fact]
//    public void ShouldRegisterStoreWithoutLoggingEnabledAndNotBackedByATopic()
//    { //throws IOException
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            new TaskId(0, 1),
//            noPartitions,
//            false,
//            stateDirectory,
//            emptyMap(),
//            changelogReader,
//            false,
//            logContext);
//        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
//        Assert.NotNull(stateMgr.getStore(nonPersistentStoreName));
//    }

//    [Fact]
//    public void ShouldNotChangeOffsetsIfAckedOffsetsIsNull()
//    { //throws IOException
//        Dictionary<TopicPartition, long> offsets = singletonMap(persistentStorePartition, 99L);
//        checkpoint.write(offsets);

//        MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            emptyMap(),
//            changelogReader,
//            false,
//            logContext);
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
//        stateMgr.Close(true);
//        Dictionary<TopicPartition, long> read = checkpoint.read();
//        Assert.Equal(read, (offsets));
//    }

//    [Fact]
//    public void ShouldIgnoreIrrelevantLoadedCheckpoints()
//    { //throws IOException
//        Dictionary<TopicPartition, long> offsets = mkMap(
//            mkEntry(persistentStorePartition, 99L),
//            mkEntry(new TopicPartition("ignoreme", 1234), 12L)
//        );
//        checkpoint.write(offsets);

//        MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            singletonMap(persistentStoreName, persistentStorePartition.Topic),
//            changelogReader,
//            false,
//            logContext);
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

//        changelogReader.setRestoredOffsets(singletonMap(persistentStorePartition, 110L));

//        stateMgr.checkpoint(emptyMap());
//        stateMgr.Close(true);
//        Dictionary<TopicPartition, long> read = checkpoint.read();
//        Assert.Equal(read, (singletonMap(persistentStorePartition, 110L)));
//    }

//    [Fact]
//    public void ShouldOverrideLoadedCheckpointsWithRestoredCheckpoints()
//    { //throws IOException
//        Dictionary<TopicPartition, long> offsets = singletonMap(persistentStorePartition, 99L);
//        checkpoint.write(offsets);

//        MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            singletonMap(persistentStoreName, persistentStorePartition.Topic),
//            changelogReader,
//            false,
//            logContext);
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

//        changelogReader.setRestoredOffsets(singletonMap(persistentStorePartition, 110L));

//        stateMgr.checkpoint(emptyMap());
//        stateMgr.Close(true);
//        Dictionary<TopicPartition, long> read = checkpoint.read();
//        Assert.Equal(read, (singletonMap(persistentStorePartition, 110L)));
//    }

//    [Fact]
//    public void ShouldIgnoreIrrelevantRestoredCheckpoints()
//    { //throws IOException
//        Dictionary<TopicPartition, long> offsets = singletonMap(persistentStorePartition, 99L);
//        checkpoint.write(offsets);

//        MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            singletonMap(persistentStoreName, persistentStorePartition.Topic),
//            changelogReader,
//            false,
//            logContext);
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

//        // should ignore irrelevant topic partitions
//        changelogReader.setRestoredOffsets(mkMap(
//            mkEntry(persistentStorePartition, 110L),
//            mkEntry(new TopicPartition("sillytopic", 5000), 1234L)
//        ));

//        stateMgr.checkpoint(emptyMap());
//        stateMgr.Close(true);
//        Dictionary<TopicPartition, long> read = checkpoint.read();
//        Assert.Equal(read, (singletonMap(persistentStorePartition, 110L)));
//    }

//    [Fact]
//    public void ShouldOverrideRestoredOffsetsWithProcessedOffsets()
//    { //throws IOException
//        Dictionary<TopicPartition, long> offsets = singletonMap(persistentStorePartition, 99L);
//        checkpoint.write(offsets);

//        MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            singletonMap(persistentStoreName, persistentStorePartition.Topic),
//            changelogReader,
//            false,
//            logContext);
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

//        // should ignore irrelevant topic partitions
//        changelogReader.setRestoredOffsets(mkMap(
//            mkEntry(persistentStorePartition, 110L),
//            mkEntry(new TopicPartition("sillytopic", 5000), 1234L)
//        ));

//        // should ignore irrelevant topic partitions
//        stateMgr.checkpoint(mkMap(
//            mkEntry(persistentStorePartition, 220L),
//            mkEntry(new TopicPartition("ignoreme", 42), 9000L)
//        ));
//        stateMgr.Close(true);
//        Dictionary<TopicPartition, long> read = checkpoint.read();

//        // the checkpoint gets incremented to be the log position _after_ the committed offset
//        Assert.Equal(read, (singletonMap(persistentStorePartition, 221L)));
//    }

//    [Fact]
//    public void ShouldWriteCheckpointForPersistentLogEnabledStore()
//    { //throws IOException
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            singletonMap(persistentStore.Name(), persistentStoreTopicName),
//            changelogReader,
//            false,
//            logContext);
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

//        stateMgr.checkpoint(singletonMap(persistentStorePartition, 10L));
//        Dictionary<TopicPartition, long> read = checkpoint.read();
//        Assert.Equal(read, (singletonMap(persistentStorePartition, 11L)));
//    }

//    [Fact]
//    public void ShouldWriteCheckpointForStandbyReplica()
//    { //throws IOException
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            true, // standby
//            stateDirectory,
//            singletonMap(persistentStore.Name(), persistentStoreTopicName),
//            changelogReader,
//            false,
//            logContext);

//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
//        byte[] bytes = Serdes.Int().Serializer.Serialize("", 10);
//        stateMgr.updateStandbyStates(
//            persistentStorePartition,
//            singletonList(new ConsumeResult<>("", 0, 0L, bytes, bytes)),
//            888L
//        );

//        stateMgr.checkpoint(emptyMap());

//        Dictionary<TopicPartition, long> read = checkpoint.read();
//        Assert.Equal(read, (singletonMap(persistentStorePartition, 889L)));

//    }

//    [Fact]
//    public void ShouldNotWriteCheckpointForNonPersistent()
//    { //throws IOException
//        TopicPartition topicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            true, // standby
//            stateDirectory,
//            singletonMap(nonPersistentStoreName, nonPersistentStoreTopicName),
//            changelogReader,
//            false,
//            logContext);

//        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
//        stateMgr.checkpoint(singletonMap(topicPartition, 876L));

//        Dictionary<TopicPartition, long> read = checkpoint.read();
//        Assert.Equal(read, (emptyMap()));
//    }

//    [Fact]
//    public void ShouldNotWriteCheckpointForStoresWithoutChangelogTopic()
//    { //throws IOException
//        ProcessorStateManager stateMgr = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            true, // standby
//            stateDirectory,
//            emptyMap(),
//            changelogReader,
//            false,
//            logContext);

//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

//        stateMgr.checkpoint(singletonMap(persistentStorePartition, 987L));

//        Dictionary<TopicPartition, long> read = checkpoint.read();
//        Assert.Equal(read, (emptyMap()));
//    }

//    [Fact]
//    public void ShouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName()
//    { //throws IOException
//        ProcessorStateManager stateManager = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            emptyMap(),
//            changelogReader,
//            false,
//            logContext);

//        try
//        {
//            stateManager.register(new MockKeyValueStore(StateManagerUtil.CHECKPOINT_FILE_NAME, true), null);
//            Assert.True(false, "should have thrown illegal argument exception when store Name same as checkpoint file");
//        }
//        catch (ArgumentException e)
//        {
//            //pass
//        }
//    }

//    [Fact]
//    public void ShouldThrowIllegalArgumentExceptionOnRegisterWhenStoreHasAlreadyBeenRegistered()
//    { //throws IOException
//        ProcessorStateManager stateManager = new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            false,
//            stateDirectory,
//            emptyMap(),
//            changelogReader,
//            false,
//            logContext);

//        stateManager.register(mockKeyValueStore, null);

//        try
//        {
//            stateManager.register(mockKeyValueStore, null);
//            Assert.True(false, "should have thrown illegal argument exception when store with same Name already registered");
//        }
//        catch (ArgumentException e)
//        {
//            // pass
//        }

//    }

//    [Fact]
//    public void ShouldThrowProcessorStateExceptionOnFlushIfStoreThrowsAnException()
//    { //throws IOException

//        ProcessorStateManager stateManager = new ProcessorStateManager(
//            taskId,
//            Collections.singleton(changelogTopicPartition),
//            false,
//            stateDirectory,
//            singletonMap(storeName, changelogTopic),
//            changelogReader,
//            false,
//            logContext);

//        MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true)
//        {


//            public void Flush()
//        {
//            throw new RuntimeException("KABOOM!");
//        }
//    };
//    stateManager.register(stateStore, stateStore.stateRestoreCallback);

//        try {
//            stateManager.Flush();
//            Assert.True(false, "Should throw ProcessorStateException if store Flush throws exception");
//        } catch (ProcessorStateException e) {
//            // pass
//        }
//    }

//    [Fact]
//    public void ShouldThrowProcessorStateExceptionOnCloseIfStoreThrowsAnException()
//    { //throws IOException

//        ProcessorStateManager stateManager = new ProcessorStateManager(
//            taskId,
//            Collections.singleton(changelogTopicPartition),
//            false,
//            stateDirectory,
//            singletonMap(storeName, changelogTopic),
//            changelogReader,
//            false,
//            logContext);

//        MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true)
//        {


//            public void Close()
//        {
//            throw new RuntimeException("KABOOM!");
//        }
//    };
//    stateManager.register(stateStore, stateStore.stateRestoreCallback);

//        try {
//            stateManager.Close(true);
//            Assert.True(false, "Should throw ProcessorStateException if store Close throws exception");
//        } catch (ProcessorStateException e) {
//            // pass
//        }
//    }

//    // if the optional is absent, it'll throw an exception and fail the test.
    
//    [Fact]
//    public void ShouldLogAWarningIfCheckpointThrowsAnIOException()
//    {
//        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//        ProcessorStateManager stateMgr;
//        try
//        {
//            stateMgr = new ProcessorStateManager(
//                taskId,
//                noPartitions,
//                false,
//                stateDirectory,
//                singletonMap(persistentStore.Name(), persistentStoreTopicName),
//                changelogReader,
//                false,
//                logContext);
//        }
//        catch (IOException e)
//        {
//            e.printStackTrace();
//            throw new AssertionError(e);
//        }
//        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

//        stateDirectory.clean();
//        stateMgr.checkpoint(singletonMap(persistentStorePartition, 10L));
//        LogCaptureAppender.Unregister(appender);

//        bool foundExpectedLogMessage = false;
//        foreach (LogCaptureAppender.Event logEvent in appender.getEvents())
//        {
//            if ("WARN".equals(logEvent.GetLevel())
//                && logEvent.GetMessage().startsWith("process-state-manager-test Failed to write offset checkpoint file to [")
//                && logEvent.GetMessage().endsWith(".checkpoint]")
//                && logEvent.GetThrowableInfo().Get().startsWith("java.io.FileNotFoundException: "))
//            {

//                foundExpectedLogMessage = true;
//                break;
//            }
//        }
//        Assert.True(foundExpectedLogMessage);
//    }

//    [Fact]
//    public void ShouldFlushAllStoresEvenIfStoreThrowsException()
//    { //throws IOException
//        AtomicBoolean flushedStore = new AtomicBoolean(false);

//        MockKeyValueStore stateStore1 = new MockKeyValueStore(storeName, true)
//        {


//            public void Flush()
//        {
//            throw new RuntimeException("KABOOM!");
//        }
//    };
//    MockKeyValueStore stateStore2 = new MockKeyValueStore(storeName + "2", true)
//    {


//            public void Flush()
//    {
//        flushedStore.set(true);
//    }
//        };
//        ProcessorStateManager stateManager = new ProcessorStateManager(
//            taskId,
//            Collections.singleton(changelogTopicPartition),
//            false,
//            stateDirectory,
//            singletonMap(storeName, changelogTopic),
//            changelogReader,
//            false,
//            logContext);

//    stateManager.register(stateStore1, stateStore1.stateRestoreCallback);
//        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

//        try {
//            stateManager.Flush();
//        } catch (ProcessorStateException expected) { /* ignode */ }
//        Assert.True(flushedStore.Get());
//    }

//    [Fact]
//    public void ShouldCloseAllStoresEvenIfStoreThrowsExcepiton()
//    { //throws IOException

//        AtomicBoolean closedStore = new AtomicBoolean(false);

//        MockKeyValueStore stateStore1 = new MockKeyValueStore(storeName, true)
//        {


//            public void Close()
//        {
//            throw new RuntimeException("KABOOM!");
//        }
//    };
//    MockKeyValueStore stateStore2 = new MockKeyValueStore(storeName + "2", true)
//    {


//            public void Close()
//    {
//        closedStore.set(true);
//    }
//        };
//        ProcessorStateManager stateManager = new ProcessorStateManager(
//            taskId,
//            Collections.singleton(changelogTopicPartition),
//            false,
//            stateDirectory,
//            singletonMap(storeName, changelogTopic),
//            changelogReader,
//            false,
//            logContext);

//    stateManager.register(stateStore1, stateStore1.stateRestoreCallback);
//        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

//        try {
//            stateManager.Close(true);
//        } catch (ProcessorStateException expected) { /* ignode */ }
//        Assert.True(closedStore.Get());
//    }

//    [Fact]
//    public void ShouldDeleteCheckpointFileOnCreationIfEosEnabled()
//    { //throws IOException
//        checkpoint.write(singletonMap(new TopicPartition(persistentStoreTopicName, 1), 123L));
//        Assert.True(checkpointFile.Exists);

//        ProcessorStateManager stateManager = null;
//        try
//        {
//            stateManager = new ProcessorStateManager(
//                taskId,
//                noPartitions,
//                false,
//                stateDirectory,
//                emptyMap(),
//                changelogReader,
//                true,
//                logContext);

//            Assert.False(checkpointFile.Exists);
//        }
//        finally
//        {
//            if (stateManager != null)
//            {
//                stateManager.Close(true);
//            }
//        }
//    }

//    [Fact]
//    public void ShouldSuccessfullyReInitializeStateStoresWithEosDisable()
//    {// throws Exception
//        shouldSuccessfullyReInitializeStateStores(false);
//    }

//    [Fact]
//    public void ShouldSuccessfullyReInitializeStateStoresWithEosEnable()
//    {// throws Exception
//        shouldSuccessfullyReInitializeStateStores(true);
//    }

//    private void ShouldSuccessfullyReInitializeStateStores(bool eosEnabled)
//    {// throws Exception
//        string store2Name = "store2";
//        string store2Changelog = "store2-changelog";
//        TopicPartition store2Partition = new TopicPartition(store2Changelog, 0);
//        List<TopicPartition> changelogPartitions = asList(changelogTopicPartition, store2Partition);
//        Dictionary<string, string> storeToChangelog = mkMap(
//                mkEntry(storeName, changelogTopic),
//                mkEntry(store2Name, store2Changelog)
//        );

//        MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true);
//        MockKeyValueStore stateStore2 = new MockKeyValueStore(store2Name, true);

//        ProcessorStateManager stateManager = new ProcessorStateManager(
//            taskId,
//            changelogPartitions,
//            false,
//            stateDirectory,
//            storeToChangelog,
//            changelogReader,
//            eosEnabled,
//            logContext);

//        stateManager.register(stateStore, stateStore.stateRestoreCallback);
//        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

//        stateStore.initialized = false;
//        stateStore2.initialized = false;

//        stateManager.reinitializeStateStoresForPartitions(changelogPartitions, new NoOpProcessorContext()
//        {


//            public void register(IStateStore store, StateRestoreCallback stateRestoreCallback)
//        {
//            stateManager.register(store, stateRestoreCallback);
//        }
//    });

//        Assert.True(stateStore.initialized);
//        Assert.True(stateStore2.initialized);
//    }

//    private ProcessorStateManager GetStandByStateManager(TaskId taskId)
//    { //throws IOException
//        return new ProcessorStateManager(
//            taskId,
//            noPartitions,
//            true,
//            stateDirectory,
//            singletonMap(persistentStoreName, persistentStoreTopicName),
//            changelogReader,
//            false,
//            logContext);
//    }

//    private MockKeyValueStore GetPersistentStore()
//    {
//        return new MockKeyValueStore("persistentStore", true);
//    }

//    private MockKeyValueStore GetConverterStore()
//    {
//        return new ConverterStore("persistentStore", true);
//    }

//    private class ConverterStore : MockKeyValueStore, ITimestampedBytesStore
//    {
//        ConverterStore(string Name,
//                       bool Persistent)
//            : base(Name, Persistent)
//        {
//        }
//    }
//}
//}