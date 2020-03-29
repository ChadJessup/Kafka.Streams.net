/*






 *

 *





 */























































using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.Tasks;
using System.Collections.Generic;
using Xunit;

public class ProcessorStateManagerTest
{

    private HashSet<TopicPartition> noPartitions = Collections.emptySet();
    private string applicationId = "test-application";
    private string persistentStoreName = "persistentStore";
    private string nonPersistentStoreName = "nonPersistentStore";
    private string persistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, persistentStoreName);
    private string nonPersistentStoreTopicName = ProcessorStateManager.storeChangelogTopic(applicationId, nonPersistentStoreName);
    private MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    private MockKeyValueStore nonPersistentStore = new MockKeyValueStore(nonPersistentStoreName, false);
    private TopicPartition persistentStorePartition = new TopicPartition(persistentStoreTopicName, 1);
    private string storeName = "mockKeyValueStore";
    private string changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, storeName);
    private TopicPartition changelogTopicPartition = new TopicPartition(changelogTopic, 0);
    private TaskId taskId = new TaskId(0, 1);
    private MockChangelogReader changelogReader = new MockChangelogReader();
    private MockKeyValueStore mockKeyValueStore = new MockKeyValueStore(storeName, true);
    private byte[] key = new byte[] { 0x0, 0x0, 0x0, 0x1 };
    private byte[] value = "the-value".getBytes(StandardCharsets.UTF_8);
    private ConsumeResult<byte[], byte[]> consumerRecord = new ConsumeResult<>(changelogTopic, 0, 0, key, value);
    private LogContext logContext = new LogContext("process-state-manager-test ");

    private File baseDir;
    private File checkpointFile;
    private OffsetCheckpoint checkpoint;
    private StateDirectory stateDirectory;


    public void Setup()
    {
        baseDir = TestUtils.tempDirectory();

        stateDirectory = new StateDirectory(new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        put(StreamsConfig.STATE_DIR_CONFIG, baseDir.getPath());
    }
}), new MockTime(), true);
        checkpointFile = new File(stateDirectory.directoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME);
checkpoint = new OffsetCheckpoint(checkpointFile);
    }

    
    public void Cleanup()
{ //throws IOException
    Utils.delete(baseDir);
}

[Xunit.Fact]
public void ShouldRestoreStoreWithBatchingRestoreSpecification()
{// throws Exception
    TaskId taskId = new TaskId(0, 2);
    MockBatchingStateRestoreListener batchingRestoreCallback = new MockBatchingStateRestoreListener();

    KeyValuePair<byte[], byte[]> expectedKeyValue = KeyValuePair.Create(key, value);

    MockKeyValueStore persistentStore = getPersistentStore();
    ProcessorStateManager stateMgr = getStandByStateManager(taskId);

    try
    {
        stateMgr.register(persistentStore, batchingRestoreCallback);
        stateMgr.updateStandbyStates(
            persistentStorePartition,
            singletonList(consumerRecord),
            consumerRecord.Offset
        );
        Assert.Equal(batchingRestoreCallback.getRestoredRecords().Count, is (1));
        Assert.True(batchingRestoreCallback.getRestoredRecords().Contains(expectedKeyValue));
    }
    finally
    {
        stateMgr.close(true);
    }
}

[Xunit.Fact]
public void ShouldRestoreStoreWithSinglePutRestoreSpecification()
{// throws Exception
    TaskId taskId = new TaskId(0, 2);
    int intKey = 1;

    MockKeyValueStore persistentStore = getPersistentStore();
    ProcessorStateManager stateMgr = getStandByStateManager(taskId);

    try
    {
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        stateMgr.updateStandbyStates(
            persistentStorePartition,
            singletonList(consumerRecord),
            consumerRecord.Offset
        );
        Assert.Equal(persistentStore.keys.Count, (1));
        Assert.True(persistentStore.keys.Contains(intKey));
        Assert.Equal(9, persistentStore.values.get(0).Length);
    }
    finally
    {
        stateMgr.close(true);
    }
}

[Xunit.Fact]
public void ShouldConvertDataOnRestoreIfStoreImplementsTimestampedBytesStore()
{// throws Exception
    TaskId taskId = new TaskId(0, 2);
    int intKey = 1;

    MockKeyValueStore persistentStore = getConverterStore();
    ProcessorStateManager stateMgr = getStandByStateManager(taskId);

    try
    {
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        stateMgr.updateStandbyStates(
            persistentStorePartition,
            singletonList(consumerRecord),
            consumerRecord.Offset
        );
        Assert.Equal(persistentStore.keys.Count, is (1));
        Assert.True(persistentStore.keys.Contains(intKey));
        Assert.Equal(17, persistentStore.values.get(0).Length);
    }
    finally
    {
        stateMgr.close(true);
    }
}

[Xunit.Fact]
public void TestRegisterPersistentStore()
{ //throws IOException
    TaskId taskId = new TaskId(0, 2);

    MockKeyValueStore persistentStore = getPersistentStore();
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        mkMap(
            mkEntry(persistentStoreName, persistentStoreTopicName),
            mkEntry(nonPersistentStoreName, nonPersistentStoreName)
        ),
        changelogReader,
        false,
        logContext);

    try
    {
        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        Assert.True(changelogReader.wasRegistered(new TopicPartition(persistentStoreTopicName, 2)));
    }
    finally
    {
        stateMgr.close(true);
    }
}

[Xunit.Fact]
public void TestRegisterNonPersistentStore()
{ //throws IOException
    MockKeyValueStore nonPersistentStore =
        new MockKeyValueStore(nonPersistentStoreName, false); // non persistent store
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        new TaskId(0, 2),
        noPartitions,
        false,
        stateDirectory,
        mkMap(
            mkEntry(persistentStoreName, persistentStoreTopicName),
            mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)
        ),
        changelogReader,
        false,
        logContext);

    try
    {
        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
        Assert.True(changelogReader.wasRegistered(new TopicPartition(nonPersistentStoreTopicName, 2)));
    }
    finally
    {
        stateMgr.close(true);
    }
}

[Xunit.Fact]
public void TestChangeLogOffsets()
{ //throws IOException
    TaskId taskId = new TaskId(0, 0);
    long storeTopic1LoadedCheckpoint = 10L;
    string storeName1 = "store1";
    string storeName2 = "store2";
    string storeName3 = "store3";

    string storeTopicName1 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName1);
    string storeTopicName2 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName2);
    string storeTopicName3 = ProcessorStateManager.storeChangelogTopic(applicationId, storeName3);

    Dictionary<string, string> storeToChangelogTopic = new HashMap<>();
    storeToChangelogTopic.put(storeName1, storeTopicName1);
    storeToChangelogTopic.put(storeName2, storeTopicName2);
    storeToChangelogTopic.put(storeName3, storeTopicName3);

    OffsetCheckpoint checkpoint = new OffsetCheckpoint(
        new File(stateDirectory.directoryForTask(taskId), StateManagerUtil.CHECKPOINT_FILE_NAME)
    );
    checkpoint.write(singletonMap(new TopicPartition(storeTopicName1, 0), storeTopic1LoadedCheckpoint));

    TopicPartition partition1 = new TopicPartition(storeTopicName1, 0);
    TopicPartition partition2 = new TopicPartition(storeTopicName2, 0);
    TopicPartition partition3 = new TopicPartition(storeTopicName3, 1);

    MockKeyValueStore store1 = new MockKeyValueStore(storeName1, true);
    MockKeyValueStore store2 = new MockKeyValueStore(storeName2, true);
    MockKeyValueStore store3 = new MockKeyValueStore(storeName3, true);

    // if there is a source partition, inherit the partition id
    HashSet<TopicPartition> sourcePartitions = Utils.mkSet(new TopicPartition(storeTopicName3, 1));

    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        sourcePartitions,
        true, // standby
        stateDirectory,
        storeToChangelogTopic,
        changelogReader,
        false,
        logContext);

    try
    {
        stateMgr.register(store1, store1.stateRestoreCallback);
        stateMgr.register(store2, store2.stateRestoreCallback);
        stateMgr.register(store3, store3.stateRestoreCallback);

        Dictionary<TopicPartition, long> changeLogOffsets = stateMgr.checkpointed();

        Assert.Equal(3, changeLogOffsets.Count);
        Assert.True(changeLogOffsets.containsKey(partition1));
        Assert.True(changeLogOffsets.containsKey(partition2));
        Assert.True(changeLogOffsets.containsKey(partition3));
        Assert.Equal(storeTopic1LoadedCheckpoint, (long)changeLogOffsets.get(partition1));
        Assert.Equal(-1L, (long)changeLogOffsets.get(partition2));
        Assert.Equal(-1L, (long)changeLogOffsets.get(partition3));

    }
    finally
    {
        stateMgr.close(true);
    }
}

[Xunit.Fact]
public void TestGetStore()
{ //throws IOException
    MockKeyValueStore mockKeyValueStore = new MockKeyValueStore(nonPersistentStoreName, false);
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        new TaskId(0, 1),
        noPartitions,
        false,
        stateDirectory,
        emptyMap(),
        changelogReader,
        false,
        logContext);
    try
    {
        stateMgr.register(mockKeyValueStore, mockKeyValueStore.stateRestoreCallback);

        assertNull(stateMgr.getStore("noSuchStore"));
        Assert.Equal(mockKeyValueStore, stateMgr.getStore(nonPersistentStoreName));

    }
    finally
    {
        stateMgr.close(true);
    }
}

[Xunit.Fact]
public void TestFlushAndClose()
{ //throws IOException
    checkpoint.write(emptyMap());

    // set up ack'ed offsets
    HashDictionary<TopicPartition, long> ackedOffsets = new HashMap<>();
    ackedOffsets.put(new TopicPartition(persistentStoreTopicName, 1), 123L);
    ackedOffsets.put(new TopicPartition(nonPersistentStoreTopicName, 1), 456L);
    ackedOffsets.put(new TopicPartition(ProcessorStateManager.storeChangelogTopic(applicationId, "otherTopic"), 1), 789L);

    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        mkMap(mkEntry(persistentStoreName, persistentStoreTopicName),
              mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)),
        changelogReader,
        false,
        logContext);
    try
    {
        // make sure the checkpoint file is not written yet
        Assert.False(checkpointFile.exists());

        stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
        stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
    }
    finally
    {
        // close the state manager with the ack'ed offsets
        stateMgr.flush();
        stateMgr.checkpoint(ackedOffsets);
        stateMgr.close(true);
    }
    // make sure all stores are closed, and the checkpoint file is written.
    Assert.True(persistentStore.flushed);
    Assert.True(persistentStore.closed);
    Assert.True(nonPersistentStore.flushed);
    Assert.True(nonPersistentStore.closed);
    Assert.True(checkpointFile.exists());

    // make sure that flush is called in the proper order
    Assert.Equal(persistentStore.getLastFlushCount(), Matchers.lessThan(nonPersistentStore.getLastFlushCount()));

    // the checkpoint file should contain an offset from the persistent store only.
    Dictionary<TopicPartition, long> checkpointedOffsets = checkpoint.read();
    Assert.Equal(checkpointedOffsets, is (singletonMap(new TopicPartition(persistentStoreTopicName, 1), 124L)));
}

[Xunit.Fact]
public void ShouldMaintainRegistrationOrderWhenReregistered()
{ //throws IOException
    checkpoint.write(emptyMap());

    // set up ack'ed offsets
    TopicPartition persistentTopicPartition = new TopicPartition(persistentStoreTopicName, 1);
    TopicPartition nonPersistentTopicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        mkMap(mkEntry(persistentStoreName, persistentStoreTopicName),
              mkEntry(nonPersistentStoreName, nonPersistentStoreTopicName)),
        changelogReader,
        false,
        logContext);
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
    stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
    // de-registers the stores, but doesn't re-register them because
    // the context isn't connected to our state manager
    stateMgr.reinitializeStateStoresForPartitions(asList(nonPersistentTopicPartition, persistentTopicPartition),
                                                  new MockInternalProcessorContext());
    // register them in backward order
    stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

    stateMgr.flush();

    // make sure that flush is called in the proper order
    Assert.True(persistentStore.flushed);
    Assert.True(nonPersistentStore.flushed);
    Assert.Equal(persistentStore.getLastFlushCount(), Matchers.lessThan(nonPersistentStore.getLastFlushCount()));
}

[Xunit.Fact]
public void ShouldRegisterStoreWithoutLoggingEnabledAndNotBackedByATopic()
{ //throws IOException
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        new TaskId(0, 1),
        noPartitions,
        false,
        stateDirectory,
        emptyMap(),
        changelogReader,
        false,
        logContext);
    stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
    assertNotNull(stateMgr.getStore(nonPersistentStoreName));
}

[Xunit.Fact]
public void ShouldNotChangeOffsetsIfAckedOffsetsIsNull()
{ //throws IOException
    Dictionary<TopicPartition, long> offsets = singletonMap(persistentStorePartition, 99L);
    checkpoint.write(offsets);

    MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        emptyMap(),
        changelogReader,
        false,
        logContext);
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
    stateMgr.close(true);
    Dictionary<TopicPartition, long> read = checkpoint.read();
    Assert.Equal(read, (offsets));
}

[Xunit.Fact]
public void ShouldIgnoreIrrelevantLoadedCheckpoints()
{ //throws IOException
    Dictionary<TopicPartition, long> offsets = mkMap(
        mkEntry(persistentStorePartition, 99L),
        mkEntry(new TopicPartition("ignoreme", 1234), 12L)
    );
    checkpoint.write(offsets);

    MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        singletonMap(persistentStoreName, persistentStorePartition.topic()),
        changelogReader,
        false,
        logContext);
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

    changelogReader.setRestoredOffsets(singletonMap(persistentStorePartition, 110L));

    stateMgr.checkpoint(emptyMap());
    stateMgr.close(true);
    Dictionary<TopicPartition, long> read = checkpoint.read();
    Assert.Equal(read, (singletonMap(persistentStorePartition, 110L)));
}

[Xunit.Fact]
public void ShouldOverrideLoadedCheckpointsWithRestoredCheckpoints()
{ //throws IOException
    Dictionary<TopicPartition, long> offsets = singletonMap(persistentStorePartition, 99L);
    checkpoint.write(offsets);

    MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        singletonMap(persistentStoreName, persistentStorePartition.topic()),
        changelogReader,
        false,
        logContext);
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

    changelogReader.setRestoredOffsets(singletonMap(persistentStorePartition, 110L));

    stateMgr.checkpoint(emptyMap());
    stateMgr.close(true);
    Dictionary<TopicPartition, long> read = checkpoint.read();
    Assert.Equal(read, (singletonMap(persistentStorePartition, 110L)));
}

[Xunit.Fact]
public void ShouldIgnoreIrrelevantRestoredCheckpoints()
{ //throws IOException
    Dictionary<TopicPartition, long> offsets = singletonMap(persistentStorePartition, 99L);
    checkpoint.write(offsets);

    MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        singletonMap(persistentStoreName, persistentStorePartition.topic()),
        changelogReader,
        false,
        logContext);
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

    // should ignore irrelevant topic partitions
    changelogReader.setRestoredOffsets(mkMap(
        mkEntry(persistentStorePartition, 110L),
        mkEntry(new TopicPartition("sillytopic", 5000), 1234L)
    ));

    stateMgr.checkpoint(emptyMap());
    stateMgr.close(true);
    Dictionary<TopicPartition, long> read = checkpoint.read();
    Assert.Equal(read, (singletonMap(persistentStorePartition, 110L)));
}

[Xunit.Fact]
public void ShouldOverrideRestoredOffsetsWithProcessedOffsets()
{ //throws IOException
    Dictionary<TopicPartition, long> offsets = singletonMap(persistentStorePartition, 99L);
    checkpoint.write(offsets);

    MockKeyValueStore persistentStore = new MockKeyValueStore(persistentStoreName, true);
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        singletonMap(persistentStoreName, persistentStorePartition.topic()),
        changelogReader,
        false,
        logContext);
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

    // should ignore irrelevant topic partitions
    changelogReader.setRestoredOffsets(mkMap(
        mkEntry(persistentStorePartition, 110L),
        mkEntry(new TopicPartition("sillytopic", 5000), 1234L)
    ));

    // should ignore irrelevant topic partitions
    stateMgr.checkpoint(mkMap(
        mkEntry(persistentStorePartition, 220L),
        mkEntry(new TopicPartition("ignoreme", 42), 9000L)
    ));
    stateMgr.close(true);
    Dictionary<TopicPartition, long> read = checkpoint.read();

    // the checkpoint gets incremented to be the log position _after_ the committed offset
    Assert.Equal(read, (singletonMap(persistentStorePartition, 221L)));
}

[Xunit.Fact]
public void ShouldWriteCheckpointForPersistentLogEnabledStore()
{ //throws IOException
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        singletonMap(persistentStore.name(), persistentStoreTopicName),
        changelogReader,
        false,
        logContext);
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

    stateMgr.checkpoint(singletonMap(persistentStorePartition, 10L));
    Dictionary<TopicPartition, long> read = checkpoint.read();
    Assert.Equal(read, (singletonMap(persistentStorePartition, 11L)));
}

[Xunit.Fact]
public void ShouldWriteCheckpointForStandbyReplica()
{ //throws IOException
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        true, // standby
        stateDirectory,
        singletonMap(persistentStore.name(), persistentStoreTopicName),
        changelogReader,
        false,
        logContext);

    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);
    byte[] bytes = Serdes.Int().Serializer.serialize("", 10);
    stateMgr.updateStandbyStates(
        persistentStorePartition,
        singletonList(new ConsumeResult<>("", 0, 0L, bytes, bytes)),
        888L
    );

    stateMgr.checkpoint(emptyMap());

    Dictionary<TopicPartition, long> read = checkpoint.read();
    Assert.Equal(read, (singletonMap(persistentStorePartition, 889L)));

}

[Xunit.Fact]
public void ShouldNotWriteCheckpointForNonPersistent()
{ //throws IOException
    TopicPartition topicPartition = new TopicPartition(nonPersistentStoreTopicName, 1);

    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        true, // standby
        stateDirectory,
        singletonMap(nonPersistentStoreName, nonPersistentStoreTopicName),
        changelogReader,
        false,
        logContext);

    stateMgr.register(nonPersistentStore, nonPersistentStore.stateRestoreCallback);
    stateMgr.checkpoint(singletonMap(topicPartition, 876L));

    Dictionary<TopicPartition, long> read = checkpoint.read();
    Assert.Equal(read, (emptyMap()));
}

[Xunit.Fact]
public void ShouldNotWriteCheckpointForStoresWithoutChangelogTopic()
{ //throws IOException
    ProcessorStateManager stateMgr = new ProcessorStateManager(
        taskId,
        noPartitions,
        true, // standby
        stateDirectory,
        emptyMap(),
        changelogReader,
        false,
        logContext);

    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

    stateMgr.checkpoint(singletonMap(persistentStorePartition, 987L));

    Dictionary<TopicPartition, long> read = checkpoint.read();
    Assert.Equal(read, (emptyMap()));
}

[Xunit.Fact]
public void ShouldThrowIllegalArgumentExceptionIfStoreNameIsSameAsCheckpointFileName()
{ //throws IOException
    ProcessorStateManager stateManager = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        emptyMap(),
        changelogReader,
        false,
        logContext);

    try
    {
        stateManager.register(new MockKeyValueStore(StateManagerUtil.CHECKPOINT_FILE_NAME, true), null);
        Assert.True(false, "should have thrown illegal argument exception when store name same as checkpoint file");
    }
    catch (IllegalArgumentException e)
    {
        //pass
    }
}

[Xunit.Fact]
public void ShouldThrowIllegalArgumentExceptionOnRegisterWhenStoreHasAlreadyBeenRegistered()
{ //throws IOException
    ProcessorStateManager stateManager = new ProcessorStateManager(
        taskId,
        noPartitions,
        false,
        stateDirectory,
        emptyMap(),
        changelogReader,
        false,
        logContext);

    stateManager.register(mockKeyValueStore, null);

    try
    {
        stateManager.register(mockKeyValueStore, null);
        Assert.True(false, "should have thrown illegal argument exception when store with same name already registered");
    }
    catch (IllegalArgumentException e)
    {
        // pass
    }

}

[Xunit.Fact]
public void ShouldThrowProcessorStateExceptionOnFlushIfStoreThrowsAnException()
{ //throws IOException

    ProcessorStateManager stateManager = new ProcessorStateManager(
        taskId,
        Collections.singleton(changelogTopicPartition),
        false,
        stateDirectory,
        singletonMap(storeName, changelogTopic),
        changelogReader,
        false,
        logContext);

    MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true)
    {


            public void flush()
    {
        throw new RuntimeException("KABOOM!");
    }
};
stateManager.register(stateStore, stateStore.stateRestoreCallback);

        try {
            stateManager.flush();
            Assert.True(false, "Should throw ProcessorStateException if store flush throws exception");
        } catch (ProcessorStateException e) {
            // pass
        }
    }

    [Xunit.Fact]
public void ShouldThrowProcessorStateExceptionOnCloseIfStoreThrowsAnException()
{ //throws IOException

    ProcessorStateManager stateManager = new ProcessorStateManager(
        taskId,
        Collections.singleton(changelogTopicPartition),
        false,
        stateDirectory,
        singletonMap(storeName, changelogTopic),
        changelogReader,
        false,
        logContext);

    MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true)
    {


            public void close()
    {
        throw new RuntimeException("KABOOM!");
    }
};
stateManager.register(stateStore, stateStore.stateRestoreCallback);

        try {
            stateManager.close(true);
            Assert.True(false, "Should throw ProcessorStateException if store close throws exception");
        } catch (ProcessorStateException e) {
            // pass
        }
    }

    // if the optional is absent, it'll throw an exception and fail the test.
    
    [Xunit.Fact]
public void ShouldLogAWarningIfCheckpointThrowsAnIOException()
{
    LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

    ProcessorStateManager stateMgr;
    try
    {
        stateMgr = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            singletonMap(persistentStore.name(), persistentStoreTopicName),
            changelogReader,
            false,
            logContext);
    }
    catch (IOException e)
    {
        e.printStackTrace();
        throw new AssertionError(e);
    }
    stateMgr.register(persistentStore, persistentStore.stateRestoreCallback);

    stateDirectory.clean();
    stateMgr.checkpoint(singletonMap(persistentStorePartition, 10L));
    LogCaptureAppender.Unregister(appender);

    bool foundExpectedLogMessage = false;
    foreach (LogCaptureAppender.Event logEvent in appender.getEvents())
    {
        if ("WARN".equals(logEvent.GetLevel())
            && logEvent.GetMessage().startsWith("process-state-manager-test Failed to write offset checkpoint file to [")
            && logEvent.GetMessage().endsWith(".checkpoint]")
            && logEvent.GetThrowableInfo().get().startsWith("java.io.FileNotFoundException: "))
        {

            foundExpectedLogMessage = true;
            break;
        }
    }
    Assert.True(foundExpectedLogMessage);
}

[Xunit.Fact]
public void ShouldFlushAllStoresEvenIfStoreThrowsException()
{ //throws IOException
    AtomicBoolean flushedStore = new AtomicBoolean(false);

    MockKeyValueStore stateStore1 = new MockKeyValueStore(storeName, true)
    {


            public void flush()
    {
        throw new RuntimeException("KABOOM!");
    }
};
MockKeyValueStore stateStore2 = new MockKeyValueStore(storeName + "2", true)
{


            public void Flush()
{
    flushedStore.set(true);
}
        };
        ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

stateManager.register(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.flush();
        } catch (ProcessorStateException expected) { /* ignode */ }
        Assert.True(flushedStore.get());
    }

    [Xunit.Fact]
public void ShouldCloseAllStoresEvenIfStoreThrowsExcepiton()
{ //throws IOException

    AtomicBoolean closedStore = new AtomicBoolean(false);

    MockKeyValueStore stateStore1 = new MockKeyValueStore(storeName, true)
    {


            public void close()
    {
        throw new RuntimeException("KABOOM!");
    }
};
MockKeyValueStore stateStore2 = new MockKeyValueStore(storeName + "2", true)
{


            public void Close()
{
    closedStore.set(true);
}
        };
        ProcessorStateManager stateManager = new ProcessorStateManager(
            taskId,
            Collections.singleton(changelogTopicPartition),
            false,
            stateDirectory,
            singletonMap(storeName, changelogTopic),
            changelogReader,
            false,
            logContext);

stateManager.register(stateStore1, stateStore1.stateRestoreCallback);
        stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

        try {
            stateManager.close(true);
        } catch (ProcessorStateException expected) { /* ignode */ }
        Assert.True(closedStore.get());
    }

    [Xunit.Fact]
public void ShouldDeleteCheckpointFileOnCreationIfEosEnabled()
{ //throws IOException
    checkpoint.write(singletonMap(new TopicPartition(persistentStoreTopicName, 1), 123L));
    Assert.True(checkpointFile.exists());

    ProcessorStateManager stateManager = null;
    try
    {
        stateManager = new ProcessorStateManager(
            taskId,
            noPartitions,
            false,
            stateDirectory,
            emptyMap(),
            changelogReader,
            true,
            logContext);

        Assert.False(checkpointFile.exists());
    }
    finally
    {
        if (stateManager != null)
        {
            stateManager.close(true);
        }
    }
}

[Xunit.Fact]
public void ShouldSuccessfullyReInitializeStateStoresWithEosDisable()
{// throws Exception
    shouldSuccessfullyReInitializeStateStores(false);
}

[Xunit.Fact]
public void ShouldSuccessfullyReInitializeStateStoresWithEosEnable()
{// throws Exception
    shouldSuccessfullyReInitializeStateStores(true);
}

private void ShouldSuccessfullyReInitializeStateStores(bool eosEnabled)
{// throws Exception
    string store2Name = "store2";
    string store2Changelog = "store2-changelog";
    TopicPartition store2Partition = new TopicPartition(store2Changelog, 0);
    List<TopicPartition> changelogPartitions = asList(changelogTopicPartition, store2Partition);
    Dictionary<string, string> storeToChangelog = mkMap(
            mkEntry(storeName, changelogTopic),
            mkEntry(store2Name, store2Changelog)
    );

    MockKeyValueStore stateStore = new MockKeyValueStore(storeName, true);
    MockKeyValueStore stateStore2 = new MockKeyValueStore(store2Name, true);

    ProcessorStateManager stateManager = new ProcessorStateManager(
        taskId,
        changelogPartitions,
        false,
        stateDirectory,
        storeToChangelog,
        changelogReader,
        eosEnabled,
        logContext);

    stateManager.register(stateStore, stateStore.stateRestoreCallback);
    stateManager.register(stateStore2, stateStore2.stateRestoreCallback);

    stateStore.initialized = false;
    stateStore2.initialized = false;

    stateManager.reinitializeStateStoresForPartitions(changelogPartitions, new NoOpProcessorContext()
    {


            public void register(StateStore store, StateRestoreCallback stateRestoreCallback)
    {
        stateManager.register(store, stateRestoreCallback);
    }
});

        Assert.True(stateStore.initialized);
        Assert.True(stateStore2.initialized);
    }

    private ProcessorStateManager GetStandByStateManager(TaskId taskId)
{ //throws IOException
    return new ProcessorStateManager(
        taskId,
        noPartitions,
        true,
        stateDirectory,
        singletonMap(persistentStoreName, persistentStoreTopicName),
        changelogReader,
        false,
        logContext);
}

private MockKeyValueStore GetPersistentStore()
{
    return new MockKeyValueStore("persistentStore", true);
}

private MockKeyValueStore GetConverterStore()
{
    return new ConverterStore("persistentStore", true);
}

private class ConverterStore : MockKeyValueStore, ITimestampedBytesStore
{
    ConverterStore(string name,
                   bool persistent)
        : base(name, persistent)
    {
    }
}
}
