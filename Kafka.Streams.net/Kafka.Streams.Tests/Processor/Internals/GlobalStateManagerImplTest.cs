namespace Kafka.Streams.Tests.Processor.Internals
{
    //public class GlobalStateManagerImplTest {


    //    private MockTime time = new MockTime();
    //    private TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
    //    private MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    //    private string storeName1 = "t1-store";
    //    private string storeName2 = "t2-store";
    //    private string storeName3 = "t3-store";
    //    private string storeName4 = "t4-store";
    //    private TopicPartition t1 = new TopicPartition("t1", 1);
    //    private TopicPartition t2 = new TopicPartition("t2", 1);
    //    private TopicPartition t3 = new TopicPartition("t3", 1);
    //    private TopicPartition t4 = new TopicPartition("t4", 1);
    //    private GlobalStateManagerImpl stateManager;
    //    private StateDirectory stateDirectory;
    //    private StreamsConfig streamsConfig;
    //    private NoOpReadOnlyStore<object, object> store1, store2, store3, store4;
    //    private MockConsumer<byte[], byte[]> consumer;
    //    private File checkpointFile;
    //    private ProcessorTopology topology;
    //    private InternalMockProcessorContext processorContext;

    //    static ProcessorTopology withGlobalStores(List<IStateStore> stateStores,
    //                                              Dictionary<string, string> storeToChangelogTopic) {
    //        return new ProcessorTopology(Collections.emptyList(),
    //                                     Collections.emptyMap(),
    //                                     Collections.emptyMap(),
    //                                     Collections.emptyList(),
    //                                     stateStores,
    //                                     storeToChangelogTopic,
    //                                     Collections.emptySet());
    //    }


    //    public void before() {
    //        Dictionary<string, string> storeToTopic = new HashMap<>();

    //        storeToTopic.put(storeName1, t1.Topic);
    //        storeToTopic.put(storeName2, t2.Topic);
    //        storeToTopic.put(storeName3, t3.Topic);
    //        storeToTopic.put(storeName4, t4.Topic);

    //        store1 = new NoOpReadOnlyStore<>(storeName1, true);
    //        store2 = new ConverterStore<>(storeName2, true);
    //        store3 = new NoOpReadOnlyStore<>(storeName3);
    //        store4 = new NoOpReadOnlyStore<>(storeName4);

    //        topology = withGlobalStores(asList(store1, store2, store3, store4), storeToTopic);

    //        streamsConfig = new StreamsConfig(new StreamsConfig() {
    //            {
    //                put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
    //                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    //                put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
    //            }
    //        });
    //        stateDirectory = new StateDirectory(streamsConfig, time, true);
    //        consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
    //        stateManager = new GlobalStateManagerImpl(
    //            new LogContext("test"),
    //            topology,
    //            consumer,
    //            stateDirectory,
    //            stateRestoreListener,
    //            streamsConfig);
    //        processorContext = new InternalMockProcessorContext(stateDirectory.globalStateDir(), streamsConfig);
    //        stateManager.setGlobalProcessorContext(processorContext);
    //        checkpointFile = new File(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME);
    //    }


    //    public void after(){ //throws IOException
    //        stateDirectory.unlockGlobalState();
    //    }

    //    [Xunit.Fact]
    //    public void shouldLockGlobalStateDirectory() {
    //        stateManager.initialize();
    //        Assert.True(new File(stateDirectory.globalStateDir(), ".Lock").Exists);
    //    }

    //    [Xunit.Fact]// (expected = LockException)
    //    public void shouldThrowLockExceptionIfCantGetLock(){ //throws IOException
    //        StateDirectory stateDir = new StateDirectory(streamsConfig, time, true);
    //        try {
    //            stateDir.lockGlobalState();
    //            stateManager.initialize();
    //        } finally {
    //            stateDir.unlockGlobalState();
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldReadCheckpointOffsets(){ //throws IOException
    //        Dictionary<TopicPartition, long> expected = writeCheckpoint();

    //        stateManager.initialize();
    //        Dictionary<TopicPartition, long> offsets = stateManager.checkpointed();
    //        Assert.Equal(expected, offsets);
    //    }

    //    [Xunit.Fact]
    //    public void shouldNotDeleteCheckpointFileAfterLoaded(){ //throws IOException
    //        writeCheckpoint();
    //        stateManager.initialize();
    //        Assert.True(checkpointFile.Exists);
    //    }

    //    [Xunit.Fact]// (expected = StreamsException)
    //    public void shouldThrowStreamsExceptionIfFailedToReadCheckpointedOffsets(){ //throws IOException
    //        writeCorruptCheckpoint();
    //        stateManager.initialize();
    //    }

    //    [Xunit.Fact]
    //    public void shouldInitializeStateStores() {
    //        stateManager.initialize();
    //        Assert.True(store1.initialized);
    //        Assert.True(store2.initialized);
    //    }

    //    [Xunit.Fact]
    //    public void shouldReturnInitializedStoreNames() {
    //        HashSet<string> storeNames = stateManager.initialize();
    //        Assert.Equal(Utils.mkSet(storeName1, storeName2, storeName3, storeName4), storeNames);
    //    }

    //    [Xunit.Fact]
    //    public void shouldThrowIllegalArgumentIfTryingToRegisterStoreThatIsNotGlobal() {
    //        stateManager.initialize();

    //        try {
    //            stateManager.register(new NoOpReadOnlyStore<>("not-in-topology"), stateRestoreCallback);
    //            Assert.True(false, "should have raised an illegal argument exception as store is not in the topology");
    //        } catch (ArgumentException e) {
    //            // pass
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldThrowIllegalArgumentExceptionIfAttemptingToRegisterStoreTwice() {
    //        stateManager.initialize();
    //        initializeConsumer(2, 0, t1);
    //        stateManager.register(store1, stateRestoreCallback);
    //        try {
    //            stateManager.register(store1, stateRestoreCallback);
    //            Assert.True(false, "should have raised an illegal argument exception as store has already been registered");
    //        } catch (ArgumentException e) {
    //            // pass
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldThrowStreamsExceptionIfNoPartitionsFoundForStore() {
    //        stateManager.initialize();
    //        try {
    //            stateManager.register(store1, stateRestoreCallback);
    //            Assert.True(false, "Should have raised a StreamsException as there are no partition for the store");
    //        } catch (StreamsException e) {
    //            // pass
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldNotConvertValuesIfStoreDoesNotImplementTimestampedBytesStore() {
    //        initializeConsumer(1, 0, t1);

    //        stateManager.initialize();
    //        stateManager.register(store1, stateRestoreCallback);

    //        KeyValuePair<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.Get(0);
    //        Assert.Equal(3, restoredRecord.key.Length);
    //        Assert.Equal(5, restoredRecord.value.Length);
    //    }

    //    [Xunit.Fact]
    //    public void shouldNotConvertValuesIfInnerStoreDoesNotImplementTimestampedBytesStore() {
    //        initializeConsumer(1, 0, t1);

    //        stateManager.initialize();
    //        stateManager.register(
    //            new WrappedStateStore<NoOpReadOnlyStore<object, object>, object, object>(store1) {
    //            },
    //            stateRestoreCallback
    //        );

    //        KeyValuePair<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.Get(0);
    //        Assert.Equal(3, restoredRecord.key.Length);
    //        Assert.Equal(5, restoredRecord.value.Length);
    //    }

    //    [Xunit.Fact]
    //    public void shouldConvertValuesIfStoreImplementsTimestampedBytesStore() {
    //        initializeConsumer(1, 0, t2);

    //        stateManager.initialize();
    //        stateManager.register(store2, stateRestoreCallback);

    //        KeyValuePair<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.Get(0);
    //        Assert.Equal(3, restoredRecord.key.Length);
    //        Assert.Equal(13, restoredRecord.value.Length);
    //    }

    //    [Xunit.Fact]
    //    public void shouldConvertValuesIfInnerStoreImplementsTimestampedBytesStore() {
    //        initializeConsumer(1, 0, t2);

    //        stateManager.initialize();
    //        stateManager.register(
    //            new WrappedStateStore<NoOpReadOnlyStore<object, object>, object, object>(store2) {
    //            },
    //            stateRestoreCallback
    //        );

    //        KeyValuePair<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.Get(0);
    //        Assert.Equal(3, restoredRecord.key.Length);
    //        Assert.Equal(13, restoredRecord.value.Length);
    //    }

    //    [Xunit.Fact]
    //    public void shouldRestoreRecordsUpToHighwatermark() {
    //        initializeConsumer(2, 0, t1);

    //        stateManager.initialize();

    //        stateManager.register(store1, stateRestoreCallback);
    //        Assert.Equal(2, stateRestoreCallback.restored.Count);
    //    }

    //    [Xunit.Fact]
    //    public void shouldRecoverFromInvalidOffsetExceptionAndRestoreRecords() {
    //        initializeConsumer(2, 0, t1);
    //        consumer.setException(new InvalidOffsetException("Try Again!") {
    //            public HashSet<TopicPartition> partitions() {
    //                return Collections.singleton(t1);
    //            }
    //        });

    //        stateManager.initialize();

    //        stateManager.register(store1, stateRestoreCallback);
    //        Assert.Equal(2, stateRestoreCallback.restored.Count);
    //    }

    //    [Xunit.Fact]
    //    public void shouldListenForRestoreEvents() {
    //        initializeConsumer(5, 1, t1);
    //        stateManager.initialize();

    //        stateManager.register(store1, stateRestoreCallback);

    //        Assert.Equal(stateRestoreListener.restoreStartOffset, (1L));
    //        Assert.Equal(stateRestoreListener.restoreEndOffset, (6L));
    //        Assert.Equal(stateRestoreListener.totalNumRestored, (5L));


    //        Assert.Equal(stateRestoreListener.storeNameCalledStates.Get(RESTORE_START), (store1.name()));
    //        Assert.Equal(stateRestoreListener.storeNameCalledStates.Get(RESTORE_BATCH), (store1.name()));
    //        Assert.Equal(stateRestoreListener.storeNameCalledStates.Get(RESTORE_END), (store1.name()));
    //    }

    //    [Xunit.Fact]
    //    public void shouldRestoreRecordsFromCheckpointToHighwatermark(){ //throws IOException
    //        initializeConsumer(5, 5, t1);

    //        OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(new File(stateManager.baseDir(),
    //                                                                                StateManagerUtil.CHECKPOINT_FILE_NAME));
    //        offsetCheckpoint.write(Collections.singletonMap(t1, 5L));

    //        stateManager.initialize();
    //        stateManager.register(store1, stateRestoreCallback);
    //        Assert.Equal(5, stateRestoreCallback.restored.Count);
    //    }


    //    [Xunit.Fact]
    //    public void shouldFlushStateStores() {
    //        stateManager.initialize();
    //        // register the stores
    //        initializeConsumer(1, 0, t1);
    //        stateManager.register(store1, stateRestoreCallback);
    //        initializeConsumer(1, 0, t2);
    //        stateManager.register(store2, stateRestoreCallback);

    //        stateManager.flush();
    //        Assert.True(store1.flushed);
    //        Assert.True(store2.flushed);
    //    }

    //    [Xunit.Fact]// (expected = ProcessorStateException)
    //    public void shouldThrowProcessorStateStoreExceptionIfStoreFlushFailed() {
    //        stateManager.initialize();
    //        // register the stores
    //        initializeConsumer(1, 0, t1);
    //        stateManager.register(new NoOpReadOnlyStore(store1.name()) {

    //            public void flush() {
    //                throw new RuntimeException("KABOOM!");
    //            }
    //        }, stateRestoreCallback);

    //        stateManager.flush();
    //    }

    //    [Xunit.Fact]
    //    public void shouldCloseStateStores(){ //throws IOException
    //        stateManager.initialize();
    //        // register the stores
    //        initializeConsumer(1, 0, t1);
    //        stateManager.register(store1, stateRestoreCallback);
    //        initializeConsumer(1, 0, t2);
    //        stateManager.register(store2, stateRestoreCallback);

    //        stateManager.close(true);
    //        Assert.False(store1.isOpen());
    //        Assert.False(store2.isOpen());
    //    }

    //    [Xunit.Fact]// (expected = ProcessorStateException)
    //    public void shouldThrowProcessorStateStoreExceptionIfStoreCloseFailed(){ //throws IOException
    //        stateManager.initialize();
    //        initializeConsumer(1, 0, t1);
    //        stateManager.register(new NoOpReadOnlyStore(store1.name()) {

    //            public void close() {
    //                throw new RuntimeException("KABOOM!");
    //            }
    //        }, stateRestoreCallback);

    //        stateManager.close(true);
    //    }

    //    [Xunit.Fact]
    //    public void shouldThrowIllegalArgumentExceptionIfCallbackIsNull() {
    //        stateManager.initialize();
    //        try {
    //            stateManager.register(store1, null);
    //            Assert.True(false, "should have thrown due to null callback");
    //        } catch (ArgumentException e) {
    //            //pass
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldUnlockGlobalStateDirectoryOnClose(){ //throws IOException
    //        stateManager.initialize();
    //        stateManager.close(true);
    //        StateDirectory stateDir = new StateDirectory(streamsConfig, new MockTime(), true);
    //        try {
    //            // should be able to get the lock now as it should've been released in close
    //            Assert.True(stateDir.lockGlobalState());
    //        } finally {
    //            stateDir.unlockGlobalState();
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldNotCloseStoresIfCloseAlreadyCalled(){ //throws IOException
    //        stateManager.initialize();
    //        initializeConsumer(1, 0, t1);
    //        stateManager.register(new NoOpReadOnlyStore("t1-store") {

    //            public void close() {
    //                if (!isOpen()) {
    //                    throw new RuntimeException("store already closed");
    //                }
    //                base.close();
    //            }
    //        }, stateRestoreCallback);
    //        stateManager.close(true);

    //        stateManager.close(true);
    //    }

    //    [Xunit.Fact]
    //    public void shouldAttemptToCloseAllStoresEvenWhenSomeException(){ //throws IOException
    //        stateManager.initialize();
    //        initializeConsumer(1, 0, t1);
    //        NoOpReadOnlyStore store = new NoOpReadOnlyStore("t1-store") {

    //            public void close() {
    //                base.close();
    //                throw new RuntimeException("KABOOM!");
    //            }
    //        };
    //        stateManager.register(store, stateRestoreCallback);

    //        initializeConsumer(1, 0, t2);
    //        stateManager.register(store2, stateRestoreCallback);

    //        try {
    //            stateManager.close(true);
    //        } catch (ProcessorStateException e) {
    //            // expected
    //        }
    //        Assert.False(store.isOpen());
    //        Assert.False(store2.isOpen());
    //    }

    //    [Xunit.Fact]
    //    public void shouldReleaseLockIfExceptionWhenLoadingCheckpoints(){ //throws IOException
    //        writeCorruptCheckpoint();
    //        try {
    //            stateManager.initialize();
    //        } catch (StreamsException e) {
    //            // expected
    //        }
    //        StateDirectory stateDir = new StateDirectory(streamsConfig, new MockTime(), true);
    //        try {
    //            // should be able to get the lock now as it should've been released
    //            Assert.True(stateDir.lockGlobalState());
    //        } finally {
    //            stateDir.unlockGlobalState();
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldCheckpointOffsets(){ //throws IOException
    //        Dictionary<TopicPartition, long> offsets = Collections.singletonMap(t1, 25L);
    //        stateManager.initialize();

    //        stateManager.checkpoint(offsets);

    //        Dictionary<TopicPartition, long> result = readOffsetsCheckpoint();
    //        Assert.Equal(result, (offsets));
    //        Assert.Equal(stateManager.checkpointed(), (offsets));
    //    }

    //    [Xunit.Fact]
    //    public void shouldNotRemoveOffsetsOfUnUpdatedTablesDuringCheckpoint() {
    //        stateManager.initialize();
    //        initializeConsumer(10, 0, t1);
    //        stateManager.register(store1, stateRestoreCallback);
    //        initializeConsumer(20, 0, t2);
    //        stateManager.register(store2, stateRestoreCallback);

    //        Dictionary<TopicPartition, long> initialCheckpoint = stateManager.checkpointed();
    //        stateManager.checkpoint(Collections.singletonMap(t1, 101L));

    //        Dictionary<TopicPartition, long> updatedCheckpoint = stateManager.checkpointed();
    //        Assert.Equal(updatedCheckpoint.Get(t2), (initialCheckpoint.Get(t2)));
    //        Assert.Equal(updatedCheckpoint.Get(t1), (101L));
    //    }

    //    [Xunit.Fact]
    //    public void shouldSkipNullKeysWhenRestoring() {
    //        HashDictionary<TopicPartition, long> startOffsets = new HashMap<>();
    //        startOffsets.put(t1, 1L);
    //        HashDictionary<TopicPartition, long> endOffsets = new HashMap<>();
    //        endOffsets.put(t1, 3L);
    //        consumer.updatePartitions(t1.Topic, Collections.singletonList(new PartitionInfo(t1.Topic, t1.Partition, null, null, null)));
    //        consumer.assign(Collections.singletonList(t1));
    //        consumer.updateEndOffsets(endOffsets);
    //        consumer.updateBeginningOffsets(startOffsets);
    //        consumer.addRecord(new ConsumeResult<>(t1.Topic, t1.Partition, 1, null, "null".getBytes()));
    //        byte[] expectedKey = "key".getBytes();
    //        byte[] expectedValue = "value".getBytes();
    //        consumer.addRecord(new ConsumeResult<>(t1.Topic, t1.Partition, 2, expectedKey, expectedValue));

    //        stateManager.initialize();
    //        stateManager.register(store1, stateRestoreCallback);
    //        KeyValuePair<byte[], byte[]> restoredKv = stateRestoreCallback.restored.Get(0);
    //        Assert.Equal(stateRestoreCallback.restored, (Collections.singletonList(KeyValuePair.Create(restoredKv.key, restoredKv.value))));
    //    }

    //    [Xunit.Fact]
    //    public void shouldCheckpointRestoredOffsetsToFile(){ //throws IOException
    //        stateManager.initialize();
    //        initializeConsumer(10, 0, t1);
    //        stateManager.register(store1, stateRestoreCallback);
    //        stateManager.checkpoint(Collections.emptyMap());
    //        stateManager.close(true);

    //        Dictionary<TopicPartition, long> checkpointMap = stateManager.checkpointed();
    //        Assert.Equal(checkpointMap, (Collections.singletonMap(t1, 10L)));
    //        Assert.Equal(readOffsetsCheckpoint(), (checkpointMap));
    //    }

    //    [Xunit.Fact]
    //    public void shouldSkipGlobalInMemoryStoreOffsetsToFile(){ //throws IOException
    //        stateManager.initialize();
    //        initializeConsumer(10, 0, t3);
    //        stateManager.register(store3, stateRestoreCallback);
    //        stateManager.close(true);

    //        Assert.Equal(readOffsetsCheckpoint(), (Collections.emptyMap()));
    //    }

    //    private Dictionary<TopicPartition, long> readOffsetsCheckpoint(){ //throws IOException
    //        OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(new File(stateManager.baseDir(),
    //                                                                                StateManagerUtil.CHECKPOINT_FILE_NAME));
    //        return offsetCheckpoint.read();
    //    }

    //    [Xunit.Fact]
    //    public void shouldThrowLockExceptionIfIOExceptionCaughtWhenTryingToLockStateDir() {
    //        stateManager = new GlobalStateManagerImpl(
    //            new LogContext("mock"),
    //            topology,
    //            consumer,
    //            new StateDirectory(streamsConfig, time, true) {

    //                public bool lockGlobalState(){ //throws IOException
    //                    throw new IOException("KABOOM!");
    //                }
    //            },
    //            stateRestoreListener,
    //            streamsConfig
    //        );

    //        try {
    //            stateManager.initialize();
    //            Assert.True(false, "Should have thrown LockException");
    //        } catch (LockException e) {
    //            // pass
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldRetryWhenEndOffsetsThrowsTimeoutException() {
    //        int retries = 2;
    //        AtomicInteger numberOfCalls = new AtomicInteger(0);
    //        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {

    //            public Dictionary<TopicPartition, long> endOffsets(Collection<org.apache.kafka.common.TopicPartition> partitions) {
    //                numberOfCalls.incrementAndGet();
    //                throw new TimeoutException();
    //            }
    //        };
    //        streamsConfig = new StreamsConfig(new StreamsConfig() {
    //            {
    //                put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
    //                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    //                put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
    //                put(StreamsConfig.RETRIES_CONFIG, retries);
    //            }
    //        });

    //        try {
    //            new GlobalStateManagerImpl(
    //                new LogContext("mock"),
    //                topology,
    //                consumer,
    //                stateDirectory,
    //                stateRestoreListener,
    //                streamsConfig);
    //        } catch (StreamsException expected) {
    //            Assert.Equal(numberOfCalls.Get(), retries);
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldRetryWhenPartitionsForThrowsTimeoutException() {
    //        int retries = 2;
    //        AtomicInteger numberOfCalls = new AtomicInteger(0);
    //        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {

    //            public List<PartitionInfo> partitionsFor(string topic) {
    //                numberOfCalls.incrementAndGet();
    //                throw new TimeoutException();
    //            }
    //        };
    //        streamsConfig = new StreamsConfig(new StreamsConfig() {
    //            {
    //                put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
    //                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    //                put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
    //                put(StreamsConfig.RETRIES_CONFIG, retries);
    //            }
    //        });

    //        try {
    //            new GlobalStateManagerImpl(
    //                new LogContext("mock"),
    //                topology,
    //                consumer,
    //                stateDirectory,
    //                stateRestoreListener,
    //                streamsConfig);
    //        } catch (StreamsException expected) {
    //            Assert.Equal(numberOfCalls.Get(), retries);
    //        }
    //    }

    //    [Xunit.Fact]
    //    public void shouldDeleteAndRecreateStoreDirectoryOnReinitialize(){ //throws IOException
    //        File storeDirectory1 = new File(stateDirectory.globalStateDir().FullName
    //                                                  + Path.DirectorySeparatorChar + "rocksdb"
    //                                                  + Path.DirectorySeparatorChar + storeName1);
    //        File storeDirectory2 = new File(stateDirectory.globalStateDir().FullName
    //                                                  + Path.DirectorySeparatorChar + "rocksdb"
    //                                                  + Path.DirectorySeparatorChar + storeName2);
    //        File storeDirectory3 = new File(stateDirectory.globalStateDir().FullName
    //                                                  + Path.DirectorySeparatorChar + storeName3);
    //        File storeDirectory4 = new File(stateDirectory.globalStateDir().FullName
    //                                                  + Path.DirectorySeparatorChar + storeName4);
    //        File testFile1 = new File(storeDirectory1.FullName + Path.DirectorySeparatorChar + "testFile");
    //        File testFile2 = new File(storeDirectory2.FullName + Path.DirectorySeparatorChar + "testFile");
    //        File testFile3 = new File(storeDirectory3.FullName + Path.DirectorySeparatorChar + "testFile");
    //        File testFile4 = new File(storeDirectory4.FullName + Path.DirectorySeparatorChar + "testFile");

    //        consumer.updatePartitions(t1.Topic, Collections.singletonList(new PartitionInfo(t1.Topic, t1.Partition, null, null, null)));
    //        consumer.updatePartitions(t2.Topic, Collections.singletonList(new PartitionInfo(t2.Topic, t2.Partition, null, null, null)));
    //        consumer.updatePartitions(t3.Topic, Collections.singletonList(new PartitionInfo(t3.Topic, t3.Partition, null, null, null)));
    //        consumer.updatePartitions(t4.Topic, Collections.singletonList(new PartitionInfo(t4.Topic, t4.Partition, null, null, null)));
    //        consumer.updateBeginningOffsets(new HashDictionary<TopicPartition, long>() {
    //            {
    //                put(t1, 0L);
    //                put(t2, 0L);
    //                put(t3, 0L);
    //                put(t4, 0L);
    //            }
    //        });
    //        consumer.updateEndOffsets(new HashDictionary<TopicPartition, long>() {
    //            {
    //                put(t1, 0L);
    //                put(t2, 0L);
    //                put(t3, 0L);
    //                put(t4, 0L);
    //            }
    //        });

    //        stateManager.initialize();
    //        stateManager.register(store1, stateRestoreCallback);
    //        stateManager.register(store2, stateRestoreCallback);
    //        stateManager.register(store3, stateRestoreCallback);
    //        stateManager.register(store4, stateRestoreCallback);

    //        testFile1.createNewFile();
    //        Assert.True(testFile1.Exists);
    //        testFile2.createNewFile();
    //        Assert.True(testFile2.Exists);
    //        testFile3.createNewFile();
    //        Assert.True(testFile3.Exists);
    //        testFile4.createNewFile();
    //        Assert.True(testFile4.Exists);

    //        // only delete and recreate store 1 and 3 -- 2 and 4 must be untouched
    //        stateManager.reinitializeStateStoresForPartitions(asList(t1, t3), processorContext);

    //        Assert.False(testFile1.Exists);
    //        Assert.True(testFile2.Exists);
    //        Assert.False(testFile3.Exists);
    //        Assert.True(testFile4.Exists);
    //    }

    //    private void writeCorruptCheckpoint(){ //throws IOException
    //        File checkpointFile = new File(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME);
    //        try { 
    // (OutputStream stream = Files.newOutputStream(checkpointFile.toPath()));
    //            stream.write("0\n1\nfoo".getBytes());
    //        }
    //    }

    //    private void initializeConsumer(long numRecords, long startOffset, TopicPartition topicPartition) {
    //        HashDictionary<TopicPartition, long> startOffsets = new HashMap<>();
    //        startOffsets.put(topicPartition, startOffset);
    //        HashDictionary<TopicPartition, long> endOffsets = new HashMap<>();
    //        endOffsets.put(topicPartition, startOffset + numRecords);
    //        consumer.updatePartitions(topicPartition.Topic, Collections.singletonList(new PartitionInfo(topicPartition.Topic, topicPartition.Partition, null, null, null)));
    //        consumer.assign(Collections.singletonList(topicPartition));
    //        consumer.updateEndOffsets(endOffsets);
    //        consumer.updateBeginningOffsets(startOffsets);

    //        for (int i = 0; i < numRecords; i++) {
    //            consumer.addRecord(new ConsumeResult<>(topicPartition.Topic, topicPartition.Partition, startOffset + i, "key".getBytes(), "value".getBytes()));
    //        }
    //    }

    //    private Dictionary<TopicPartition, long> writeCheckpoint(){ //throws IOException
    //        OffsetCheckpoint checkpoint = new OffsetCheckpoint(checkpointFile);
    //        Dictionary<TopicPartition, long> expected = Collections.singletonMap(t1, 1L);
    //        checkpoint.write(expected);
    //        return expected;
    //    }

    //    private static class TheStateRestoreCallback : StateRestoreCallback {
    //        private List<KeyValuePair<byte[], byte[]>> restored = new ArrayList<>();


    //        public void restore(byte[] key, byte[] value) {
    //            restored.Add(KeyValuePair.Create(key, value));
    //        }
    //    }

    //    private class ConverterStore<K, V> : NoOpReadOnlyStore<K, V> : TimestampedBytesStore {
    //        ConverterStore(string name,
    //                       bool rocksdbStore) {
    //            super(name, rocksdbStore);
    //        }
    //    }

    //}
}