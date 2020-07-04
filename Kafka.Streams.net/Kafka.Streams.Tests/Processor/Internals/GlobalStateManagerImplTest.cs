//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.State.Internals;
//using Kafka.Streams.State.TimeStamped;
//using Kafka.Streams.Temporary;
//using System;
//using System.Collections.Generic;
//using System.IO;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class GlobalStateManagerImplTest
//    {
//        private MockTime time = new MockTime();
//        private TheStateRestoreCallback stateRestoreCallback = new TheStateRestoreCallback();
//        private MockStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
//        private string storeName1 = "t1-store";
//        private string storeName2 = "t2-store";
//        private string storeName3 = "t3-store";
//        private string storeName4 = "t4-store";
//        private TopicPartition t1 = new TopicPartition("t1", 1);
//        private TopicPartition t2 = new TopicPartition("t2", 1);
//        private TopicPartition t3 = new TopicPartition("t3", 1);
//        private TopicPartition t4 = new TopicPartition("t4", 1);
//        private GlobalStateManager stateManager;
//        private StateDirectory stateDirectory;
//        private StreamsConfig streamsConfig;
//        //private NoOpReadOnlyStore<object, object> store1, store2, store3, store4;
//        private MockConsumer<byte[], byte[]> consumer;
//        private FileInfo checkpointFile;
//        private ProcessorTopology topology;
//        //private InternalMockProcessorContext processorContext;

//        static ProcessorTopology withGlobalStores(List<IStateStore> stateStores,
//                                                  Dictionary<string, string> storeToChangelogTopic)
//        {
//            return new ProcessorTopology(Collections.emptyList(),
//                                         Collections.emptyMap(),
//                                         Collections.emptyMap(),
//                                         Collections.emptyList(),
//                                         stateStores,
//                                         storeToChangelogTopic,
//                                         Collections.emptySet());
//        }


//        public void before()
//        {
//            Dictionary<string, string> storeToTopic = new HashMap<>();

//            storeToTopic.Put(storeName1, t1.Topic);
//            storeToTopic.Put(storeName2, t2.Topic);
//            storeToTopic.Put(storeName3, t3.Topic);
//            storeToTopic.Put(storeName4, t4.Topic);

//            store1 = new NoOpReadOnlyStore<>(storeName1, true);
//            store2 = new ConverterStore<>(storeName2, true);
//            store3 = new NoOpReadOnlyStore<>(storeName3);
//            store4 = new NoOpReadOnlyStore<>(storeName4);

//            topology = withGlobalStores(asList(store1, store2, store3, store4), storeToTopic);

//            streamsConfig = new StreamsConfig
//            {
//                Put(StreamsConfig.ApplicationIdConfig, "appId"),
//                Put(StreamsConfig.BootstrapServersConfig, "dummy:1234"),
//                Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath()),
//            };

//            stateDirectory = new StateDirectory(streamsConfig, time, true);
//            consumer = new MockConsumer<>(OffsetResetStrategy.NONE);
//            stateManager = new GlobalStateManager(
//                new LogContext("test"),
//                topology,
//                consumer,
//                stateDirectory,
//                stateRestoreListener,
//                streamsConfig);
//            processorContext = new InternalMockProcessorContext(stateDirectory.GlobalStateDir(), streamsConfig);
//            stateManager.setGlobalProcessorContext(processorContext);
//            checkpointFile = new FileInfo(Path.Combine(stateManager, StateManagerUtil.CHECKPOINT_FILE_NAME));
//        }


//        public void after()
//        { //throws IOException
//            stateDirectory.UnlockGlobalState();
//        }

//        [Fact]
//        public void shouldLockGlobalStateDirectory()
//        {
//            stateManager.initialize();
//            Assert.True(new FileInfo(Path.Combine(stateDirectory.GlobalStateDir().FullName, ".Lock")).Exists);
//        }

//        [Fact]// (expected = LockException)
//        public void shouldThrowLockExceptionIfCantGetLock()
//        { //throws IOException
//            StateDirectory stateDir = new StateDirectory(streamsConfig, time, true);
//            try
//            {
//                stateDir.LockGlobalState();
//                stateManager.initialize();
//            }
//            finally
//            {
//                stateDir.UnlockGlobalState();
//            }
//        }

//        [Fact]
//        public void shouldReadCheckpointOffsets()
//        { //throws IOException
//            Dictionary<TopicPartition, long?> expected = WriteCheckpoint();

//            stateManager.initialize();
//            Dictionary<TopicPartition, long?> offsets = stateManager.checkpointed();
//            Assert.Equal(expected, offsets);
//        }

//        [Fact]
//        public void shouldNotDeleteCheckpointFileAfterLoaded()
//        { //throws IOException
//            WriteCheckpoint();
//            stateManager.initialize();
//            Assert.True(checkpointFile.Exists);
//        }

//        [Fact]// (expected = StreamsException)
//        public void shouldThrowStreamsExceptionIfFailedToReadCheckpointedOffsets()
//        { //throws IOException
//            WriteCorruptCheckpoint();
//            stateManager.initialize();
//        }

//        [Fact]
//        public void shouldInitializeStateStores()
//        {
//            stateManager.initialize();
//            Assert.True(store1.initialized);
//            Assert.True(store2.initialized);
//        }

//        [Fact]
//        public void shouldReturnInitializedStoreNames()
//        {
//            HashSet<string> storeNames = stateManager.initialize();
//            Assert.Equal(new HashSet<string> { storeName1, storeName2, storeName3, storeName4 }, storeNames);
//        }

//        [Fact]
//        public void shouldThrowIllegalArgumentIfTryingToRegisterStoreThatIsNotGlobal()
//        {
//            stateManager.initialize();

//            try
//            {
//                stateManager.register(new NoOpReadOnlyStore<>("not-in-topology"), stateRestoreCallback);
//                Assert.True(false, "should have raised an illegal argument exception as store is not in the topology");
//            }
//            catch (ArgumentException e)
//            {
//                // pass
//            }
//        }

//        [Fact]
//        public void shouldThrowIllegalArgumentExceptionIfAttemptingToRegisterStoreTwice()
//        {
//            stateManager.initialize();
//            initializeConsumer(2, 0, t1);
//            stateManager.register(store1, stateRestoreCallback);
//            try
//            {
//                stateManager.register(store1, stateRestoreCallback);
//                Assert.True(false, "should have raised an illegal argument exception as store has already been registered");
//            }
//            catch (ArgumentException e)
//            {
//                // pass
//            }
//        }

//        [Fact]
//        public void shouldThrowStreamsExceptionIfNoPartitionsFoundForStore()
//        {
//            stateManager.initialize();
//            try
//            {
//                stateManager.register(store1, stateRestoreCallback);
//                Assert.True(false, "Should have raised a StreamsException as there are no partition for the store");
//            }
//            catch (StreamsException e)
//            {
//                // pass
//            }
//        }

//        [Fact]
//        public void shouldNotConvertValuesIfStoreDoesNotImplementTimestampedBytesStore()
//        {
//            initializeConsumer(1, 0, t1);

//            stateManager.initialize();
//            stateManager.register(store1, stateRestoreCallback);

//            KeyValuePair<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.Get(0);
//            Assert.Equal(3, restoredRecord.Key.Length);
//            Assert.Equal(5, restoredRecord.Value.Length);
//        }

//        [Fact]
//        public void shouldNotConvertValuesIfInnerStoreDoesNotImplementTimestampedBytesStore()
//        {
//            initializeConsumer(1, 0, t1);

//            stateManager.initialize();
//            stateManager.register(
//                new WrappedStateStore<NoOpReadOnlyStore<object, object>, object, object>(store1)
//                {
//                },
//                stateRestoreCallback
//            );

//            KeyValuePair<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.Get(0);
//            Assert.Equal(3, restoredRecord.Key.Length);
//            Assert.Equal(5, restoredRecord.Value.Length);
//        }

//        [Fact]
//        public void shouldConvertValuesIfStoreImplementsTimestampedBytesStore()
//        {
//            initializeConsumer(1, 0, t2);

//            stateManager.initialize();
//            stateManager.register(store2, stateRestoreCallback);

//            KeyValuePair<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.Get(0);
//            Assert.Equal(3, restoredRecord.Key.Length);
//            Assert.Equal(13, restoredRecord.Value.Length);
//        }

//        [Fact]
//        public void shouldConvertValuesIfInnerStoreImplementsTimestampedBytesStore()
//        {
//            initializeConsumer(1, 0, t2);

//            stateManager.initialize();
//            stateManager.register(
//                new WrappedStateStore<NoOpReadOnlyStore<object, object>, object, object>(store2)
//                {
//                },
//                stateRestoreCallback
//            );

//            KeyValuePair<byte[], byte[]> restoredRecord = stateRestoreCallback.restored.Get(0);
//            Assert.Equal(3, restoredRecord.Key.Length);
//            Assert.Equal(13, restoredRecord.Value.Length);
//        }

//        [Fact]
//        public void shouldRestoreRecordsUpToHighwatermark()
//        {
//            initializeConsumer(2, 0, t1);

//            stateManager.initialize();

//            stateManager.register(store1, stateRestoreCallback);
//            Assert.Equal(2, stateRestoreCallback.restored.Count);
//        }

//        [Fact]
//        public void shouldRecoverFromInvalidOffsetExceptionAndRestoreRecords()
//        {
//            initializeConsumer(2, 0, t1);
//            //            consumer.setException(new InvalidOffsetException("Try Again!")
//            //            {
//            //                public HashSet<TopicPartition> partitions()
//            //            {
//            //                return Collections.singleton(t1);
//            //            }
//            //        });

//            stateManager.initialize();

//            stateManager.register(store1, stateRestoreCallback);
//            Assert.Equal(2, stateRestoreCallback.restored.Count);
//        }

//        [Fact]
//        public void shouldListenForRestoreEvents()
//        {
//            initializeConsumer(5, 1, t1);
//            stateManager.initialize();

//            stateManager.register(store1, stateRestoreCallback);

//            Assert.Equal(stateRestoreListener.restoreStartOffset, 1L);
//            Assert.Equal(stateRestoreListener.restoreEndOffset, 6L);
//            Assert.Equal(stateRestoreListener.totalNumRestored, 5L);

//            Assert.Equal(stateRestoreListener.storeNameCalledStates.Get(RESTORE_START), store1.Name());
//            Assert.Equal(stateRestoreListener.storeNameCalledStates.Get(RESTORE_BATCH), store1.Name());
//            Assert.Equal(stateRestoreListener.storeNameCalledStates.Get(RESTORE_END), store1.Name());
//        }

//        [Fact]
//        public void shouldRestoreRecordsFromCheckpointToHighwatermark()
//        { //throws IOException
//            initializeConsumer(5, 5, t1);

//            OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(
//                new FileInfo(Path.Combine(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME)));

//            offsetCheckpoint.Write(t1, 5L);

//            stateManager.initialize();
//            stateManager.register(store1, stateRestoreCallback);
//            Assert.Equal(5, stateRestoreCallback.restored.Count);
//        }


//        [Fact]
//        public void shouldFlushStateStores()
//        {
//            stateManager.initialize();
//            // register the stores
//            initializeConsumer(1, 0, t1);
//            stateManager.register(store1, stateRestoreCallback);
//            initializeConsumer(1, 0, t2);
//            stateManager.register(store2, stateRestoreCallback);

//            stateManager.Flush();
//            Assert.True(store1.flushed);
//            Assert.True(store2.flushed);
//        }

//        [Fact]// (expected = ProcessorStateException)
//        public void shouldThrowProcessorStateStoreExceptionIfStoreFlushFailed()
//        {
//            stateManager.initialize();
//            // register the stores
//            initializeConsumer(1, 0, t1);
//            //        stateManager.register(new NoOpReadOnlyStore(store1.Name())
//            //        {
//            //
//            //                public void Flush()
//            //        {
//            //            throw new RuntimeException("KABOOM!");
//            //        }
//            //    }, stateRestoreCallback);
//            //
//            //            stateManager.Flush();
//        }

//        [Fact]
//        public void shouldCloseStateStores()
//        { //throws IOException
//            stateManager.initialize();
//            // register the stores
//            initializeConsumer(1, 0, t1);
//            stateManager.register(store1, stateRestoreCallback);
//            initializeConsumer(1, 0, t2);
//            stateManager.register(store2, stateRestoreCallback);

//            stateManager.Close(true);
//            Assert.False(store1.IsOpen());
//            Assert.False(store2.IsOpen());
//        }

//        [Fact]// (expected = ProcessorStateException)
//        public void shouldThrowProcessorStateStoreExceptionIfStoreCloseFailed()
//        { //throws IOException
//            stateManager.initialize();
//            initializeConsumer(1, 0, t1);
//            //        stateManager.register(new NoOpReadOnlyStore(store1.Name())
//            //        {
//            //
//            //                public void Close()
//            //        {
//            //            throw new RuntimeException("KABOOM!");
//            //        }
//            //    }, stateRestoreCallback);
//            //
//            //            stateManager.Close(true);
//        }

//        [Fact]
//        public void shouldThrowIllegalArgumentExceptionIfCallbackIsNull()
//        {
//            stateManager.initialize();
//            try
//            {
//                stateManager.register(store1, null);
//                Assert.True(false, "should have thrown due to null callback");
//            }
//            catch (ArgumentException e)
//            {
//                //pass
//            }
//        }

//        [Fact]
//        public void shouldUnlockGlobalStateDirectoryOnClose()
//        { //throws IOException
//            stateManager.initialize();
//            stateManager.Close(true);
//            StateDirectory stateDir = new StateDirectory(streamsConfig, new MockTime(), true);
//            try
//            {
//                // should be able to get the lock now as it should've been released in Close
//                Assert.True(stateDir.LockGlobalState());
//            }
//            finally
//            {
//                stateDir.UnlockGlobalState();
//            }
//        }

//        [Fact]
//        public void shouldNotCloseStoresIfCloseAlreadyCalled()
//        { //throws IOException
//            stateManager.initialize();
//            InitializeConsumer(1, 0, t1);
//            stateManager.register(new NoOpReadOnlyStore("t1-store"),
//            //{
//            //    public void Close()
//            //    {
//            //        if (!IsOpen())
//            //        {
//            //            throw new RuntimeException("store already closed");
//            //        }
//            //        base.Close();
//            //    }
//            //},
//            stateRestoreCallback);

//            stateManager.Close(true);

//            stateManager.Close(true);
//        }

//        [Fact]
//        public void shouldAttemptToCloseAllStoresEvenWhenSomeException()
//        { //throws IOException
//            stateManager.initialize();
//            initializeConsumer(1, 0, t1);
//            //NoOpReadOnlyStore store = new NoOpReadOnlyStore("t1-store");l
//            //    {
//            //
//            //                public void Close()
//            //    {
//            //        base.Close();
//            //        throw new RuntimeException("KABOOM!");
//            //    }
//            //};
//            stateManager.register(store, stateRestoreCallback);

//            initializeConsumer(1, 0, t2);
//            stateManager.register(store2, stateRestoreCallback);

//            try
//            {
//                stateManager.Close(true);
//            }
//            catch (ProcessorStateException e)
//            {
//                // expected
//            }
//            Assert.False(store.IsOpen());
//            Assert.False(store2.IsOpen());
//        }

//        [Fact]
//        public void shouldReleaseLockIfExceptionWhenLoadingCheckpoints()
//        { //throws IOException
//            WriteCorruptCheckpoint();
//            try
//            {
//                stateManager.initialize();
//            }
//            catch (StreamsException e)
//            {
//                // expected
//            }
//            StateDirectory stateDir = new StateDirectory(streamsConfig, new MockTime(), true);
//            try
//            {
//                // should be able to get the lock now as it should've been released
//                Assert.True(stateDir.LockGlobalState());
//            }
//            finally
//            {
//                stateDir.UnlockGlobalState();
//            }
//        }

//        [Fact]
//        public void shouldCheckpointOffsets()
//        { //throws IOException
//            Dictionary<TopicPartition, long> offsets = Collections.singletonMap(t1, 25L);
//            stateManager.initialize();

//            stateManager.checkpoint(offsets);

//            Dictionary<TopicPartition, long> result = readOffsetsCheckpoint();
//            Assert.Equal(result, offsets);
//            Assert.Equal(stateManager.checkpointed(), offsets);
//        }

//        [Fact]
//        public void shouldNotRemoveOffsetsOfUnUpdatedTablesDuringCheckpoint()
//        {
//            stateManager.initialize();
//            initializeConsumer(10, 0, t1);
//            stateManager.register(store1, stateRestoreCallback);
//            initializeConsumer(20, 0, t2);
//            stateManager.register(store2, stateRestoreCallback);

//            Dictionary<TopicPartition, long> initialCheckpoint = stateManager.checkpointed();
//            stateManager.checkpoint(Collections.singletonMap(t1, 101L));

//            Dictionary<TopicPartition, long> updatedCheckpoint = stateManager.checkpointed();
//            Assert.Equal(updatedCheckpoint[t2], initialCheckpoint[t2]);
//            Assert.Equal(101L, updatedCheckpoint[t1]);
//        }

//        [Fact]
//        public void shouldSkipNullKeysWhenRestoring()
//        {
//            Dictionary<TopicPartition, long> startOffsets = new HashMap<>();
//            startOffsets.Put(t1, 1L);
//            Dictionary<TopicPartition, long> endOffsets = new HashMap<>();
//            endOffsets.Put(t1, 3L);
//            consumer.updatePartitions(t1.Topic, Collections.singletonList(new PartitionInfo(t1.Topic, t1.Partition, null, null, null)));
//            consumer.Assign(Collections.singletonList(t1));
//            consumer.updateEndOffsets(endOffsets);
//            consumer.UpdateBeginningOffsets(startOffsets);
//            consumer.AddRecord(new ConsumeResult<>(t1.Topic, t1.Partition, 1, null, "null".GetBytes()));
//            byte[] expectedKey = "key".GetBytes();
//            byte[] expectedValue = "value".GetBytes();
//            consumer.AddRecord(new ConsumeResult<>(t1.Topic, t1.Partition, 2, expectedKey, expectedValue));

//            stateManager.initialize();
//            stateManager.register(store1, stateRestoreCallback);
//            KeyValuePair<byte[], byte[]> restoredKv = stateRestoreCallback.restored.Get(0);
//            Assert.Equal(stateRestoreCallback.restored, Collections.singletonList(KeyValuePair.Create(restoredKv.Key, restoredKv.Value)));
//        }

//        [Fact]
//        public void shouldCheckpointRestoredOffsetsToFile()
//        { //throws IOException
//            stateManager.initialize();
//            initializeConsumer(10, 0, t1);
//            stateManager.register(store1, stateRestoreCallback);
//            stateManager.checkpoint(Collections.emptyMap());
//            stateManager.Close(true);

//            Dictionary<TopicPartition, long> checkpointMap = stateManager.checkpointed();
//            Assert.Equal(checkpointMap, Collections.singletonMap(t1, 10L));
//            Assert.Equal(readOffsetsCheckpoint(), checkpointMap);
//        }

//        [Fact]
//        public void shouldSkipGlobalInMemoryStoreOffsetsToFile()
//        { //throws IOException
//            stateManager.initialize();
//            initializeConsumer(10, 0, t3);
//            stateManager.register(store3, stateRestoreCallback);
//            stateManager.Close(true);

//            Assert.Equal(readOffsetsCheckpoint(), Collections.emptyMap());
//        }

//        private Dictionary<TopicPartition, long> readOffsetsCheckpoint()
//        { //throws IOException
//            OffsetCheckpoint offsetCheckpoint = new OffsetCheckpoint(
//                new FileInfo(Path.Combine(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME)));

//            return offsetCheckpoint.Read();
//        }

//        [Fact]
//        public void shouldThrowLockExceptionIfIOExceptionCaughtWhenTryingToLockStateDir()
//        {
//            //        stateManager = new GlobalStateManagerImpl(
//            //            new LogContext("mock"),
//            //            topology,
//            //            consumer,
//            //            new StateDirectory(streamsConfig, time, true)
//            //            {
//            //    
//            //                    public bool lockGlobalState()
//            //        { //throws IOException
//            //            throw new IOException("KABOOM!");
//            //        }
//            //    },
//            //                stateRestoreListener,
//            //                streamsConfig
//            //            );
//            //
//            //            try {
//            //                stateManager.initialize();
//            //                Assert.True(false, "Should have thrown LockException");
//            //            } catch (LockException e) {
//            //                // pass
//            //            }
//        }

//        [Fact]
//        public void shouldRetryWhenEndOffsetsThrowsTimeoutException()
//        {
//            int retries = 2;
//            int numberOfCalls = 0;
//            //        consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST)
//            //        {
//            //
//            //                public Dictionary<TopicPartition, long> endOffsets(Collection<org.apache.kafka.common.TopicPartition> partitions)
//            //        {
//            //            numberOfCalls.incrementAndGet();
//            //            throw new TimeoutException();
//            //        }
//            //    };
//            // streamsConfig = new StreamsConfig(new StreamsConfig()
//            // {
//            //     {
//            //         Put(StreamsConfig.ApplicationIdConfig, "appId");
//            //         Put(StreamsConfig.BootstrapServersConfig, "dummy:1234");
//            //         Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
//            //         Put(StreamsConfig.RETRIES_CONFIG, retries);
//            //     }
//            // });

//            //            try {
//            //                new GlobalStateManagerImpl(
//            //                    new LogContext("mock"),
//            //                    topology,
//            //                    consumer,
//            //                    stateDirectory,
//            //                    stateRestoreListener,
//            //                    streamsConfig);
//            //            } catch (StreamsException expected) {
//            //                Assert.Equal(numberOfCalls.Get(), retries);
//            //            }
//        }

//        [Fact]
//        public void shouldRetryWhenPartitionsForThrowsTimeoutException()
//        {
//            int retries = 2;
//            int numberOfCalls = 0;
//            //            consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST)
//            //            {
//            //
//            //                public List<PartitionInfo> partitionsFor(string topic)
//            //            {
//            //                numberOfCalls.incrementAndGet();
//            //                throw new TimeoutException();
//            //            }
//            //        };
//            //            streamsConfig = new StreamsConfig(new StreamsConfig()
//            //        {
//            //            {
//            //                Put(StreamsConfig.ApplicationIdConfig, "appId");
//            //            Put(StreamsConfig.BootstrapServersConfig, "dummy:1234");
//            //            Put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().getPath());
//            //            Put(StreamsConfig.RETRIES_CONFIG, retries);
//            //        }
//            //    });

//            try
//            {
//                new GlobalStateManagerImpl(
//                    new LogContext("mock"),
//                    topology,
//                    consumer,
//                    stateDirectory,
//                    stateRestoreListener,
//                    streamsConfig);
//            }
//            catch (StreamsException expected)
//            {
//                Assert.Equal(numberOfCalls, retries);
//            }
//        }

//        [Fact]
//        public void ShouldDeleteAndRecreateStoreDirectoryOnReinitialize()
//        { //throws IOException
//            var storeDirectory1 = new DirectoryInfo(stateDirectory.GlobalStateDir().FullName
//                                                      + Path.DirectorySeparatorChar + "rocksdb"
//                                                      + Path.DirectorySeparatorChar + storeName1);

//            var storeDirectory2 = new DirectoryInfo(stateDirectory.GlobalStateDir().FullName
//                                                      + Path.DirectorySeparatorChar + "rocksdb"
//                                                      + Path.DirectorySeparatorChar + storeName2);
//            var storeDirectory3 = new DirectoryInfo(stateDirectory.GlobalStateDir().FullName
//                                                      + Path.DirectorySeparatorChar + storeName3);
//            var storeDirectory4 = new DirectoryInfo(stateDirectory.GlobalStateDir().FullName
//                                                      + Path.DirectorySeparatorChar + storeName4);
//            var testFile1 = new FileInfo(storeDirectory1.FullName + Path.DirectorySeparatorChar + "testFile");
//            var testFile2 = new FileInfo(storeDirectory2.FullName + Path.DirectorySeparatorChar + "testFile");
//            var testFile3 = new FileInfo(storeDirectory3.FullName + Path.DirectorySeparatorChar + "testFile");
//            var testFile4 = new FileInfo(storeDirectory4.FullName + Path.DirectorySeparatorChar + "testFile");

//            consumer.UpdatePartitions(t1.Topic, Collections.singletonList(new PartitionInfo(t1.Topic, t1.Partition, null, null, null)));
//            consumer.UpdatePartitions(t2.Topic, Collections.singletonList(new PartitionInfo(t2.Topic, t2.Partition, null, null, null)));
//            consumer.UpdatePartitions(t3.Topic, Collections.singletonList(new PartitionInfo(t3.Topic, t3.Partition, null, null, null)));
//            consumer.UpdatePartitions(t4.Topic, Collections.singletonList(new PartitionInfo(t4.Topic, t4.Partition, null, null, null)));
//            //    consumer.UpdateBeginningOffsets(new Dictionary<TopicPartition, long>() {
//            //                {
//            //                    Put(t1, 0L);
//            //    Put(t2, 0L);
//            //    Put(t3, 0L);
//            //    Put(t4, 0L);
//            //}
//            //            });
//            //            consumer.updateEndOffsets(new Dictionary<TopicPartition, long>() {
//            //                {
//            //                    Put(t1, 0L);
//            //Put(t2, 0L);
//            //Put(t3, 0L);
//            //Put(t4, 0L);
//            //                }
//            //            });

//            stateManager.initialize();
//            stateManager.register(store1, stateRestoreCallback);
//            stateManager.register(store2, stateRestoreCallback);
//            stateManager.register(store3, stateRestoreCallback);
//            stateManager.register(store4, stateRestoreCallback);

//            testFile1.Create().Close();
//            Assert.True(testFile1.Exists);
//            testFile2.Create().Close();
//            Assert.True(testFile2.Exists);
//            testFile3.Create().Close();
//            Assert.True(testFile3.Exists);
//            testFile4.Create().Close();
//            Assert.True(testFile4.Exists);

//            // only delete and recreate store 1 and 3 -- 2 and 4 must be untouched
//            stateManager.reinitializeStateStoresForPartitions(Arrays.asList(t1, t3), processorContext);

//            Assert.False(testFile1.Exists);
//            Assert.True(testFile2.Exists);
//            Assert.False(testFile3.Exists);
//            Assert.True(testFile4.Exists);
//        }

//        private void WriteCorruptCheckpoint()
//        {
//            var checkpointFile = new FileInfo(Path.Combine(stateManager.baseDir(), StateManagerUtil.CHECKPOINT_FILE_NAME));
//            StreamWriter stream = new StreamWriter(checkpointFile.FullName);
//            stream.Write("0\n1\nfoo".GetBytes());
//        }

//        private void initializeConsumer(long numRecords, long startOffset, TopicPartition topicPartition)
//        {
//            Dictionary<TopicPartition, long> startOffsets = new Dictionary<TopicPartition, long>();
//            startOffsets.Put(topicPartition, startOffset);
//            Dictionary<TopicPartition, long> endOffsets = new Dictionary<TopicPartition, long>();
//            endOffsets.Put(topicPartition, startOffset + numRecords);
//            consumer.UpdatePartitions(topicPartition.Topic, Collections.singletonList(new PartitionInfo(topicPartition.Topic, topicPartition.Partition, null, null, null)));
//            consumer.Assign(Collections.singletonList(topicPartition));
//            consumer.UpdateEndOffsets(endOffsets);
//            consumer.UpdateBeginningOffsets(startOffsets);

//            for (int i = 0; i < numRecords; i++)
//            {
//                consumer.AddRecord(new ConsumeResult<>(topicPartition.Topic, topicPartition.Partition, startOffset + i, "key".GetBytes(), "value".GetBytes()));
//            }
//        }

//        private Dictionary<TopicPartition, long?> WriteCheckpoint()
//        {
//            OffsetCheckpoint checkpoint = new OffsetCheckpoint(checkpointFile);
//            Dictionary<TopicPartition, long?> expected = new Dictionary<TopicPartition, long?> { { t1, 1L } };
//            checkpoint.Write(expected);
//            return expected;
//        }

//        private class TheStateRestoreCallback : IStateRestoreCallback
//        {
//            private List<KeyValuePair<byte[], byte[]>> restored = new List<KeyValuePair<byte[], byte[]>>();

//            public void Restore(byte[] key, byte[] value)
//            {
//                restored.Add(KeyValuePair.Create(key, value));
//            }
//        }

//        private class ConverterStore<K, V>
//            : NoOpReadOnlyStore<K, V>,
//            ITimestampedBytesStore
//        {
//            public ConverterStore(string Name, bool rocksdbStore)
//                : base(Name, rocksdbStore)
//            {
//            }
//        }
//    }
//}
