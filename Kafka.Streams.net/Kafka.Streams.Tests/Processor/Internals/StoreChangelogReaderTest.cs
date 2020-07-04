//using Confluent.Kafka;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.Tasks;
//using Kafka.Streams.Temporary;
//using Moq;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class StoreChangelogReaderTest
//    {
//        private IRestoringTasks active;
//        private StreamTask task;

//        private MockStateRestoreListener callback = new MockStateRestoreListener();
//        private CompositeRestoreListener restoreListener = new CompositeRestoreListener(callback);
//        private MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
//        private IStateRestoreListener stateRestoreListener = new MockStateRestoreListener();
//        private TopicPartition topicPartition = new TopicPartition("topic", 0);
//        //private LogContext logContext = new LogContext("test-reader ");
//        private StoreChangelogReader changelogReader = new StoreChangelogReader(
//            consumer,
//            TimeSpan.Zero,
//            stateRestoreListener);
//            //logContext);

//        public StoreChangelogReaderTest()
//        {
//            restoreListener.SetUserRestoreListener(stateRestoreListener);
//        }

//        [Fact]
//        public void ShouldRequestTopicsAndHandleTimeoutException()
//        {
//            bool functionCalled = false;
//            MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
//            //            {
//            //
//            //
//            //            public Dictionary<string, List<PartitionInfo>> listTopics()
//            //            {
//            //                functionCalled.set(true);
//            //                throw new TimeoutException("KABOOM!");
//            //            }
//            //        };

//            StoreChangelogReader changelogReader = new StoreChangelogReader(
//                consumer,
//                TimeSpan.Zero,
//                stateRestoreListener,
//                logContext);

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));
//            changelogReader.Restore(active);
//            Assert.True(functionCalled.Get());
//        }

//        [Fact]
//        public void ShouldThrowExceptionIfConsumerHasCurrentSubscription()
//        {
//            StateRestorer mockRestorer = Mock.Of<StateRestorer>();
//            mockRestorer.SetUserRestoreListener(stateRestoreListener);
//            expect(mockRestorer.Partition)
//                    .andReturn(new TopicPartition("sometopic", 0))
//                    .andReturn(new TopicPartition("sometopic", 0))
//                    .andReturn(new TopicPartition("sometopic", 0))
//                    .andReturn(new TopicPartition("sometopic", 0));
//            EasyMock.replay(mockRestorer);
//            changelogReader.register(mockRestorer);

//            consumer.subscribe(Collections.singleton("sometopic"));

//            try
//            {
//                changelogReader.Restore(active);
//                Assert.True(false, "Should have thrown IllegalStateException");
//            }
//            catch (StreamsException expected)
//            {
//                // ok
//            }
//        }

//        [Fact]
//        public void ShouldRestoreAllMessagesFromBeginningWhenCheckpointNull()
//        {
//            int messages = 10;
//            setupConsumer(messages, topicPartition);
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);

//            Assert.Equal(callback.restored.Count, messages);
//        }

//        [Fact]
//        public void ShouldRecoverFromInvalidOffsetExceptionAndFinishRestore()
//        {
//            int messages = 10;
//            setupConsumer(messages, topicPartition);
//            consumer.SetException(new InvalidOffsetException("Try Again!"));
//            //        {
//            //
//            //
//            //            public HashSet<TopicPartition> partitions()
//            //        {
//            //            return Collections.singleton(topicPartition);
//            //        }
//            //    });
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));

//            EasyMock.expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            EasyMock.replay(active, task);

//            // first restore call "fails" but we should not die with an exception
//            Assert.Empty(changelogReader.Restore(active));

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));
//            // retry restore should succeed
//            Assert.Single(changelogReader.Restore(active));
//            Assert.Equal(callback.restored.Count, (messages));
//        }

//        [Fact]
//        public void ShouldRecoverFromOffsetOutOfRangeExceptionAndRestoreFromStart()
//        {
//            int messages = 10;
//            int startOffset = 5;
//            long expiredCheckpoint = 1L;
//            assignPartition(messages, topicPartition);
//            consumer.UpdateBeginningOffsets(Collections.singletonMap(topicPartition, startOffset));
//            consumer.updateEndOffsets(Collections.singletonMap(topicPartition, messages + startOffset));

//            addRecords(messages, topicPartition, startOffset);
//            consumer.Assign(Collections.emptyList());

//            StateRestorer stateRestorer = new StateRestorer(
//                topicPartition,
//                restoreListener,
//                expiredCheckpoint,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity());
//            changelogReader.register(stateRestorer);

//            EasyMock.expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            EasyMock.replay(active, task);

//            // first restore call "fails" since OffsetOutOfRangeException but we should not die with an exception
//            Assert.Empty(changelogReader.Restore(active));
//            //the starting offset for stateRestorer is set to NO_CHECKPOINT
//            Assert.Equal(stateRestorer.checkpoint(), -1L);

//            //restore the active task again
//            changelogReader.register(stateRestorer);
//            //the restored task should return completed partition without Exception.
//            Assert.Single(changelogReader.Restore(active));
//            //the restored size should be equal to message length.
//            Assert.Equal(callback.restored.Count, messages);
//        }

//        [Fact]
//        public void ShouldRestoreMessagesFromCheckpoint()
//        {
//            int messages = 10;
//            setupConsumer(messages, topicPartition);
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                5L,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 5);
//        }

//        [Fact]
//        public void ShouldClearAssignmentAtEndOfRestore()
//        {
//            int messages = 1;
//            setupConsumer(messages, topicPartition);
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);
//            Assert.Equal(consumer.Assignment(), Collections.emptySet<TopicPartition>());
//        }

//        [Fact]
//        public void ShouldRestoreToLimitWhenSupplied()
//        {
//            setupConsumer(10, topicPartition);
//            StateRestorer restorer = new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                3,
//                true,
//                "storeName",
//                identity());
//            changelogReader.register(restorer);
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 3);
//            Assert.Equal(restorer.restoredOffset(), 3L);
//        }

//        [Fact]
//        public void ShouldRestoreMultipleStores()
//        {
//            TopicPartition one = new TopicPartition("one", 0);
//            TopicPartition two = new TopicPartition("two", 0);
//            MockRestoreCallback callbackOne = new MockRestoreCallback();
//            MockRestoreCallback callbackTwo = new MockRestoreCallback();
//            CompositeRestoreListener restoreListener1 = new CompositeRestoreListener(callbackOne);
//            CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);
//            setupConsumer(10, topicPartition);
//            setupConsumer(5, one);
//            setupConsumer(3, two);

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName1",
//                identity()));
//            changelogReader.register(new StateRestorer(
//                one,
//                restoreListener1,
//                null,
//                long.MaxValue,
//                true,
//                "storeName2",
//                identity()));
//            changelogReader.register(new StateRestorer(
//                two,
//                restoreListener2,
//                null,
//                long.MaxValue,
//                true,
//                "storeName3",
//                identity()));

//            expect(active.restoringTaskFor(one)).andStubReturn(task);
//            expect(active.restoringTaskFor(two)).andStubReturn(task);
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);

//            Assert.Equal(callback.restored.Count, 10);
//            Assert.Equal(callbackOne.restored.Count, 5);
//            Assert.Equal(callbackTwo.restored.Count, 3);
//        }

//        [Fact]
//        public void ShouldRestoreAndNotifyMultipleStores()
//        {
//            TopicPartition one = new TopicPartition("one", 0);
//            TopicPartition two = new TopicPartition("two", 0);
//            MockStateRestoreListener callbackOne = new MockStateRestoreListener();
//            MockStateRestoreListener callbackTwo = new MockStateRestoreListener();
//            CompositeRestoreListener restoreListener1 = new CompositeRestoreListener(callbackOne);
//            CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);
//            setupConsumer(10, topicPartition);
//            setupConsumer(5, one);
//            setupConsumer(3, two);

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                0L,
//                long.MaxValue,
//                true,
//                "storeName1",
//                identity()));
//            changelogReader.register(new StateRestorer(
//                one,
//                restoreListener1,
//                0L,
//                long.MaxValue,
//                true,
//                "storeName2",
//                identity()));
//            changelogReader.register(new StateRestorer(
//                two,
//                restoreListener2,
//                0L,
//                long.MaxValue,
//                true,
//                "storeName3",
//                identity()));

//            expect(active.restoringTaskFor(one)).andReturn(task);
//            expect(active.restoringTaskFor(two)).andReturn(task);
//            expect(active.restoringTaskFor(topicPartition)).andReturn(task);
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);

//            changelogReader.Restore(active);

//            Assert.Equal(callback.restored.Count, 10);
//            Assert.Equal(callbackOne.restored.Count, 5);
//            Assert.Equal(callbackTwo.restored.Count, 3);

//            assertAllCallbackStatesExecuted(callback, "storeName1");
//            assertCorrectOffsetsReportedByListener(callback, 0L, 9L, 10L);

//            assertAllCallbackStatesExecuted(callbackOne, "storeName2");
//            assertCorrectOffsetsReportedByListener(callbackOne, 0L, 4L, 5L);

//            assertAllCallbackStatesExecuted(callbackTwo, "storeName3");
//            assertCorrectOffsetsReportedByListener(callbackTwo, 0L, 2L, 3L);
//        }

//        [Fact]
//        public void ShouldOnlyReportTheLastRestoredOffset()
//        {
//            setupConsumer(10, topicPartition);
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                0L,
//                5,
//                true,
//                "storeName1",
//                identity()));
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);

//            Assert.Equal(callback.restored.Count, 5);
//            assertAllCallbackStatesExecuted(callback, "storeName1");
//            assertCorrectOffsetsReportedByListener(callback, 0L, 4L, 5L);
//        }

//        private void AssertAllCallbackStatesExecuted(MockStateRestoreListener restoreListener,
//                                                     string storeName)
//        {
//            Assert.Equal(restoreListener.storeNameCalledStates.Get(RESTORE_START), storeName);
//            Assert.Equal(restoreListener.storeNameCalledStates.Get(RESTORE_BATCH), storeName);
//            Assert.Equal(restoreListener.storeNameCalledStates.Get(RESTORE_END), storeName);
//        }

//        private void AssertCorrectOffsetsReportedByListener(MockStateRestoreListener restoreListener,
//                                                            long startOffset,
//                                                            long batchOffset,
//                                                            long totalRestored)
//        {

//            Assert.Equal(restoreListener.restoreStartOffset, startOffset);
//            Assert.Equal(restoreListener.restoredBatchOffset, batchOffset);
//            Assert.Equal(restoreListener.totalNumRestored, totalRestored);
//        }

//        [Fact]
//        public void ShouldNotRestoreAnythingWhenPartitionIsEmpty()
//        {
//            StateRestorer restorer = new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity());
//            setupConsumer(0, topicPartition);
//            changelogReader.register(restorer);

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 0);
//            Assert.Equal(restorer.restoredOffset(), 0L);
//        }

//        [Fact]
//        public void ShouldNotRestoreAnythingWhenCheckpointAtEndOffset()
//        {
//            long endOffset = 10L;
//            setupConsumer(endOffset, topicPartition);
//            StateRestorer restorer = new StateRestorer(
//                topicPartition,
//                restoreListener,
//                endOffset,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity());

//            changelogReader.register(restorer);

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 0);
//            Assert.Equal(restorer.restoredOffset(), endOffset);
//        }

//        [Fact]
//        public void ShouldReturnRestoredOffsetsForPersistentStores()
//        {
//            setupConsumer(10, topicPartition);
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);

//            Dictionary<TopicPartition, long> restoredOffsets = changelogReader.restoredOffsets();
//            Assert.Equal(restoredOffsets, Collections.singletonMap(topicPartition, 10L));
//        }

//        [Fact]
//        public void ShouldNotReturnRestoredOffsetsForNonPersistentStore()
//        {
//            setupConsumer(10, topicPartition);
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                false,
//                "storeName",
//                identity()));
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);
//            Dictionary<TopicPartition, long> restoredOffsets = changelogReader.restoredOffsets();
//            Assert.Equal(restoredOffsets, Collections.emptyMap());
//        }

//        [Fact]
//        public void ShouldIgnoreNullKeysWhenRestoring()
//        {
//            assignPartition(3, topicPartition);
//            byte[] bytes = System.Array.Empty<byte>();
//            consumer.AddRecord(new ConsumeResult<>(topicPartition.Topic, topicPartition.Partition, 0, bytes, bytes));
//            consumer.AddRecord(new ConsumeResult<>(topicPartition.Topic, topicPartition.Partition, 1, null, bytes));
//            consumer.AddRecord(new ConsumeResult<>(topicPartition.Topic, topicPartition.Partition, 2, bytes, bytes));
//            consumer.Assign(Collections.singletonList(topicPartition));
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                false,
//                "storeName",
//                identity()));
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);
//            changelogReader.Restore(active);

//            Assert.Equal(callback.restored, CoreMatchers.equalTo(asList(KeyValuePair.Create(bytes, bytes), KeyValuePair.Create(bytes, bytes))));
//        }

//        [Fact]
//        public void ShouldCompleteImmediatelyWhenEndOffsetIs0()
//        {
//            Collection<TopicPartition> expected = Collections.singleton(topicPartition);
//            setupConsumer(0, topicPartition);
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "store",
//                identity()));
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            replay(active, task);

//            Collection<TopicPartition> restored = changelogReader.Restore(active);
//            Assert.Equal(restored, expected);
//        }

//        [Fact]
//        public void ShouldRestorePartitionsRegisteredPostInitialization()
//        {
//            MockRestoreCallback callbackTwo = new MockRestoreCallback();
//            CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);

//            setupConsumer(1, topicPartition);
//            consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 10L));
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                false,
//                "storeName",
//                identity()));

//            TopicPartition postInitialization = new TopicPartition("other", 0);
//            expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
//            expect(active.restoringTaskFor(postInitialization)).andStubReturn(task);
//            replay(active, task);

//            Assert.True(changelogReader.Restore(active).IsEmpty());

//            addRecords(9, topicPartition, 1);

//            setupConsumer(3, postInitialization);
//            consumer.UpdateBeginningOffsets(Collections.singletonMap(postInitialization, 0L));
//            consumer.updateEndOffsets(Collections.singletonMap(postInitialization, 3L));

//            changelogReader.register(new StateRestorer(
//                postInitialization,
//                restoreListener2,
//                null,
//                long.MaxValue,
//                false,
//                "otherStore",
//                identity()));

//            Collection<TopicPartition> expected = Utils.mkSet(topicPartition, postInitialization);
//            consumer.Assign(expected);

//            Assert.Equal(changelogReader.Restore(active), expected);
//            Assert.Equal(callback.restored.Count, 10);
//            Assert.Equal(callbackTwo.restored.Count, 3);
//        }

//        [Fact]
//        public void ShouldNotThrowTaskMigratedExceptionIfSourceTopicUpdatedDuringRestoreProcess()
//        {
//            int messages = 10;
//            setupConsumer(messages, topicPartition);
//            // in this case first call to endOffsets returns correct value, but a second thread has updated the source topic
//            // but since it's a source topic, the second check should not fire hence no exception
//            consumer.addEndOffsets(Collections.singletonMap(topicPartition, 15L));
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                9L,
//                true,
//                "storeName",
//                identity()));

//            expect(active.restoringTaskFor(topicPartition)).andReturn(task);
//            replay(active);

//            changelogReader.Restore(active);
//        }

//        [Fact]
//        public void ShouldNotThrowTaskMigratedExceptionDuringRestoreForChangelogTopicWhenEndOffsetNotExceededEOSEnabled()
//        {
//            int totalMessages = 10;
//            setupConsumer(totalMessages, topicPartition);
//            // records have offsets of 0..9 10 is commit marker so 11 is end offset
//            consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 11L));

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));

//            expect(active.restoringTaskFor(topicPartition)).andReturn(task);
//            replay(active);

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 10);
//        }

//        [Fact]
//        public void ShouldNotThrowTaskMigratedExceptionDuringRestoreForChangelogTopicWhenEndOffsetNotExceededEOSDisabled()
//        {
//            int totalMessages = 10;
//            setupConsumer(totalMessages, topicPartition);

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                long.MaxValue,
//                true,
//                "storeName",
//                identity()));

//            expect(active.restoringTaskFor(topicPartition)).andReturn(task);
//            replay(active);

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 10);
//        }

//        [Fact]
//        public void ShouldNotThrowTaskMigratedExceptionIfEndOffsetGetsExceededDuringRestoreForSourceTopic()
//        {
//            int messages = 10;
//            setupConsumer(messages, topicPartition);
//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                5,
//                true,
//                "storeName",
//                identity()));

//            expect(active.restoringTaskFor(topicPartition)).andReturn(task);
//            replay(active);

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 5);
//        }

//        [Fact]
//        public void ShouldNotThrowTaskMigratedExceptionIfEndOffsetNotExceededDuringRestoreForSourceTopic()
//        {
//            int messages = 10;
//            setupConsumer(messages, topicPartition);

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                10,
//                true,
//                "storeName",
//                identity()));

//            expect(active.restoringTaskFor(topicPartition)).andReturn(task);
//            replay(active);

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 10);
//        }

//        [Fact]
//        public void ShouldNotThrowTaskMigratedExceptionIfEndOffsetGetsExceededDuringRestoreForSourceTopicEOSEnabled()
//        {
//            int totalMessages = 10;
//            assignPartition(totalMessages, topicPartition);
//            // records 0..4 last offset before commit is 4
//            addRecords(5, topicPartition, 0);
//            //EOS enabled so commit marker at offset 5 so records start at 6
//            addRecords(5, topicPartition, 6);
//            consumer.Assign(Collections.emptyList());
//            // commit marker is 5 so ending offset is 12
//            consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 12L));

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                6,
//                true,
//                "storeName",
//                identity()));

//            expect(active.restoringTaskFor(topicPartition)).andReturn(task);
//            replay(active);

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 5);
//        }

//        [Fact]
//        public void ShouldNotThrowTaskMigratedExceptionIfEndOffsetNotExceededDuringRestoreForSourceTopicEOSEnabled()
//        {
//            int totalMessages = 10;
//            setupConsumer(totalMessages, topicPartition);
//            // records have offsets 0..9 10 is commit marker so 11 is ending offset
//            consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 11L));

//            changelogReader.register(new StateRestorer(
//                topicPartition,
//                restoreListener,
//                null,
//                11,
//                true,
//                "storeName",
//                identity()));

//            expect(active.restoringTaskFor(topicPartition)).andReturn(task);
//            replay(active);

//            changelogReader.Restore(active);
//            Assert.Equal(callback.restored.Count, 10);
//        }

//        private void SetupConsumer(long messages,
//                                   TopicPartition topicPartition)
//        {
//            assignPartition(messages, topicPartition);
//            addRecords(messages, topicPartition, 0);
//            consumer.Assign(Collections.emptyList());
//        }

//        private void AddRecords(long messages,
//                                TopicPartition topicPartition,
//                                int startingOffset)
//        {
//            for (int i = 0; i < messages; i++)
//            {
//                consumer.AddRecord(new ConsumeResult<>(
//                    topicPartition.Topic,
//                    topicPartition.Partition,
//                    startingOffset + i,
//                    System.Array.Empty<byte>(),
//                    System.Array.Empty<byte>()));
//            }
//        }

//        private void AssignPartition(long messages,
//                                     TopicPartition topicPartition)
//        {
//            consumer.updatePartitions(
//                topicPartition.Topic,
//                Collections.singletonList(new PartitionInfo(
//                    topicPartition.Topic,
//                    topicPartition.Partition,
//                    null,
//                    null,
//                    null)));
//            consumer.UpdateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
//            consumer.updateEndOffsets(Collections.singletonMap(topicPartition, Math.Max(0, messages)));
//            consumer.Assign(Collections.singletonList(topicPartition));
//        }
//    }
//}
