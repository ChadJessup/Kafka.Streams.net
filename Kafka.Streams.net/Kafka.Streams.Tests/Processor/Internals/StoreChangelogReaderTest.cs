/*






 *

 *





 */















































public class StoreChangelogReaderTest {

    @Mock(type = MockType.NICE)
    private RestoringTasks active;
    @Mock(type = MockType.NICE)
    private StreamTask task;

    private MockStateRestoreListener callback = new MockStateRestoreListener();
    private CompositeRestoreListener restoreListener = new CompositeRestoreListener(callback);
    private MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private StateRestoreListener stateRestoreListener = new MockStateRestoreListener();
    private TopicPartition topicPartition = new TopicPartition("topic", 0);
    private LogContext logContext = new LogContext("test-reader ");
    private StoreChangelogReader changelogReader = new StoreChangelogReader(
        consumer,
        Duration.ZERO,
        stateRestoreListener,
        logContext);

    
    public void setUp() {
        restoreListener.setUserRestoreListener(stateRestoreListener);
    }

    [Xunit.Fact]
    public void shouldRequestTopicsAndHandleTimeoutException() {
        AtomicBoolean functionCalled = new AtomicBoolean(false);
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
            
            public Dictionary<string, List<PartitionInfo>> listTopics() {
                functionCalled.set(true);
                throw new TimeoutException("KABOOM!");
            }
        };

        StoreChangelogReader changelogReader = new StoreChangelogReader(
            consumer,
            Duration.ZERO,
            stateRestoreListener,
            logContext);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity()));
        changelogReader.restore(active);
        Assert.True(functionCalled.get());
    }

    [Xunit.Fact]
    public void shouldThrowExceptionIfConsumerHasCurrentSubscription() {
        StateRestorer mockRestorer = EasyMock.mock(StateRestorer);
        mockRestorer.setUserRestoreListener(stateRestoreListener);
        expect(mockRestorer.partition())
                .andReturn(new TopicPartition("sometopic", 0))
                .andReturn(new TopicPartition("sometopic", 0))
                .andReturn(new TopicPartition("sometopic", 0))
                .andReturn(new TopicPartition("sometopic", 0));
        EasyMock.replay(mockRestorer);
        changelogReader.register(mockRestorer);

        consumer.subscribe(Collections.singleton("sometopic"));

        try {
            changelogReader.restore(active);
            Assert.True(false, "Should have thrown IllegalStateException");
        } catch (StreamsException expected) {
            // ok
        }
    }

    [Xunit.Fact]
    public void shouldRestoreAllMessagesFromBeginningWhenCheckpointNull() {
        int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity()));
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);

        Assert.Equal(callback.restored.Count, (messages));
    }

    [Xunit.Fact]
    public void shouldRecoverFromInvalidOffsetExceptionAndFinishRestore() {
        int messages = 10;
        setupConsumer(messages, topicPartition);
        consumer.setException(new InvalidOffsetException("Try Again!") {
            
            public HashSet<TopicPartition> partitions() {
                return Collections.singleton(topicPartition);
            }
        });
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity()));

        EasyMock.expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        EasyMock.replay(active, task);

        // first restore call "fails" but we should not die with an exception
        Assert.Equal(0, changelogReader.restore(active).Count);

        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity()));
        // retry restore should succeed
        Assert.Equal(1, changelogReader.restore(active).Count);
        Assert.Equal(callback.restored.Count, (messages));
    }

    [Xunit.Fact]
    public void shouldRecoverFromOffsetOutOfRangeExceptionAndRestoreFromStart() {
        int messages = 10;
        int startOffset = 5;
        long expiredCheckpoint = 1L;
        assignPartition(messages, topicPartition);
        consumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, (long) startOffset));
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, (long) (messages + startOffset)));

        addRecords(messages, topicPartition, startOffset);
        consumer.assign(Collections.emptyList());

        StateRestorer stateRestorer = new StateRestorer(
            topicPartition,
            restoreListener,
            expiredCheckpoint,
            long.MaxValue,
            true,
            "storeName",
            identity());
        changelogReader.register(stateRestorer);

        EasyMock.expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        EasyMock.replay(active, task);

        // first restore call "fails" since OffsetOutOfRangeException but we should not die with an exception
        Assert.Equal(0, changelogReader.restore(active).Count);
        //the starting offset for stateRestorer is set to NO_CHECKPOINT
        Assert.Equal(stateRestorer.checkpoint(), (-1L));

        //restore the active task again
        changelogReader.register(stateRestorer);
        //the restored task should return completed partition without Exception.
        Assert.Equal(1, changelogReader.restore(active).Count);
        //the restored size should be equal to message length.
        Assert.Equal(callback.restored.Count, (messages));
    }

    [Xunit.Fact]
    public void shouldRestoreMessagesFromCheckpoint() {
        int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            5L,
            long.MaxValue,
            true,
            "storeName",
            identity()));

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (5));
    }

    [Xunit.Fact]
    public void shouldClearAssignmentAtEndOfRestore() {
        int messages = 1;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity()));
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);
        Assert.Equal(consumer.assignment(), (Collections.<TopicPartition>emptySet()));
    }

    [Xunit.Fact]
    public void shouldRestoreToLimitWhenSupplied() {
        setupConsumer(10, topicPartition);
        StateRestorer restorer = new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            3,
            true,
            "storeName",
            identity());
        changelogReader.register(restorer);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (3));
        Assert.Equal(restorer.restoredOffset(), (3L));
    }

    [Xunit.Fact]
    public void shouldRestoreMultipleStores() {
        TopicPartition one = new TopicPartition("one", 0);
        TopicPartition two = new TopicPartition("two", 0);
        MockRestoreCallback callbackOne = new MockRestoreCallback();
        MockRestoreCallback callbackTwo = new MockRestoreCallback();
        CompositeRestoreListener restoreListener1 = new CompositeRestoreListener(callbackOne);
        CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);
        setupConsumer(10, topicPartition);
        setupConsumer(5, one);
        setupConsumer(3, two);

        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName1",
            identity()));
        changelogReader.register(new StateRestorer(
            one,
            restoreListener1,
            null,
            long.MaxValue,
            true,
            "storeName2",
            identity()));
        changelogReader.register(new StateRestorer(
            two,
            restoreListener2,
            null,
            long.MaxValue,
            true,
            "storeName3",
            identity()));

        expect(active.restoringTaskFor(one)).andStubReturn(task);
        expect(active.restoringTaskFor(two)).andStubReturn(task);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);

        Assert.Equal(callback.restored.Count, (10));
        Assert.Equal(callbackOne.restored.Count, (5));
        Assert.Equal(callbackTwo.restored.Count, (3));
    }

    [Xunit.Fact]
    public void shouldRestoreAndNotifyMultipleStores() {
        TopicPartition one = new TopicPartition("one", 0);
        TopicPartition two = new TopicPartition("two", 0);
        MockStateRestoreListener callbackOne = new MockStateRestoreListener();
        MockStateRestoreListener callbackTwo = new MockStateRestoreListener();
        CompositeRestoreListener restoreListener1 = new CompositeRestoreListener(callbackOne);
        CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);
        setupConsumer(10, topicPartition);
        setupConsumer(5, one);
        setupConsumer(3, two);

        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            0L,
            long.MaxValue,
            true,
            "storeName1",
            identity()));
        changelogReader.register(new StateRestorer(
            one,
            restoreListener1,
            0L,
            long.MaxValue,
            true,
            "storeName2",
            identity()));
        changelogReader.register(new StateRestorer(
            two,
            restoreListener2,
            0L,
            long.MaxValue,
            true,
            "storeName3",
            identity()));

        expect(active.restoringTaskFor(one)).andReturn(task);
        expect(active.restoringTaskFor(two)).andReturn(task);
        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);

        changelogReader.restore(active);

        Assert.Equal(callback.restored.Count, (10));
        Assert.Equal(callbackOne.restored.Count, (5));
        Assert.Equal(callbackTwo.restored.Count, (3));

        assertAllCallbackStatesExecuted(callback, "storeName1");
        assertCorrectOffsetsReportedByListener(callback, 0L, 9L, 10L);

        assertAllCallbackStatesExecuted(callbackOne, "storeName2");
        assertCorrectOffsetsReportedByListener(callbackOne, 0L, 4L, 5L);

        assertAllCallbackStatesExecuted(callbackTwo, "storeName3");
        assertCorrectOffsetsReportedByListener(callbackTwo, 0L, 2L, 3L);
    }

    [Xunit.Fact]
    public void shouldOnlyReportTheLastRestoredOffset() {
        setupConsumer(10, topicPartition);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            0L,
            5,
            true,
            "storeName1",
            identity()));
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);

        Assert.Equal(callback.restored.Count, (5));
        assertAllCallbackStatesExecuted(callback, "storeName1");
        assertCorrectOffsetsReportedByListener(callback, 0L, 4L, 5L);
    }

    private void assertAllCallbackStatesExecuted(MockStateRestoreListener restoreListener,
                                                 string storeName) {
        Assert.Equal(restoreListener.storeNameCalledStates.get(RESTORE_START), (storeName));
        Assert.Equal(restoreListener.storeNameCalledStates.get(RESTORE_BATCH), (storeName));
        Assert.Equal(restoreListener.storeNameCalledStates.get(RESTORE_END), (storeName));
    }

    private void assertCorrectOffsetsReportedByListener(MockStateRestoreListener restoreListener,
                                                        long startOffset,
                                                        long batchOffset,
                                                        long totalRestored) {

        Assert.Equal(restoreListener.restoreStartOffset, (startOffset));
        Assert.Equal(restoreListener.restoredBatchOffset, (batchOffset));
        Assert.Equal(restoreListener.totalNumRestored, (totalRestored));
    }

    [Xunit.Fact]
    public void shouldNotRestoreAnythingWhenPartitionIsEmpty() {
        StateRestorer restorer = new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity());
        setupConsumer(0, topicPartition);
        changelogReader.register(restorer);

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (0));
        Assert.Equal(restorer.restoredOffset(), (0L));
    }

    [Xunit.Fact]
    public void shouldNotRestoreAnythingWhenCheckpointAtEndOffset() {
        long endOffset = 10L;
        setupConsumer(endOffset, topicPartition);
        StateRestorer restorer = new StateRestorer(
            topicPartition,
            restoreListener,
            endOffset,
            long.MaxValue,
            true,
            "storeName",
            identity());

        changelogReader.register(restorer);

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (0));
        Assert.Equal(restorer.restoredOffset(), (endOffset));
    }

    [Xunit.Fact]
    public void shouldReturnRestoredOffsetsForPersistentStores() {
        setupConsumer(10, topicPartition);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity()));
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);

        Dictionary<TopicPartition, long> restoredOffsets = changelogReader.restoredOffsets();
        Assert.Equal(restoredOffsets, (Collections.singletonMap(topicPartition, 10L)));
    }

    [Xunit.Fact]
    public void shouldNotReturnRestoredOffsetsForNonPersistentStore() {
        setupConsumer(10, topicPartition);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            false,
            "storeName",
            identity()));
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);
        Dictionary<TopicPartition, long> restoredOffsets = changelogReader.restoredOffsets();
        Assert.Equal(restoredOffsets, (Collections.emptyMap()));
    }

    [Xunit.Fact]
    public void shouldIgnoreNullKeysWhenRestoring() {
        assignPartition(3, topicPartition);
        byte[] bytes = new byte[0];
        consumer.addRecord(new ConsumeResult<>(topicPartition.topic(), topicPartition.partition(), 0, bytes, bytes));
        consumer.addRecord(new ConsumeResult<>(topicPartition.topic(), topicPartition.partition(), 1, null, bytes));
        consumer.addRecord(new ConsumeResult<>(topicPartition.topic(), topicPartition.partition(), 2, bytes, bytes));
        consumer.assign(Collections.singletonList(topicPartition));
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            false,
            "storeName",
            identity()));
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);
        changelogReader.restore(active);

        Assert.Equal(callback.restored, CoreMatchers.equalTo(asList(KeyValuePair.Create(bytes, bytes), KeyValuePair.Create(bytes, bytes))));
    }

    [Xunit.Fact]
    public void shouldCompleteImmediatelyWhenEndOffsetIs0() {
        Collection<TopicPartition> expected = Collections.singleton(topicPartition);
        setupConsumer(0, topicPartition);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "store",
            identity()));
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        replay(active, task);

        Collection<TopicPartition> restored = changelogReader.restore(active);
        Assert.Equal(restored, (expected));
    }

    [Xunit.Fact]
    public void shouldRestorePartitionsRegisteredPostInitialization() {
        MockRestoreCallback callbackTwo = new MockRestoreCallback();
        CompositeRestoreListener restoreListener2 = new CompositeRestoreListener(callbackTwo);

        setupConsumer(1, topicPartition);
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 10L));
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            false,
            "storeName",
            identity()));

        TopicPartition postInitialization = new TopicPartition("other", 0);
        expect(active.restoringTaskFor(topicPartition)).andStubReturn(task);
        expect(active.restoringTaskFor(postInitialization)).andStubReturn(task);
        replay(active, task);

        Assert.True(changelogReader.restore(active).isEmpty());

        addRecords(9, topicPartition, 1);

        setupConsumer(3, postInitialization);
        consumer.updateBeginningOffsets(Collections.singletonMap(postInitialization, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(postInitialization, 3L));

        changelogReader.register(new StateRestorer(
            postInitialization,
            restoreListener2,
            null,
            long.MaxValue,
            false,
            "otherStore",
            identity()));

        Collection<TopicPartition> expected = Utils.mkSet(topicPartition, postInitialization);
        consumer.assign(expected);

        Assert.Equal(changelogReader.restore(active), (expected));
        Assert.Equal(callback.restored.Count, (10));
        Assert.Equal(callbackTwo.restored.Count, (3));
    }

    [Xunit.Fact]
    public void shouldNotThrowTaskMigratedExceptionIfSourceTopicUpdatedDuringRestoreProcess() {
        int messages = 10;
        setupConsumer(messages, topicPartition);
        // in this case first call to endOffsets returns correct value, but a second thread has updated the source topic
        // but since it's a source topic, the second check should not fire hence no exception
        consumer.addEndOffsets(Collections.singletonMap(topicPartition, 15L));
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            9L,
            true,
            "storeName",
            identity()));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
    }

    [Xunit.Fact]
    public void shouldNotThrowTaskMigratedExceptionDuringRestoreForChangelogTopicWhenEndOffsetNotExceededEOSEnabled() {
        int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);
        // records have offsets of 0..9 10 is commit marker so 11 is end offset
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 11L));

        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity()));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (10));
    }

    [Xunit.Fact]
    public void shouldNotThrowTaskMigratedExceptionDuringRestoreForChangelogTopicWhenEndOffsetNotExceededEOSDisabled() {
        int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);

        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            long.MaxValue,
            true,
            "storeName",
            identity()));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (10));
    }

    [Xunit.Fact]
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetGetsExceededDuringRestoreForSourceTopic() {
        int messages = 10;
        setupConsumer(messages, topicPartition);
        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            5,
            true,
            "storeName",
            identity()));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (5));
    }

    [Xunit.Fact]
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetNotExceededDuringRestoreForSourceTopic() {
        int messages = 10;
        setupConsumer(messages, topicPartition);

        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            10,
            true,
            "storeName",
            identity()));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (10));
    }

    [Xunit.Fact]
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetGetsExceededDuringRestoreForSourceTopicEOSEnabled() {
        int totalMessages = 10;
        assignPartition(totalMessages, topicPartition);
        // records 0..4 last offset before commit is 4
        addRecords(5, topicPartition, 0);
        //EOS enabled so commit marker at offset 5 so records start at 6
        addRecords(5, topicPartition, 6);
        consumer.assign(Collections.emptyList());
        // commit marker is 5 so ending offset is 12
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 12L));

        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            6,
            true,
            "storeName",
            identity()));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (5));
    }

    [Xunit.Fact]
    public void shouldNotThrowTaskMigratedExceptionIfEndOffsetNotExceededDuringRestoreForSourceTopicEOSEnabled() {
        int totalMessages = 10;
        setupConsumer(totalMessages, topicPartition);
        // records have offsets 0..9 10 is commit marker so 11 is ending offset
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, 11L));

        changelogReader.register(new StateRestorer(
            topicPartition,
            restoreListener,
            null,
            11,
            true,
            "storeName",
            identity()));

        expect(active.restoringTaskFor(topicPartition)).andReturn(task);
        replay(active);

        changelogReader.restore(active);
        Assert.Equal(callback.restored.Count, (10));
    }

    private void setupConsumer(long messages,
                               TopicPartition topicPartition) {
        assignPartition(messages, topicPartition);
        addRecords(messages, topicPartition, 0);
        consumer.assign(Collections.emptyList());
    }

    private void addRecords(long messages,
                            TopicPartition topicPartition,
                            int startingOffset) {
        for (int i = 0; i < messages; i++) {
            consumer.addRecord(new ConsumeResult<>(
                topicPartition.topic(),
                topicPartition.partition(),
                startingOffset + i,
                new byte[0],
                new byte[0]));
        }
    }

    private void assignPartition(long messages,
                                 TopicPartition topicPartition) {
        consumer.updatePartitions(
            topicPartition.topic(),
            Collections.singletonList(new PartitionInfo(
                topicPartition.topic(),
                topicPartition.partition(),
                null,
                null,
                null)));
        consumer.updateBeginningOffsets(Collections.singletonMap(topicPartition, 0L));
        consumer.updateEndOffsets(Collections.singletonMap(topicPartition, Math.max(0, messages)));
        consumer.assign(Collections.singletonList(topicPartition));
    }

}
