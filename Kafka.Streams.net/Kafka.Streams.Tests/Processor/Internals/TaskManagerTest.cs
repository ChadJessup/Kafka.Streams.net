//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    /*






//    *

//    *





//    */















































//    public class TaskManagerTest
//    {

//        private TaskId taskId0 = new TaskId(0, 0);
//        private TopicPartition t1p0 = new TopicPartition("t1", 0);
//        private HashSet<TopicPartition> taskId0Partitions = Utils.mkSet(t1p0);
//        private Dictionary<TaskId, HashSet<TopicPartition>> taskId0Assignment = Collections.singletonMap(taskId0, taskId0Partitions);

//        (type = MockType.STRICT)
//    private InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates;
//        (type = MockType.STRICT)
//    private InternalTopologyBuilder topologyBuilder;
//        (type = MockType.STRICT)
//    private StateDirectory stateDirectory;
//        (type = MockType.NICE)
//    private ChangelogReader changeLogReader;
//        (type = MockType.NICE)
//    private StreamsMetadataState streamsMetadataState;
//        (type = MockType.NICE)
//    private Consumer<byte[], byte[]> restoreConsumer;
//        (type = MockType.NICE)
//    private Consumer<byte[], byte[]> consumer;
//        (type = MockType.NICE)
//    private StreamThread.AbstractTaskCreator<StreamTask> activeTaskCreator;
//        (type = MockType.NICE)
//    private StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator;
//        (type = MockType.NICE)
//    private Admin adminClient;
//        (type = MockType.NICE)
//    private StreamTask streamTask;
//        (type = MockType.NICE)
//    private StandbyTask standbyTask;
//        (type = MockType.NICE)
//    private AssignedStreamsTasks active;
//        (type = MockType.NICE)
//    private AssignedStandbyTasks standby;

//        private TaskManager taskManager;

//        private readonly string topic1 = "topic1";
//        private readonly string topic2 = "topic2";
//        private TopicPartition t1p1 = new TopicPartition(topic1, 1);
//        private TopicPartition t1p2 = new TopicPartition(topic1, 2);
//        private TopicPartition t1p3 = new TopicPartition(topic1, 3);
//        private TopicPartition t2p1 = new TopicPartition(topic2, 1);
//        private TopicPartition t2p2 = new TopicPartition(topic2, 2);
//        private TopicPartition t2p3 = new TopicPartition(topic2, 3);

//        private TaskId task01 = new TaskId(0, 1);
//        private TaskId task02 = new TaskId(0, 2);
//        private TaskId task03 = new TaskId(0, 3);
//        private TaskId task11 = new TaskId(1, 1);


//        public TemporaryFolder testFolder = new TemporaryFolder();


//        public void SetUp()
//        {
//            taskManager = new TaskManager(changeLogReader,
//                                          UUID.randomUUID(),
//                                          "",
//                                          restoreConsumer,
//                                          streamsMetadataState,
//                                          activeTaskCreator,
//                                          standbyTaskCreator,
//                                          adminClient,
//                                          active,
//                                          standby);
//            taskManager.setConsumer(consumer);
//        }

//        private void Replay()
//        {
//            EasyMock.replay(changeLogReader,
//                            restoreConsumer,
//                            consumer,
//                            activeTaskCreator,
//                            standbyTaskCreator,
//                            active,
//                            standby,
//                            adminClient);
//        }

//        [Xunit.Fact]
//        public void ShouldUpdateSubscriptionFromAssignment()
//        {
//            MockTopologyBuilder();
//            expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));
//            topologyBuilder.updateSubscribedTopics(EasyMock.eq(Utils.mkSet(topic1, topic2)), EasyMock.anyString());
//            expectLastCall().once();

//            EasyMock.replay(activeTaskCreator,
//                            topologyBuilder,
//                            subscriptionUpdates);

//            taskManager.updateSubscriptionsFromAssignment(asList(t1p1, t2p1));

//            EasyMock.verify(activeTaskCreator,
//                            topologyBuilder,
//                            subscriptionUpdates);
//        }

//        [Xunit.Fact]
//        public void ShouldNotUpdateSubscriptionFromAssignment()
//        {
//            MockTopologyBuilder();
//            expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1, topic2));

//            EasyMock.replay(activeTaskCreator,
//                            topologyBuilder,
//                            subscriptionUpdates);

//            taskManager.updateSubscriptionsFromAssignment(asList(t1p1));

//            EasyMock.verify(activeTaskCreator,
//                            topologyBuilder,
//                            subscriptionUpdates);
//        }

//        [Xunit.Fact]
//        public void ShouldUpdateSubscriptionFromMetadata()
//        {
//            MockTopologyBuilder();
//            expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));
//            topologyBuilder.updateSubscribedTopics(EasyMock.eq(Utils.mkSet(topic1, topic2)), EasyMock.anyString());
//            expectLastCall().once();

//            EasyMock.replay(activeTaskCreator,
//                    topologyBuilder,
//                    subscriptionUpdates);

//            taskManager.updateSubscriptionsFromMetadata(Utils.mkSet(topic1, topic2));

//            EasyMock.verify(activeTaskCreator,
//                    topologyBuilder,
//                    subscriptionUpdates);
//        }

//        [Xunit.Fact]
//        public void ShouldNotUpdateSubscriptionFromMetadata()
//        {
//            MockTopologyBuilder();
//            expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));

//            EasyMock.replay(activeTaskCreator,
//                    topologyBuilder,
//                    subscriptionUpdates);

//            taskManager.updateSubscriptionsFromMetadata(Utils.mkSet(topic1));

//            EasyMock.verify(activeTaskCreator,
//                    topologyBuilder,
//                    subscriptionUpdates);
//        }

//        [Xunit.Fact]
//        public void ShouldReturnCachedTaskIdsFromDirectory()
//        { //throws IOException
//            File[] taskFolders = asList(testFolder.newFolder("0_1"),
//                                                    testFolder.newFolder("0_2"),
//                                                    testFolder.newFolder("0_3"),
//                                                    testFolder.newFolder("1_1"),
//                                                    testFolder.newFolder("dummy")).ToArray(System.Array.Empty<File>());

//            Assert.True((new File(taskFolders[0], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());
//            Assert.True((new File(taskFolders[1], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());
//            Assert.True((new File(taskFolders[3], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());

//            expect(activeTaskCreator.stateDirectory()).andReturn(stateDirectory).once();
//            expect(stateDirectory.listTaskDirectories()).andReturn(taskFolders).once();

//            EasyMock.replay(activeTaskCreator, stateDirectory);

//            HashSet<TaskId> tasks = taskManager.cachedTasksIds();

//            EasyMock.verify(activeTaskCreator, stateDirectory);

//            Assert.Equal(tasks, (Utils.mkSet(task01, task02, task11)));
//        }

//        [Xunit.Fact]
//        public void ShouldCloseActiveUnAssignedSuspendedTasksWhenCreatingNewTasks()
//        {
//            MockSingleActiveTask();
//            active.closeNonAssignedSuspendedTasks(taskId0Assignment);
//            expectLastCall();
//            Replay();

//            taskManager.setAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
//            taskManager.createTasks(taskId0Partitions);

//            verify(active);
//        }

//        [Xunit.Fact]
//        public void ShouldCloseStandbyUnAssignedSuspendedTasksWhenCreatingNewTasks()
//        {
//            MockSingleActiveTask();
//            standby.closeNonAssignedSuspendedTasks(taskId0Assignment);
//            expectLastCall();
//            Replay();

//            taskManager.setAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
//            taskManager.createTasks(taskId0Partitions);

//            verify(active);
//        }

//        [Xunit.Fact]
//        public void ShouldAddNonResumedActiveTasks()
//        {
//            MockSingleActiveTask();
//            expect(active.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(false);
//            active.addNewTask(EasyMock.same(streamTask));
//            Replay();

//            taskManager.setAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
//            taskManager.createTasks(taskId0Partitions);

//            verify(activeTaskCreator, active);
//        }

//        [Xunit.Fact]
//        public void ShouldNotAddResumedActiveTasks()
//        {
//            checkOrder(active, true);
//            expect(active.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
//            Replay();

//            taskManager.setAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
//            taskManager.createTasks(taskId0Partitions);

//            // should be no calls to activeTaskCreator and no calls to active.addNewTasks(..)
//            verify(active, activeTaskCreator);
//        }

//        [Xunit.Fact]
//        public void ShouldAddNonResumedStandbyTasks()
//        {
//            MockStandbyTaskExpectations();
//            expect(standby.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(false);
//            standby.addNewTask(EasyMock.same(standbyTask));
//            Replay();

//            taskManager.setAssignmentMetadata(Collections.< TaskId, HashSet < TopicPartition >> emptyMap(), taskId0Assignment);
//            taskManager.createTasks(taskId0Partitions);

//            verify(standbyTaskCreator, active);
//        }

//        [Xunit.Fact]
//        public void ShouldNotAddResumedStandbyTasks()
//        {
//            checkOrder(active, true);
//            expect(standby.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
//            Replay();

//            taskManager.setAssignmentMetadata(Collections.< TaskId, HashSet < TopicPartition >> emptyMap(), taskId0Assignment);
//            taskManager.createTasks(taskId0Partitions);

//            // should be no calls to standbyTaskCreator and no calls to standby.addNewTasks(..)
//            verify(standby, standbyTaskCreator);
//        }

//        [Xunit.Fact]
//        public void ShouldPauseActivePartitions()
//        {
//            MockSingleActiveTask();
//            consumer.pause(taskId0Partitions);
//            expectLastCall();
//            Replay();

//            taskManager.setAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
//            taskManager.createTasks(taskId0Partitions);
//            verify(consumer);
//        }

//        [Xunit.Fact]
//        public void ShouldSuspendActiveTasks()
//        {
//            expect(active.suspend()).andReturn(null);
//            Replay();

//            taskManager.suspendTasksAndState();
//            verify(active);
//        }

//        [Xunit.Fact]
//        public void ShouldSuspendStandbyTasks()
//        {
//            expect(standby.suspend()).andReturn(null);
//            Replay();

//            taskManager.suspendTasksAndState();
//            verify(standby);
//        }

//        [Xunit.Fact]
//        public void ShouldUnassignChangelogPartitionsOnSuspend()
//        {
//            restoreConsumer.unsubscribe();
//            expectLastCall();
//            Replay();

//            taskManager.suspendTasksAndState();
//            verify(restoreConsumer);
//        }

//        [Xunit.Fact]
//        public void ShouldThrowStreamsExceptionAtEndIfExceptionDuringSuspend()
//        {
//            expect(active.suspend()).andReturn(new RuntimeException(""));
//            expect(standby.suspend()).andReturn(new RuntimeException(""));
//            expectLastCall();
//            restoreConsumer.unsubscribe();

//            Replay();
//            try
//            {
//                taskManager.suspendTasksAndState();
//                Assert.True(false, "Should have thrown streams exception");
//            }
//            catch (StreamsException e)
//            {
//                // expected
//            }
//            verify(restoreConsumer, active, standby);
//        }

//        [Xunit.Fact]
//        public void ShouldCloseActiveTasksOnShutdown()
//        {
//            active.close(true);
//            expectLastCall();
//            Replay();

//            taskManager.shutdown(true);
//            verify(active);
//        }

//        [Xunit.Fact]
//        public void ShouldCloseStandbyTasksOnShutdown()
//        {
//            standby.close(false);
//            expectLastCall();
//            Replay();

//            taskManager.shutdown(false);
//            verify(standby);
//        }

//        [Xunit.Fact]
//        public void ShouldUnassignChangelogPartitionsOnShutdown()
//        {
//            restoreConsumer.unsubscribe();
//            expectLastCall();
//            Replay();

//            taskManager.shutdown(true);
//            verify(restoreConsumer);
//        }

//        [Xunit.Fact]
//        public void ShouldInitializeNewActiveTasks()
//        {
//            active.updateRestored(EasyMock.< Collection < TopicPartition >> anyObject());
//            expectLastCall();
//            Replay();

//            taskManager.updateNewAndRestoringTasks();
//            verify(active);
//        }

//        [Xunit.Fact]
//        public void ShouldInitializeNewStandbyTasks()
//        {
//            active.updateRestored(EasyMock.< Collection < TopicPartition >> anyObject());
//            expectLastCall();
//            Replay();

//            taskManager.updateNewAndRestoringTasks();
//            verify(standby);
//        }

//        [Xunit.Fact]
//        public void ShouldRestoreStateFromChangeLogReader()
//        {
//            expect(changeLogReader.restore(active)).andReturn(taskId0Partitions);
//            active.updateRestored(taskId0Partitions);
//            expectLastCall();
//            Replay();

//            taskManager.updateNewAndRestoringTasks();
//            verify(changeLogReader, active);
//        }

//        [Xunit.Fact]
//        public void ShouldResumeRestoredPartitions()
//        {
//            expect(changeLogReader.restore(active)).andReturn(taskId0Partitions);
//            expect(active.allTasksRunning()).andReturn(true);
//            expect(consumer.assignment()).andReturn(taskId0Partitions);
//            expect(standby.running()).andReturn(Collections.< StandbyTask > emptySet());

//            consumer.resume(taskId0Partitions);
//            expectLastCall();
//            Replay();

//            taskManager.updateNewAndRestoringTasks();
//            verify(consumer);
//        }

//        [Xunit.Fact]
//        public void ShouldAssignStandbyPartitionsWhenAllActiveTasksAreRunning()
//        {
//            MockAssignStandbyPartitions(1L);
//            Replay();

//            taskManager.updateNewAndRestoringTasks();
//            verify(restoreConsumer);
//        }

//        [Xunit.Fact]
//        public void ShouldReturnTrueWhenActiveAndStandbyTasksAreRunning()
//        {
//            MockAssignStandbyPartitions(1L);
//            expect(standby.allTasksRunning()).andReturn(true);
//            Replay();

//            Assert.True(taskManager.updateNewAndRestoringTasks());
//        }

//        [Xunit.Fact]
//        public void ShouldReturnFalseWhenOnlyActiveTasksAreRunning()
//        {
//            MockAssignStandbyPartitions(1L);
//            expect(standby.allTasksRunning()).andReturn(false);
//            Replay();

//            Assert.False(taskManager.updateNewAndRestoringTasks());
//        }

//        [Xunit.Fact]
//        public void ShouldReturnFalseWhenThereAreStillNonRunningTasks()
//        {
//            expect(active.allTasksRunning()).andReturn(false);
//            Replay();

//            Assert.False(taskManager.updateNewAndRestoringTasks());
//        }

//        [Xunit.Fact]
//        public void ShouldSeekToCheckpointedOffsetOnStandbyPartitionsWhenOffsetGreaterThanEqualTo0()
//        {
//            MockAssignStandbyPartitions(1L);
//            restoreConsumer.seek(t1p0, 1L);
//            expectLastCall();
//            Replay();

//            taskManager.updateNewAndRestoringTasks();
//            verify(restoreConsumer);
//        }

//        [Xunit.Fact]
//        public void ShouldSeekToBeginningIfOffsetIsLessThan0()
//        {
//            MockAssignStandbyPartitions(-1L);
//            restoreConsumer.seekToBeginning(taskId0Partitions);
//            expectLastCall();
//            Replay();

//            taskManager.updateNewAndRestoringTasks();
//            verify(restoreConsumer);
//        }

//        [Xunit.Fact]
//        public void ShouldCommitActiveAndStandbyTasks()
//        {
//            expect(active.commit()).andReturn(1);
//            expect(standby.commit()).andReturn(2);

//            Replay();

//            Assert.Equal(taskManager.commitAll(), (3));
//            verify(active, standby);
//        }

//        [Xunit.Fact]
//        public void ShouldPropagateExceptionFromActiveCommit()
//        {
//            // upgrade to strict mock to ensure no calls
//            checkOrder(standby, true);
//            active.commit();
//            expectLastCall().andThrow(new RuntimeException(""));
//            Replay();

//            try
//            {
//                taskManager.commitAll();
//                Assert.True(false, "should have thrown first exception");
//            }
//            catch (Exception e)
//            {
//                // ok
//            }
//            verify(active, standby);
//        }

//        [Xunit.Fact]
//        public void ShouldPropagateExceptionFromStandbyCommit()
//        {
//            expect(standby.commit()).andThrow(new RuntimeException(""));
//            Replay();

//            try
//            {
//                taskManager.commitAll();
//                Assert.True(false, "should have thrown exception");
//            }
//            catch (Exception e)
//            {
//                // ok
//            }
//            verify(standby);
//        }

//        [Xunit.Fact]
//        public void ShouldSendPurgeData()
//        {
//            KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
//            Dictionary<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
//            DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>)futureDeletedRecords));

//            futureDeletedRecords.complete(null);

//            expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
//            expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).times(2);
//            Replay();

//            taskManager.maybePurgeCommitedRecords();
//            taskManager.maybePurgeCommitedRecords();
//            verify(active, adminClient);
//        }

//        [Xunit.Fact]
//        public void ShouldNotSendPurgeDataIfPreviousNotDone()
//        {
//            KafkaFuture<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
//            Dictionary<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
//            DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, futureDeletedRecords));

//            expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).once();
//            expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).once();
//            Replay();

//            taskManager.maybePurgeCommitedRecords();
//            // second call should be no-op as the previous one is not done yet
//            taskManager.maybePurgeCommitedRecords();
//            verify(active, adminClient);
//        }

//        [Xunit.Fact]
//        public void ShouldIgnorePurgeDataErrors()
//        {
//            KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
//            Dictionary<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
//            DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>)futureDeletedRecords));

//            futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));

//            expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
//            expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).times(2);
//            Replay();

//            taskManager.maybePurgeCommitedRecords();
//            taskManager.maybePurgeCommitedRecords();
//            verify(active, adminClient);
//        }

//        [Xunit.Fact]
//        public void ShouldMaybeCommitActiveTasks()
//        {
//            expect(active.maybeCommitPerUserRequested()).andReturn(5);
//            Replay();

//            Assert.Equal(taskManager.maybeCommitActiveTasksPerUserRequested(), (5));
//            verify(active);
//        }

//        [Xunit.Fact]
//        public void ShouldProcessActiveTasks()
//        {
//            expect(active.process(0L)).andReturn(10);
//            Replay();

//            Assert.Equal(taskManager.process(0L), (10));
//            verify(active);
//        }

//        [Xunit.Fact]
//        public void ShouldPunctuateActiveTasks()
//        {
//            expect(active.punctuate()).andReturn(20);
//            Replay();

//            Assert.Equal(taskManager.punctuate(), (20));
//            verify(active);
//        }

//        [Xunit.Fact]
//        public void ShouldNotResumeConsumptionUntilAllStoresRestored()
//        {
//            expect(active.allTasksRunning()).andReturn(false);
//            Consumer<byte[], byte[]> consumer = EasyMock.createStrictMock(Consumer);
//            taskManager.setConsumer(consumer);
//            EasyMock.replay(active, consumer);

//            // shouldn't invoke `resume` method in consumer
//            taskManager.updateNewAndRestoringTasks();
//            EasyMock.verify(consumer);
//        }

//        [Xunit.Fact]
//        public void ShouldUpdateTasksFromPartitionAssignment()
//        {
//            Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
//            Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks = new HashMap<>();

//            taskManager.setAssignmentMetadata(activeTasks, standbyTasks);
//            Assert.True(taskManager.assignedActiveTasks().isEmpty());

//            // assign two active tasks with two partitions each
//            activeTasks.put(task01, new HashSet<>(asList(t1p1, t2p1)));
//            activeTasks.put(task02, new HashSet<>(asList(t1p2, t2p2)));

//            // assign one standby task with two partitions
//            standbyTasks.put(task03, new HashSet<>(asList(t1p3, t2p3)));
//            taskManager.setAssignmentMetadata(activeTasks, standbyTasks);

//            Assert.Equal(taskManager.assignedActiveTasks(), (activeTasks));
//            Assert.Equal(taskManager.assignedStandbyTasks(), (standbyTasks));
//        }

//        private void MockAssignStandbyPartitions(long offset)
//        {
//            StandbyTask task = EasyMock.createNiceMock(StandbyTask);
//            expect(active.allTasksRunning()).andReturn(true);
//            expect(standby.running()).andReturn(Collections.singletonList(task));
//            expect(task.checkpointedOffsets()).andReturn(Collections.singletonMap(t1p0, offset));
//            restoreConsumer.assign(taskId0Partitions);

//            expectLastCall();
//            EasyMock.replay(task);
//        }

//        private void MockStandbyTaskExpectations()
//        {
//            expect(standbyTaskCreator.createTasks(EasyMock.< Consumer<byte[], byte[]> > anyObject(),
//                                                       EasyMock.eq(taskId0Assignment)))
//                    .andReturn(Collections.singletonList(standbyTask));

//        }

//        private void MockSingleActiveTask()
//        {
//            expect(activeTaskCreator.createTasks(EasyMock.< Consumer<byte[], byte[]> > anyObject(),
//                                                      EasyMock.eq(taskId0Assignment)))
//                    .andReturn(Collections.singletonList(streamTask));

//        }

//        private void MockTopologyBuilder()
//        {
//            expect(activeTaskCreator.builder()).andReturn(topologyBuilder).anyTimes();
//            expect(topologyBuilder.sourceTopicPattern()).andReturn(new Regex("abc", RegexOptions.Compiled));
//            expect(topologyBuilder.subscriptionUpdates()).andReturn(subscriptionUpdates);
//        }
//    }
//}
///*






//*

//*





//*/















































