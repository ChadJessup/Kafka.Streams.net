using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Tasks;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class TaskManagerTest
    {
        private TaskId taskId0 = new TaskId(0, 0);
        private TopicPartition t1p0 = new TopicPartition("t1", 0);
        private HashSet<TopicPartition> taskId0Partitions = Utils.mkSet(t1p0);
        private Dictionary<TaskId, HashSet<TopicPartition>> taskId0Assignment = Collections.singletonMap(taskId0, taskId0Partitions);

        (type = MockType.STRICT)
    private InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates;
        (type = MockType.STRICT)
    private InternalTopologyBuilder topologyBuilder;
        (type = MockType.STRICT)
    private StateDirectory stateDirectory;
        (type = MockType.NICE)
    private ChangelogReader changeLogReader;
        (type = MockType.NICE)
    private StreamsMetadataState streamsMetadataState;
        (type = MockType.NICE)
    private IConsumer<byte[], byte[]> restoreConsumer;
        (type = MockType.NICE)
    private IConsumer<byte[], byte[]> consumer;
        (type = MockType.NICE)
    private StreamThread.AbstractTaskCreator<StreamTask> activeTaskCreator;
        (type = MockType.NICE)
    private StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator;
        (type = MockType.NICE)
    private Admin adminClient;
        (type = MockType.NICE)
    private StreamTask streamTask;
        (type = MockType.NICE)
    private StandbyTask standbyTask;
        (type = MockType.NICE)
    private AssignedStreamsTasks active;
        (type = MockType.NICE)
    private AssignedStandbyTasks standby;

        private TaskManager taskManager;

        private readonly string topic1 = "topic1";
        private readonly string topic2 = "topic2";
        private TopicPartition t1p1 = new TopicPartition(topic1, 1);
        private TopicPartition t1p2 = new TopicPartition(topic1, 2);
        private TopicPartition t1p3 = new TopicPartition(topic1, 3);
        private TopicPartition t2p1 = new TopicPartition(topic2, 1);
        private TopicPartition t2p2 = new TopicPartition(topic2, 2);
        private TopicPartition t2p3 = new TopicPartition(topic2, 3);

        private TaskId task01 = new TaskId(0, 1);
        private TaskId task02 = new TaskId(0, 2);
        private TaskId task03 = new TaskId(0, 3);
        private TaskId task11 = new TaskId(1, 1);


        public TemporaryFolder testFolder = new TemporaryFolder();


        public void SetUp()
        {
            taskManager = new TaskManager(changeLogReader,
                                          UUID.randomUUID(),
                                          "",
                                          restoreConsumer,
                                          streamsMetadataState,
                                          activeTaskCreator,
                                          standbyTaskCreator,
                                          adminClient,
                                          active,
                                          standby);
            taskManager.SetConsumer(consumer);
        }

        private void Replay()
        {
            EasyMock.replay(changeLogReader,
                            restoreConsumer,
                            consumer,
                            activeTaskCreator,
                            standbyTaskCreator,
                            active,
                            standby,
                            adminClient);
        }

        [Fact]
        public void ShouldUpdateSubscriptionFromAssignment()
        {
            MockTopologyBuilder();
            expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));
            topologyBuilder.updateSubscribedTopics(EasyMock.eq(Utils.mkSet(topic1, topic2)), EasyMock.string.Empty);
            expectLastCall().once();

            EasyMock.replay(activeTaskCreator,
                            topologyBuilder,
                            subscriptionUpdates);

            taskManager.UpdateSubscriptionsFromAssignment(asList(t1p1, t2p1));

            EasyMock.verify(activeTaskCreator,
                            topologyBuilder,
                            subscriptionUpdates);
        }

        [Fact]
        public void ShouldNotUpdateSubscriptionFromAssignment()
        {
            MockTopologyBuilder();
            expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1, topic2));

            EasyMock.replay(activeTaskCreator,
                            topologyBuilder,
                            subscriptionUpdates);

            taskManager.UpdateSubscriptionsFromAssignment(asList(t1p1));

            EasyMock.verify(activeTaskCreator,
                            topologyBuilder,
                            subscriptionUpdates);
        }

        [Fact]
        public void ShouldUpdateSubscriptionFromMetadata()
        {
            MockTopologyBuilder();
            expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));
            topologyBuilder.updateSubscribedTopics(EasyMock.eq(Utils.mkSet(topic1, topic2)), EasyMock.string.Empty);
            expectLastCall().once();

            EasyMock.replay(activeTaskCreator,
                    topologyBuilder,
                    subscriptionUpdates);

            taskManager.UpdateSubscriptionsFromMetadata(Utils.mkSet(topic1, topic2));

            EasyMock.verify(activeTaskCreator,
                    topologyBuilder,
                    subscriptionUpdates);
        }

        [Fact]
        public void ShouldNotUpdateSubscriptionFromMetadata()
        {
            MockTopologyBuilder();
            expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));

            EasyMock.replay(activeTaskCreator,
                    topologyBuilder,
                    subscriptionUpdates);

            taskManager.UpdateSubscriptionsFromMetadata(Utils.mkSet(topic1));

            EasyMock.verify(activeTaskCreator,
                    topologyBuilder,
                    subscriptionUpdates);
        }

        [Fact]
        public void ShouldReturnCachedTaskIdsFromDirectory()
        { //throws IOException
            DirectoryInfo[] taskFolders = Arrays.asList(testFolder.newFolder("0_1"),
                                                    testFolder.newFolder("0_2"),
                                                    testFolder.newFolder("0_3"),
                                                    testFolder.newFolder("1_1"),
                                                    testFolder.newFolder("dummy"))
                                        .ToArray();

            Assert.True(new FileInfo(Path.Combine(taskFolders[0].FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)).Create());
            Assert.True(new FileInfo(Path.Combine(taskFolders[1].FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)).Create());
            Assert.True(new FileInfo(Path.Combine(taskFolders[3].FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)).Create());

            expect(activeTaskCreator.stateDirectory()).andReturn(stateDirectory).once();
            expect(stateDirectory.listTaskDirectories()).andReturn(taskFolders).once();

            EasyMock.replay(activeTaskCreator, stateDirectory);

            HashSet<TaskId> tasks = taskManager.CachedTasksIds();

            EasyMock.verify(activeTaskCreator, stateDirectory);

            Assert.Equal(tasks, Utils.mkSet(task01, task02, task11));
        }

        [Fact]
        public void ShouldCloseActiveUnAssignedSuspendedTasksWhenCreatingNewTasks()
        {
            MockSingleActiveTask();
            active.CloseNonAssignedSuspendedTasks(taskId0Assignment);
            expectLastCall();
            Replay();

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
            taskManager.CreateTasks(taskId0Partitions);

            verify(active);
        }

        [Fact]
        public void ShouldCloseStandbyUnAssignedSuspendedTasksWhenCreatingNewTasks()
        {
            MockSingleActiveTask();
            standby.CloseNonAssignedSuspendedTasks(taskId0Assignment);
            expectLastCall();
            Replay();

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
            taskManager.CreateTasks(taskId0Partitions);

            verify(active);
        }

        [Fact]
        public void ShouldAddNonResumedActiveTasks()
        {
            MockSingleActiveTask();
            expect(active.MaybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(false);
            active.AddNewTask(EasyMock.same(streamTask));
            Replay();

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
            taskManager.CreateTasks(taskId0Partitions);

            verify(activeTaskCreator, active);
        }

        [Fact]
        public void ShouldNotAddResumedActiveTasks()
        {
            checkOrder(active, true);
            expect(active.MaybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
            Replay();

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
            taskManager.CreateTasks(taskId0Partitions);

            // should be no calls to activeTaskCreator and no calls to active.addNewTasks(..)
            verify(active, activeTaskCreator);
        }

        [Fact]
        public void ShouldAddNonResumedStandbyTasks()
        {
            MockStandbyTaskExpectations();
            expect(standby.MaybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(false);
            standby.AddNewTask(EasyMock.same(standbyTask));
            Replay();

            taskManager.SetAssignmentMetadata(Collections.< TaskId, HashSet < TopicPartition >> emptyMap(), taskId0Assignment);
            taskManager.CreateTasks(taskId0Partitions);

            verify(standbyTaskCreator, active);
        }

        [Fact]
        public void ShouldNotAddResumedStandbyTasks()
        {
            checkOrder(active, true);
            expect(standby.MaybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
            Replay();

            taskManager.SetAssignmentMetadata(Collections.< TaskId, HashSet < TopicPartition >> emptyMap(), taskId0Assignment);
            taskManager.CreateTasks(taskId0Partitions);

            // should be no calls to standbyTaskCreator and no calls to standby.addNewTasks(..)
            verify(standby, standbyTaskCreator);
        }

        [Fact]
        public void ShouldPauseActivePartitions()
        {
            MockSingleActiveTask();
            consumer.pause(taskId0Partitions);
            expectLastCall();
            Replay();

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.< TaskId, HashSet < TopicPartition >> emptyMap());
            taskManager.CreateTasks(taskId0Partitions);
            verify(consumer);
        }

        [Fact]
        public void ShouldSuspendActiveTasks()
        {
            expect(active.Suspend()).andReturn(null);
            Replay();

            taskManager.SuspendTasksAndState();
            verify(active);
        }

        [Fact]
        public void ShouldSuspendStandbyTasks()
        {
            expect(standby.Suspend()).andReturn(null);
            Replay();

            taskManager.SuspendTasksAndState();
            verify(standby);
        }

        [Fact]
        public void ShouldUnassignChangelogPartitionsOnSuspend()
        {
            restoreConsumer.Unsubscribe();
            expectLastCall();
            Replay();

            taskManager.SuspendTasksAndState();
            verify(restoreConsumer);
        }

        [Fact]
        public void ShouldThrowStreamsExceptionAtEndIfExceptionDuringSuspend()
        {
            expect(active.Suspend()).andReturn(new RuntimeException(""));
            expect(standby.Suspend()).andReturn(new RuntimeException(""));
            expectLastCall();
            restoreConsumer.unsubscribe();

            Replay();
            try
            {
                taskManager.SuspendTasksAndState();
                Assert.True(false, "Should have thrown streams exception");
            }
            catch (StreamsException e)
            {
                // expected
            }
            verify(restoreConsumer, active, standby);
        }

        [Fact]
        public void ShouldCloseActiveTasksOnShutdown()
        {
            active.Close(true);
            expectLastCall();
            Replay();

            taskManager.Shutdown(true);
            verify(active);
        }

        [Fact]
        public void ShouldCloseStandbyTasksOnShutdown()
        {
            standby.Close(false);
            expectLastCall();
            Replay();

            taskManager.Shutdown(false);
            verify(standby);
        }

        [Fact]
        public void ShouldUnassignChangelogPartitionsOnShutdown()
        {
            restoreConsumer.unsubscribe();
            expectLastCall();
            Replay();

            taskManager.Shutdown(true);
            verify(restoreConsumer);
        }

        [Fact]
        public void ShouldInitializeNewActiveTasks()
        {
            active.updateRestored(EasyMock.< Collection < TopicPartition >> default());
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(active);
        }

        [Fact]
        public void ShouldInitializeNewStandbyTasks()
        {
            active.updateRestored(EasyMock.< Collection < TopicPartition >> default());
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(standby);
        }

        [Fact]
        public void ShouldRestoreStateFromChangeLogReader()
        {
            expect(changeLogReader.restore(active)).andReturn(taskId0Partitions);
            active.updateRestored(taskId0Partitions);
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(changeLogReader, active);
        }

        [Fact]
        public void ShouldResumeRestoredPartitions()
        {
            expect(changeLogReader.restore(active)).andReturn(taskId0Partitions);
            expect(active.allTasksRunning()).andReturn(true);
            expect(consumer.assignment()).andReturn(taskId0Partitions);
            expect(standby.running()).andReturn(Collections.< StandbyTask > emptySet());

            consumer.resume(taskId0Partitions);
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(consumer);
        }

        [Fact]
        public void ShouldAssignStandbyPartitionsWhenAllActiveTasksAreRunning()
        {
            MockAssignStandbyPartitions(1L);
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(restoreConsumer);
        }

        [Fact]
        public void ShouldReturnTrueWhenActiveAndStandbyTasksAreRunning()
        {
            MockAssignStandbyPartitions(1L);
            expect(standby.allTasksRunning()).andReturn(true);
            Replay();

            Assert.True(taskManager.UpdateNewAndRestoringTasks());
        }

        [Fact]
        public void ShouldReturnFalseWhenOnlyActiveTasksAreRunning()
        {
            MockAssignStandbyPartitions(1L);
            expect(standby.allTasksRunning()).andReturn(false);
            Replay();

            Assert.False(taskManager.UpdateNewAndRestoringTasks());
        }

        [Fact]
        public void ShouldReturnFalseWhenThereAreStillNonRunningTasks()
        {
            expect(active.allTasksRunning()).andReturn(false);
            Replay();

            Assert.False(taskManager.UpdateNewAndRestoringTasks());
        }

        [Fact]
        public void ShouldSeekToCheckpointedOffsetOnStandbyPartitionsWhenOffsetGreaterThanEqualTo0()
        {
            MockAssignStandbyPartitions(1L);
            restoreConsumer.seek(t1p0, 1L);
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(restoreConsumer);
        }

        [Fact]
        public void ShouldSeekToBeginningIfOffsetIsLessThan0()
        {
            MockAssignStandbyPartitions(-1L);
            restoreConsumer.seekToBeginning(taskId0Partitions);
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(restoreConsumer);
        }

        [Fact]
        public void ShouldCommitActiveAndStandbyTasks()
        {
            expect(active.Commit()).andReturn(1);
            expect(standby.Commit()).andReturn(2);

            Replay();

            Assert.Equal(taskManager.CommitAll(), 3);
            verify(active, standby);
        }

        [Fact]
        public void ShouldPropagateExceptionFromActiveCommit()
        {
            // upgrade to strict mock to ensure no calls
            checkOrder(standby, true);
            active.Commit();
            expectLastCall().andThrow(new RuntimeException(""));
            Replay();

            try
            {
                taskManager.CommitAll();
                Assert.True(false, "should have thrown first exception");
            }
            catch (Exception e)
            {
                // ok
            }
            verify(active, standby);
        }

        [Fact]
        public void ShouldPropagateExceptionFromStandbyCommit()
        {
            expect(standby.Commit()).andThrow(new RuntimeException(""));
            Replay();

            try
            {
                taskManager.CommitAll();
                Assert.True(false, "should have thrown exception");
            }
            catch (Exception e)
            {
                // ok
            }
            verify(standby);
        }

        [Fact]
        public void ShouldSendPurgeData()
        {
            KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
            Dictionary<TopicPartition, RecordsToDelete> RecordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
            DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>)futureDeletedRecords));

            futureDeletedRecords.complete(null);

            expect(active.RecordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
            expect(adminClient.deleteRecords(RecordsToDelete)).andReturn(deleteRecordsResult).times(2);
            Replay();

            taskManager.MaybePurgeCommitedRecords();
            taskManager.MaybePurgeCommitedRecords();
            verify(active, adminClient);
        }

        [Fact]
        public void ShouldNotSendPurgeDataIfPreviousNotDone()
        {
            KafkaFuture<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
            Dictionary<TopicPartition, RecordsToDelete> RecordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
            DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, futureDeletedRecords));

            expect(active.RecordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).once();
            expect(adminClient.deleteRecords(RecordsToDelete)).andReturn(deleteRecordsResult).once();
            Replay();

            taskManager.MaybePurgeCommitedRecords();
            // second call should be no-op as the previous one is not done yet
            taskManager.MaybePurgeCommitedRecords();
            verify(active, adminClient);
        }

        [Fact]
        public void ShouldIgnorePurgeDataErrors()
        {
            KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
            Dictionary<TopicPartition, RecordsToDelete> RecordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
            DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>)futureDeletedRecords));

            futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));

            expect(active.RecordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
            expect(adminClient.deleteRecords(RecordsToDelete)).andReturn(deleteRecordsResult).times(2);
            Replay();

            taskManager.MaybePurgeCommitedRecords();
            taskManager.MaybePurgeCommitedRecords();
            verify(active, adminClient);
        }

        [Fact]
        public void ShouldMaybeCommitActiveTasks()
        {
            expect(active.MaybeCommitPerUserRequested()).andReturn(5);
            Replay();

            Assert.Equal(taskManager.MaybeCommitActiveTasksPerUserRequested(), 5);
            verify(active);
        }

        [Fact]
        public void ShouldProcessActiveTasks()
        {
            expect(active.Process(0L)).andReturn(10);
            Replay();

            Assert.Equal(taskManager.Process(0L), 10);
            verify(active);
        }

        [Fact]
        public void ShouldPunctuateActiveTasks()
        {
            expect(active.Punctuate()).andReturn(20);
            Replay();

            Assert.Equal(taskManager.Punctuate(), 20);
            verify(active);
        }

        [Fact]
        public void ShouldNotResumeConsumptionUntilAllStoresRestored()
        {
            expect(active.allTasksRunning()).andReturn(false);
            IConsumer<byte[], byte[]> consumer = EasyMock.createStrictMock(Consumer);
            taskManager.SetConsumer(consumer);
            EasyMock.replay(active, consumer);

            // shouldn't invoke `resume` method in consumer
            taskManager.UpdateNewAndRestoringTasks();
            EasyMock.verify(consumer);
        }

        [Fact]
        public void ShouldUpdateTasksFromPartitionAssignment()
        {
            Dictionary<TaskId, HashSet<TopicPartition>> activeTasks = new HashMap<>();
            Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks = new HashMap<>();

            taskManager.SetAssignmentMetadata(activeTasks, standbyTasks);
            Assert.True(taskManager.assignedActiveTasks().IsEmpty());

            // assign two active tasks with two partitions each
            activeTasks.Put(task01, new HashSet<>(asList(t1p1, t2p1)));
            activeTasks.Put(task02, new HashSet<>(asList(t1p2, t2p2)));

            // assign one standby task with two partitions
            standbyTasks.Put(task03, new HashSet<>(asList(t1p3, t2p3)));
            taskManager.SetAssignmentMetadata(activeTasks, standbyTasks);

            Assert.Equal(taskManager.assignedActiveTasks(), activeTasks);
            Assert.Equal(taskManager.assignedStandbyTasks(), standbyTasks);
        }

        private void MockAssignStandbyPartitions(long offset)
        {
            StandbyTask task = Mock.Of<StandbyTask);
            expect(active.allTasksRunning()).andReturn(true);
            expect(standby.running()).andReturn(Collections.singletonList(task));
            expect(task.checkpointedOffsets()).andReturn(Collections.singletonMap(t1p0, offset));
            restoreConsumer.Assign(taskId0Partitions);

            expectLastCall();
            EasyMock.replay(task);
        }

        private void MockStandbyTaskExpectations()
        {
            expect(standbyTaskCreator.CreateTasks(EasyMock.< IConsumer<byte[], byte[]> > default(),
                                                       EasyMock.eq(taskId0Assignment)))
                    .andReturn(Collections.singletonList(standbyTask));

        }

        private void MockSingleActiveTask()
        {
            expect(activeTaskCreator.CreateTasks(EasyMock.< IConsumer<byte[], byte[]> > default(),
                                                      EasyMock.eq(taskId0Assignment)))
                    .andReturn(Collections.singletonList(streamTask));

        }

        private void MockTopologyBuilder()
        {
            expect(activeTaskCreator.builder()).andReturn(topologyBuilder).anyTimes();
            expect(topologyBuilder.sourceTopicPattern()).andReturn(new Regex("abc", RegexOptions.Compiled));
            expect(topologyBuilder.subscriptionUpdates()).andReturn(subscriptionUpdates);
        }
    }
}
/*






*

*





*/















































