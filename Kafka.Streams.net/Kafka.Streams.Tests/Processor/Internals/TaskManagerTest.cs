using Confluent.Kafka;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using Kafka.Streams.Topologies;
using Moq;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class TaskManagerTest
    {
        private TaskId taskId0 = new TaskId(0, 0);
        private TopicPartition t1p0 = new TopicPartition("t1", 0);
        private List<TopicPartition> taskId0Partitions = new List<TopicPartition> { t1p0 };
        private Dictionary<TaskId, HashSet<TopicPartition>> taskId0Assignment = Collections.singletonMap(taskId0, taskId0Partitions);

        private SubscriptionUpdates subscriptionUpdates;
        private InternalTopologyBuilder topologyBuilder;
        private StateDirectory stateDirectory;
        private IChangelogReader changeLogReader;
        private StreamsMetadataState streamsMetadataState;
        private IConsumer<byte[], byte[]> restoreConsumer;
        private IConsumer<byte[], byte[]> consumer;
        private AbstractTaskCreator<StreamTask> activeTaskCreator;
        private AbstractTaskCreator<StandbyTask> standbyTaskCreator;
        private IAdminClient adminClient;

        private StreamTask streamTask;
        private StandbyTask standbyTask;
        private AssignedStreamsTasks active;
        private AssignedStandbyTasks standby;
        private TaskManager taskManager;

        private const string topic1 = "topic1";
        private const string topic2 = "topic2";
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

        //public TemporaryFolder testFolder = new TemporaryFolder();

        public TaskManagerTest()
        {
            taskManager = new TaskManager(
                null,
                null,
                changeLogReader,
                Guid.NewGuid(),
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
            expect(subscriptionUpdates.GetUpdates()).andReturn(Utils.mkSet(topic1));
            topologyBuilder.UpdateSubscribedTopics(EasyMock.eq(Utils.mkSet(topic1, topic2)), string.Empty);
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

            expect(subscriptionUpdates.GetUpdates()).andReturn(Utils.mkSet(topic1, topic2));

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
            expect(subscriptionUpdates.GetUpdates()).andReturn(Utils.mkSet(topic1));
            topologyBuilder.UpdateSubscribedTopics(EasyMock.eq(Utils.mkSet(topic1, topic2)), string.Empty);
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
            expect(subscriptionUpdates.GetUpdates()).andReturn(Utils.mkSet(topic1));

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
            DirectoryInfo[] taskFolders = Arrays.asList(
                testFolder.newFolder("0_1"),
                testFolder.newFolder("0_2"),
                testFolder.newFolder("0_3"),
                testFolder.newFolder("1_1"),
                testFolder.newFolder("dummy"))
                .ToArray();

            Assert.NotNull(new FileInfo(Path.Combine(taskFolders[0].FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)).Create());
            Assert.NotNull(new FileInfo(Path.Combine(taskFolders[1].FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)).Create());
            Assert.NotNull(new FileInfo(Path.Combine(taskFolders[3].FullName, StateManagerUtil.CHECKPOINT_FILE_NAME)).Create());

            expect(activeTaskCreator.stateDirectory).andReturn(stateDirectory).once();
            expect(stateDirectory.ListTaskDirectories()).andReturn(taskFolders).once();

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

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.emptyMap<TaskId, HashSet<TopicPartition>>());
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

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.emptyMap<TaskId, HashSet<TopicPartition>>());
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

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.emptyMap<TaskId, HashSet<TopicPartition>>());
            taskManager.CreateTasks(taskId0Partitions);

            verify(activeTaskCreator, active);
        }

        [Fact]
        public void ShouldNotAddResumedActiveTasks()
        {
            checkOrder(active, true);
            expect(active.MaybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
            Replay();

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.emptyMap<TaskId, HashSet<TopicPartition>>());
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

            taskManager.SetAssignmentMetadata(Collections.emptyMap<TaskId, HashSet<TopicPartition>>(), taskId0Assignment);
            taskManager.CreateTasks(taskId0Partitions);

            verify(standbyTaskCreator, active);
        }

        [Fact]
        public void ShouldNotAddResumedStandbyTasks()
        {
            checkOrder(active, true);
            expect(standby.MaybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
            Replay();

            taskManager.SetAssignmentMetadata(Collections.emptyMap<TaskId, HashSet<TopicPartition>>(), taskId0Assignment);
            taskManager.CreateTasks(taskId0Partitions);

            // should be no calls to standbyTaskCreator and no calls to standby.addNewTasks(..)
            verify(standby, standbyTaskCreator);
        }

        [Fact]
        public void ShouldPauseActivePartitions()
        {
            MockSingleActiveTask();
            consumer.Pause(taskId0Partitions);
            expectLastCall();
            Replay();

            taskManager.SetAssignmentMetadata(taskId0Assignment, Collections.emptyMap<TaskId, HashSet<TopicPartition>>());
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
            restoreConsumer.Unsubscribe();

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
            restoreConsumer.Unsubscribe();
            expectLastCall();
            Replay();

            taskManager.Shutdown(true);
            verify(restoreConsumer);
        }

        [Fact]
        public void ShouldInitializeNewActiveTasks()
        {
            active.UpdateRestored(Enumerable.Empty<TopicPartition>());
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(active);
        }

        [Fact]
        public void ShouldInitializeNewStandbyTasks()
        {
            active.UpdateRestored(Mock.Of<Collection<TopicPartition>>());
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(standby);
        }

        [Fact]
        public void ShouldRestoreStateFromChangeLogReader()
        {
            expect(changeLogReader.Restore(active)).andReturn(taskId0Partitions);
            active.UpdateRestored(taskId0Partitions);
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(changeLogReader, active);
        }

        [Fact]
        public void ShouldResumeRestoredPartitions()
        {
            expect(changeLogReader.Restore(active)).andReturn(taskId0Partitions);
            expect(active.AllTasksRunning()).andReturn(true);
            expect(consumer.Assignment).andReturn(taskId0Partitions);
            expect(standby.running).andReturn(Collections.emptySet<StandbyTask>());

            consumer.Resume(taskId0Partitions);
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
            expect(standby.AllTasksRunning()).andReturn(true);
            Replay();

            Assert.True(taskManager.UpdateNewAndRestoringTasks());
        }

        [Fact]
        public void ShouldReturnFalseWhenOnlyActiveTasksAreRunning()
        {
            MockAssignStandbyPartitions(1L);
            expect(standby.AllTasksRunning()).andReturn(false);
            Replay();

            Assert.False(taskManager.UpdateNewAndRestoringTasks());
        }

        [Fact]
        public void ShouldReturnFalseWhenThereAreStillNonRunningTasks()
        {
            expect(active.AllTasksRunning()).andReturn(false);
            Replay();

            Assert.False(taskManager.UpdateNewAndRestoringTasks());
        }

        [Fact]
        public void ShouldSeekToCheckpointedOffsetOnStandbyPartitionsWhenOffsetGreaterThanEqualTo0()
        {
            MockAssignStandbyPartitions(1L);
            restoreConsumer.Seek(t1p0, 1L);
            expectLastCall();
            Replay();

            taskManager.UpdateNewAndRestoringTasks();
            verify(restoreConsumer);
        }

        [Fact]
        public void ShouldSeekToBeginningIfOffsetIsLessThan0()
        {
            MockAssignStandbyPartitions(-1L);
            restoreConsumer.SeekToBeginning(taskId0Partitions);
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

            Assert.Equal(3, taskManager.CommitAll());
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
            Task<DeletedRecords> futureDeletedRecords = new Task<DeleteRecords>();
            Dictionary<TopicPartition, RecordsToDelete> RecordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
            DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>)futureDeletedRecords));

            futureDeletedRecords.complete(null);

            expect(active.RecordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
            expect(adminClient.DeleteRecords(RecordsToDelete)).andReturn(deleteRecordsResult).times(2);
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
            expect(adminClient.DeleteRecords(RecordsToDelete)).andReturn(deleteRecordsResult).once();
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
            expect(adminClient.DeleteRecords(RecordsToDelete)).andReturn(deleteRecordsResult).times(2);
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

            Assert.Equal(5, taskManager.MaybeCommitActiveTasksPerUserRequested());
            verify(active);
        }

        [Fact]
        public void ShouldProcessActiveTasks()
        {
            expect(active.Process(DateTime.MinValue)).andReturn(10);
            Replay();

            Assert.Equal(10, taskManager.Process(DateTime.MinValue));
            verify(active);
        }

        [Fact]
        public void ShouldPunctuateActiveTasks()
        {
            expect(active.Punctuate()).andReturn(20);
            Replay();

            Assert.Equal(20, taskManager.Punctuate());
            verify(active);
        }

        [Fact]
        public void ShouldNotResumeConsumptionUntilAllStoresRestored()
        {
            expect(active.AllTasksRunning()).andReturn(false);
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
            Dictionary<TaskId, HashSet<TopicPartition>> ActiveTasks = new HashMap<>();
            Dictionary<TaskId, HashSet<TopicPartition>> StandbyTasks = new HashMap<>();

            taskManager.SetAssignmentMetadata(ActiveTasks, StandbyTasks);
            Assert.True(taskManager.assignedActiveTasks.IsEmpty());

            // assign two active tasks with two partitions each
            ActiveTasks.Put(task01, new HashSet<TopicPartition>(Arrays.asList(t1p1, t2p1)));
            ActiveTasks.Put(task02, new HashSet<TopicPartition>(Arrays.asList(t1p2, t2p2)));

            // assign one standby task with two partitions
            StandbyTasks.Put(task03, new HashSet<TopicPartition>(Arrays.asList(t1p3, t2p3)));
            taskManager.SetAssignmentMetadata(ActiveTasks, StandbyTasks);

            Assert.Equal(taskManager.assignedActiveTasks, ActiveTasks);
            Assert.Equal(taskManager.assignedStandbyTasks, StandbyTasks);
        }

        private void MockAssignStandbyPartitions(long offset)
        {
            StandbyTask task = Mock.Of<StandbyTask>();
            expect(active.AllTasksRunning()).andReturn(true);
            expect(standby.running).andReturn(Collections.singletonList(task));
            expect(task.checkpointedOffsets).andReturn(Collections.singletonMap(t1p0, offset));
            restoreConsumer.Assign(taskId0Partitions);

            expectLastCall();
            EasyMock.replay(task);
        }

        private void MockStandbyTaskExpectations()
        {
            expect(standbyTaskCreator.CreateTasks(
                Mock.Of<IConsumer<byte[], byte[]>>(),
                EasyMock.eq(taskId0Assignment)))
                    .andReturn(Collections.singletonList(standbyTask));

        }

        private void MockSingleActiveTask()
        {
            expect(activeTaskCreator.CreateTasks(Mock.Of<IConsumer<byte[], byte[]>>(),
                                                      EasyMock.eq(taskId0Assignment)))
                    .andReturn(Collections.singletonList(streamTask));

        }

        private void MockTopologyBuilder()
        {
            expect(activeTaskCreator.builder).andReturn(topologyBuilder).anyTimes();
            expect(topologyBuilder.SourceTopicPattern()).andReturn(new Regex("abc", RegexOptions.Compiled));
            expect(topologyBuilder.SubscriptionUpdates).andReturn(subscriptionUpdates);
        }
    }
}
