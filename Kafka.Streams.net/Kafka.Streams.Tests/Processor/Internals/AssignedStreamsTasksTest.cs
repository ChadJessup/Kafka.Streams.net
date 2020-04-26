using Confluent.Kafka;
using Kafka.Streams.Tasks;
using Moq;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals
{
    public class AssignedStreamsTasksTest
    {

        private StreamTask t1 = Mock.Of<StreamTask>();
        private StreamTask t2 = Mock.Of<StreamTask>();
        private TopicPartition tp1 = new TopicPartition("t1", 0);
        private TopicPartition tp2 = new TopicPartition("t2", 0);
        private TopicPartition changeLog1 = new TopicPartition("cl1", 0);
        private TopicPartition changeLog2 = new TopicPartition("cl2", 0);
        private TaskId taskId1 = new TaskId(0, 0);
        private TaskId taskId2 = new TaskId(1, 0);
        private AssignedStreamsTasks AssignedTasks;

        void Before()
        {
            AssignedTasks = new AssignedStreamsTasks(new LogContext("log "));
            EasyMock.expect(t1.id()).andReturn(taskId1).anyTimes();
            EasyMock.expect(t2.id()).andReturn(taskId2).anyTimes();
        }

        [Fact]
        public void ShouldInitializeNewTasks()
        {
            EasyMock.expect(t1.InitializeStateStores()).andReturn(false);
            EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
            EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
            EasyMock.replay(t1);

            AddAndInitTask();

            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldMoveInitializedTasksNeedingRestoreToRestoring()
        {
            EasyMock.expect(t1.InitializeStateStores()).andReturn(false);
            t1.InitializeTopology();
            EasyMock.expectLastCall().once();
            EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
            EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
            EasyMock.expect(t2.InitializeStateStores()).andReturn(true);
            t2.InitializeTopology();
            EasyMock.expectLastCall().once();
            HashSet<TopicPartition> t2partitions = Collections.singleton(tp2);
            EasyMock.expect(t2.partitions()).andReturn(t2partitions);
            EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.emptyList());

            EasyMock.replay(t1, t2);

            AssignedTasks.AddNewTask(t1);
            AssignedTasks.AddNewTask(t2);

            AssignedTasks.InitializeNewTasks();

            Collection<StreamTask> restoring = AssignedTasks.restoringTasks();
            Assert.Equal(1, restoring.Count);
            Assert.Same(restoring.iterator().MoveNext(), t1);
        }

        [Fact]
        public void ShouldMoveInitializedTasksThatDontNeedRestoringToRunning()
        {
            EasyMock.expect(t2.InitializeStateStores()).andReturn(true);
            t2.InitializeTopology();
            EasyMock.expectLastCall().once();
            EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
            EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.emptyList());

            EasyMock.replay(t2);

            AssignedTasks.AddNewTask(t2);
            AssignedTasks.InitializeNewTasks();

            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.singleton(taskId2));
        }

        [Fact]
        public void ShouldTransitionFullyRestoredTasksToRunning()
        {
            HashSet<TopicPartition> task1Partitions = Utils.mkSet(tp1);
            EasyMock.expect(t1.InitializeStateStores()).andReturn(false);
            EasyMock.expect(t1.partitions()).andReturn(task1Partitions).anyTimes();
            EasyMock.expect(t1.changelogPartitions()).andReturn(Utils.mkSet(changeLog1, changeLog2)).anyTimes();
            EasyMock.expect(t1.HasStateStores()).andReturn(true).anyTimes();
            t1.InitializeTopology();
            EasyMock.expectLastCall().once();
            EasyMock.replay(t1);

            AddAndInitTask();

            AssignedTasks.UpdateRestored(Utils.mkSet(changeLog1));
            Assert.Equal(AssignedTasks.runningTaskIds(), Collections < TaskId > emptySet());
            AssignedTasks.UpdateRestored(Utils.mkSet(changeLog2));
            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.singleton(taskId1));
        }

        [Fact]
        public void ShouldSuspendRunningTasks()
        {
            MockRunningTaskSuspension();
            EasyMock.replay(t1);

            Assert.Equal(SuspendTask(), nullValue());

            Assert.Equal(AssignedTasks.PreviousTaskIds(), Collections.singleton(taskId1));
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCloseRestoringTasks()
        {
            EasyMock.expect(t1.InitializeStateStores()).andReturn(false);
            EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
            EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
            t1.CloseStateManager(true);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);

            AddAndInitTask();
            Assert.Equal(AssignedTasks.CloseAllRestoringTasks(), nullValue());

            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldClosedUnInitializedTasksOnSuspend()
        {
            t1.Close(false, false);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);

            AssignedTasks.AddNewTask(t1);
            Assert.Equal(AssignedTasks.Suspend(), nullValue());

            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldNotSuspendSuspendedTasks()
        {
            MockRunningTaskSuspension();
            EasyMock.replay(t1);

            Assert.Equal(SuspendTask(), nullValue());
            Assert.Equal(AssignedTasks.Suspend(), nullValue());
            EasyMock.verify(t1);
        }


        [Fact]
        public void ShouldCloseTaskOnSuspendWhenRuntimeException()
        {
            MockTaskInitialization();
            t1.Suspend();
            EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!"));
            t1.Close(false, false);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);

            Assert.Equal(SuspendTask(), not(nullValue()));
            Assert.Equal(AssignedTasks.PreviousTaskIds(), Collections.singleton(taskId1));
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCloseTaskOnSuspendIfTaskMigratedException()
        {
            MockTaskInitialization();
            t1.Suspend();
            EasyMock.expectLastCall().andThrow(new TaskMigratedException());
            t1.Close(false, true);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);

            Assert.Equal(SuspendTask(), nullValue());
            Assert.True(AssignedTasks.PreviousTaskIds().IsEmpty());
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldResumeMatchingSuspendedTasks()
        {
            MockRunningTaskSuspension();
            t1.Resume();
            EasyMock.expectLastCall();
            t1.InitializeTopology();
            EasyMock.expectLastCall().once();
            EasyMock.replay(t1);

            Assert.Equal(SuspendTask(), nullValue());

            Assert.True(AssignedTasks.MaybeResumeSuspendedTask(taskId1, Collections.singleton(tp1)));
            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.singleton(taskId1));
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCloseTaskOnResumeSuspendedIfTaskMigratedException()
        {
            MockRunningTaskSuspension();
            t1.Resume();
            t1.InitializeTopology();
            EasyMock.expectLastCall().andThrow(new TaskMigratedException());
            t1.Close(false, true);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);

            Assert.Equal(SuspendTask(), nullValue());

            try
            {
                AssignedTasks.MaybeResumeSuspendedTask(taskId1, Collections.singleton(tp1));
                Assert.True(false, "Should have thrown TaskMigratedException.");
            }
            catch (TaskMigratedException expected) { /* ignore */ }

            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.EMPTY_SET);
            EasyMock.verify(t1);
        }

        private void MockTaskInitialization()
        {
            EasyMock.expect(t1.InitializeStateStores()).andReturn(true);
            t1.InitializeTopology();
            EasyMock.expectLastCall().once();
            EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
            EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptyList());
        }

        [Fact]
        public void ShouldCommitRunningTasks()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.commitNeeded()).andReturn(true);
            t1.Commit();
            EasyMock.expectLastCall();
            EasyMock.replay(t1);

            AddAndInitTask();

            AssignedTasks.Commit();
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCloseTaskOnCommitIfTaskMigratedException()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.commitNeeded()).andReturn(true);
            t1.Commit();
            EasyMock.expectLastCall().andThrow(new TaskMigratedException());
            t1.Close(false, true);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);
            AddAndInitTask();

            try
            {
                AssignedTasks.Commit();
                Assert.True(false, "Should have thrown TaskMigratedException.");
            }
            catch (TaskMigratedException expected) { /* ignore */ }

            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.EMPTY_SET);
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldThrowExceptionOnCommitWhenNotCommitFailedOrProducerFenced()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.commitNeeded()).andReturn(true);
            t1.Commit();
            EasyMock.expectLastCall().andThrow(new RuntimeException(""));
            EasyMock.replay(t1);
            AddAndInitTask();

            try
            {
                AssignedTasks.Commit();
                Assert.True(false, "Should have thrown exception");
            }
            catch (Exception e)
            {
                // ok
            }
            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.singleton(taskId1));
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCommitRunningTasksIfNeeded()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.commitRequested()).andReturn(true);
            EasyMock.expect(t1.commitNeeded()).andReturn(true);
            t1.Commit();
            EasyMock.expectLastCall();
            EasyMock.replay(t1);

            AddAndInitTask();

            Assert.Equal(1, AssignedTasks.MaybeCommitPerUserRequested());
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCloseTaskOnMaybeCommitIfTaskMigratedException()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.commitRequested()).andReturn(true);
            EasyMock.expect(t1.commitNeeded()).andReturn(true);
            t1.Commit();
            EasyMock.expectLastCall().andThrow(new TaskMigratedException());
            t1.Close(false, true);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);
            AddAndInitTask();

            try
            {
                AssignedTasks.MaybeCommitPerUserRequested();
                Assert.True(false, "Should have thrown TaskMigratedException.");
            }
            catch (TaskMigratedException expected) { /* ignore */ }

            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.EMPTY_SET);
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCloseTaskOnProcessesIfTaskMigratedException()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.isProcessable(0L)).andReturn(true);
            t1.Process();
            EasyMock.expectLastCall().andThrow(new TaskMigratedException());
            t1.Close(false, true);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);
            AddAndInitTask();

            try
            {
                AssignedTasks.Process(0L);
                Assert.True(false, "Should have thrown TaskMigratedException.");
            }
            catch (TaskMigratedException expected) { /* ignore */ }

            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.EMPTY_SET);
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldNotProcessUnprocessableTasks()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.isProcessable(0L)).andReturn(false);
            EasyMock.replay(t1);
            AddAndInitTask();

            Assert.Equal(0, AssignedTasks.Process(0L));

            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldAlwaysProcessProcessableTasks()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.isProcessable(0L)).andReturn(true);
            EasyMock.expect(t1.Process()).andReturn(true).once();

            EasyMock.replay(t1);

            AddAndInitTask();

            Assert.Equal(1, AssignedTasks.Process(0L));

            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldPunctuateRunningTasks()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
            EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(true);
            EasyMock.replay(t1);

            AddAndInitTask();

            Assert.Equal(2, AssignedTasks.Punctuate());
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCloseTaskOnMaybePunctuateStreamTimeIfTaskMigratedException()
        {
            MockTaskInitialization();
            t1.maybePunctuateStreamTime();
            EasyMock.expectLastCall().andThrow(new TaskMigratedException());
            t1.Close(false, true);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);
            AddAndInitTask();

            try
            {
                AssignedTasks.Punctuate();
                Assert.True(false, "Should have thrown TaskMigratedException.");
            }
            catch (TaskMigratedException expected) { /* ignore */ }

            Assert.Equal(AssignedTasks.runningTaskIds(), Collections.EMPTY_SET);
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldCloseTaskOnMaybePunctuateSystemTimeIfTaskMigratedException()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
            t1.maybePunctuateSystemTime();
            EasyMock.expectLastCall().andThrow(new TaskMigratedException());
            t1.Close(false, true);
            EasyMock.expectLastCall();
            EasyMock.replay(t1);
            AddAndInitTask();

            try
            {
                AssignedTasks.Punctuate();
                Assert.True(false, "Should have thrown TaskMigratedException.");
            }
            catch (TaskMigratedException expected) { /* ignore */ }
            EasyMock.verify(t1);
        }

        [Fact]
        public void ShouldReturnNumberOfPunctuations()
        {
            MockTaskInitialization();
            EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
            EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(false);
            EasyMock.replay(t1);

            AddAndInitTask();

            Assert.Equal(1, AssignedTasks.Punctuate());
            EasyMock.verify(t1);
        }

        private void AddAndInitTask()
        {
            AssignedTasks.AddNewTask(t1);
            AssignedTasks.InitializeNewTasks();
        }

        private RuntimeException SuspendTask()
        {
            AddAndInitTask();
            return AssignedTasks.Suspend();
        }

        private void MockRunningTaskSuspension()
        {
            EasyMock.expect(t1.InitializeStateStores()).andReturn(true);
            t1.InitializeTopology();
            EasyMock.expectLastCall().once();
            EasyMock.expect(t1.HasStateStores()).andReturn(false).anyTimes();
            EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1)).anyTimes();
            EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptyList()).anyTimes();
            t1.Suspend();
            EasyMock.expectLastCall();
        }
    }
}
