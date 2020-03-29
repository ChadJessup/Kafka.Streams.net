/*






 *

 *





 */
























public class AssignedStreamsTasksTest {

    private StreamTask t1 = EasyMock.createMock(StreamTask);
    private StreamTask t2 = EasyMock.createMock(StreamTask);
    private TopicPartition tp1 = new TopicPartition("t1", 0);
    private TopicPartition tp2 = new TopicPartition("t2", 0);
    private TopicPartition changeLog1 = new TopicPartition("cl1", 0);
    private TopicPartition changeLog2 = new TopicPartition("cl2", 0);
    private TaskId taskId1 = new TaskId(0, 0);
    private TaskId taskId2 = new TaskId(1, 0);
    private AssignedStreamsTasks assignedTasks;

    
    public void before() {
        assignedTasks = new AssignedStreamsTasks(new LogContext("log "));
        EasyMock.expect(t1.id()).andReturn(taskId1).anyTimes();
        EasyMock.expect(t2.id()).andReturn(taskId2).anyTimes();
    }

    [Xunit.Fact]
    public void shouldInitializeNewTasks() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.replay(t1);

        addAndInitTask();

        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldMoveInitializedTasksNeedingRestoreToRestoring() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
        EasyMock.expect(t2.initializeStateStores()).andReturn(true);
        t2.initializeTopology();
        EasyMock.expectLastCall().once();
        HashSet<TopicPartition> t2partitions = Collections.singleton(tp2);
        EasyMock.expect(t2.partitions()).andReturn(t2partitions);
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.emptyList());

        EasyMock.replay(t1, t2);

        assignedTasks.addNewTask(t1);
        assignedTasks.addNewTask(t2);

        assignedTasks.initializeNewTasks();

        Collection<StreamTask> restoring = assignedTasks.restoringTasks();
        Assert.Equal(restoring.Count, (1));
        assertSame(restoring.iterator().next(), t1);
    }

    [Xunit.Fact]
    public void shouldMoveInitializedTasksThatDontNeedRestoringToRunning() {
        EasyMock.expect(t2.initializeStateStores()).andReturn(true);
        t2.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t2.partitions()).andReturn(Collections.singleton(tp2));
        EasyMock.expect(t2.changelogPartitions()).andReturn(Collections.emptyList());

        EasyMock.replay(t2);

        assignedTasks.addNewTask(t2);
        assignedTasks.initializeNewTasks();

        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.singleton(taskId2)));
    }

    [Xunit.Fact]
    public void shouldTransitionFullyRestoredTasksToRunning() {
        HashSet<TopicPartition> task1Partitions = Utils.mkSet(tp1);
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(task1Partitions).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Utils.mkSet(changeLog1, changeLog2)).anyTimes();
        EasyMock.expect(t1.hasStateStores()).andReturn(true).anyTimes();
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.replay(t1);

        addAndInitTask();

        assignedTasks.updateRestored(Utils.mkSet(changeLog1));
        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.<TaskId>emptySet()));
        assignedTasks.updateRestored(Utils.mkSet(changeLog2));
        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.singleton(taskId1)));
    }

    [Xunit.Fact]
    public void shouldSuspendRunningTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        Assert.Equal(suspendTask(), nullValue());

        Assert.Equal(assignedTasks.previousTaskIds(), (Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCloseRestoringTasks() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(false);
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptySet());
        t1.closeStateManager(true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();
        Assert.Equal(assignedTasks.closeAllRestoringTasks(), nullValue());

        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldClosedUnInitializedTasksOnSuspend() {
        t1.close(false, false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        assignedTasks.addNewTask(t1);
        Assert.Equal(assignedTasks.suspend(), nullValue());

        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldNotSuspendSuspendedTasks() {
        mockRunningTaskSuspension();
        EasyMock.replay(t1);

        Assert.Equal(suspendTask(), nullValue());
        Assert.Equal(assignedTasks.suspend(), nullValue());
        EasyMock.verify(t1);
    }


    [Xunit.Fact]
    public void shouldCloseTaskOnSuspendWhenRuntimeException() {
        mockTaskInitialization();
        t1.suspend();
        EasyMock.expectLastCall().andThrow(new RuntimeException("KABOOM!"));
        t1.close(false, false);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        Assert.Equal(suspendTask(), not(nullValue()));
        Assert.Equal(assignedTasks.previousTaskIds(), (Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCloseTaskOnSuspendIfTaskMigratedException() {
        mockTaskInitialization();
        t1.suspend();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        Assert.Equal(suspendTask(), nullValue());
        Assert.True(assignedTasks.previousTaskIds().isEmpty());
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldResumeMatchingSuspendedTasks() {
        mockRunningTaskSuspension();
        t1.resume();
        EasyMock.expectLastCall();
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.replay(t1);

        Assert.Equal(suspendTask(), nullValue());

        Assert.True(assignedTasks.maybeResumeSuspendedTask(taskId1, Collections.singleton(tp1)));
        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCloseTaskOnResumeSuspendedIfTaskMigratedException() {
        mockRunningTaskSuspension();
        t1.resume();
        t1.initializeTopology();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        Assert.Equal(suspendTask(), nullValue());

        try {
            assignedTasks.maybeResumeSuspendedTask(taskId1, Collections.singleton(tp1));
            Assert.True(false, "Should have thrown TaskMigratedException.");
        } catch (TaskMigratedException expected) { /* ignore */ }

        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    private void mockTaskInitialization() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(true);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1));
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptyList());
    }

    [Xunit.Fact]
    public void shouldCommitRunningTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();

        assignedTasks.commit();
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCloseTaskOnCommitIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.commit();
            Assert.True(false, "Should have thrown TaskMigratedException.");
        } catch (TaskMigratedException expected) { /* ignore */ }

        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldThrowExceptionOnCommitWhenNotCommitFailedOrProducerFenced() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new RuntimeException(""));
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.commit();
            Assert.True(false, "Should have thrown exception");
        } catch (Exception e) {
            // ok
        }
        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.singleton(taskId1)));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCommitRunningTasksIfNeeded() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitRequested()).andReturn(true);
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall();
        EasyMock.replay(t1);

        addAndInitTask();

        Assert.Equal(assignedTasks.maybeCommitPerUserRequested(), (1));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCloseTaskOnMaybeCommitIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.commitRequested()).andReturn(true);
        EasyMock.expect(t1.commitNeeded()).andReturn(true);
        t1.commit();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.maybeCommitPerUserRequested();
            Assert.True(false, "Should have thrown TaskMigratedException.");
        } catch (TaskMigratedException expected) { /* ignore */ }

        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCloseTaskOnProcessesIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(true);
        t1.process();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.process(0L);
            Assert.True(false, "Should have thrown TaskMigratedException.");
        } catch (TaskMigratedException expected) { /* ignore */ }

        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldNotProcessUnprocessableTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(false);
        EasyMock.replay(t1);
        addAndInitTask();

        Assert.Equal(assignedTasks.process(0L), (0));

        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldAlwaysProcessProcessableTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.isProcessable(0L)).andReturn(true);
        EasyMock.expect(t1.process()).andReturn(true).once();

        EasyMock.replay(t1);

        addAndInitTask();

        Assert.Equal(assignedTasks.process(0L), (1));

        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldPunctuateRunningTasks() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(true);
        EasyMock.replay(t1);

        addAndInitTask();

        Assert.Equal(assignedTasks.punctuate(), (2));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCloseTaskOnMaybePunctuateStreamTimeIfTaskMigratedException() {
        mockTaskInitialization();
        t1.maybePunctuateStreamTime();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.punctuate();
            Assert.True(false, "Should have thrown TaskMigratedException.");
        } catch (TaskMigratedException expected) { /* ignore */ }

        Assert.Equal(assignedTasks.runningTaskIds(), (Collections.EMPTY_SET));
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldCloseTaskOnMaybePunctuateSystemTimeIfTaskMigratedException() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        t1.maybePunctuateSystemTime();
        EasyMock.expectLastCall().andThrow(new TaskMigratedException());
        t1.close(false, true);
        EasyMock.expectLastCall();
        EasyMock.replay(t1);
        addAndInitTask();

        try {
            assignedTasks.punctuate();
            Assert.True(false, "Should have thrown TaskMigratedException.");
        } catch (TaskMigratedException expected) { /* ignore */ }
        EasyMock.verify(t1);
    }

    [Xunit.Fact]
    public void shouldReturnNumberOfPunctuations() {
        mockTaskInitialization();
        EasyMock.expect(t1.maybePunctuateStreamTime()).andReturn(true);
        EasyMock.expect(t1.maybePunctuateSystemTime()).andReturn(false);
        EasyMock.replay(t1);

        addAndInitTask();

        Assert.Equal(assignedTasks.punctuate(), (1));
        EasyMock.verify(t1);
    }

    private void addAndInitTask() {
        assignedTasks.addNewTask(t1);
        assignedTasks.initializeNewTasks();
    }

    private RuntimeException suspendTask() {
        addAndInitTask();
        return assignedTasks.suspend();
    }

    private void mockRunningTaskSuspension() {
        EasyMock.expect(t1.initializeStateStores()).andReturn(true);
        t1.initializeTopology();
        EasyMock.expectLastCall().once();
        EasyMock.expect(t1.hasStateStores()).andReturn(false).anyTimes();
        EasyMock.expect(t1.partitions()).andReturn(Collections.singleton(tp1)).anyTimes();
        EasyMock.expect(t1.changelogPartitions()).andReturn(Collections.emptyList()).anyTimes();
        t1.suspend();
        EasyMock.expectLastCall();
    }


}