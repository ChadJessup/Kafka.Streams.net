/*






 *

 *





 */













public class ClientStateTest {

    private ClientState client = new ClientState(1);

    [Xunit.Fact]
    public void shouldHaveNotReachedCapacityWhenAssignedTasksLessThanCapacity() {
        Assert.False(client.reachedCapacity());
    }

    [Xunit.Fact]
    public void shouldHaveReachedCapacityWhenAssignedTasksGreaterThanOrEqualToCapacity() {
        client.assign(new TaskId(0, 1), true);
        Assert.True(client.reachedCapacity());
    }


    [Xunit.Fact]
    public void shouldAddActiveTasksToBothAssignedAndActive() {
        TaskId tid = new TaskId(0, 1);

        client.assign(tid, true);
        Assert.Equal(client.activeTasks(), (Collections.singleton(tid)));
        Assert.Equal(client.assignedTasks(), (Collections.singleton(tid)));
        Assert.Equal(client.assignedTaskCount(), (1));
        Assert.Equal(client.standbyTasks().Count, (0));
    }

    [Xunit.Fact]
    public void shouldAddStandbyTasksToBothStandbyAndActive() {
        TaskId tid = new TaskId(0, 1);

        client.assign(tid, false);
        Assert.Equal(client.assignedTasks(), (Collections.singleton(tid)));
        Assert.Equal(client.standbyTasks(), (Collections.singleton(tid)));
        Assert.Equal(client.assignedTaskCount(), (1));
        Assert.Equal(client.activeTasks().Count, (0));
    }

    [Xunit.Fact]
    public void shouldAddPreviousActiveTasksToPreviousAssignedAndPreviousActive() {
        TaskId tid1 = new TaskId(0, 1);
        TaskId tid2 = new TaskId(0, 2);

        client.addPreviousActiveTasks(Utils.mkSet(tid1, tid2));
        Assert.Equal(client.previousActiveTasks(), (Utils.mkSet(tid1, tid2)));
        Assert.Equal(client.previousAssignedTasks(), (Utils.mkSet(tid1, tid2)));
    }

    [Xunit.Fact]
    public void shouldAddPreviousStandbyTasksToPreviousAssigned() {
        TaskId tid1 = new TaskId(0, 1);
        TaskId tid2 = new TaskId(0, 2);

        client.addPreviousStandbyTasks(Utils.mkSet(tid1, tid2));
        Assert.Equal(client.previousActiveTasks().Count, (0));
        Assert.Equal(client.previousAssignedTasks(), (Utils.mkSet(tid1, tid2)));
    }

    [Xunit.Fact]
    public void shouldHaveAssignedTaskIfActiveTaskAssigned() {
        TaskId tid = new TaskId(0, 2);

        client.assign(tid, true);
        Assert.True(client.hasAssignedTask(tid));
    }

    [Xunit.Fact]
    public void shouldHaveAssignedTaskIfStandbyTaskAssigned() {
        TaskId tid = new TaskId(0, 2);

        client.assign(tid, false);
        Assert.True(client.hasAssignedTask(tid));
    }

    [Xunit.Fact]
    public void shouldNotHaveAssignedTaskIfTaskNotAssigned() {

        client.assign(new TaskId(0, 2), true);
        Assert.False(client.hasAssignedTask(new TaskId(0, 3)));
    }

    [Xunit.Fact]
    public void shouldHaveMoreAvailableCapacityWhenCapacityTheSameButFewerAssignedTasks() {
        ClientState c2 = new ClientState(1);
        client.assign(new TaskId(0, 1), true);
        Assert.True(c2.hasMoreAvailableCapacityThan(client));
        Assert.False(client.hasMoreAvailableCapacityThan(c2));
    }

    [Xunit.Fact]
    public void shouldHaveMoreAvailableCapacityWhenCapacityHigherAndSameAssignedTaskCount() {
        ClientState c2 = new ClientState(2);
        Assert.True(c2.hasMoreAvailableCapacityThan(client));
        Assert.False(client.hasMoreAvailableCapacityThan(c2));
    }

    [Xunit.Fact]
    public void shouldUseMultiplesOfCapacityToDetermineClientWithMoreAvailableCapacity() {
        ClientState c2 = new ClientState(2);

        for (int i = 0; i < 7; i++) {
            c2.assign(new TaskId(0, i), true);
        }

        for (int i = 7; i < 11; i++) {
            client.assign(new TaskId(0, i), true);
        }

        Assert.True(c2.hasMoreAvailableCapacityThan(client));
    }

    [Xunit.Fact]
    public void shouldHaveMoreAvailableCapacityWhenCapacityIsTheSameButAssignedTasksIsLess() {
        ClientState c1 = new ClientState(3);
        ClientState c2 = new ClientState(3);
        for (int i = 0; i < 4; i++) {
            c1.assign(new TaskId(0, i), true);
            c2.assign(new TaskId(0, i), true);
        }
        c2.assign(new TaskId(0, 5), true);
        Assert.True(c1.hasMoreAvailableCapacityThan(c2));
    }

    [Xunit.Fact]// (expected = IllegalStateException)
    public void shouldThrowIllegalStateExceptionIfCapacityOfThisClientStateIsZero() {
        ClientState c1 = new ClientState(0);
        c1.hasMoreAvailableCapacityThan(new ClientState(1));
    }

    [Xunit.Fact]// (expected = IllegalStateException)
    public void shouldThrowIllegalStateExceptionIfCapacityOfOtherClientStateIsZero() {
        ClientState c1 = new ClientState(1);
        c1.hasMoreAvailableCapacityThan(new ClientState(0));
    }

    [Xunit.Fact]
    public void shouldHaveUnfulfilledQuotaWhenActiveTaskSizeLessThanCapacityTimesTasksPerThread() {
        ClientState client = new ClientState(1);
        client.assign(new TaskId(0, 1), true);
        Assert.True(client.hasUnfulfilledQuota(2));
    }

    [Xunit.Fact]
    public void shouldNotHaveUnfulfilledQuotaWhenActiveTaskSizeGreaterEqualThanCapacityTimesTasksPerThread() {
        ClientState client = new ClientState(1);
        client.assign(new TaskId(0, 1), true);
        Assert.False(client.hasUnfulfilledQuota(1));
    }

}