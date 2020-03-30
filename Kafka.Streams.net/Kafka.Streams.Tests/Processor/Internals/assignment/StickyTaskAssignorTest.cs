/*






 *

 *





 */
























public class StickyTaskAssignorTest {

    private TaskId task00 = new TaskId(0, 0);
    private TaskId task01 = new TaskId(0, 1);
    private TaskId task02 = new TaskId(0, 2);
    private TaskId task03 = new TaskId(0, 3);
    private TaskId task04 = new TaskId(0, 4);
    private TaskId task05 = new TaskId(0, 5);

    private TaskId task10 = new TaskId(1, 0);
    private TaskId task11 = new TaskId(1, 1);
    private TaskId task12 = new TaskId(1, 2);
    private TaskId task20 = new TaskId(2, 0);
    private TaskId task21 = new TaskId(2, 1);
    private TaskId task22 = new TaskId(2, 2);

    private List<int> expectedTopicGroupIds = Array.asList(1, 2);

    private Dictionary<int, ClientState> clients = new TreeMap<>();
    private readonly int p1 = 1;
    private readonly int p2 = 2;
    private readonly int p3 = 3;
    private readonly int p4 = 4;

    [Xunit.Fact]
    public void ShouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount() {
        CreateClient(p1, 1);
        CreateClient(p2, 1);
        CreateClient(p3, 1);

        StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(0);

        foreach (int processId in clients.keySet()) {
            Assert.Equal(clients.get(processId).activeTaskCount(), (1));
        }
    }

    [Xunit.Fact]
    public void ShouldAssignTopicGroupIdEvenlyAcrossClientsWithNoStandByTasks() {
        CreateClient(p1, 2);
        CreateClient(p2, 2);
        CreateClient(p3, 2);

        StickyTaskAssignor taskAssignor = createTaskAssignor(task10, task11, task22, task20, task21, task12);
        taskAssignor.assign(0);
        AssertActiveTaskTopicGroupIdsEvenlyDistributed();
    }

    [Xunit.Fact]
    public void ShouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks() {
        CreateClient(p1, 2);
        CreateClient(p2, 2);
        CreateClient(p3, 2);

        StickyTaskAssignor taskAssignor = createTaskAssignor(task20, task11, task12, task10, task21, task22);
        taskAssignor.assign(1);
        AssertActiveTaskTopicGroupIdsEvenlyDistributed();
    }

    [Xunit.Fact]
    public void ShouldNotMigrateActiveTaskToOtherProcess() {
        createClientWithPreviousActiveTasks(p1, 1, task00);
        createClientWithPreviousActiveTasks(p2, 1, task01);

        StickyTaskAssignor firstAssignor = createTaskAssignor(task00, task01, task02);
        firstAssignor.assign(0);

        Assert.Equal(clients.get(p1).activeTasks(), hasItems(task00));
        Assert.Equal(clients.get(p2).activeTasks(), hasItems(task01));
        Assert.Equal(AllActiveTasks(), (Array.asList(task00, task01, task02)));

        clients.Clear();

        // flip the previous active tasks assignment around.
        createClientWithPreviousActiveTasks(p1, 1, task01);
        createClientWithPreviousActiveTasks(p2, 1, task02);

        StickyTaskAssignor secondAssignor = createTaskAssignor(task00, task01, task02);
        secondAssignor.assign(0);

        Assert.Equal(clients.get(p1).activeTasks(), hasItems(task01));
        Assert.Equal(clients.get(p2).activeTasks(), hasItems(task02));
        Assert.Equal(AllActiveTasks(), (Array.asList(task00, task01, task02)));
    }

    [Xunit.Fact]
    public void ShouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments() {
        createClientWithPreviousActiveTasks(p1, 1, task00, task02);
        createClientWithPreviousActiveTasks(p2, 1, task01);
        CreateClient(p3, 1);

        StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);

        Assert.Equal(clients.get(p2).activeTasks(), (Collections.singleton(task01)));
        Assert.Equal(clients.get(p1).activeTasks().Count, (1));
        Assert.Equal(clients.get(p3).activeTasks().Count, (1));
        Assert.Equal(AllActiveTasks(), (Array.asList(task00, task01, task02)));
    }

    [Xunit.Fact]
    public void ShouldAssignBasedOnCapacity() {
        CreateClient(p1, 1);
        CreateClient(p2, 2);
        StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);
        Assert.Equal(clients.get(p1).activeTasks().Count, (1));
        Assert.Equal(clients.get(p2).activeTasks().Count, (2));
    }

    [Xunit.Fact]
    public void ShouldAssignTasksEvenlyWithUnequalTopicGroupSizes() {

        createClientWithPreviousActiveTasks(p1, 1, task00, task01, task02, task03,
                                                            task04, task05, task10);

        CreateClient(p2, 1);

        StickyTaskAssignor taskAssignor = createTaskAssignor(task10, task00, task01, task02, task03, task04, task05);

        HashSet<TaskId> expectedClientITasks = new HashSet<>(Array.asList(task00, task01, task10, task05));
        HashSet<TaskId> expectedClientIITasks = new HashSet<>(Array.asList(task02, task03, task04));

        taskAssignor.assign(0);

        Assert.Equal(clients.get(p1).activeTasks(), (expectedClientITasks));
        Assert.Equal(clients.get(p2).activeTasks(), (expectedClientIITasks));
    }

    [Xunit.Fact]
    public void ShouldKeepActiveTaskStickynessWhenMoreClientThanActiveTasks() {
        int p5 = 5;
        createClientWithPreviousActiveTasks(p1, 1, task00);
        createClientWithPreviousActiveTasks(p2, 1, task02);
        createClientWithPreviousActiveTasks(p3, 1, task01);
        CreateClient(p4, 1);
        CreateClient(p5, 1);

        StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(0);

        Assert.Equal(clients.get(p1).activeTasks(), (Collections.singleton(task00)));
        Assert.Equal(clients.get(p2).activeTasks(), (Collections.singleton(task02)));
        Assert.Equal(clients.get(p3).activeTasks(), (Collections.singleton(task01)));

        // change up the assignment and make sure it is still sticky
        clients.Clear();
        CreateClient(p1, 1);
        createClientWithPreviousActiveTasks(p2, 1, task00);
        CreateClient(p3, 1);
        createClientWithPreviousActiveTasks(p4, 1, task02);
        createClientWithPreviousActiveTasks(p5, 1, task01);

        StickyTaskAssignor secondAssignor = createTaskAssignor(task00, task01, task02);
        secondAssignor.assign(0);

        Assert.Equal(clients.get(p2).activeTasks(), (Collections.singleton(task00)));
        Assert.Equal(clients.get(p4).activeTasks(), (Collections.singleton(task02)));
        Assert.Equal(clients.get(p5).activeTasks(), (Collections.singleton(task01)));


    }

    [Xunit.Fact]
    public void ShouldAssignTasksToClientWithPreviousStandbyTasks() {
        ClientState client1 = CreateClient(p1, 1);
        client1.addPreviousStandbyTasks(Utils.mkSet(task02));
        ClientState client2 = CreateClient(p2, 1);
        client2.addPreviousStandbyTasks(Utils.mkSet(task01));
        ClientState client3 = CreateClient(p3, 1);
        client3.addPreviousStandbyTasks(Utils.mkSet(task00));

        StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);

        Assert.Equal(clients.get(p1).activeTasks(), (Collections.singleton(task02)));
        Assert.Equal(clients.get(p2).activeTasks(), (Collections.singleton(task01)));
        Assert.Equal(clients.get(p3).activeTasks(), (Collections.singleton(task00)));
    }

    [Xunit.Fact]
    public void ShouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks() {
        ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00);
        c1.addPreviousStandbyTasks(Utils.mkSet(task01));
        ClientState c2 = createClientWithPreviousActiveTasks(p2, 2, task02);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01));

        StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

        taskAssignor.assign(0);

        Assert.Equal(clients.get(p1).activeTasks(), (Collections.singleton(task00)));
        Assert.Equal(clients.get(p2).activeTasks(), (Utils.mkSet(task02, task01)));
    }

    [Xunit.Fact]
    public void ShouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssingedTo() {
        createClientWithPreviousActiveTasks(p1, 1, task00);
        createClientWithPreviousActiveTasks(p2, 1, task01);
        createClientWithPreviousActiveTasks(p3, 1, task02);
        createClientWithPreviousActiveTasks(p4, 1, task03);

        StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02, task03);
        taskAssignor.assign(1);

        Assert.Equal(clients.get(p1).standbyTasks(), not(hasItems(task00)));
        Assert.True(clients.get(p1).standbyTasks().Count <= 2);
        Assert.Equal(clients.get(p2).standbyTasks(), not(hasItems(task01)));
        Assert.True(clients.get(p2).standbyTasks().Count <= 2);
        Assert.Equal(clients.get(p3).standbyTasks(), not(hasItems(task02)));
        Assert.True(clients.get(p3).standbyTasks().Count <= 2);
        Assert.Equal(clients.get(p4).standbyTasks(), not(hasItems(task03)));
        Assert.True(clients.get(p4).standbyTasks().Count <= 2);

        int nonEmptyStandbyTaskCount = 0;
        foreach (int client in clients.keySet()) {
            nonEmptyStandbyTaskCount += clients.get(client).standbyTasks().isEmpty() ? 0 : 1;
        }

        Assert.True(nonEmptyStandbyTaskCount >= 3);
        Assert.Equal(AllStandbyTasks(), (Array.asList(task00, task01, task02, task03)));
    }



    [Xunit.Fact]
    public void ShouldAssignMultipleReplicasOfStandbyTask() {
        createClientWithPreviousActiveTasks(p1, 1, task00);
        createClientWithPreviousActiveTasks(p2, 1, task01);
        createClientWithPreviousActiveTasks(p3, 1, task02);

        StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(2);

        Assert.Equal(clients.get(p1).standbyTasks(), (Utils.mkSet(task01, task02)));
        Assert.Equal(clients.get(p2).standbyTasks(), (Utils.mkSet(task02, task00)));
        Assert.Equal(clients.get(p3).standbyTasks(), (Utils.mkSet(task00, task01)));
    }

    [Xunit.Fact]
    public void ShouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned() {
        CreateClient(p1, 1);
        StickyTaskAssignor taskAssignor = createTaskAssignor(task00);
        taskAssignor.assign(1);
        Assert.Equal(clients.get(p1).standbyTasks().Count, (0));
    }

    [Xunit.Fact]
    public void ShouldAssignActiveAndStandbyTasks() {
        CreateClient(p1, 1);
        CreateClient(p2, 1);
        CreateClient(p3, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(1);

        Assert.Equal(AllActiveTasks(), (Array.asList(task00, task01, task02)));
        Assert.Equal(AllStandbyTasks(), (Array.asList(task00, task01, task02)));
    }


    [Xunit.Fact]
    public void ShouldAssignAtLeastOneTaskToEachClientIfPossible() {
        CreateClient(p1, 3);
        CreateClient(p2, 1);
        CreateClient(p3, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(0);
        Assert.Equal(clients.get(p1).assignedTaskCount(), (1));
        Assert.Equal(clients.get(p2).assignedTaskCount(), (1));
        Assert.Equal(clients.get(p3).assignedTaskCount(), (1));
    }

    [Xunit.Fact]
    public void ShouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks() {
        CreateClient(p1, 1);
        CreateClient(p2, 1);
        CreateClient(p3, 1);
        CreateClient(p4, 1);
        CreateClient(5, 1);
        CreateClient(6, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(0);

        Assert.Equal(AllActiveTasks(), (Array.asList(task00, task01, task02)));
    }

    [Xunit.Fact]
    public void ShouldBalanceActiveAndStandbyTasksAcrossAvailableClients() {
        CreateClient(p1, 1);
        CreateClient(p2, 1);
        CreateClient(p3, 1);
        CreateClient(p4, 1);
        CreateClient(5, 1);
        CreateClient(6, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02);
        taskAssignor.assign(1);

        foreach (ClientState clientState in clients.values()) {
            Assert.Equal(clientState.assignedTaskCount(), (1));
        }
    }

    [Xunit.Fact]
    public void ShouldAssignMoreTasksToClientWithMoreCapacity() {
        CreateClient(p2, 2);
        CreateClient(p1, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00,
                                                                            task01,
                                                                            task02,
                                                                            new TaskId(1, 0),
                                                                            new TaskId(1, 1),
                                                                            new TaskId(1, 2),
                                                                            new TaskId(2, 0),
                                                                            new TaskId(2, 1),
                                                                            new TaskId(2, 2),
                                                                            new TaskId(3, 0),
                                                                            new TaskId(3, 1),
                                                                            new TaskId(3, 2));

        taskAssignor.assign(0);
        Assert.Equal(clients.get(p2).assignedTaskCount(), (8));
        Assert.Equal(clients.get(p1).assignedTaskCount(), (4));
    }

    [Xunit.Fact]
    public void ShouldEvenlyDistributeByTaskIdAndPartition() {
        CreateClient(p1, 4);
        CreateClient(p2, 4);
        CreateClient(p3, 4);
        CreateClient(p4, 4);

        List<TaskId> taskIds = new ArrayList<>();
        TaskId[] taskIdArray = new TaskId[16];

        for (int i = 1; i <= 2; i++) {
            for (int j = 0; j < 8; j++) {
                taskIds.add(new TaskId(i, j));
            }
        }

        Collections.shuffle(taskIds);
        taskIds.toArray(taskIdArray);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(taskIdArray);
        taskAssignor.assign(0);

        Collections.sort(taskIds);
        HashSet<TaskId> expectedClientOneAssignment = getExpectedTaskIdAssignment(taskIds, 0, 4, 8, 12);
        HashSet<TaskId> expectedClientTwoAssignment = getExpectedTaskIdAssignment(taskIds, 1, 5, 9, 13);
        HashSet<TaskId> expectedClientThreeAssignment = getExpectedTaskIdAssignment(taskIds, 2, 6, 10, 14);
        HashSet<TaskId> expectedClientFourAssignment = getExpectedTaskIdAssignment(taskIds, 3, 7, 11, 15);

        Dictionary<int, HashSet<TaskId>> sortedAssignments = sortClientAssignments(clients);

        Assert.Equal(sortedAssignments.get(p1), (expectedClientOneAssignment));
        Assert.Equal(sortedAssignments.get(p2), (expectedClientTwoAssignment));
        Assert.Equal(sortedAssignments.get(p3), (expectedClientThreeAssignment));
        Assert.Equal(sortedAssignments.get(p4), (expectedClientFourAssignment));
    }


    [Xunit.Fact]
    public void ShouldNotHaveSameAssignmentOnAnyTwoHosts() {
        CreateClient(p1, 1);
        CreateClient(p2, 1);
        CreateClient(p3, 1);
        CreateClient(p4, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(1);

        for (int i = p1; i <= p4; i++) {
            HashSet<TaskId> taskIds = clients.get(i).assignedTasks();
            for (int j = p1; j <= p4; j++) {
                if (j != i) {
                    Assert.Equal("clients shouldn't have same task assignment", clients.get(j).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }

        }
    }

    [Xunit.Fact]
    public void ShouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousActiveTasks() {
        createClientWithPreviousActiveTasks(p1, 1, task01, task02);
        createClientWithPreviousActiveTasks(p2, 1, task03);
        createClientWithPreviousActiveTasks(p3, 1, task00);
        CreateClient(p4, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(1);

        for (int i = p1; i <= p4; i++) {
            HashSet<TaskId> taskIds = clients.get(i).assignedTasks();
            for (int j = p1; j <= p4; j++) {
                if (j != i) {
                    Assert.Equal("clients shouldn't have same task assignment", clients.get(j).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }

        }
    }

    [Xunit.Fact]
    public void ShouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousStandbyTasks() {
        ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task02);
        c1.addPreviousStandbyTasks(Utils.mkSet(task03, task00));
        ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task03, task00);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01, task02));

        CreateClient(p3, 1);
        CreateClient(p4, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(1);

        for (int i = p1; i <= p4; i++) {
            HashSet<TaskId> taskIds = clients.get(i).assignedTasks();
            for (int j = p1; j <= p4; j++) {
                if (j != i) {
                    Assert.Equal("clients shouldn't have same task assignment", clients.get(j).assignedTasks(),
                               not(equalTo(taskIds)));
                }
            }

        }
    }

    [Xunit.Fact]
    public void ShouldReBalanceTasksAcrossAllClientsWhenCapacityAndTaskCountTheSame() {
        createClientWithPreviousActiveTasks(p3, 1, task00, task01, task02, task03);
        CreateClient(p1, 1);
        CreateClient(p2, 1);
        CreateClient(p4, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(0);

        Assert.Equal(clients.get(p1).assignedTaskCount(), (1));
        Assert.Equal(clients.get(p2).assignedTaskCount(), (1));
        Assert.Equal(clients.get(p3).assignedTaskCount(), (1));
        Assert.Equal(clients.get(p4).assignedTaskCount(), (1));
    }

    [Xunit.Fact]
    public void ShouldReBalanceTasksAcrossClientsWhenCapacityLessThanTaskCount() {
        createClientWithPreviousActiveTasks(p3, 1, task00, task01, task02, task03);
        CreateClient(p1, 1);
        CreateClient(p2, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(0);

        Assert.Equal(clients.get(p3).assignedTaskCount(), (2));
        Assert.Equal(clients.get(p1).assignedTaskCount(), (1));
        Assert.Equal(clients.get(p2).assignedTaskCount(), (1));
    }

    [Xunit.Fact]
    public void ShouldRebalanceTasksToClientsBasedOnCapacity() {
        createClientWithPreviousActiveTasks(p2, 1, task00, task03, task02);
        CreateClient(p3, 2);
        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task03);
        taskAssignor.assign(0);
        Assert.Equal(clients.get(p2).assignedTaskCount(), (1));
        Assert.Equal(clients.get(p3).assignedTaskCount(), (2));
    }

    [Xunit.Fact]
    public void ShouldMoveMinimalNumberOfTasksWhenPreviouslyAboveCapacityAndNewClientAdded() {
        HashSet<TaskId> p1PrevTasks = Utils.mkSet(task00, task02);
        HashSet<TaskId> p2PrevTasks = Utils.mkSet(task01, task03);

        createClientWithPreviousActiveTasks(p1, 1, task00, task02);
        createClientWithPreviousActiveTasks(p2, 1, task01, task03);
        createClientWithPreviousActiveTasks(p3, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
        taskAssignor.assign(0);

        HashSet<TaskId> p3ActiveTasks = clients.get(p3).activeTasks();
        Assert.Equal(p3ActiveTasks.Count, (1));
        if (p1PrevTasks.removeAll(p3ActiveTasks)) {
            Assert.Equal(clients.get(p2).activeTasks(), (p2PrevTasks));
        } else {
            Assert.Equal(clients.get(p1).activeTasks(), (p1PrevTasks));
        }
    }

    [Xunit.Fact]
    public void ShouldNotMoveAnyTasksWhenNewTasksAdded() {
        createClientWithPreviousActiveTasks(p1, 1, task00, task01);
        createClientWithPreviousActiveTasks(p2, 1, task02, task03);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task03, task01, task04, task02, task00, task05);
        taskAssignor.assign(0);

        Assert.Equal(clients.get(p1).activeTasks(), hasItems(task00, task01));
        Assert.Equal(clients.get(p2).activeTasks(), hasItems(task02, task03));
    }

    [Xunit.Fact]
    public void ShouldAssignNewTasksToNewClientWhenPreviousTasksAssignedToOldClients() {

        createClientWithPreviousActiveTasks(p1, 1, task02, task01);
        createClientWithPreviousActiveTasks(p2, 1, task00, task03);
        CreateClient(p3, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task03, task01, task04, task02, task00, task05);
        taskAssignor.assign(0);

        Assert.Equal(clients.get(p1).activeTasks(), hasItems(task02, task01));
        Assert.Equal(clients.get(p2).activeTasks(), hasItems(task00, task03));
        Assert.Equal(clients.get(p3).activeTasks(), hasItems(task04, task05));
    }

    [Xunit.Fact]
    public void ShouldAssignTasksNotPreviouslyActiveToNewClient() {
        TaskId task10 = new TaskId(0, 10);
        TaskId task11 = new TaskId(0, 11);
        TaskId task12 = new TaskId(1, 2);
        TaskId task13 = new TaskId(1, 3);
        TaskId task20 = new TaskId(2, 0);
        TaskId task21 = new TaskId(2, 1);
        TaskId task22 = new TaskId(2, 2);
        TaskId task23 = new TaskId(2, 3);

        ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task12, task13);
        c1.addPreviousStandbyTasks(Utils.mkSet(task00, task11, task20, task21, task23));
        ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task00, task11, task22);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01, task10, task02, task20, task03, task12, task21, task13, task23));
        ClientState c3 = createClientWithPreviousActiveTasks(p3, 1, task20, task21, task23);
        c3.addPreviousStandbyTasks(Utils.mkSet(task02, task12));

        ClientState newClient = CreateClient(p4, 1);
        newClient.addPreviousStandbyTasks(Utils.mkSet(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23));

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23);
        taskAssignor.assign(0);

        Assert.Equal(c1.activeTasks(), (Utils.mkSet(task01, task12, task13)));
        Assert.Equal(c2.activeTasks(), (Utils.mkSet(task00, task11, task22)));
        Assert.Equal(c3.activeTasks(), (Utils.mkSet(task20, task21, task23)));
        Assert.Equal(newClient.activeTasks(), (Utils.mkSet(task02, task03, task10)));
    }

    [Xunit.Fact]
    public void ShouldAssignTasksNotPreviouslyActiveToMultipleNewClients() {
        TaskId task10 = new TaskId(0, 10);
        TaskId task11 = new TaskId(0, 11);
        TaskId task12 = new TaskId(1, 2);
        TaskId task13 = new TaskId(1, 3);
        TaskId task20 = new TaskId(2, 0);
        TaskId task21 = new TaskId(2, 1);
        TaskId task22 = new TaskId(2, 2);
        TaskId task23 = new TaskId(2, 3);

        ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task12, task13);
        c1.addPreviousStandbyTasks(Utils.mkSet(task00, task11, task20, task21, task23));
        ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task00, task11, task22);
        c2.addPreviousStandbyTasks(Utils.mkSet(task01, task10, task02, task20, task03, task12, task21, task13, task23));

        ClientState bounce1 = CreateClient(p3, 1);
        bounce1.addPreviousStandbyTasks(Utils.mkSet(task20, task21, task23));

        ClientState bounce2 = CreateClient(p4, 1);
        bounce2.addPreviousStandbyTasks(Utils.mkSet(task02, task03, task10));

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23);
        taskAssignor.assign(0);

        Assert.Equal(c1.activeTasks(), (Utils.mkSet(task01, task12, task13)));
        Assert.Equal(c2.activeTasks(), (Utils.mkSet(task00, task11, task22)));
        Assert.Equal(bounce1.activeTasks(), (Utils.mkSet(task20, task21, task23)));
        Assert.Equal(bounce2.activeTasks(), (Utils.mkSet(task02, task03, task10)));
    }

    [Xunit.Fact]
    public void ShouldAssignTasksToNewClient() {
        createClientWithPreviousActiveTasks(p1, 1, task01, task02);
        CreateClient(p2, 1);
        createTaskAssignor(task01, task02).assign(0);
        Assert.Equal(clients.get(p1).activeTaskCount(), (1));
    }

    [Xunit.Fact]
    public void ShouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingClients() {
        ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00, task01, task02);
        ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task03, task04, task05);
        ClientState newClient = CreateClient(p3, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02, task03, task04, task05);
        taskAssignor.assign(0);
        Assert.Equal(c1.activeTasks(), not(hasItem(task03)));
        Assert.Equal(c1.activeTasks(), not(hasItem(task04)));
        Assert.Equal(c1.activeTasks(), not(hasItem(task05)));
        Assert.Equal(c1.activeTaskCount(), (2));
        Assert.Equal(c2.activeTasks(), not(hasItems(task00)));
        Assert.Equal(c2.activeTasks(), not(hasItems(task01)));
        Assert.Equal(c2.activeTasks(), not(hasItems(task02)));
        Assert.Equal(c2.activeTaskCount(), (2));
        Assert.Equal(newClient.activeTaskCount(), (2));
    }

    [Xunit.Fact]
    public void ShouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingAndBouncedClients() {
        TaskId task06 = new TaskId(0, 6);
        ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00, task01, task02, task06);
        ClientState c2 = CreateClient(p2, 1);
        c2.addPreviousStandbyTasks(Utils.mkSet(task03, task04, task05));
        ClientState newClient = CreateClient(p3, 1);

        StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02, task03, task04, task05, task06);
        taskAssignor.assign(0);
        Assert.Equal(c1.activeTasks(), not(hasItem(task03)));
        Assert.Equal(c1.activeTasks(), not(hasItem(task04)));
        Assert.Equal(c1.activeTasks(), not(hasItem(task05)));
        Assert.Equal(c1.activeTaskCount(), (3));
        Assert.Equal(c2.activeTasks(), not(hasItems(task00)));
        Assert.Equal(c2.activeTasks(), not(hasItems(task01)));
        Assert.Equal(c2.activeTasks(), not(hasItems(task02)));
        Assert.Equal(c2.activeTaskCount(), (2));
        Assert.Equal(newClient.activeTaskCount(), (2));
    }

    private StickyTaskAssignor<int> CreateTaskAssignor(TaskId... tasks) {
        List<TaskId> taskIds = Array.asList(tasks);
        Collections.shuffle(taskIds);
        return new StickyTaskAssignor<>(clients,
                                        new HashSet<>(taskIds));
    }

    private List<TaskId> AllActiveTasks() {
        List<TaskId> allActive = new ArrayList<>();
        foreach (ClientState client in clients.values()) {
            allActive.addAll(client.activeTasks());
        }
        Collections.sort(allActive);
        return allActive;
    }

    private List<TaskId> AllStandbyTasks() {
        List<TaskId> tasks = new ArrayList<>();
        foreach (ClientState client in clients.values()) {
            tasks.addAll(client.standbyTasks());
        }
        Collections.sort(tasks);
        return tasks;
    }

    private ClientState CreateClient(int processId, int capacity) {
        return createClientWithPreviousActiveTasks(processId, capacity);
    }

    private ClientState CreateClientWithPreviousActiveTasks(int processId, int capacity, TaskId... taskIds) {
        ClientState clientState = new ClientState(capacity);
        clientState.addPreviousActiveTasks(Utils.mkSet(taskIds));
        clients.put(processId, clientState);
        return clientState;
    }

    private void AssertActiveTaskTopicGroupIdsEvenlyDistributed() {
        foreach (Map.Entry<int, ClientState> clientStateEntry in clients.entrySet()) {
            List<int> topicGroupIds = new ArrayList<>();
            HashSet<TaskId> activeTasks = clientStateEntry.getValue().activeTasks();
            foreach (TaskId activeTask in activeTasks) {
                topicGroupIds.add(activeTask.topicGroupId);
            }
            Collections.sort(topicGroupIds);
            Assert.Equal(topicGroupIds, (expectedTopicGroupIds));
        }
    }

    private Dictionary<int, HashSet<TaskId>> SortClientAssignments(Map<int, ClientState> clients) {
        Dictionary<int, HashSet<TaskId>> sortedAssignments = new HashMap<>();
        foreach (Map.Entry<int, ClientState> entry in clients.entrySet()) {
            HashSet<TaskId> sorted = new TreeSet<>(entry.getValue().activeTasks());
            sortedAssignments.put(entry.getKey(), sorted);
        }
        return sortedAssignments;
    }

    private HashSet<TaskId> GetExpectedTaskIdAssignment(List<TaskId> tasks, int... indices) {
        HashSet<TaskId> sortedAssignment = new TreeSet<>();
        foreach (int index in indices) {
            sortedAssignment.add(tasks.get(index));
        }
        return sortedAssignment;
    }

}
