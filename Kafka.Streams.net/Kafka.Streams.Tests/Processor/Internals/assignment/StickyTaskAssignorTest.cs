using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals.Assignment
{
    public class StickyTaskAssignorTest
    {
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

        private List<int> expectedTopicGroupIds = Arrays.asList(1, 2);

        private Dictionary<int, ClientState> clients = new TreeMap<>();
        private readonly int p1 = 1;
        private readonly int p2 = 2;
        private readonly int p3 = 3;
        private readonly int p4 = 4;

        [Fact]
        public void ShouldAssignOneActiveTaskToEachProcessWhenTaskCountSameAsProcessCount()
        {
            CreateClient(p1, 1);
            CreateClient(p2, 1);
            CreateClient(p3, 1);

            StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
            taskAssignor.Assign(0);

            foreach (int processId in clients.Keys)
            {
                Assert.Equal(clients[processId].activeTaskCount(), 1);
            }
        }

        [Fact]
        public void ShouldAssignTopicGroupIdEvenlyAcrossClientsWithNoStandByTasks()
        {
            CreateClient(p1, 2);
            CreateClient(p2, 2);
            CreateClient(p3, 2);

            StickyTaskAssignor taskAssignor = createTaskAssignor(task10, task11, task22, task20, task21, task12);
            taskAssignor.Assign(0);
            AssertActiveTaskTopicGroupIdsEvenlyDistributed();
        }

        [Fact]
        public void ShouldAssignTopicGroupIdEvenlyAcrossClientsWithStandByTasks()
        {
            CreateClient(p1, 2);
            CreateClient(p2, 2);
            CreateClient(p3, 2);

            StickyTaskAssignor taskAssignor = createTaskAssignor(task20, task11, task12, task10, task21, task22);
            taskAssignor.Assign(1);
            AssertActiveTaskTopicGroupIdsEvenlyDistributed();
        }

        [Fact]
        public void ShouldNotMigrateActiveTaskToOtherProcess()
        {
            createClientWithPreviousActiveTasks(p1, 1, task00);
            createClientWithPreviousActiveTasks(p2, 1, task01);

            StickyTaskAssignor firstAssignor = createTaskAssignor(task00, task01, task02);
            firstAssignor.Assign(0);

            Assert.Equal(clients[p1].ActiveTasks(), hasItems(task00));
            Assert.Equal(clients[p2].ActiveTasks(), hasItems(task01));
            Assert.Equal(AllActiveTasks(), Arrays.asList(task00, task01, task02));

            clients.Clear();

            // flip the previous active tasks assignment around.
            createClientWithPreviousActiveTasks(p1, 1, task01);
            createClientWithPreviousActiveTasks(p2, 1, task02);

            StickyTaskAssignor secondAssignor = createTaskAssignor(task00, task01, task02);
            secondAssignor.Assign(0);

            Assert.Equal(clients.Get(p1).ActiveTasks(), hasItems(task01));
            Assert.Equal(clients.Get(p2).ActiveTasks(), hasItems(task02));
            Assert.Equal(AllActiveTasks(), Arrays.asList(task00, task01, task02));
        }

        [Fact]
        public void ShouldMigrateActiveTasksToNewProcessWithoutChangingAllAssignments()
        {
            createClientWithPreviousActiveTasks(p1, 1, task00, task02);
            createClientWithPreviousActiveTasks(p2, 1, task01);
            CreateClient(p3, 1);

            StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p2).ActiveTasks(), Collections.singleton(task01));
            Assert.Equal(clients.Get(p1).ActiveTasks().Count, 1);
            Assert.Equal(clients.Get(p3).ActiveTasks().Count, 1);
            Assert.Equal(AllActiveTasks(), Arrays.asList(task00, task01, task02));
        }

        [Fact]
        public void ShouldAssignBasedOnCapacity()
        {
            CreateClient(p1, 1);
            CreateClient(p2, 2);
            StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

            taskAssignor.Assign(0);
            Assert.Equal(clients.Get(p1).ActiveTasks().Count, 1);
            Assert.Equal(clients.Get(p2).ActiveTasks().Count, 2);
        }

        [Fact]
        public void ShouldAssignTasksEvenlyWithUnequalTopicGroupSizes()
        {

            createClientWithPreviousActiveTasks(p1, 1, task00, task01, task02, task03,
                                                                task04, task05, task10);

            CreateClient(p2, 1);

            StickyTaskAssignor taskAssignor = createTaskAssignor(task10, task00, task01, task02, task03, task04, task05);

            HashSet<TaskId> expectedClientITasks = new HashSet<>(Arrays.asList(task00, task01, task10, task05));
            HashSet<TaskId> expectedClientIITasks = new HashSet<>(Arrays.asList(task02, task03, task04));

            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p1).ActiveTasks(), expectedClientITasks);
            Assert.Equal(clients.Get(p2).ActiveTasks(), expectedClientIITasks);
        }

        [Fact]
        public void ShouldKeepActiveTaskStickynessWhenMoreClientThanActiveTasks()
        {
            int p5 = 5;
            createClientWithPreviousActiveTasks(p1, 1, task00);
            createClientWithPreviousActiveTasks(p2, 1, task02);
            createClientWithPreviousActiveTasks(p3, 1, task01);
            CreateClient(p4, 1);
            CreateClient(p5, 1);

            StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p1).ActiveTasks(), Collections.singleton(task00));
            Assert.Equal(clients.Get(p2).ActiveTasks(), Collections.singleton(task02));
            Assert.Equal(clients.Get(p3).ActiveTasks(), Collections.singleton(task01));

            // change up the assignment and make sure it is still sticky
            clients.Clear();
            CreateClient(p1, 1);
            createClientWithPreviousActiveTasks(p2, 1, task00);
            CreateClient(p3, 1);
            createClientWithPreviousActiveTasks(p4, 1, task02);
            createClientWithPreviousActiveTasks(p5, 1, task01);

            StickyTaskAssignor secondAssignor = createTaskAssignor(task00, task01, task02);
            secondAssignor.Assign(0);

            Assert.Equal(clients.Get(p2).ActiveTasks(), Collections.singleton(task00));
            Assert.Equal(clients.Get(p4).ActiveTasks(), Collections.singleton(task02));
            Assert.Equal(clients.Get(p5).ActiveTasks(), Collections.singleton(task01));


        }

        [Fact]
        public void ShouldAssignTasksToClientWithPreviousStandbyTasks()
        {
            ClientState client1 = CreateClient(p1, 1);
            client1.AddPreviousStandbyTasks(Utils.mkSet(task02));
            ClientState client2 = CreateClient(p2, 1);
            client2.AddPreviousStandbyTasks(Utils.mkSet(task01));
            ClientState client3 = CreateClient(p3, 1);
            client3.AddPreviousStandbyTasks(Utils.mkSet(task00));

            StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p1).ActiveTasks(), Collections.singleton(task02));
            Assert.Equal(clients.Get(p2).ActiveTasks(), Collections.singleton(task01));
            Assert.Equal(clients.Get(p3).ActiveTasks(), Collections.singleton(task00));
        }

        [Fact]
        public void ShouldAssignBasedOnCapacityWhenMultipleClientHaveStandbyTasks()
        {
            ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00);
            c1.AddPreviousStandbyTasks(Utils.mkSet(task01));
            ClientState c2 = createClientWithPreviousActiveTasks(p2, 2, task02);
            c2.AddPreviousStandbyTasks(Utils.mkSet(task01));

            StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);

            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p1).ActiveTasks(), Collections.singleton(task00));
            Assert.Equal(clients.Get(p2).ActiveTasks(), Utils.mkSet(task02, task01));
        }

        [Fact]
        public void ShouldAssignStandbyTasksToDifferentClientThanCorrespondingActiveTaskIsAssingedTo()
        {
            createClientWithPreviousActiveTasks(p1, 1, task00);
            createClientWithPreviousActiveTasks(p2, 1, task01);
            createClientWithPreviousActiveTasks(p3, 1, task02);
            createClientWithPreviousActiveTasks(p4, 1, task03);

            StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02, task03);
            taskAssignor.Assign(1);

            Assert.Equal(clients.Get(p1).StandbyTasks(), not(hasItems(task00)));
            Assert.True(clients.Get(p1).StandbyTasks().Count <= 2);
            Assert.Equal(clients.Get(p2).StandbyTasks(), not(hasItems(task01)));
            Assert.True(clients.Get(p2).StandbyTasks().Count <= 2);
            Assert.Equal(clients.Get(p3).StandbyTasks(), not(hasItems(task02)));
            Assert.True(clients.Get(p3).StandbyTasks().Count <= 2);
            Assert.Equal(clients.Get(p4).StandbyTasks(), not(hasItems(task03)));
            Assert.True(clients.Get(p4).StandbyTasks().Count <= 2);

            int nonEmptyStandbyTaskCount = 0;
            foreach (int client in clients.keySet())
            {
                nonEmptyStandbyTaskCount += clients.Get(client).StandbyTasks().IsEmpty() ? 0 : 1;
            }

            Assert.True(nonEmptyStandbyTaskCount >= 3);
            Assert.Equal(AllStandbyTasks(), Arrays.asList(task00, task01, task02, task03));
        }



        [Fact]
        public void ShouldAssignMultipleReplicasOfStandbyTask()
        {
            createClientWithPreviousActiveTasks(p1, 1, task00);
            createClientWithPreviousActiveTasks(p2, 1, task01);
            createClientWithPreviousActiveTasks(p3, 1, task02);

            StickyTaskAssignor taskAssignor = createTaskAssignor(task00, task01, task02);
            taskAssignor.Assign(2);

            Assert.Equal(clients.Get(p1).StandbyTasks(), Utils.mkSet(task01, task02));
            Assert.Equal(clients.Get(p2).StandbyTasks(), Utils.mkSet(task02, task00));
            Assert.Equal(clients.Get(p3).StandbyTasks(), Utils.mkSet(task00, task01));
        }

        [Fact]
        public void ShouldNotAssignStandbyTaskReplicasWhenNoClientAvailableWithoutHavingTheTaskAssigned()
        {
            CreateClient(p1, 1);
            StickyTaskAssignor taskAssignor = createTaskAssignor(task00);
            taskAssignor.Assign(1);
            Assert.Equal(clients.Get(p1).StandbyTasks().Count, 0);
        }

        [Fact]
        public void ShouldAssignActiveAndStandbyTasks()
        {
            CreateClient(p1, 1);
            CreateClient(p2, 1);
            CreateClient(p3, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02);
            taskAssignor.Assign(1);

            Assert.Equal(AllActiveTasks(), Arrays.asList(task00, task01, task02));
            Assert.Equal(AllStandbyTasks(), Arrays.asList(task00, task01, task02));
        }


        [Fact]
        public void ShouldAssignAtLeastOneTaskToEachClientIfPossible()
        {
            CreateClient(p1, 3);
            CreateClient(p2, 1);
            CreateClient(p3, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02);
            taskAssignor.Assign(0);
            Assert.Equal(clients.Get(p1).AssignedTaskCount(), 1);
            Assert.Equal(clients.Get(p2).AssignedTaskCount(), 1);
            Assert.Equal(clients.Get(p3).AssignedTaskCount(), 1);
        }

        [Fact]
        public void ShouldAssignEachActiveTaskToOneClientWhenMoreClientsThanTasks()
        {
            CreateClient(p1, 1);
            CreateClient(p2, 1);
            CreateClient(p3, 1);
            CreateClient(p4, 1);
            CreateClient(5, 1);
            CreateClient(6, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02);
            taskAssignor.Assign(0);

            Assert.Equal(AllActiveTasks(), Arrays.asList(task00, task01, task02));
        }

        [Fact]
        public void ShouldBalanceActiveAndStandbyTasksAcrossAvailableClients()
        {
            CreateClient(p1, 1);
            CreateClient(p2, 1);
            CreateClient(p3, 1);
            CreateClient(p4, 1);
            CreateClient(5, 1);
            CreateClient(6, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02);
            taskAssignor.Assign(1);

            foreach (ClientState clientState in clients.values())
            {
                Assert.Equal(1, clientState.AssignedTaskCount());
            }
        }

        [Fact]
        public void ShouldAssignMoreTasksToClientWithMoreCapacity()
        {
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

            taskAssignor.Assign(0);
            Assert.Equal(clients.Get(p2).AssignedTaskCount(), 8);
            Assert.Equal(clients.Get(p1).AssignedTaskCount(), 4);
        }

        [Fact]
        public void ShouldEvenlyDistributeByTaskIdAndPartition()
        {
            CreateClient(p1, 4);
            CreateClient(p2, 4);
            CreateClient(p3, 4);
            CreateClient(p4, 4);

            List<TaskId> taskIds = new List<TaskId>();
            TaskId[] taskIdArray = new TaskId[16];

            for (int i = 1; i <= 2; i++)
            {
                for (int j = 0; j < 8; j++)
                {
                    taskIds.Add(new TaskId(i, j));
                }
            }

            Collections.shuffle(taskIds);
            taskIds.ToArray(taskIdArray);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(taskIdArray);
            taskAssignor.Assign(0);

            Collections.sort(taskIds);
            HashSet<TaskId> expectedClientOneAssignment = getExpectedTaskIdAssignment(taskIds, 0, 4, 8, 12);
            HashSet<TaskId> expectedClientTwoAssignment = getExpectedTaskIdAssignment(taskIds, 1, 5, 9, 13);
            HashSet<TaskId> expectedClientThreeAssignment = getExpectedTaskIdAssignment(taskIds, 2, 6, 10, 14);
            HashSet<TaskId> expectedClientFourAssignment = getExpectedTaskIdAssignment(taskIds, 3, 7, 11, 15);

            Dictionary<int, HashSet<TaskId>> sortedAssignments = sortClientAssignments(clients);

            Assert.Equal(sortedAssignments.Get(p1), expectedClientOneAssignment);
            Assert.Equal(sortedAssignments.Get(p2), expectedClientTwoAssignment);
            Assert.Equal(sortedAssignments.Get(p3), expectedClientThreeAssignment);
            Assert.Equal(sortedAssignments.Get(p4), expectedClientFourAssignment);
        }


        [Fact]
        public void ShouldNotHaveSameAssignmentOnAnyTwoHosts()
        {
            CreateClient(p1, 1);
            CreateClient(p2, 1);
            CreateClient(p3, 1);
            CreateClient(p4, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
            taskAssignor.Assign(1);

            for (int i = p1; i <= p4; i++)
            {
                HashSet<TaskId> taskIds = clients.Get(i).AssignedTasks();
                for (int j = p1; j <= p4; j++)
                {
                    if (j != i)
                    {
                        Assert.Equal("clients shouldn't have same task assignment", clients.Get(j).AssignedTasks(),
                                   not(equalTo(taskIds)));
                    }
                }

            }
        }

        [Fact]
        public void ShouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousActiveTasks()
        {
            createClientWithPreviousActiveTasks(p1, 1, task01, task02);
            createClientWithPreviousActiveTasks(p2, 1, task03);
            createClientWithPreviousActiveTasks(p3, 1, task00);
            CreateClient(p4, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
            taskAssignor.Assign(1);

            for (int i = p1; i <= p4; i++)
            {
                HashSet<TaskId> taskIds = clients.Get(i).AssignedTasks();
                for (int j = p1; j <= p4; j++)
                {
                    if (j != i)
                    {
                        Assert.Equal("clients shouldn't have same task assignment", clients.Get(j).AssignedTasks(),
                                   not(equalTo(taskIds)));
                    }
                }

            }
        }

        [Fact]
        public void ShouldNotHaveSameAssignmentOnAnyTwoHostsWhenThereArePreviousStandbyTasks()
        {
            ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task02);
            c1.AddPreviousStandbyTasks(Utils.mkSet(task03, task00));
            ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task03, task00);
            c2.AddPreviousStandbyTasks(Utils.mkSet(task01, task02));

            CreateClient(p3, 1);
            CreateClient(p4, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
            taskAssignor.Assign(1);

            for (int i = p1; i <= p4; i++)
            {
                HashSet<TaskId> taskIds = clients.Get(i).AssignedTasks();
                for (int j = p1; j <= p4; j++)
                {
                    if (j != i)
                    {
                        Assert.Equal("clients shouldn't have same task assignment", clients.Get(j).AssignedTasks(),
                                   not(equalTo(taskIds)));
                    }
                }

            }
        }

        [Fact]
        public void ShouldReBalanceTasksAcrossAllClientsWhenCapacityAndTaskCountTheSame()
        {
            createClientWithPreviousActiveTasks(p3, 1, task00, task01, task02, task03);
            CreateClient(p1, 1);
            CreateClient(p2, 1);
            CreateClient(p4, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p1).AssignedTaskCount(), 1);
            Assert.Equal(clients.Get(p2).AssignedTaskCount(), 1);
            Assert.Equal(clients.Get(p3).AssignedTaskCount(), 1);
            Assert.Equal(clients.Get(p4).AssignedTaskCount(), 1);
        }

        [Fact]
        public void ShouldReBalanceTasksAcrossClientsWhenCapacityLessThanTaskCount()
        {
            createClientWithPreviousActiveTasks(p3, 1, task00, task01, task02, task03);
            CreateClient(p1, 1);
            CreateClient(p2, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p3).AssignedTaskCount(), 2);
            Assert.Equal(clients.Get(p1).AssignedTaskCount(), 1);
            Assert.Equal(clients.Get(p2).AssignedTaskCount(), 1);
        }

        [Fact]
        public void ShouldRebalanceTasksToClientsBasedOnCapacity()
        {
            createClientWithPreviousActiveTasks(p2, 1, task00, task03, task02);
            CreateClient(p3, 2);
            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task03);
            taskAssignor.Assign(0);
            Assert.Equal(clients.Get(p2).AssignedTaskCount(), 1);
            Assert.Equal(clients.Get(p3).AssignedTaskCount(), 2);
        }

        [Fact]
        public void ShouldMoveMinimalNumberOfTasksWhenPreviouslyAboveCapacityAndNewClientAdded()
        {
            HashSet<TaskId> p1PrevTasks = Utils.mkSet(task00, task02);
            HashSet<TaskId> p2PrevTasks = Utils.mkSet(task01, task03);

            createClientWithPreviousActiveTasks(p1, 1, task00, task02);
            createClientWithPreviousActiveTasks(p2, 1, task01, task03);
            createClientWithPreviousActiveTasks(p3, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task02, task01, task03);
            taskAssignor.Assign(0);

            HashSet<TaskId> p3ActiveTasks = clients.Get(p3).ActiveTasks();
            Assert.Single(p3ActiveTasks);
            if (p1PrevTasks.removeAll(p3ActiveTasks))
            {
                Assert.Equal(clients.Get(p2).ActiveTasks(), p2PrevTasks);
            }
            else
            {
                Assert.Equal(clients.Get(p1).ActiveTasks(), p1PrevTasks);
            }
        }

        [Fact]
        public void ShouldNotMoveAnyTasksWhenNewTasksAdded()
        {
            createClientWithPreviousActiveTasks(p1, 1, task00, task01);
            createClientWithPreviousActiveTasks(p2, 1, task02, task03);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task03, task01, task04, task02, task00, task05);
            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p1).ActiveTasks(), hasItems(task00, task01));
            Assert.Equal(clients.Get(p2).ActiveTasks(), hasItems(task02, task03));
        }

        [Fact]
        public void ShouldAssignNewTasksToNewClientWhenPreviousTasksAssignedToOldClients()
        {

            createClientWithPreviousActiveTasks(p1, 1, task02, task01);
            createClientWithPreviousActiveTasks(p2, 1, task00, task03);
            CreateClient(p3, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task03, task01, task04, task02, task00, task05);
            taskAssignor.Assign(0);

            Assert.Equal(clients.Get(p1).ActiveTasks(), hasItems(task02, task01));
            Assert.Equal(clients.Get(p2).ActiveTasks(), hasItems(task00, task03));
            Assert.Equal(clients.Get(p3).ActiveTasks(), hasItems(task04, task05));
        }

        [Fact]
        public void ShouldAssignTasksNotPreviouslyActiveToNewClient()
        {
            TaskId task10 = new TaskId(0, 10);
            TaskId task11 = new TaskId(0, 11);
            TaskId task12 = new TaskId(1, 2);
            TaskId task13 = new TaskId(1, 3);
            TaskId task20 = new TaskId(2, 0);
            TaskId task21 = new TaskId(2, 1);
            TaskId task22 = new TaskId(2, 2);
            TaskId task23 = new TaskId(2, 3);

            ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task12, task13);
            c1.AddPreviousStandbyTasks(Utils.mkSet(task00, task11, task20, task21, task23));
            ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task00, task11, task22);
            c2.AddPreviousStandbyTasks(Utils.mkSet(task01, task10, task02, task20, task03, task12, task21, task13, task23));
            ClientState c3 = createClientWithPreviousActiveTasks(p3, 1, task20, task21, task23);
            c3.AddPreviousStandbyTasks(Utils.mkSet(task02, task12));

            ClientState newClient = CreateClient(p4, 1);
            newClient.AddPreviousStandbyTasks(Utils.mkSet(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23));

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23);
            taskAssignor.Assign(0);

            Assert.Equal(c1.ActiveTasks(), Utils.mkSet(task01, task12, task13));
            Assert.Equal(c2.ActiveTasks(), Utils.mkSet(task00, task11, task22));
            Assert.Equal(c3.ActiveTasks(), Utils.mkSet(task20, task21, task23));
            Assert.Equal(newClient.ActiveTasks(), Utils.mkSet(task02, task03, task10));
        }

        [Fact]
        public void ShouldAssignTasksNotPreviouslyActiveToMultipleNewClients()
        {
            TaskId task10 = new TaskId(0, 10);
            TaskId task11 = new TaskId(0, 11);
            TaskId task12 = new TaskId(1, 2);
            TaskId task13 = new TaskId(1, 3);
            TaskId task20 = new TaskId(2, 0);
            TaskId task21 = new TaskId(2, 1);
            TaskId task22 = new TaskId(2, 2);
            TaskId task23 = new TaskId(2, 3);

            ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task01, task12, task13);
            c1.AddPreviousStandbyTasks(Utils.mkSet(task00, task11, task20, task21, task23));
            ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task00, task11, task22);
            c2.AddPreviousStandbyTasks(Utils.mkSet(task01, task10, task02, task20, task03, task12, task21, task13, task23));

            ClientState bounce1 = CreateClient(p3, 1);
            bounce1.AddPreviousStandbyTasks(Utils.mkSet(task20, task21, task23));

            ClientState bounce2 = CreateClient(p4, 1);
            bounce2.AddPreviousStandbyTasks(Utils.mkSet(task02, task03, task10));

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task10, task01, task02, task11, task20, task03, task12, task21, task13, task22, task23);
            taskAssignor.Assign(0);

            Assert.Equal(c1.ActiveTasks(), Utils.mkSet(task01, task12, task13));
            Assert.Equal(c2.ActiveTasks(), Utils.mkSet(task00, task11, task22));
            Assert.Equal(bounce1.ActiveTasks(), Utils.mkSet(task20, task21, task23));
            Assert.Equal(bounce2.ActiveTasks(), Utils.mkSet(task02, task03, task10));
        }

        [Fact]
        public void ShouldAssignTasksToNewClient()
        {
            createClientWithPreviousActiveTasks(p1, 1, task01, task02);
            CreateClient(p2, 1);
            createTaskAssignor(task01, task02).Assign(0);
            Assert.Equal(clients.Get(p1).activeTaskCount(), 1);
        }

        [Fact]
        public void ShouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingClients()
        {
            ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00, task01, task02);
            ClientState c2 = createClientWithPreviousActiveTasks(p2, 1, task03, task04, task05);
            ClientState newClient = CreateClient(p3, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02, task03, task04, task05);
            taskAssignor.Assign(0);
            Assert.Equal(c1.ActiveTasks(), not(hasItem(task03)));
            Assert.Equal(c1.ActiveTasks(), not(hasItem(task04)));
            Assert.Equal(c1.ActiveTasks(), not(hasItem(task05)));
            Assert.Equal(c1.activeTaskCount(), 2);
            Assert.Equal(c2.ActiveTasks(), not(hasItems(task00)));
            Assert.Equal(c2.ActiveTasks(), not(hasItems(task01)));
            Assert.Equal(c2.ActiveTasks(), not(hasItems(task02)));
            Assert.Equal(c2.activeTaskCount(), 2);
            Assert.Equal(newClient.activeTaskCount(), 2);
        }

        [Fact]
        public void ShouldAssignTasksToNewClientWithoutFlippingAssignmentBetweenExistingAndBouncedClients()
        {
            TaskId task06 = new TaskId(0, 6);
            ClientState c1 = createClientWithPreviousActiveTasks(p1, 1, task00, task01, task02, task06);
            ClientState c2 = CreateClient(p2, 1);
            c2.AddPreviousStandbyTasks(Utils.mkSet(task03, task04, task05));
            ClientState newClient = CreateClient(p3, 1);

            StickyTaskAssignor<int> taskAssignor = createTaskAssignor(task00, task01, task02, task03, task04, task05, task06);
            taskAssignor.Assign(0);
            Assert.Equal(c1.ActiveTasks(), not(hasItem(task03)));
            Assert.Equal(c1.ActiveTasks(), not(hasItem(task04)));
            Assert.Equal(c1.ActiveTasks(), not(hasItem(task05)));
            Assert.Equal(c1.activeTaskCount(), 3);
            Assert.Equal(c2.ActiveTasks(), not(hasItems(task00)));
            Assert.Equal(c2.ActiveTasks(), not(hasItems(task01)));
            Assert.Equal(c2.ActiveTasks(), not(hasItems(task02)));
            Assert.Equal(c2.activeTaskCount(), 2);
            Assert.Equal(newClient.activeTaskCount(), 2);
        }

        private StickyTaskAssignor<int> CreateTaskAssignor(params TaskId[] tasks)
        {
            List<TaskId> taskIds = Arrays.asList(tasks);
            Collections.shuffle(taskIds);
            return new StickyTaskAssignor<>(clients,
                                            new HashSet<>(taskIds));
        }

        private List<TaskId> AllActiveTasks()
        {
            List<TaskId> allActive = new List<TaskId>();
            foreach (var client in clients.Values)
            {
                allActive.AddAll(client.ActiveTasks());
            }

            Collections.sort(allActive);
            return allActive;
        }

        private List<TaskId> AllStandbyTasks()
        {
            List<TaskId> tasks = new List<TaskId>();
            foreach (ClientState client in clients.values())
            {
                tasks.addAll(client.StandbyTasks());
            }
            Collections.sort(tasks);
            return tasks;
        }

        private ClientState CreateClient(int processId, int capacity)
        {
            return createClientWithPreviousActiveTasks(processId, capacity);
        }

        private ClientState CreateClientWithPreviousActiveTasks(int processId, int capacity, params TaskId[] taskIds)
        {
            ClientState clientState = new ClientState(capacity);
            clientState.AddPreviousActiveTasks(Utils.mkSet(taskIds));
            clients.Put(processId, clientState);
            return clientState;
        }

        private void AssertActiveTaskTopicGroupIdsEvenlyDistributed()
        {
            foreach (var clientStateEntry in clients)
            {
                List<int> topicGroupIds = new List<int>();
                HashSet<TaskId> ActiveTasks = clientStateEntry.Value.ActiveTasks();
                foreach (TaskId activeTask in ActiveTasks)
                {
                    topicGroupIds.Add(activeTask.topicGroupId);
                }
                Collections.sort(topicGroupIds);
                Assert.Equal(topicGroupIds, expectedTopicGroupIds);
            }
        }

        private Dictionary<int, HashSet<TaskId>> SortClientAssignments(Map<int, ClientState> clients)
        {
            Dictionary<int, HashSet<TaskId>> sortedAssignments = new HashMap<>();
            foreach (Map.Entry<int, ClientState> entry in clients)
            {
                HashSet<TaskId> sorted = new TreeSet<>(entry.Value.ActiveTasks());
                sortedAssignments.Put(entry.Key, sorted);
            }
            return sortedAssignments;
        }

        private HashSet<TaskId> GetExpectedTaskIdAssignment(List<TaskId> tasks, params int[] indices)
        {
            HashSet<TaskId> sortedAssignment = new TreeSet<>();
            foreach (int index in indices)
            {
                sortedAssignment.Add(tasks.Get(index));
            }
            return sortedAssignment;
        }

    }
}
