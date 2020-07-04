using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals.Assignment
{
    public class ClientStateTest
    {
        private readonly ClientState client = new ClientState(1);

        [Fact]
        public void ShouldHaveNotReachedCapacityWhenAssignedTasksLessThanCapacity()
        {
            Assert.False(client.ReachedCapacity());
        }

        [Fact]
        public void ShouldHaveReachedCapacityWhenAssignedTasksGreaterThanOrEqualToCapacity()
        {
            client.Assign(new TaskId(0, 1), true);
            Assert.True(client.ReachedCapacity());
        }

        [Fact]
        public void ShouldAddActiveTasksToBothAssignedAndActive()
        {
            TaskId tid = new TaskId(0, 1);

            client.Assign(tid, true);
            Assert.Equal(client.ActiveTasks, Collections.singleton(tid));
            Assert.Equal(client.AssignedTasks, Collections.singleton(tid));
            Assert.Equal(1, client.AssignedTaskCount());
            Assert.Empty(client.StandbyTasks);
        }

        [Fact]
        public void ShouldAddStandbyTasksToBothStandbyAndActive()
        {
            TaskId tid = new TaskId(0, 1);

            client.Assign(tid, false);
            Assert.Equal(client.AssignedTasks, Collections.singleton(tid));
            Assert.Equal(client.StandbyTasks, Collections.singleton(tid));
            Assert.Equal(1, client.AssignedTaskCount());
            Assert.Empty(client.ActiveTasks);
        }

        [Fact]
        public void ShouldAddPreviousActiveTasksToPreviousAssignedAndPreviousActive()
        {
            TaskId tid1 = new TaskId(0, 1);
            TaskId tid2 = new TaskId(0, 2);

            client.AddPreviousActiveTasks(new HashSet<TaskId> { tid1, tid2 });
            Assert.Equal(client.PrevActiveTasks, new HashSet<TaskId> { tid1, tid2 });
            Assert.Equal(client.PrevAssignedTasks, new HashSet<TaskId> { tid1, tid2 });
        }

        [Fact]
        public void ShouldAddPreviousStandbyTasksToPreviousAssigned()
        {
            TaskId tid1 = new TaskId(0, 1);
            TaskId tid2 = new TaskId(0, 2);

            client.AddPreviousStandbyTasks(new HashSet<TaskId> { tid1, tid2 });
            Assert.Empty(client.PrevActiveTasks);
            Assert.Equal(client.PrevAssignedTasks, new HashSet<TaskId> { tid1, tid2 });
        }

        [Fact]
        public void ShouldHaveAssignedTaskIfActiveTaskAssigned()
        {
            TaskId tid = new TaskId(0, 2);

            client.Assign(tid, true);
            Assert.True(client.HasAssignedTask(tid));
        }

        [Fact]
        public void ShouldHaveAssignedTaskIfStandbyTaskAssigned()
        {
            TaskId tid = new TaskId(0, 2);

            client.Assign(tid, false);
            Assert.True(client.HasAssignedTask(tid));
        }

        [Fact]
        public void ShouldNotHaveAssignedTaskIfTaskNotAssigned()
        {

            client.Assign(new TaskId(0, 2), true);
            Assert.False(client.HasAssignedTask(new TaskId(0, 3)));
        }

        [Fact]
        public void ShouldHaveMoreAvailableCapacityWhenCapacityTheSameButFewerAssignedTasks()
        {
            ClientState c2 = new ClientState(1);
            client.Assign(new TaskId(0, 1), true);
            Assert.True(c2.HasMoreAvailableCapacityThan(client));
            Assert.False(client.HasMoreAvailableCapacityThan(c2));
        }

        [Fact]
        public void ShouldHaveMoreAvailableCapacityWhenCapacityHigherAndSameAssignedTaskCount()
        {
            ClientState c2 = new ClientState(2);
            Assert.True(c2.HasMoreAvailableCapacityThan(client));
            Assert.False(client.HasMoreAvailableCapacityThan(c2));
        }

        [Fact]
        public void ShouldUseMultiplesOfCapacityToDetermineClientWithMoreAvailableCapacity()
        {
            ClientState c2 = new ClientState(2);

            for (int i = 0; i < 7; i++)
            {
                c2.Assign(new TaskId(0, i), true);
            }

            for (int i = 7; i < 11; i++)
            {
                client.Assign(new TaskId(0, i), true);
            }

            Assert.True(c2.HasMoreAvailableCapacityThan(client));
        }

        [Fact]
        public void ShouldHaveMoreAvailableCapacityWhenCapacityIsTheSameButAssignedTasksIsLess()
        {
            ClientState c1 = new ClientState(3);
            ClientState c2 = new ClientState(3);
            for (int i = 0; i < 4; i++)
            {
                c1.Assign(new TaskId(0, i), true);
                c2.Assign(new TaskId(0, i), true);
            }
            c2.Assign(new TaskId(0, 5), true);
            Assert.True(c1.HasMoreAvailableCapacityThan(c2));
        }

        [Fact]
        public void ShouldThrowIllegalStateExceptionIfCapacityOfThisClientStateIsZero()
        {
            ClientState c1 = new ClientState(0);
            Assert.Throws<InvalidOperationException>(() => c1.HasMoreAvailableCapacityThan(new ClientState(1)));
        }

        [Fact]
        public void ShouldThrowIllegalStateExceptionIfCapacityOfOtherClientStateIsZero()
        {
            ClientState c1 = new ClientState(1);
            Assert.Throws<InvalidOperationException>(() => c1.HasMoreAvailableCapacityThan(new ClientState(0)));
        }

        [Fact]
        public void ShouldHaveUnfulfilledQuotaWhenActiveTaskSizeLessThanCapacityTimesTasksPerThread()
        {
            ClientState client = new ClientState(1);
            client.Assign(new TaskId(0, 1), true);
            Assert.True(client.HasUnfulfilledQuota(2));
        }

        [Fact]
        public void ShouldNotHaveUnfulfilledQuotaWhenActiveTaskSizeGreaterEqualThanCapacityTimesTasksPerThread()
        {
            ClientState client = new ClientState(1);
            client.Assign(new TaskId(0, 1), true);
            Assert.False(client.HasUnfulfilledQuota(1));
        }

    }
}
