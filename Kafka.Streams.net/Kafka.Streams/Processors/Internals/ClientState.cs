using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Streams.Tasks;

namespace Kafka.Streams.Processors.Internals
{
    public class ClientState
    {
        internal void assignActiveToConsumer(TaskId taskId, string consumer)
        {
            throw new NotImplementedException();
        }

        internal string previousOwnerForPartition(TopicPartition partition)
        {
            throw new NotImplementedException();
        }

        internal string prevOwnedActiveTasksByConsumer()
        {
            throw new NotImplementedException();
        }

        internal object[] prevOwnedStandbyByConsumer()
        {
            throw new NotImplementedException();
        }

        internal object assignedActiveTasksByConsumer()
        {
            throw new NotImplementedException();
        }

        internal object revokingActiveTasksByConsumer()
        {
            throw new NotImplementedException();
        }

        internal object assignedStandbyTasksByConsumer()
        {
            throw new NotImplementedException();
        }

        internal void assignStandbyToConsumer(TaskId task, string consumer)
        {
            throw new NotImplementedException();
        }

        internal void assignStandby(TaskId task)
        {
            throw new NotImplementedException();
        }

        internal void unassignActive(TaskId taskId)
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<TaskId> statefulActiveTasks()
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<TaskId> standbyTasks()
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<TaskId> statelessActiveTasks()
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<TaskId> ActiveTasks()
        {
            throw new NotImplementedException();
        }

        internal void AddOwnedPartitions(IEnumerable<TopicPartition> ownedPartitions, string consumerMemberId)
        {
            throw new NotImplementedException();
        }

        internal void IncrementCapacity()
        {
            throw new NotImplementedException();
        }

        internal void AddPreviousTasksAndOffsetSums(string consumerId, Dictionary<TaskId, long> taskOffsetSums)
        {
            throw new NotImplementedException();
        }

        internal void computeTaskLags(Guid uuid, Dictionary<TaskId, long> allTaskEndOffsetSums)
        {
            throw new NotImplementedException();
        }

        internal void initializePrevTasks(Dictionary<TopicPartition, TaskId> taskForPartition)
        {
            throw new NotImplementedException();
        }

        internal string currentAssignment()
        {
            throw new NotImplementedException();
        }

        internal void revokeActiveFromConsumer(TaskId taskId, string consumer)
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<TaskId> prevOwnedStatefulTasksByConsumer(string consumer)
        {
            throw new NotImplementedException();
        }
    }
}
