using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Processor.Internals.Assignment
{
    internal class ClientState
    {
        private int v;

        public ClientState(int v)
        {
            this.v = v;
        }

        internal void Assign(TaskId taskId, bool v)
        {
            throw new NotImplementedException();
        }

        internal bool ReachedCapacity()
        {
            throw new NotImplementedException();
        }

        internal bool HasMoreAvailableCapacityThan(ClientState client)
        {
            throw new NotImplementedException();
        }

        internal bool HasUnfulfilledQuota(int v)
        {
            throw new NotImplementedException();
        }

        internal bool HasAssignedTask(TaskId taskId)
        {
            throw new NotImplementedException();
        }

        internal ICollection<object> PreviousActiveTasks()
        {
            throw new NotImplementedException();
        }

        internal IEnumerable<object> PreviousAssignedTasks()
        {
            throw new NotImplementedException();
        }

        internal void AddPreviousActiveTasks(object p)
        {
            throw new NotImplementedException();
        }

        internal void AddPreviousStandbyTasks(object p)
        {
            throw new NotImplementedException();
        }

        internal ICollection<TaskId> AssignedTasks()
        {
            throw new NotImplementedException();
        }

        internal ICollection<TaskId> StandbyTasks()
        {
            throw new NotImplementedException();
        }

        internal ICollection<TaskId> ActiveTasks()
        {
            throw new NotImplementedException();
        }

        internal int AssignedTaskCount()
        {
            throw new NotImplementedException();
        }
    }
}