using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Processor.Internals.Assignment
{
    public class ClientState
    {
        public HashSet<TaskId> ActiveTasks { get; }
        public HashSet<TaskId> StandbyTasks { get; }
        public HashSet<TaskId> AssignedTasks { get; }
        public HashSet<TaskId> PrevActiveTasks { get; }
        public HashSet<TaskId> PrevStandbyTasks { get; }
        public HashSet<TaskId> PrevAssignedTasks { get; }

        private int capacity;


        public ClientState()
            : this(0)
        {
        }

        public ClientState(int capacity)
            : this(
                  new HashSet<TaskId>(),
                  new HashSet<TaskId>(),
                  new HashSet<TaskId>(),
                  new HashSet<TaskId>(),
                  new HashSet<TaskId>(),
                  new HashSet<TaskId>(),
                  capacity)
        {
        }

        private ClientState(
            HashSet<TaskId> activeTasks,
            HashSet<TaskId> standbyTasks,
            HashSet<TaskId> assignedTasks,
            HashSet<TaskId> prevActiveTasks,
            HashSet<TaskId> prevStandbyTasks,
            HashSet<TaskId> prevAssignedTasks,
            int capacity)
        {
            this.ActiveTasks = activeTasks;
            this.StandbyTasks = standbyTasks;
            this.AssignedTasks = assignedTasks;
            this.PrevActiveTasks = prevActiveTasks;
            this.PrevStandbyTasks = prevStandbyTasks;
            this.PrevAssignedTasks = prevAssignedTasks;
            this.capacity = capacity;
        }

        public ClientState Copy()
        {
            return new ClientState(
                new HashSet<TaskId>(ActiveTasks),
                new HashSet<TaskId>(StandbyTasks),
                new HashSet<TaskId>(AssignedTasks),
                new HashSet<TaskId>(PrevActiveTasks),
                new HashSet<TaskId>(PrevStandbyTasks),
                new HashSet<TaskId>(PrevAssignedTasks),
                capacity);
        }

        public void Assign(TaskId taskId, bool active)
        {
            if (active)
            {
                ActiveTasks.Add(taskId);
            }
            else
            {
                StandbyTasks.Add(taskId);
            }

            AssignedTasks.Add(taskId);
        }

        public int AssignedTaskCount()
        {
            return AssignedTasks.Count;
        }

        public void IncrementCapacity()
        {
            capacity++;
        }

        public int ActiveTaskCount()
        {
            return ActiveTasks.Count;
        }

        public void AddPreviousActiveTasks(HashSet<TaskId> prevTasks)
        {
            PrevActiveTasks.AddRange(prevTasks);
            PrevAssignedTasks.AddRange(prevTasks);
        }

        public void AddPreviousStandbyTasks(HashSet<TaskId> standbyTasks)
        {
            PrevStandbyTasks.AddRange(standbyTasks);
            PrevAssignedTasks.AddRange(standbyTasks);
        }

        public override string ToString()
        {
            return "[activeTasks: (" + ActiveTasks.ToJoinedString() +
                    ") standbyTasks: (" + StandbyTasks.ToJoinedString() +
                    ") assignedTasks: (" + AssignedTasks.ToJoinedString() +
                    ") prevActiveTasks: (" + PrevActiveTasks.ToJoinedString() +
                    ") prevStandbyTasks: (" + PrevStandbyTasks.ToJoinedString() +
                    ") prevAssignedTasks: (" + PrevAssignedTasks.ToJoinedString() +
                    ") capacity: " + capacity +
                    "]";
        }

        public bool ReachedCapacity()
        {
            return AssignedTasks.Count >= capacity;
        }

        public bool HasMoreAvailableCapacityThan(ClientState other)
        {
            if (this.capacity <= 0)
            {
                throw new InvalidOperationException("Capacity of this ClientState must be greater than 0.");
            }

            if (other.capacity <= 0)
            {
                throw new InvalidOperationException("Capacity of other ClientState must be greater than 0");
            }

            double otherLoad = (double)other.AssignedTaskCount() / other.capacity;
            double thisLoad = (double)AssignedTaskCount() / capacity;

            if (thisLoad < otherLoad)
            {
                return true;
            }
            else if (thisLoad > otherLoad)
            {
                return false;
            }
            else
            {
                return capacity > other.capacity;
            }
        }

        public HashSet<TaskId> PreviousStandbyTasks()
        {
            HashSet<TaskId> standby = new HashSet<TaskId>(PrevAssignedTasks);
            standby.ExceptWith(PrevActiveTasks);
            return standby;
        }

        public bool HasAssignedTask(TaskId taskId)
        {
            return AssignedTasks.Contains(taskId);
        }

        public bool HasUnfulfilledQuota(int tasksPerThread)
        {
            return ActiveTasks.Count < capacity * tasksPerThread;
        }
    }
}
