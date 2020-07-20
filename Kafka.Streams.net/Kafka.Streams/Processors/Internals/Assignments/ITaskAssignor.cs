using System;
using System.Collections.Generic;
using Kafka.Streams.Tasks;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    public interface ITaskAssignor
    {
        void Assign(int numStandbyReplicas);
        bool Assign(Dictionary<Guid, ClientState> clientStates, HashSet<TaskId> allTasks, HashSet<TaskId> statefulTasks, AssignmentConfigs assignmentConfigs);
    }

    public interface ITaskAssignor<T> : ITaskAssignor
         where T : IComparable<T>
    {
    }
}
