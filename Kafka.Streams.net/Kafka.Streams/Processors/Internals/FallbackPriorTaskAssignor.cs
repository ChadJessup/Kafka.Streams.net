using System;
using System.Collections.Generic;
using Kafka.Streams.Processors.Internals.Assignments;
using Kafka.Streams.Tasks;

namespace Kafka.Streams.Processors.Internals
{
    internal class FallbackPriorTaskAssignor : ITaskAssignor
    {
        public void Assign(int numStandbyReplicas)
        {
            throw new NotImplementedException();
        }

        public bool Assign(Dictionary<Guid, ClientState> clientStates, HashSet<TaskId> allTasks, HashSet<TaskId> statefulTasks, AssignmentConfigs assignmentConfigs)
        {
            throw new NotImplementedException();
        }
    }
}
