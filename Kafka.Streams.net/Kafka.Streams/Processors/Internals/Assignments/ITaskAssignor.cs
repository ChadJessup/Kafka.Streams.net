using System;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    public interface ITaskAssignor<C, T>
         where T : IComparable<T>
    {
        void Assign(int numStandbyReplicas);
    }
}
