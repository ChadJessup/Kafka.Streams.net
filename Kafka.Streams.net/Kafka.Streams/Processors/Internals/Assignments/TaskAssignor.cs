using System;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    public interface TaskAssignor<C, T>
         where T : IComparable<T>
    {
        void assign(int numStandbyReplicas);
    }
}