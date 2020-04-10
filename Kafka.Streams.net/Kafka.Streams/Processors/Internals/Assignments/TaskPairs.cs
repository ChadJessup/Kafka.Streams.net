
using Kafka.Streams.Tasks;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    public class TaskPairs
    {

        private readonly HashSet<Pair> pairs;
        private readonly int maxPairs;

        private Pair Pair(TaskId task1, TaskId task2)
        {
            if (task1.CompareTo(task2) < 0)
            {
                return new Pair(task1, task2);
            }
            return new Pair(task2, task1);
        }
    }
}
