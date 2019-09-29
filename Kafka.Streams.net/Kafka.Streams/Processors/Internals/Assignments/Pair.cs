using Kafka.Streams.Tasks;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    public class Pair
    {

        private readonly TaskId task1;
        private readonly TaskId task2;

        public Pair(TaskId task1, TaskId task2)
        {
            this.task1 = task1;
            this.task2 = task2;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            Pair pair = (Pair)o;

            return (task1.Equals(pair.task1) &&
                    task2.Equals(pair.task2));
        }

        public override int GetHashCode()
        {
            return (task1, task2).GetHashCode();
        }
    }
}