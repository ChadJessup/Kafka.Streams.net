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

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var pair = (Pair)o;

            return (this.task1.Equals(pair.task1) &&
                    this.task2.Equals(pair.task2));
        }

        public override int GetHashCode()
        {
            return (this.task1, this.task2).GetHashCode();
        }
    }
}