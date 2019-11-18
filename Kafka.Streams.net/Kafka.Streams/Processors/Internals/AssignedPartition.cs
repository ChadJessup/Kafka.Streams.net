
//using Confluent.Kafka;
//using System;

//namespace Kafka.Streams.Processors.Internals
//{
//    public class AssignedPartition : IComparable<AssignedPartition>
//    {
//        public TaskId taskId;
//        public TopicPartition partition;

//        public AssignedPartition(TaskId taskId,
//                          TopicPartition partition)
//        {
//            this.taskId = taskId;
//            this.partition = partition;
//        }


//        public int CompareTo(AssignedPartition that)
//        {
//            return PARTITION_COMPARATOR.compare(this.partition, that.partition);
//        }


//        public bool Equals(object o)
//        {
//            if (!(o is AssignedPartition))
//            {
//                return false;
//            }
//            AssignedPartition other = (AssignedPartition)o;
//            return CompareTo(other) == 0;
//        }


//        public int GetHashCode()
//        {
//            // Only partition is important for CompareTo, Equals and GetHashCode().
//            return partition.GetHashCode();
//        }
//    }
//}