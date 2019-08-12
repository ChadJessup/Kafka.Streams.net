///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Confluent.Kafka;
//using System;

//namespace Kafka.Streams.Processor.Internals
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