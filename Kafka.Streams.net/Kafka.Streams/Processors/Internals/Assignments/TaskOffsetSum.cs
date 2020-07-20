using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    internal class TaskOffsetSum
    {
        public IEnumerable<PartitionToOffsetSum> partitionToOffsetSum { get; internal set; }

        internal int topicGroupId()
        {
            throw new NotImplementedException();
        }

        internal void SetTopicGroupId(int key)
        {
            throw new NotImplementedException();
        }

        internal void SetPartitionToOffsetSum(List<PartitionToOffsetSum> value)
        {
            throw new NotImplementedException();
        }
    }
}