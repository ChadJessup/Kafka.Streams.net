using System;

namespace Kafka.Streams.Processors.Internals.Assignments
{
    internal class PartitionToOffsetSum
    {
        internal int partition()
        {
            throw new NotImplementedException();
        }

        internal long offsetSum()
        {
            throw new NotImplementedException();
        }

        public PartitionToOffsetSum setPartition(int partition)
        {
            return this;
        }

        public PartitionToOffsetSum setOffsetSum(long value)
        {
            return this;
        }
    }
}
