using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Kafka.Streams.Processor.Internals
{
    public class InvalidOffsetException : Exception
    {
        internal HashSet<TopicPartition> partitions()
        {
            throw new NotImplementedException();
        }
    }
}