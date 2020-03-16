using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace Kafka.Streams.Processors.Internals
{
    public class InvalidOffsetException : Exception
    {
        internal HashSet<TopicPartition> partitions()
        {
            throw new NotImplementedException();
        }

        public InvalidOffsetException(string message) : base(message)
        {
        }

        public InvalidOffsetException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}